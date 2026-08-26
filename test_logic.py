"""
Self-contained logic test for the VOD episode-sweep plugin.

Runs WITHOUT Dispatcharr, Celery, Redis or a DB. It injects fake `core.utils`,
`core.models`, `core.scheduling`, `apps.plugins.models`, `apps.vod.models`,
`apps.m3u.models`, `apps.vod.tasks`, `apps.output.views`, `django.db` and
`dispatcharr.app_initialization` modules so the lazily-imported bits of patch.py
resolve, and drives a controllable fake clock.

Checks (v0.2.0 model: inline run + opportunistic daily tick, no beat/plugin task):
  * Observation records the series behind an xc_get_series_info call, dedupes
    within the window, and the wrapper returns the native response unchanged.
  * The sweep enqueues batch_refresh_series_episodes.delay(acct, series_ids=[…])
    once per active XC account, with the right per-account series ids, and skips
    inactive / non-XC accounts.
  * TTL pruning drops stale watched series.
  * A disabled plugin's sweep no-ops.
  * The daily tick fires once per day, only at/after the configured hour, and is
    coordinated across workers via a Redis claim.
  * install() is reload-safe and cleans up the legacy beat task; inactive wrapper
    is a pure passthrough.
  * clear_watchlist empties the list.

Run:  python test_logic.py     (or: py -3 test_logic.py)
"""

import importlib.util
import os
import sys
import types
import time as _real_time

_here = os.path.dirname(os.path.abspath(__file__))
_scratch = os.path.join(_here, "_test_debug.json")


# --------------------------------------------------------------------------- #
# Controllable clock (delegates unknown attrs to the real time module)
# --------------------------------------------------------------------------- #
class Clock:
    def __init__(self):
        self.t = 1_000_000.0
        # Fixed, tz-independent struct for localtime() so hour/date gating is
        # deterministic regardless of the test machine's timezone.
        self._struct = _real_time.struct_time((2026, 8, 14, 12, 0, 0, 3, 226, 0))

    def time(self):
        return self.t

    def monotonic(self):
        return self.t

    def localtime(self, t=None):
        return self._struct

    def set_hour(self, hour):
        self._struct = _real_time.struct_time((2026, 8, 14, hour, 0, 0, 3, 226, 0))

    def advance(self, seconds):
        self.t += seconds

    def __getattr__(self, name):
        # strftime etc. delegate to the real module (strftime takes a struct arg).
        return getattr(_real_time, name)


CLOCK = Clock()


# --------------------------------------------------------------------------- #
# Test environment
# --------------------------------------------------------------------------- #
class Env:
    def __init__(self):
        self.coresettings = {}     # key -> value dict
        self.settings = {}         # PluginConfig.settings
        self.enabled = True        # PluginConfig.enabled
        self.relations = []        # [{id, series_id, m3u_account_id, active, xc}]
        self.series = {}           # series_pk -> name
        self.enqueued = []         # (account_id, series_ids)
        self.countdowns = []       # apply_async countdowns, in dispatch order
        self.delay_raises = False
        self.delete_calls = 0      # remove_schedule calls
        self.schedule = None       # last create_or_update_periodic_task kwargs
        self.schedule_calls = 0
        self.schedule_queue = None  # queue set on the PeriodicTask
        self.redis = {}            # fake Redis store (daily claim keys)


ENV = Env()


# --------------------------------------------------------------------------- #
# Fake Redis (daily-claim key)
# --------------------------------------------------------------------------- #
class FakeRedis:
    def set(self, key, val, nx=False, ex=None):
        if nx and key in ENV.redis:
            return None
        ENV.redis[key] = val
        return True


# --------------------------------------------------------------------------- #
# Fake query helpers
# --------------------------------------------------------------------------- #
class RelQS:
    def __init__(self, rows):
        self.rows = rows

    def values(self, *fields):
        return RelQS([{f: r[f] for f in fields} for r in self.rows])

    def values_list(self, field, flat=False):
        vals = [r[field] for r in self.rows]
        return RelQS(vals if flat else [(v,) for v in vals])

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class RelManager:
    def filter(self, id=None, series_id__in=None, m3u_account__is_active=None,
               m3u_account__account_type=None, **kw):
        rows = list(ENV.relations)
        if id is not None:
            rows = [r for r in rows if r["id"] == id]
        if series_id__in is not None:
            rows = [r for r in rows if r["series_id"] in series_id__in]
        if m3u_account__is_active:
            rows = [r for r in rows if r.get("active", True)]
        if m3u_account__account_type is not None:
            rows = [r for r in rows if r.get("xc", True)]
        return RelQS(rows)


class SeriesManager:
    def filter(self, id__in=None, **kw):
        ids = id__in or []
        return RelQS([{"id": pk, "name": ENV.series.get(pk, "?")} for pk in ids
                      if pk in ENV.series])


# --------------------------------------------------------------------------- #
# Fake CoreSettings
# --------------------------------------------------------------------------- #
class FakeRow:
    def __init__(self, key):
        self._key = key
        self.value = ENV.coresettings.get(key)

    def save(self, update_fields=None):
        ENV.coresettings[self._key] = self.value


class CSQuery:
    def __init__(self):
        self.key = None
        self._values = False

    def filter(self, key=None, **kw):
        self.key = key
        return self

    def values(self, *f):
        self._values = True
        return self

    def first(self):
        if self.key not in ENV.coresettings:
            return None
        if self._values:
            return {"value": ENV.coresettings[self.key]}
        return FakeRow(self.key)


class CSManager:
    def filter(self, key=None, **kw):
        q = CSQuery()
        return q.filter(key=key)

    def select_for_update(self):
        return CSQuery()

    def create(self, key=None, name=None, value=None):
        ENV.coresettings[key] = value
        return FakeRow(key)


# --------------------------------------------------------------------------- #
# Fake PluginConfig
# --------------------------------------------------------------------------- #
class PCQuery:
    def __init__(self):
        self._field = None

    def filter(self, key=None, **kw):
        return self

    def values(self, *f):
        self._field = f[0] if f else None
        return self

    def first(self):
        if self._field == "settings":
            return {"settings": ENV.settings}
        if self._field == "enabled":
            return {"enabled": ENV.enabled}
        return {}


class PCManager:
    def filter(self, key=None, **kw):
        return PCQuery().filter(key=key)


# --------------------------------------------------------------------------- #
# Install fake modules
# --------------------------------------------------------------------------- #
def _install_fake_modules():
    celery_mod = types.ModuleType("celery")

    def _fake_shared_task(*dargs, **dkwargs):
        def deco(fn):
            return fn
        if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
            return dargs[0]
        return deco
    celery_mod.shared_task = _fake_shared_task
    sys.modules["celery"] = celery_mod

    core = types.ModuleType("core")
    core_utils = types.ModuleType("core.utils")

    class RedisClient:
        _client = FakeRedis()

        @staticmethod
        def get_client():
            return RedisClient._client
    core_utils.RedisClient = RedisClient

    core_models = types.ModuleType("core.models")

    class CoreSettings:
        objects = CSManager()

        @classmethod
        def get_system_time_zone(cls):
            # Empty -> the plugin falls back to process-local time; the daily-tick
            # test monkeypatches _now_local directly for deterministic hours.
            return ""
    core_models.CoreSettings = CoreSettings

    core_sched = types.ModuleType("core.scheduling")

    class _FakePeriodicTask:
        def __init__(self, name):
            self.name = name
            self.queue = None

        def save(self, update_fields=None):
            ENV.schedule_queue = self.queue

    def create_or_update_periodic_task(task_name=None, celery_task_path=None,
                                       cron_expression="", enabled=True, **kw):
        ENV.schedule = {
            "task_name": task_name, "celery_task_path": celery_task_path,
            "cron_expression": cron_expression, "enabled": enabled,
        }
        ENV.schedule_calls += 1
        return _FakePeriodicTask(task_name)

    def delete_periodic_task(name):
        ENV.delete_calls += 1
        return True
    core_sched.create_or_update_periodic_task = create_or_update_periodic_task
    core_sched.delete_periodic_task = delete_periodic_task

    plugins_models = types.ModuleType("apps.plugins.models")

    class PluginConfig:
        objects = PCManager()
    plugins_models.PluginConfig = PluginConfig

    vod_models = types.ModuleType("apps.vod.models")

    class M3USeriesRelation:
        objects = RelManager()

    class Series:
        objects = SeriesManager()
    vod_models.M3USeriesRelation = M3USeriesRelation
    vod_models.Series = Series

    m3u_models = types.ModuleType("apps.m3u.models")

    class _Types:
        XC = "XC"

    class M3UAccount:
        Types = _Types
    m3u_models.M3UAccount = M3UAccount

    vod_tasks = types.ModuleType("apps.vod.tasks")

    class _Task:
        def delay(self, account_id, series_ids=None):
            if ENV.delay_raises:
                raise RuntimeError("broker down")
            ENV.enqueued.append((account_id, series_ids))

        def apply_async(self, args=None, kwargs=None, countdown=0):
            if ENV.delay_raises:
                raise RuntimeError("broker down")
            args = args or []
            kwargs = kwargs or {}
            account_id = args[0] if args else kwargs.get("account_id")
            ENV.enqueued.append((account_id, kwargs.get("series_ids")))
            ENV.countdowns.append(countdown)
    vod_tasks.batch_refresh_series_episodes = _Task()

    output_views = types.ModuleType("apps.output.views")

    def _orig_gsi(request, user, series_id):
        return ("NATIVE", series_id)
    output_views.xc_get_series_info = _orig_gsi

    django_db = types.ModuleType("django.db")

    class _Atomic:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Transaction:
        def atomic(self):
            return _Atomic()
    django_db.transaction = _Transaction()
    django_db.close_old_connections = lambda: None

    app_init = types.ModuleType("dispatcharr.app_initialization")
    app_init.should_skip_initialization = lambda: True  # import-time: don't schedule

    for name in ("apps", "apps.plugins", "apps.vod", "apps.m3u",
                 "apps.output", "dispatcharr"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["core"] = core
    sys.modules["core.utils"] = core_utils
    sys.modules["core.models"] = core_models
    sys.modules["core.scheduling"] = core_sched
    sys.modules["apps.plugins.models"] = plugins_models
    sys.modules["apps.vod.models"] = vod_models
    sys.modules["apps.m3u.models"] = m3u_models
    sys.modules["apps.vod.tasks"] = vod_tasks
    sys.modules["apps.output.views"] = output_views
    sys.modules["django"] = types.ModuleType("django")
    sys.modules["django.db"] = django_db
    sys.modules["dispatcharr.app_initialization"] = app_init


_install_fake_modules()

spec = importlib.util.spec_from_file_location(
    "vodsweep_patch", os.path.join(_here, "patch.py")
)
patch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(patch)

patch.time = CLOCK
patch._debug_file_path = lambda: _scratch


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
PASS = "PASS"
FAIL = "FAIL"
_failures = []


def check(name, cond):
    print(f"  [{PASS if cond else FAIL}] {name}")
    if not cond:
        _failures.append(name)


def reset(**kw):
    global ENV
    ENV = Env()
    for k, v in kw.items():
        setattr(ENV, k, v)
    patch._ACTIVE = True
    patch.invalidate_config_cache()
    patch._invalidate_watchlist_cache()
    with patch._observe_lock:
        patch._recent_observed.clear()
    patch._daily_checked_date = None
    patch._ip_daily_date = None
    patch._invalidate_tz_cache()
    CLOCK.set_hour(12)
    # restore native handler each scenario
    sys.modules["apps.output.views"].xc_get_series_info = lambda request, user, series_id: ("NATIVE", series_id)


def seed_relations():
    """series 100 -> relation 11 (acct 7) + relation 12 (acct 9, a 4K category);
    series 200 -> relation 21 (acct 7); plus an inactive + a non-XC relation."""
    ENV.series = {100: "Show A", 200: "Show B"}
    ENV.relations = [
        {"id": 11, "series_id": 100, "m3u_account_id": 7, "active": True, "xc": True},
        {"id": 12, "series_id": 100, "m3u_account_id": 9, "active": True, "xc": True},
        {"id": 21, "series_id": 200, "m3u_account_id": 7, "active": True, "xc": True},
        {"id": 31, "series_id": 100, "m3u_account_id": 8, "active": False, "xc": True},
        {"id": 41, "series_id": 200, "m3u_account_id": 5, "active": True, "xc": False},
    ]


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #
def test_observation_records_and_dedupes():
    print("test_observation_records_and_dedupes")
    reset()
    seed_relations()
    patch._observe(11)  # relation 11 -> series 100
    wl = patch.get_watchlist(force=True)["series"]
    check("observing a series_id records its Series PK", "100" in wl)

    before = ENV.coresettings.get(patch.WATCHLIST_CORE_KEY, {}).get("series", {}).get("100")
    patch._observe(11)  # immediate repeat -> deduped, no rewrite
    after = ENV.coresettings.get(patch.WATCHLIST_CORE_KEY, {}).get("series", {}).get("100")
    check("immediate repeat is deduped (timestamp unchanged)", before == after)

    patch._observe(21)  # relation 21 -> series 200
    wl = patch.get_watchlist(force=True)["series"]
    check("a different series is recorded too", "200" in wl and len(wl) == 2)


def test_wrapper_passthrough():
    print("test_wrapper_passthrough")
    reset()
    seed_relations()
    patch._orig_xc_get_series_info = lambda request, user, series_id: ("NATIVE", series_id)
    out = patch.patched_xc_get_series_info("req", "user", 11)
    check("wrapper returns the native response unchanged", out == ("NATIVE", 11))
    check("wrapper still recorded the series", "100" in patch.get_watchlist(force=True)["series"])

    patch._ACTIVE = False
    ENV2 = patch.get_watchlist(force=True)["series"]
    out2 = patch.patched_xc_get_series_info("req", "user", 21)
    check("inactive wrapper is a pure passthrough", out2 == ("NATIVE", 21))
    check("inactive wrapper does not record", "200" not in patch.get_watchlist(force=True)["series"])
    patch._ACTIVE = True


def test_sweep_enqueues_per_account():
    print("test_sweep_enqueues_per_account")
    reset()
    seed_relations()
    patch._observe(11)  # series 100
    patch._observe(21)  # series 200
    summary = patch.run_sweep_impl()

    by_acct = {a: sids for a, sids in ENV.enqueued}
    check("one enqueue per active XC account", sorted(by_acct.keys()) == [7, 9])
    check("account 7 carries both watched series", by_acct.get(7) == [100, 200])
    check("account 9 carries only series 100 (its 4K category)", by_acct.get(9) == [100])
    check("inactive account 8 excluded", 8 not in by_acct)
    check("non-XC account 5 excluded", 5 not in by_acct)
    check("summary counts are right", summary["watched"] == 2 and summary["tasks"] == 2)


def test_ttl_prune():
    print("test_ttl_prune")
    reset(settings={"watchlist_ttl_days": 30})
    seed_relations()
    patch._observe(11)   # series 100 at t0
    CLOCK.advance(40 * 86400)  # 40 days later
    patch._observe(21)   # series 200 now (fresh)
    summary = patch.run_sweep_impl()
    wl = patch.get_watchlist(force=True)["series"]
    check("stale series (40d) pruned", "100" not in wl)
    check("fresh series kept", "200" in wl)
    check("summary records prune", summary["pruned"] == 1)
    check("only the fresh series is refreshed", [a for a, _ in ENV.enqueued] == [7])


def test_disabled_noops():
    print("test_disabled_noops")
    reset(enabled=False)
    seed_relations()
    patch._ACTIVE = True
    patch._observe(11)  # observe still records regardless of enabled flag
    summary = patch.run_sweep_impl()
    check("disabled sweep enqueues nothing", ENV.enqueued == [])
    check("disabled sweep reports skipped", summary["skipped"] == "plugin disabled")


def test_install_reload_safe():
    print("test_install_reload_safe")
    reset()
    ok1 = patch.install(manage_schedule=False)
    orig1 = patch._orig_xc_get_series_info
    ok2 = patch.install(manage_schedule=False)  # reload: must not self-capture
    check("install succeeds", ok1 and ok2)
    check("original preserved across reload (not self-captured)",
          patch._orig_xc_get_series_info is orig1)
    check("live handler is our wrapper",
          sys.modules["apps.output.views"].xc_get_series_info is patch.patched_xc_get_series_info)
    patch.uninstall()
    check("uninstall restores the native handler",
          sys.modules["apps.output.views"].xc_get_series_info is orig1)
    check("uninstall removes the schedule", ENV.delete_calls >= 1)


def test_enable_creates_routed_schedule():
    print("test_enable_creates_routed_schedule")
    reset(settings={"sweep_hour": 5})  # scheduled_sweep defaults True
    patch.install(manage_schedule=True)
    check("enable creates the beat schedule", ENV.schedule_calls == 1)
    check("schedule uses configured hour as cron",
          ENV.schedule and ENV.schedule.get("cron_expression") == "0 5 * * *")
    check("schedule targets the plugin task path",
          ENV.schedule.get("celery_task_path") == patch.SWEEP_TASK_PATH)
    check("schedule routed to the dvr queue", ENV.schedule_queue == "dvr")


def test_scheduled_sweep_disabled_removes():
    print("test_scheduled_sweep_disabled_removes")
    reset(settings={"scheduled_sweep": False})
    ENV.schedule_calls = 0
    ENV.delete_calls = 0
    present = patch.ensure_schedule()
    check("ensure_schedule returns False when disabled", present is False)
    check("no schedule created", ENV.schedule_calls == 0)
    check("existing schedule removed", ENV.delete_calls == 1)


def test_custom_schedule_queue():
    print("test_custom_schedule_queue")
    reset(settings={"schedule_queue": "celery_alt"})
    patch.ensure_schedule()
    check("custom queue honoured", ENV.schedule_queue == "celery_alt")


def test_run_sweep_task_dedups_via_claim():
    print("test_run_sweep_task_dedups_via_claim")
    reset()
    seed_relations()
    patch._observe(11)
    patch._observe(21)
    r1 = patch.run_sweep()  # beat entry point
    check("first scheduled run enqueues", sorted(a for a, _ in ENV.enqueued) == [7, 9])
    n_after_first = len(ENV.enqueued)
    r2 = patch.run_sweep()  # same day -> claim already held
    check("second scheduled run is skipped", r2 == {"skipped": "already ran today"})
    check("second run enqueues nothing further", len(ENV.enqueued) == n_after_first)


def test_daily_tick():
    print("test_daily_tick")
    from datetime import datetime as _dt
    reset(settings={"sweep_hour": 3})
    seed_relations()
    patch._observe(11)
    spawned = []
    saved_spawn = patch._spawn_sweep
    saved_now = patch._now_local
    patch._spawn_sweep = lambda reason="tick": spawned.append(reason)
    try:
        # _now_local drives the tick's hour/date (v0.3.1 uses Dispatcharr's system
        # TZ, not the container clock); monkeypatch it for deterministic hours.
        patch._now_local = lambda: _dt(2026, 8, 14, 2, 0, 0)  # before the hour
        patch._maybe_daily_sweep()
        check("before the hour: does not fire", spawned == [])
        check("before the hour: day not marked (can re-check later)",
              patch._daily_checked_date is None)

        patch._now_local = lambda: _dt(2026, 8, 14, 4, 0, 0)  # at/after the hour
        patch._maybe_daily_sweep()
        check("at/after the hour: fires once", spawned == ["tick"])
        check("claimed the day in redis",
              any(k.startswith(patch.DAILY_PREFIX) for k in ENV.redis))

        patch._maybe_daily_sweep()  # same worker, same day
        check("same day: does not fire again", spawned == ["tick"])

        # A second worker (fresh in-process guard) must also not double-fire,
        # because the Redis claim already exists.
        patch._daily_checked_date = None
        patch._maybe_daily_sweep()
        check("another worker: Redis claim prevents a second run", spawned == ["tick"])
    finally:
        patch._spawn_sweep = saved_spawn
        patch._now_local = saved_now


def test_now_local_falls_back_without_tz():
    print("test_now_local_falls_back_without_tz")
    from datetime import datetime as _dt
    reset()
    # fake get_system_time_zone returns "" -> _get_system_tzinfo is None
    check("no system TZ resolves to None", patch._get_system_tzinfo() is None)
    check("_now_local still returns a datetime", isinstance(patch._now_local(), _dt))
    check("_fmt_epoch renders a string", isinstance(patch._fmt_epoch(1_700_000_000), str))


def test_refresh_now_runs_inline():
    print("test_refresh_now_runs_inline")
    reset()
    seed_relations()
    patch._observe(11)
    patch._observe(21)
    summary = patch.run_sweep_impl()  # what the Run-now action calls
    check("inline run enqueues per-account core tasks",
          sorted(a for a, _ in ENV.enqueued) == [7, 9])
    check("inline run reports a summary", summary["tasks"] == 2 and summary["skipped"] is None)


def test_clear_watchlist():
    print("test_clear_watchlist")
    reset()
    seed_relations()
    patch._observe(11)
    patch._observe(21)
    check("two series before clear", len(patch.get_watchlist(force=True)["series"]) == 2)
    removed = patch.clear_watchlist()
    check("clear removes all", removed == 2 and patch.get_watchlist(force=True)["series"] == {})


def test_throttle_chunks_and_staggers():
    print("test_throttle_chunks_and_staggers")
    reset(settings={"sweep_batch_size": 2, "sweep_spacing_seconds": 5})
    # account 7 carries 5 watched series -> 3 chunks (2,2,1); no other account.
    ENV.series = {n: f"S{n}" for n in (100, 101, 102, 103, 104)}
    ENV.relations = [
        {"id": n, "series_id": n, "m3u_account_id": 7, "active": True, "xc": True}
        for n in (100, 101, 102, 103, 104)
    ]
    for n in (100, 101, 102, 103, 104):
        patch.record_series(n)
    summary = patch.run_sweep_impl()
    check("5 series at batch 2 -> 3 tasks", summary["tasks"] == 3)
    check("each task carries <= batch_size series", all(len(s) <= 2 for _, s in ENV.enqueued))
    check("all 5 series covered across chunks",
          sorted(x for _, s in ENV.enqueued for x in s) == [100, 101, 102, 103, 104])
    check("countdowns are staggered by spacing (0,5,10)", ENV.countdowns == [0, 5, 10])
    check("summary reports the spread", summary["spread_seconds"] == 10)


def test_stale_audit_surfaced():
    print("test_stale_audit_surfaced")
    reset()
    seed_relations()
    patch._observe(11)
    saved = patch._stale_by_account
    patch._stale_by_account = lambda pks, hours=25: {"Provider A": 3, "Provider B": 1}
    try:
        summary = patch.run_sweep_impl()
    finally:
        patch._stale_by_account = saved
    check("stale total summed", summary["stale_total"] == 4)
    check("stale broken down by account",
          summary["stale_by_account"] == {"Provider A": 3, "Provider B": 1})


def test_enqueue_failure_tolerated():
    print("test_enqueue_failure_tolerated")
    reset(delay_raises=True)
    seed_relations()
    patch._observe(11)
    summary = patch.run_sweep_impl()
    check("broker-down sweep does not crash", isinstance(summary, dict))
    check("nothing recorded as enqueued", ENV.enqueued == [])


if __name__ == "__main__":
    test_observation_records_and_dedupes()
    test_wrapper_passthrough()
    test_sweep_enqueues_per_account()
    test_ttl_prune()
    test_disabled_noops()
    test_install_reload_safe()
    test_enable_creates_routed_schedule()
    test_scheduled_sweep_disabled_removes()
    test_custom_schedule_queue()
    test_run_sweep_task_dedups_via_claim()
    test_daily_tick()
    test_now_local_falls_back_without_tz()
    test_refresh_now_runs_inline()
    test_throttle_chunks_and_staggers()
    test_stale_audit_surfaced()
    test_clear_watchlist()
    test_enqueue_failure_tolerated()
    try:
        os.remove(_scratch)
    except OSError:
        pass
    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED: {_failures}")
        sys.exit(1)
    print("All checks passed.")
