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
  * COMPAT GUARDS (v1.0.1): the wrapper accepts AND forwards parameters it does
    not know about -- on the active and inactive paths alike -- so a future
    upstream signature change cannot raise TypeError during argument binding;
    drift leaves exactly one warning; the router's real call shape (module-global
    lookup, string series_id from the query string) reaches the patch; and the
    plugin logger carries its own level.

A NOTE ON FIDELITY, since it is what makes these guards worth anything: a fake
that only reproduces call shapes the plugin already handles cannot express a
compatibility break. These fakes therefore mirror upstream's CURRENT signature
and the way upstream actually calls it -- including that series_id arrives as a
string. The structural parity test encodes no parameter list at all, so it
survives this release and the next.

Run:  python test_logic.py     (or: py -3 test_logic.py)
"""

import importlib.util
import logging
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
        self.accounts = []         # [{id, name, server_url}] for the XC probe
        self.auth_calls = []       # server_urls the pre-flight probe hit
        self.auth_fail = {}        # server_url -> True (always) | int (n times)


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
            # Django coerces a string PK lookup to int on an integer field, and
            # the XC router hands series_id straight from the query string
            # (`request.GET.get("series_id")`), so it IS a string in production.
            # Coerce here or the fake would be more forgiving than the real ORM.
            try:
                id = int(id)
            except (TypeError, ValueError):
                return RelQS([])
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

    class FakeAccount:
        """Mirrors the M3UAccount attributes the pre-flight probe reads."""

        def __init__(self, row):
            self.id = row["id"]
            self.name = row.get("name", f"acct{row['id']}")
            self.server_url = row.get("server_url", f"http://p{row['id']}.example")
            self.username = "user"
            self.password = "secret"

        def get_user_agent_string(self):
            return "UA/1.0"

    class AcctManager:
        def filter(self, id=None, **kw):
            rows = [r for r in ENV.accounts if id is None or r["id"] == id]
            return RelQS([FakeAccount(r) for r in rows])

    class M3UAccount:
        Types = _Types
        objects = AcctManager()
    m3u_models.M3UAccount = M3UAccount

    # core.xtream_codes.Client -- only `authenticate()` is exercised, by the
    # pre-flight reachability probe. Its failure message deliberately mimics
    # upstream's, which embeds credentials in the URL, so the credential-
    # stripping test has something realistic to chew on.
    xc_mod = types.ModuleType("core.xtream_codes")

    class FakeXCClient:
        def __init__(self, server_url, username, password, user_agent=None):
            self.server_url = server_url

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def authenticate(self):
            ENV.auth_calls.append(self.server_url)
            fail = ENV.auth_fail.get(self.server_url)
            if not fail:
                return True
            if fail is not True:
                ENV.auth_fail[self.server_url] = fail - 1
            raise RuntimeError(
                "520 Server Error: <none> for url: "
                "http://panel.example/player_api.php?username=user&password=secret"
            )
    xc_mod.Client = FakeXCClient

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

    # django.utils.timezone -- the staleness code compares relation timestamps
    # against "now", so an aware UTC clock is enough. Tests that assert on age
    # pass an explicit `now` rather than relying on this.
    django_utils = types.ModuleType("django.utils")
    django_tz = types.ModuleType("django.utils.timezone")

    def _tz_now():
        from datetime import datetime, timezone as _dtz
        return datetime.now(_dtz.utc)
    django_tz.now = _tz_now
    django_utils.timezone = django_tz

    app_init = types.ModuleType("dispatcharr.app_initialization")
    app_init.should_skip_initialization = lambda: True  # import-time: don't schedule

    for name in ("apps", "apps.plugins", "apps.vod", "apps.m3u",
                 "apps.output", "dispatcharr"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["core"] = core
    sys.modules["core.utils"] = core_utils
    sys.modules["core.models"] = core_models
    sys.modules["core.scheduling"] = core_sched
    sys.modules["core.xtream_codes"] = xc_mod
    sys.modules["apps.plugins.models"] = plugins_models
    sys.modules["apps.vod.models"] = vod_models
    sys.modules["apps.m3u.models"] = m3u_models
    sys.modules["apps.vod.tasks"] = vod_tasks
    sys.modules["apps.output.views"] = output_views
    sys.modules["django"] = types.ModuleType("django")
    sys.modules["django.db"] = django_db
    sys.modules["django.utils"] = django_utils
    sys.modules["django.utils.timezone"] = django_tz
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
    # Accounts the pre-flight probe can look up. Reachable unless a test adds
    # an ENV.auth_fail entry for the account's server_url.
    ENV.accounts = [{"id": i, "server_url": f"http://p{i}.example"}
                    for i in (5, 7, 8, 9)]
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


def call_wrapper(*args, **kwargs):
    """Invoke the wrapper, turning a binding TypeError into a returned value.

    A TypeError here IS the production failure mode, so it must be reported as a
    failed check rather than raised -- otherwise it aborts the runner and hides
    every test after it, which is the opposite of what a compat guard is for.
    """
    try:
        return patch.patched_xc_get_series_info(*args, **kwargs)
    except TypeError as exc:
        return ("TYPE_ERROR", str(exc))


class stubbed_tick:
    """Suppress the opportunistic daily tick for wrapper-focused tests.

    The tick would claim the day and spawn a real background sweep thread,
    which makes assertions depend on thread timing and on the test machine's
    wall-clock hour. These tests are about the wrapper's call contract.
    """

    def __enter__(self):
        self._saved = patch._maybe_daily_sweep
        patch._maybe_daily_sweep = lambda: None
        return self

    def __exit__(self, *exc):
        patch._maybe_daily_sweep = self._saved
        return False


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


def test_wrapper_signature_is_open():
    """Structural guard: encodes no parameter list, so it survives the NEXT
    upstream change too."""
    print("test_wrapper_signature_is_open")
    import inspect
    reset()
    params = inspect.signature(patch.patched_xc_get_series_info).parameters
    kinds = [p.kind for p in params.values()]
    check("wrapper still mirrors core's three named params",
          list(params)[:3] == ["request", "user", "series_id"])
    check("wrapper accepts extra positionals (*args)",
          inspect.Parameter.VAR_POSITIONAL in kinds)
    check("wrapper accepts extra keywords (**kwargs)",
          inspect.Parameter.VAR_KEYWORD in kinds)


def test_signature_parity_accepts_and_forwards():
    """The durable compat guard. A parameter added to core's xc_get_series_info
    must be ACCEPTED (no TypeError at argument binding, which precedes the
    _ACTIVE guard and the internal try) *and* FORWARDED (a swallowed parameter
    silently drops something core relied on -- worse than a crash)."""
    print("test_signature_parity_accepts_and_forwards")
    reset()
    seed_relations()
    seen = {}

    def spy(request, user, series_id, *args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return ("NATIVE", series_id)

    patch._orig_xc_get_series_info = spy

    with stubbed_tick():
        # Mirror how core introduced its last new parameter: an unconditional
        # keyword whose value is usually None. Add a positional too.
        out = call_wrapper("req", "user", 11, "extra_positional", future_param=None)
        check("unknown args accepted (no TypeError)", out == ("NATIVE", 11))
        check("unknown positional forwarded to core",
              seen.get("args") == ("extra_positional",))
        check("unknown keyword forwarded to core",
              seen.get("kwargs") == {"future_param": None})
        check("observation still happens alongside",
              "100" in patch.get_watchlist(force=True)["series"])

        # The inactive path must forward too -- otherwise turning the feature
        # off reintroduces the very crash this guards against.
        seen.clear()
        patch._ACTIVE = False
        out2 = call_wrapper("req", "user", 21, "extra2", future_param=7)
        patch._ACTIVE = True
        check("inactive path accepts unknown args", out2 == ("NATIVE", 21))
        check("inactive path forwards unknown positional",
              seen.get("args") == ("extra2",))
        check("inactive path forwards unknown keyword",
              seen.get("kwargs") == {"future_param": 7})


def test_signature_drift_is_logged_once():
    print("test_signature_drift_is_logged_once")
    reset()
    seed_relations()
    patch._orig_xc_get_series_info = lambda request, user, series_id, *a, **k: ("NATIVE", series_id)
    patch._extra_args_logged = False

    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Capture()
    saved_propagate = patch.logger.propagate
    patch.logger.addHandler(handler)
    patch.logger.propagate = False  # keep the expected warning out of stderr
    try:
        with stubbed_tick():
            call_wrapper("req", "user", 11, future_param=None)
            warnings = [r for r in records if r.levelno == logging.WARNING]
            check("drift leaves exactly one warning", len(warnings) == 1)
            call_wrapper("req", "user", 21, future_param=None)
            warnings = [r for r in records if r.levelno == logging.WARNING]
            check("sensor is one-shot, not per-request", len(warnings) == 1)

            records.clear()
            patch._extra_args_logged = False
            call_wrapper("req", "user", 11)  # today's shape
            check("no warning when core's signature is unchanged",
                  [r for r in records if r.levelno == logging.WARNING] == [])
    finally:
        patch.logger.removeHandler(handler)
        patch.logger.propagate = saved_propagate
        patch._extra_args_logged = False


def test_router_call_shape_reaches_the_patch():
    """Fidelity check on the CALL SITE, not just the signature: Dispatcharr's XC
    router resolves the module global at call time and passes three positionals,
    with series_id coming from the query string (so a STRING)."""
    print("test_router_call_shape_reaches_the_patch")
    reset()
    seed_relations()
    patch.install(manage_schedule=False)
    try:
        def fake_xc_player_api(series_id_param):
            views = sys.modules["apps.output.views"]
            return views.xc_get_series_info("req", "user", series_id_param)

        with stubbed_tick():
            out = fake_xc_player_api("11")
        check("router's module-global lookup reaches the patched handler",
              out == ("NATIVE", "11"))
        check("string series_id from the query string still resolves",
              "100" in patch.get_watchlist(force=True)["series"])
    finally:
        patch.uninstall()


def test_plugin_logger_has_its_own_level():
    """plugins.* is absent from Dispatcharr's LOGGING config, so without this the
    logger inherits root -- which Celery's prefork pool leaves at WARNING in
    forked children, silently discarding every plugin INFO record there."""
    print("test_plugin_logger_has_its_own_level")
    reset()
    check("plugin logger is not left at NOTSET",
          patch.logger.level != logging.NOTSET)
    check("level adopted from the apps logger (so DISPATCHARR_LOG_LEVEL applies)",
          patch.logger.level == logging.getLogger("apps").getEffectiveLevel())
    check("plugin.py shares the same logger object",
          logging.getLogger("plugins.dispatcharr_vod_episode_sweep") is patch.logger)


def _stub_stale(rows):
    """Stub the single stale query with explicit rows.

    The fake ORM can't express the real chained `.filter(Q(...)|Q(...))`, and
    that query is verified by source diff rather than by fakes. What IS worth
    testing is everything DERIVED from it -- grouping, dedupe, ordering, and
    which accounts get retried -- so both consumers are driven from one stub.
    """
    saved = patch._stale_rows
    patch._stale_rows = lambda pks, hours=25: list(rows)
    return saved


def test_stale_derivations_share_one_definition():
    print("test_stale_derivations_share_one_definition")
    reset()
    rows = [
        {"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": 100},
        {"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": 100},
        {"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": 200},
        {"m3u_account_id": 9, "m3u_account__name": "Provider2", "series_id": 100},
    ]
    saved = _stub_stale(rows)
    try:
        counts = patch._stale_by_account([100, 200])
        by_acct = patch._stale_series_by_account([100, 200])
    finally:
        patch._stale_rows = saved
    check("counts are per RELATION (duplicates counted)",
          counts == {"Provider1": 3, "Provider2": 1})
    check("retry set is per SERIES (duplicates collapsed)",
          by_acct == {7: [100, 200], 9: [100]})
    check("retry series ids are sorted", by_acct[7] == sorted(by_acct[7]))


def test_preflight_fails_open():
    print("test_preflight_fails_open")
    reset()
    # Transient: fails once, succeeds on the second attempt -> must NOT skip.
    ENV.auth_fail = {"http://p7.example": 1}
    check("one-off probe failure still counts as reachable",
          patch._provider_reachable(7) is True)
    check("probe retried rather than giving up", len(ENV.auth_calls) == 2)

    # Unknown account id -> cannot probe -> must not block the sweep.
    reset()
    check("unknown account is treated as reachable",
          patch._provider_reachable(999) is True)


def test_preflight_skips_only_a_dead_panel():
    print("test_preflight_skips_only_a_dead_panel")
    reset()
    ENV.auth_fail = {"http://p9.example": True}  # fails every attempt
    check("account failing every attempt is unreachable",
          patch._provider_reachable(9) is False)
    check("gave it more than one chance", len(ENV.auth_calls) == 2)


def test_sweep_skips_unreachable_account():
    print("test_sweep_skips_unreachable_account")
    reset()
    seed_relations()
    ENV.auth_fail = {"http://p9.example": True}   # account 9 panel is dead
    patch._observe(11)  # series 100 -> accounts 7 and 9
    patch._observe(21)  # series 200 -> account 7
    summary = patch.run_sweep_impl()

    accounts_enqueued = sorted({a for a, _ in ENV.enqueued})
    check("dead account gets no refresh tasks", accounts_enqueued == [7])
    check("healthy account still swept", 7 in accounts_enqueued)
    check("skip is recorded in the summary", summary["skipped_accounts"] == [9])
    check("account count reflects only accounts actually swept",
          summary["accounts"] == 1)


def test_retry_stale_targets_only_stale_series():
    print("test_retry_stale_targets_only_stale_series")
    reset()
    seed_relations()
    patch._observe(11)   # watchlist: series 100
    patch._observe(21)   # watchlist: series 200
    # Only series 200 on account 7 is stale.
    rows = [{"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": 200}]
    saved = _stub_stale(rows)
    try:
        summary = patch.retry_stale_impl()
    finally:
        patch._stale_rows = saved

    check("only the stale account is dispatched to",
          sorted({a for a, _ in ENV.enqueued}) == [7])
    check("only the stale series is retried",
          [sids for _, sids in ENV.enqueued] == [[200]])
    check("summary reports the stale relation total", summary["stale_total"] == 1)
    check("summary breaks stale down per account (a skipped provider keeps its "
          "backlog, which would otherwise mask progress elsewhere)",
          summary["stale_by_account"] == {"Provider1": 1})
    check("summary reports how many series were retried",
          summary["retried_series"] == 1)
    check("retry does NOT claim the day (scheduled sweep still runs)",
          not any(k.startswith(patch.DAILY_PREFIX) for k in ENV.redis))


def test_retry_stale_noop_when_clean():
    print("test_retry_stale_noop_when_clean")
    reset()
    seed_relations()
    patch._observe(11)
    saved = _stub_stale([])
    try:
        summary = patch.retry_stale_impl()
    finally:
        patch._stale_rows = saved
    check("nothing enqueued when nothing is stale", ENV.enqueued == [])
    check("reports nothing stale", summary["skipped"] == "nothing stale")


def test_retry_stale_respects_throttle():
    print("test_retry_stale_respects_throttle")
    reset(settings={"sweep_batch_size": 2, "sweep_spacing_seconds": 5})
    ENV.series = {n: f"S{n}" for n in (100, 101, 102, 103, 104)}
    ENV.relations = [
        {"id": n, "series_id": n, "m3u_account_id": 7, "active": True, "xc": True}
        for n in (100, 101, 102, 103, 104)
    ]
    for n in (100, 101, 102, 103, 104):
        patch.record_series(n)
    rows = [{"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": n}
            for n in (100, 101, 102, 103, 104)]
    saved = _stub_stale(rows)
    try:
        summary = patch.retry_stale_impl()
    finally:
        patch._stale_rows = saved
    check("retry chunks like the sweep does", summary["tasks"] == 3)
    check("retry staggers countdowns too", ENV.countdowns == [0, 5, 10])


def test_error_text_never_carries_credentials():
    """Upstream's XC errors embed username/password in the failing URL. Nothing
    this plugin logs may copy them."""
    print("test_error_text_never_carries_credentials")
    reset()
    exc = RuntimeError(
        "520 Server Error: <none> for url: "
        "http://panel.example/player_api.php?username=user&password=secret"
    )
    out = patch._safe_err(exc)
    check("error type preserved", "RuntimeError" in out)
    check("status text preserved", "520 Server Error" in out)
    check("URL stripped", "http" not in out)
    check("username stripped", "username" not in out)
    check("password stripped", "password" not in out and "secret" not in out)


def _aged_row(account_id, name, series_id, hours_ago, now):
    """A stale row whose relation last refreshed `hours_ago` (None = never)."""
    from datetime import timedelta
    return {
        "m3u_account_id": account_id, "m3u_account__name": name,
        "series_id": series_id,
        "last_episode_refresh": None if hours_ago is None else now - timedelta(hours=hours_ago),
    }


def test_stale_age_buckets():
    """A relation stale for several cycles is very unlikely to be merely
    unlucky, so the age split separates transient provider flakiness from
    genuine dead content."""
    print("test_stale_age_buckets")
    from datetime import datetime, timezone as _tz
    reset()
    now = datetime(2026, 1, 10, 8, 0, 0, tzinfo=_tz.utc)
    rows = [
        _aged_row(7, "Provider1", 1, 26, now),    # 1 cycle
        _aged_row(7, "Provider1", 2, 30, now),    # 1 cycle
        _aged_row(7, "Provider1", 3, 50, now),    # 2 cycles
        _aged_row(7, "Provider1", 4, 100, now),   # 3+ cycles
        _aged_row(7, "Provider1", 5, None, now),  # never refreshed
    ]
    buckets = patch._stale_age_buckets(rows, now=now)
    check("one-cycle stale counted", buckets["1"] == 2)
    check("two-cycle stale counted", buckets["2"] == 1)
    check("3+ cycle stale bucketed together", buckets["3+"] == 1)
    check("never-refreshed tracked separately", buckets["never"] == 1)


def test_retry_prioritises_worst_stale_first():
    print("test_retry_prioritises_worst_stale_first")
    from datetime import datetime, timezone as _tz
    reset()
    now = datetime(2026, 1, 10, 8, 0, 0, tzinfo=_tz.utc)
    rows = [
        _aged_row(7, "Provider1", 100, 26, now),    # freshest stale
        _aged_row(7, "Provider1", 200, 100, now),   # very stale
        _aged_row(7, "Provider1", 300, None, now),  # never refreshed -> worst
        _aged_row(7, "Provider1", 400, 50, now),    # middling
    ]
    order = patch._stale_series_by_account([100, 200, 300, 400], rows=rows, now=now)[7]
    check("never-refreshed goes first", order[0] == 300)
    check("then oldest-to-newest by staleness", order == [300, 200, 400, 100])

    # A series whose WORST relation is ancient ranks by that relation.
    rows2 = [
        _aged_row(7, "Provider1", 100, 26, now),
        _aged_row(7, "Provider1", 100, 200, now),  # same series, much staler
        _aged_row(7, "Provider1", 200, 50, now),
    ]
    order2 = patch._stale_series_by_account([100, 200], rows=rows2, now=now)[7]
    check("series ranked by its worst relation", order2 == [100, 200])


def test_live_stale_summary():
    print("test_live_stale_summary")
    from datetime import datetime, timezone as _tz
    reset()
    seed_relations()
    patch._observe(11)
    now = datetime(2026, 1, 10, 8, 0, 0, tzinfo=_tz.utc)
    rows = [
        _aged_row(7, "Provider1", 100, 26, now),
        _aged_row(9, "Provider2", 100, None, now),
    ]
    saved = _stub_stale(rows)
    try:
        live = patch.live_stale_summary()
    finally:
        patch._stale_rows = saved
    check("live audit totals relations", live["total"] == 2)
    check("live audit splits by account",
          live["by_account"] == {"Provider1": 1, "Provider2": 1})
    check("live audit includes an age breakdown", sum(live["by_age"].values()) == 2)
    check("no error recorded on the happy path", live["error"] is None)


def test_retry_result_is_persisted():
    print("test_retry_result_is_persisted")
    reset()
    seed_relations()
    patch._observe(11)
    rows = [{"m3u_account_id": 7, "m3u_account__name": "Provider1",
             "series_id": 100, "last_episode_refresh": None}]
    saved = _stub_stale(rows)
    try:
        patch.retry_stale_impl()
    finally:
        patch._stale_rows = saved
    wl = patch.get_watchlist(force=True)
    check("retry outcome persisted as last_retry", wl.get("last_retry") is not None)
    check("retry did NOT overwrite the sweep's record", wl.get("last_sweep") is None)
    check("persisted retry survives a re-read (not dropped by the row parser)",
          patch.get_watchlist(force=True)["last_retry"]["retried_series"] == 1)


def test_run_record_formatting_stays_short():
    """The UI's action-result box truncates. v1.1.0 first shipped status dumping
    raw dicts and the message was cut off mid-field, hiding the per-account
    stale breakdown -- so keep these lines terse and assert it."""
    print("test_run_record_formatting_stays_short")
    reset()
    sweep = {
        "at": "2026-09-17 08:00:00", "watched": 899, "accounts": 4, "tasks": 116,
        "pruned": 0, "spread_seconds": 1725, "stale_total": 994,
        "stale_by_account": {"Provider1": 187, "Provider2": 807},
        "skipped_accounts": [], "skipped": None,
    }
    line = patch.format_run_record("last sweep", sweep)
    check("record renders on a single line", "\n" not in line)
    check("record stays well under a truncating UI box", len(line) < 200)
    check("the per-account breakdown survives",
          "Provider1 187" in line and "Provider2 807" in line)
    check("stale total labelled as pre-run", "stale before run 994" in line)
    check("no raw dict repr leaks in", "{'" not in line)

    retry = {
        "at": "2026-09-17 23:34:22", "retried_series": 168, "accounts": 1,
        "tasks": 9, "spread_seconds": 120, "stale_total": 181,
        "stale_by_account": {"Provider1": 181}, "skipped_accounts": [9],
        "skipped": None,
    }
    rline = patch.format_run_record("last retry", retry)
    check("retry record reports retried count", "168 retried" in rline)
    check("unreachable accounts surfaced", "UNREACHABLE [9]" in rline)
    check("absent record degrades gracefully",
          patch.format_run_record("last retry", None) == "last retry: none yet")


def test_manifest_parity():
    """plugin.py's Plugin class must match plugin.json.

    This matters more here than it looks: for an ENABLED plugin the loader
    treats the Plugin CLASS as authoritative for name/description/fields/
    actions and uses the manifest only as a fallback. So editing plugin.json
    alone changes nothing in the running UI -- a silent no-op -- and the two
    files drift apart precisely when someone tweaks wording. Parsed with `ast`
    rather than imported, because importing plugin.py would run install().
    """
    print("test_manifest_parity")
    import ast
    import json
    here = os.path.dirname(os.path.abspath(__file__))
    manifest = json.load(open(os.path.join(here, "plugin.json"), encoding="utf-8"))
    tree = ast.parse(open(os.path.join(here, "plugin.py"), encoding="utf-8").read())

    cls = next((n for n in tree.body
                if isinstance(n, ast.ClassDef) and n.name == "Plugin"), None)
    check("plugin.py defines a Plugin class", cls is not None)
    if cls is None:
        return

    attrs = {}
    for node in cls.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                try:
                    attrs[target.id] = ast.literal_eval(node.value)
                except Exception:
                    pass

    check("version matches the manifest",
          attrs.get("version") == manifest.get("version"))
    check("field ids match the manifest",
          [f["id"] for f in attrs.get("fields", [])]
          == [f["id"] for f in manifest.get("fields", [])])

    py_actions = {a["id"]: a for a in attrs.get("actions", [])}
    js_actions = {a["id"]: a for a in manifest.get("actions", [])}
    check("action ids match the manifest",
          sorted(py_actions) == sorted(js_actions))
    mismatched = [
        aid for aid in py_actions
        if aid in js_actions and any(
            py_actions[aid].get(k) != js_actions[aid].get(k)
            for k in ("label", "description", "button_label", "button_variant")
        )
    ]
    check(f"action text/styling identical in both files (drifted: {mismatched})",
          not mismatched)


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
    test_wrapper_signature_is_open()
    test_signature_parity_accepts_and_forwards()
    test_signature_drift_is_logged_once()
    test_router_call_shape_reaches_the_patch()
    test_plugin_logger_has_its_own_level()
    test_stale_derivations_share_one_definition()
    test_preflight_fails_open()
    test_preflight_skips_only_a_dead_panel()
    test_sweep_skips_unreachable_account()
    test_retry_stale_targets_only_stale_series()
    test_retry_stale_noop_when_clean()
    test_retry_stale_respects_throttle()
    test_error_text_never_carries_credentials()
    test_stale_age_buckets()
    test_retry_prioritises_worst_stale_first()
    test_live_stale_summary()
    test_retry_result_is_persisted()
    test_run_record_formatting_stays_short()
    test_manifest_parity()
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
