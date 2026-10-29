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
        self.plugin_tasks = []     # plugin @shared_task dispatches (auto-retry chain)
        # Release shape. False/False = 0.32.0 (XC series_id is a relation PK,
        # stop() owns schedule cleanup); True/True = 0.33.0. None = the model
        # has no readable _meta, so the release can't be told.
        self.series_has_language = False
        self.core_suspends_schedules = False
        self.auth_raise = None     # exception class the probe raises instead


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
        # Columns a scenario didn't seed read as None (a never-refreshed
        # relation, no provider date, no account name), which is what the
        # real rows hold for a relation the catalog/episode refresh hasn't
        # touched yet.
        return RelQS([{f: r.get(f) for f in fields} for r in self.rows])

    def values_list(self, field, flat=False):
        vals = [r[field] for r in self.rows]
        return RelQS(vals if flat else [(v,) for v in vals])

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class RelManager:
    def filter(self, id=None, series_id=None, series_id__in=None,
               m3u_account__is_active=None, m3u_account__account_type=None, **kw):
        rows = list(ENV.relations)
        # Django coerces a string lookup to int on an integer field, and the
        # XC router hands series_id straight from the query string
        # (`request.GET.get("series_id")`), so it IS a string in production.
        # Coerce here or the fake would be more forgiving than the real ORM.
        for field, val in (("id", id), ("series_id", series_id)):
            if val is None:
                continue
            try:
                val = int(val)
            except (TypeError, ValueError):
                return RelQS([])
            rows = [r for r in rows if r[field] == val]
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
        """Mimic Celery closely enough to test DISPATCH without executing.

        Real @shared_task attaches apply_async/delay to the function; the old
        stub returned the bare function, so a plugin task's own apply_async did
        not exist. Recording dispatches (name, args, countdown, queue) is what
        lets the auto-retry chain be tested -- the queue in particular, since a
        plugin task on the default prefork queue is silently rejected.
        """
        def deco(fn):
            def _apply_async(args=None, kwargs=None, countdown=0, queue=None):
                ENV.plugin_tasks.append({
                    "name": dkwargs.get("name"),
                    "args": list(args or []),
                    "kwargs": dict(kwargs or {}),
                    "countdown": countdown,
                    "queue": queue,
                })
            fn.apply_async = _apply_async
            fn.delay = lambda *a, **k: _apply_async(args=list(a), kwargs=k)
            return fn
        if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
            return deco(dargs[0])
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

    class _Field:
        def __init__(self, name):
            self.name = name

    class _Meta:
        """Model._meta.get_fields(), with one optional field toggled by ENV.

        This is how the plugin tells releases apart, so each fake model carries
        the field that marks the newer release only when the scenario says so.
        """

        def __init__(self, base, optional, env_attr):
            self.base, self.optional, self.env_attr = base, optional, env_attr

        def get_fields(self):
            flag = getattr(ENV, self.env_attr)
            if flag is None:
                raise RuntimeError("model meta unavailable")
            names = list(self.base) + ([self.optional] if flag else [])
            return [_Field(n) for n in names]

    plugins_models = types.ModuleType("apps.plugins.models")

    class PluginConfig:
        objects = PCManager()
        _meta = _Meta(("key", "enabled", "settings"), "suspended_schedules",
                      "core_suspends_schedules")
    plugins_models.PluginConfig = PluginConfig

    vod_models = types.ModuleType("apps.vod.models")

    class M3USeriesRelation:
        objects = RelManager()

    class Series:
        objects = SeriesManager()
        _meta = _Meta(("id", "name", "tmdb_id"), "language", "series_has_language")
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
            if ENV.auth_raise is not None:
                raise ENV.auth_raise("simulated core API change")
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
    patch._series_id_mode = None
    patch._probe_code_error_logged = False
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
    # Tick stubbed: left live it spawns a real background sweep that can
    # finish after the NEXT test has reset the environment, and write into it.
    with stubbed_tick():
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
    """1.2.0: unseen for the TTL is no longer enough on its own. With tiered
    refresh an old show is bumped weekly at best (and only when its top
    provider answers), so a time-only rule would drop shows still followed."""
    print("test_ttl_prune")
    reset(settings={"watchlist_ttl_days": 30})
    seed_relations()
    patch._observe(11)   # series 100 at t0
    CLOCK.advance(40 * 86400)  # 40 days later, and no bump ever reached the client
    patch._observe(21)   # series 200 now (fresh)
    saved = patch._visible_bumps
    patch._visible_bumps = lambda pks: {}
    try:
        summary = patch.run_sweep_impl()
    finally:
        patch._visible_bumps = saved
    wl = patch.get_watchlist(force=True)["series"]
    check("40 days unseen but never given a chance to re-sync: kept", "100" in wl)
    check("fresh series kept", "200" in wl)
    check("summary records no prune", summary["pruned"] == 0)

    CLOCK.advance((patch.EXPIRY_CEILING_DAYS - 40 + 1) * 86400)
    patch._observe(21)
    patch._visible_bumps = lambda pks: {}
    try:
        summary = patch.run_sweep_impl()
    finally:
        patch._visible_bumps = saved
    wl = patch.get_watchlist(force=True)["series"]
    check("unseen past the ceiling: pruned regardless", "100" not in wl)
    check("summary records the prune", summary["pruned"] == 1)
    check("still-seen series kept", "200" in wl)


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
    check("stale total labelled as pre-run", "stale before 994" in line)
    check("per-account split NOT repeated here (it is in the live audit, and "
          "repeating it grew these lines until the UI clipped them)",
          "Provider1 187" not in line and "Provider2 807" not in line)
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


def test_stale_cutoff_uses_last_sweep_time():
    """The fixed 25h window could not answer "did the last sweep refresh this?".

    With a ~24h cadence a relation that fails at 08:00 is still only ~24.5h
    past its previous success at 08:33, so it stayed invisible for another day
    -- which made a retry running shortly after a sweep structurally unable to
    see the failures it exists to repair. The cutoff is now the sweep's own
    start time, with the old window kept as a floor.
    """
    print("test_stale_cutoff_uses_last_sweep_time")
    from datetime import datetime, timedelta, timezone as _tz
    reset()
    now = datetime(2026, 1, 10, 8, 33, 0, tzinfo=_tz.utc)

    # No sweep recorded yet -> fall back to the fixed window.
    cut = patch._stale_cutoff(now=now)
    check("falls back to the fixed window with no recorded sweep",
          cut == now - timedelta(hours=patch.STALE_HOURS))

    # A sweep 33 minutes ago -> that is the cutoff, NOT now-25h.
    swept = datetime(2026, 1, 10, 8, 0, 0, tzinfo=_tz.utc)
    ENV.coresettings[patch.WATCHLIST_CORE_KEY] = {
        "series": {}, "last_sweep": {"at_epoch": swept.timestamp()},
    }
    patch._invalidate_watchlist_cache()
    cut = patch._stale_cutoff(now=now)
    check("uses the sweep start once one is recorded", cut == swept)
    check("a relation last refreshed BEFORE the sweep is now stale",
          (now - timedelta(hours=24, minutes=30)) < cut)
    check("a relation refreshed DURING the sweep is not stale",
          (swept + timedelta(minutes=5)) > cut)

    # A long-stalled sweep must not freeze the metric: the window floor wins.
    old = datetime(2026, 1, 5, 8, 0, 0, tzinfo=_tz.utc)
    ENV.coresettings[patch.WATCHLIST_CORE_KEY] = {
        "series": {}, "last_sweep": {"at_epoch": old.timestamp()},
    }
    patch._invalidate_watchlist_cache()
    cut = patch._stale_cutoff(now=now)
    check("a stalled sweep falls back to the window so the outage stays visible",
          cut == now - timedelta(hours=patch.STALE_HOURS))


def test_sweep_records_machine_readable_time():
    print("test_sweep_records_machine_readable_time")
    reset()
    seed_relations()
    patch._observe(11)
    summary = patch.run_sweep_impl()
    check("sweep records an epoch alongside the display string",
          isinstance(summary.get("at_epoch"), float))
    stored = patch.get_watchlist(force=True)["last_sweep"]
    check("epoch survives the round-trip through storage",
          stored.get("at_epoch") == summary["at_epoch"])
    check("later audits can derive a cutoff from it",
          patch._last_sweep_epoch() == summary["at_epoch"])


def test_auto_retry_config():
    print("test_auto_retry_config")
    reset()
    cfg = patch._load_config(force=True)
    check("auto-retry defaults on", cfg["auto_retry"] is True)
    check("default passes is 2", cfg["retry_passes"] == 2)

    reset(settings={"retry_passes": 999})
    check("passes clamped to the hard ceiling",
          patch._load_config(force=True)["retry_passes"] == patch.MAX_RETRY_PASSES)

    reset(settings={"retry_passes": -3})
    check("negative passes clamp to 0",
          patch._load_config(force=True)["retry_passes"] == 0)

    reset(settings={"auto_retry": False})
    check("auto-retry can be turned off",
          patch._load_config(force=True)["auto_retry"] is False)


def test_sweep_arms_the_retry_chain():
    print("test_sweep_arms_the_retry_chain")
    reset(settings={"retry_passes": 3})
    seed_relations()
    patch._observe(11)
    patch._observe(21)
    summary = patch.run_sweep_impl()

    chain = [t for t in ENV.plugin_tasks if t["name"] == patch.RETRY_TASK_PATH]
    check("sweep armed exactly one retry pass", len(chain) == 1)
    check("chain carries the configured pass count", chain[0]["args"] == [3])
    check("chain routed to the plugin-capable queue (NOT the prefork default)",
          chain[0]["queue"] == "dvr")
    check("chain waits out the sweep's own spread plus a settle margin",
          chain[0]["countdown"] == summary["spread_seconds"] + patch.RETRY_SETTLE_SECONDS)
    check("sweep records that a retry was armed",
          summary["auto_retry_passes"] == 3)


def test_sweep_does_not_arm_when_disabled():
    print("test_sweep_does_not_arm_when_disabled")
    reset(settings={"auto_retry": False})
    seed_relations()
    patch._observe(11)
    summary = patch.run_sweep_impl()
    check("no chain armed when auto-retry is off",
          [t for t in ENV.plugin_tasks if t["name"] == patch.RETRY_TASK_PATH] == [])
    check("summary reports none armed", summary["auto_retry_passes"] == 0)

    reset(settings={"retry_passes": 0})
    seed_relations()
    patch._observe(11)
    patch.run_sweep_impl()
    check("passes=0 also means no chain",
          [t for t in ENV.plugin_tasks if t["name"] == patch.RETRY_TASK_PATH] == [])


def test_auto_retry_chain_continues_then_stops():
    print("test_auto_retry_chain_continues_then_stops")
    reset()
    seed_relations()
    patch._observe(11)
    rows = [{"m3u_account_id": 7, "m3u_account__name": "Provider1",
             "series_id": 100, "last_episode_refresh": None}]
    saved = _stub_stale(rows)
    try:
        summary = patch.run_retry(3)   # stale remains -> should chain
    finally:
        patch._stale_rows = saved
    chain = [t for t in ENV.plugin_tasks if t["name"] == patch.RETRY_TASK_PATH]
    check("still-stale pass schedules the next one", len(chain) == 1)
    check("pass count decrements", chain[0]["args"] == [2])
    # The countdown is load-bearing: audit too early and the next pass re-tries
    # work still in flight, burning the whole chain for nothing.
    check("next pass waits out this pass's spread plus the settle margin",
          chain[0]["countdown"]
          == summary["spread_seconds"] + patch.RETRY_SETTLE_SECONDS)
    check("chain countdown is never zero",
          chain[0]["countdown"] >= patch.RETRY_SETTLE_SECONDS)
    check("chain stays on the plugin-capable queue", chain[0]["queue"] == "dvr")
    check("trigger records that this was automatic",
          "auto" in (summary.get("trigger") or ""))

    # Nothing stale -> chain must END, not burn the remaining passes.
    reset()
    seed_relations()
    patch._observe(11)
    saved = _stub_stale([])
    try:
        patch.run_retry(3)
    finally:
        patch._stale_rows = saved
    check("a clean pass ends the chain",
          [t for t in ENV.plugin_tasks if t["name"] == patch.RETRY_TASK_PATH] == [])

    # Last pass -> no further scheduling even with work outstanding.
    reset()
    seed_relations()
    patch._observe(11)
    saved = _stub_stale(rows)
    try:
        patch.run_retry(1)
    finally:
        patch._stale_rows = saved
    check("final pass does not chain",
          [t for t in ENV.plugin_tasks if t["name"] == patch.RETRY_TASK_PATH] == [])


def test_status_message_fits_the_ui_box():
    """The UI's result box clips a long message with no indication -- it did so
    twice in practice, both times losing the per-account stale breakdown, which
    is the thing you actually need during an incident. The earlier guard only
    measured ONE record line in isolation, so it never saw the assembled
    message grow with the number of accounts. Measure the whole thing."""
    print("test_status_message_fits_the_ui_box")
    reset()
    names = ["Provider%d" % i for i in range(1, 9)]
    live = {
        "total": 1234,
        "by_account": {n: 150 + i for i, n in enumerate(names)},
        "by_age": {"never": 4, "1": 57, "2": 846, "3+": 12},
        "error": None,
    }
    cfg = patch._load_config(force=True)
    wl = {
        "series": {str(i): 0 for i in range(900)},
        "last_sweep": {
            "at": "2026-10-05 08:00:00", "watched": 895, "accounts": 8,
            "tasks": 116, "spread_seconds": 1725, "stale_total": 911,
            "stale_by_account": {n: 100 for n in names},
            "auto_retry_passes": 2, "skipped_accounts": [], "skipped": None,
            "due": {"n": 12, "r": 210, "h": 341, "p": 25, "c": 1436},
        },
        "last_retry": {
            "at": "2026-10-05 12:26:57", "retried_series": 683, "accounts": 8,
            "tasks": 35, "spread_seconds": 510, "stale_total": 907,
            "stale_by_account": {n: 100 for n in names},
            "trigger": "manual", "skipped_accounts": [], "skipped": None,
        },
    }
    msg = patch.build_status_message(cfg, wl, live, 273, True, id_mode="relation")
    check("whole message fits the budget (len=%d)" % len(msg),
          len(msg) <= patch.STATUS_MAX_CHARS)
    check("every account survives in the live breakdown",
          all(n in msg for n in names))
    check("live audit comes before the stored records",
          msg.index("STALE NOW") < msg.index("last sweep"))
    check("age buckets present", "age(runs)" in msg)
    check("stored records do not repeat the per-account split",
          msg.count("Provider1 ") == 1)

    # A realistic day must show IN FULL. Measured live: a ~435-character status
    # was cut mid-line in the toast while ~420 showed whole, so the budget is
    # set below that. Fixture = a real bad day (a whole provider down at sweep
    # time, ~900 stale) on a 4-account setup, with 9-character account names, a
    # 5-digit pid and every age bucket in use.
    four = {"Provider%d" % i: 300 + i for i in range(1, 5)}
    live4 = {"total": 1210, "by_account": four,
             "by_age": {"never": 12, "1": 907, "2": 250, "3+": 41}, "error": None}
    wl4 = {
        "series": {str(i): 0 for i in range(1200)},
        "last_sweep": dict(wl["last_sweep"], watched=1200, accounts=4,
                           tasks=205, spread_seconds=4080, stale_total=1210),
        "last_retry": dict(wl["last_retry"], retried_series=683, accounts=4,
                           tasks=46, spread_seconds=900, stale_total=907,
                           trigger="auto, 1 left"),
    }
    msg4 = patch.build_status_message(cfg, wl4, live4, 27300, True,
                                      id_mode="relation", tmdb="1188/1196")
    check("a bad 4-account day fits without clipping (len=%d)" % len(msg4),
          len(msg4) <= patch.STATUS_MAX_CHARS and not msg4.endswith(" ..."))
    check("  ...including the last retry's stale count",
          msg4.rstrip().endswith("stale before 907"))
    check("budget stays under the measured toast limit",
          patch.STATUS_MAX_CHARS <= 420)
    check("no blank separator lines (they cost toast space)", "\n\n" not in msg4)

    # An overrun must announce itself rather than vanish mid-field.
    live["by_account"] = {("VeryLongProviderName%02d" % i): 999 for i in range(40)}
    long_msg = patch.build_status_message(cfg, wl, live, 273, True)
    check("an overrun is capped", len(long_msg) <= patch.STATUS_MAX_CHARS)
    check("and is marked as truncated", long_msg.endswith(" ..."))


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


class captured_warnings:
    """Collect the plugin logger's WARNING+ messages for the duration."""

    def __enter__(self):
        self.messages = []
        outer = self

        class _H(logging.Handler):
            def emit(self, record):
                if record.levelno >= logging.WARNING:
                    outer.messages.append(record.getMessage())
        self._h = _H()
        self._saved_propagate = patch.logger.propagate
        patch.logger.addHandler(self._h)
        patch.logger.propagate = False   # keep expected warnings off the console
        return self

    def __exit__(self, *exc):
        patch.logger.removeHandler(self._h)
        patch.logger.propagate = self._saved_propagate
        return False


def seed_overlapping_ids():
    """Relation PKs and Series PKs are independent auto-increment ranges, so
    the same integer is routinely valid as both. That overlap is what makes a
    wrong-meaning lookup SILENT rather than a miss, so the fixture builds it in:

      Series 100 ("Show A")  <- relations 11 (acct 7), 12 (acct 9)
      Series 555 ("Show C")  <- relation 100 (acct 7)   <- id 100 again
      Series 300 ("Show D")  <- relation 31 on an INACTIVE account only
    """
    ENV.series = {100: "Show A", 555: "Show C", 300: "Show D"}
    ENV.relations = [
        {"id": 11, "series_id": 100, "m3u_account_id": 7, "active": True, "xc": True},
        {"id": 12, "series_id": 100, "m3u_account_id": 9, "active": True, "xc": True},
        {"id": 100, "series_id": 555, "m3u_account_id": 7, "active": True, "xc": True},
        {"id": 31, "series_id": 300, "m3u_account_id": 8, "active": False, "xc": True},
    ]


def test_series_id_mode_detection():
    print("test_series_id_mode_detection")
    reset(series_has_language=False)
    check("0.32.0 shape (no Series.language) -> relation ids",
          patch._xc_series_id_mode() == "relation")

    reset(series_has_language=True)
    check("0.33.0 shape (Series.language) -> Series ids",
          patch._xc_series_id_mode() == "series")
    ENV.series_has_language = False
    check("mode is cached once detected (no per-request introspection)",
          patch._xc_series_id_mode() == "series")

    reset(series_has_language=None)
    seed_overlapping_ids()
    check("unreadable model meta -> mode unknown",
          patch._xc_series_id_mode() is None)
    patch._observe("100")
    check("unknown mode observes NOTHING rather than guessing",
          patch.get_watchlist(force=True)["series"] == {})
    ENV.series_has_language = True
    check("an unknown mode is not cached -- detection retries",
          patch._xc_series_id_mode() == "series")


def test_v033_series_id_is_the_series_pk():
    """The 0.33.0 break: the XC id is now a Series PK. With overlapping ids the
    old relation-PK lookup does not miss -- it records the WRONG series."""
    print("test_v033_series_id_is_the_series_pk")
    reset(series_has_language=True)
    seed_overlapping_ids()
    with stubbed_tick():
        patch._observe("100")   # string, as the XC router passes it
    wl = patch.get_watchlist(force=True)["series"]
    check("id 100 is recorded as Series 100 (the series the client asked for)",
          "100" in wl)
    check("NOT as Series 555, whose relation happens to have PK 100",
          "555" not in wl)

    patch._observe("300")
    check("a Series with no ACTIVE relation is not recorded",
          "300" not in patch.get_watchlist(force=True)["series"])
    patch._observe("999")
    check("an unknown Series id is not recorded",
          "999" not in patch.get_watchlist(force=True)["series"])
    patch._observe("abc")
    check("a non-numeric id is ignored, not raised",
          len(patch.get_watchlist(force=True)["series"]) == 1)

    patch.run_sweep_impl()
    by_acct = {a: sids for a, sids in ENV.enqueued}
    check("the sweep refreshes the RIGHT series on every relation",
          by_acct == {7: [100], 9: [100]})


def test_v032_series_id_is_still_a_relation_pk():
    print("test_v032_series_id_is_still_a_relation_pk")
    reset(series_has_language=False)
    seed_overlapping_ids()
    patch._observe("100")
    wl = patch.get_watchlist(force=True)["series"]
    check("on 0.32.0, id 100 is relation 100 -> Series 555", "555" in wl)
    check("and not Series 100", "100" not in wl)
    patch._observe("11")
    check("relation 11 -> Series 100, unchanged from 1.1.0",
          "100" in patch.get_watchlist(force=True)["series"])


def test_stop_keeps_the_schedule_where_it_should():
    print("test_stop_keeps_the_schedule_where_it_should")
    cases = [
        # (reason, core suspends schedules?, row removed?)
        ("delete", False, True),
        ("delete", True, True),
        ("reload", False, False),
        ("reload", True, False),
        ("disable", True, False),   # 0.33.0: core suspends + restores it
        ("disable", False, True),   # 0.32.0: nothing would, so remove it
        ("disable", None, True),    # can't tell -> the old behavior
        (None, True, True),         # no reason given -> the old behavior
    ]
    for reason, suspends, removed in cases:
        reset(core_suspends_schedules=suspends)
        patch.install(manage_schedule=False)
        patch.stop_for_reason(reason)
        check(f"stop(reason={reason!r}, core suspends={suspends}) "
              f"{'removes' if removed else 'keeps'} the schedule row",
              (ENV.delete_calls == 1) == removed)
        check(f"  ...and reverts the patch either way",
              patch._ACTIVE is False and
              sys.modules["apps.output.views"].xc_get_series_info
              is not patch.patched_xc_get_series_info)


def test_plugin_stop_passes_the_reason():
    """plugin.py can't be imported here (import runs install()), so check
    statically that stop() routes through the reason-aware helper."""
    print("test_plugin_stop_passes_the_reason")
    import ast
    tree = ast.parse(open(os.path.join(_here, "plugin.py"), encoding="utf-8").read())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Plugin")
    stop = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "stop")
    src = ast.unparse(stop)
    check("Plugin.stop calls stop_for_reason", "stop_for_reason(" in src)
    check("Plugin.stop reads context['reason']", "'reason'" in src)
    check("Plugin.stop no longer calls uninstall() directly", "uninstall(" not in src)


def test_preflight_fails_open_on_code_errors():
    print("test_preflight_fails_open_on_code_errors")
    for exc in (TypeError, AttributeError):
        reset(auth_raise=exc)
        with captured_warnings() as cap:
            ok = patch._provider_reachable(7)
            patch._provider_reachable(9)
        check(f"{exc.__name__} in the probe counts as reachable", ok is True)
        check(f"  ...and leaves exactly one warning naming it",
              len(cap.messages) == 1 and exc.__name__ in cap.messages[0])

    reset(auth_raise=TypeError)
    seed_relations()
    patch._observe(11)
    patch._observe(21)
    with captured_warnings():
        summary = patch.run_sweep_impl()
    check("a core API change does not skip every account",
          summary["skipped_accounts"] == [] and summary["tasks"] == 2)

    reset(auth_raise=ConnectionError)
    with captured_warnings():
        down = patch._provider_reachable(7)
    check("a network error still counts as down", down is False)


def test_import_time_schedule_follows_should_skip():
    """Dispatcharr 0.32.0+ returns False from should_skip_initialization() in
    one uWSGI worker per boot, so this import-time branch runs in production."""
    print("test_import_time_schedule_follows_should_skip")
    app_init = sys.modules["dispatcharr.app_initialization"]
    saved = app_init.should_skip_initialization
    try:
        app_init.should_skip_initialization = lambda: False
        reset(settings={"sweep_hour": 6})
        patch.install()
        check("the initializing worker creates the schedule",
              ENV.schedule_calls == 1 and ENV.delete_calls == 0)
        check("  ...at the configured hour, on the dvr queue",
              ENV.schedule["cron_expression"] == "0 6 * * *"
              and ENV.schedule_queue == "dvr")

        reset(settings={"scheduled_sweep": False})
        patch.install()
        check("with the auto-run toggle off it removes the schedule instead",
              ENV.schedule_calls == 0 and ENV.delete_calls == 1)

        app_init.should_skip_initialization = lambda: True
        reset()
        patch.install()
        check("every other worker leaves the schedule alone",
              ENV.schedule_calls == 0 and ENV.delete_calls == 0)

        def _boom():
            raise RuntimeError("no app_initialization")
        app_init.should_skip_initialization = _boom
        reset()
        ok = patch.install()
        check("an unusable should_skip check fails safe (patch on, schedule untouched)",
              ok and ENV.schedule_calls == 0 and ENV.delete_calls == 0)
    finally:
        app_init.should_skip_initialization = saved
        patch.uninstall(remove_sched=False)


def test_status_shows_the_id_mode():
    print("test_status_shows_the_id_mode")
    reset()
    cfg = patch._load_config(force=True)
    live = {"total": 0, "by_account": {}, "by_age": {}, "error": None}
    msg = patch.build_status_message(cfg, {"series": {}}, live, 1, True,
                                     id_mode="series")
    check("status names the detected XC id mode", "xc_ids=series" in msg)
    msg = patch.build_status_message(cfg, {"series": {}}, live, 1, True)
    check("and says so when it is unknown", "xc_ids=?" in msg)


def test_enqueue_failure_tolerated():
    print("test_enqueue_failure_tolerated")
    reset(delay_raises=True)
    seed_relations()
    patch._observe(11)
    summary = patch.run_sweep_impl()
    check("broker-down sweep does not crash", isinstance(summary, dict))
    check("nothing recorded as enqueued", ENV.enqueued == [])


# --------------------------------------------------------------------------- #
# 1.2.0: tiered refresh, TMDB, watchlist expiry
# --------------------------------------------------------------------------- #
from datetime import date as _date, datetime as _dt, timedelta as _td, timezone as _utc_tz

_DAY = 86400.0
_TODAY = _date(2026, 11, 2)
_NOW = _dt(2026, 11, 2, 8, 0, 0, tzinfo=_utc_tz.utc).timestamp()
_LM = patch.PROVIDER_LM_FIELD


def _rel(aid, sid, ler=None, lm=None):
    """A watched relation row as _relation_rows returns it (epochs for dates)."""
    return {"m3u_account_id": aid, "m3u_account__name": "Provider%d" % aid,
            "series_id": sid, "last_episode_refresh": ler, _LM: lm}


def _slot_ids(n_on, n_off, rotation=7, start=1000):
    """Series ids on / off today's rotation slot."""
    slot = _TODAY.toordinal() % rotation
    on = [i for i in range(start, start + 200) if i % rotation == slot][:n_on]
    off = [i for i in range(start, start + 200) if i % rotation != slot][:n_off]
    return on, off


def _cfg(**kw):
    reset(settings=kw)
    return patch._load_config(force=True)


def test_compute_due_tiers():
    print("test_compute_due_tiers")
    cfg = _cfg()
    (on1,), (newb, pend, carried, hotb, promo, offslot, overdue) = _slot_ids(1, 7)
    y = _NOW - _DAY           # refreshed yesterday
    rows = [
        _rel(7, newb, ler=None),                         # never refreshed
        _rel(7, pend, ler=y),                            # new to the watchlist
        _rel(7, carried, ler=y),                         # missed an earlier run
        _rel(7, hotb, ler=y),                            # airing
        _rel(7, promo, ler=y, lm=str(int(y + 3600))),    # provider changed it
        _rel(9, promo, ler=y),                           # ...on account 7 only
        _rel(7, on1, ler=_NOW - 3 * _DAY),               # cold, its slot today
        _rel(7, offslot, ler=y),                         # cold, not today
        _rel(7, overdue, ler=_NOW - 10 * _DAY),          # cold, past rotation+slack
    ]
    due, noisy = patch.compute_due(
        rows, {hotb}, cfg, _TODAY, _NOW, pending_new=[str(pend)],
        misses={patch._pair_key(7, carried): 1})
    check("never-refreshed relation is new", due.get((7, newb)) == "n")
    check("series new to the watchlist is new", due.get((7, pend)) == "n")
    check("missed earlier run is carried", due.get((7, carried)) == "r")
    check("airing series is hot", due.get((7, hotb)) == "h")
    check("provider-changed copy is promoted", due.get((7, promo)) == "p")
    check("...but only on the account that changed it", (9, promo) not in due)
    check("cold series on its slot is due", due.get((7, on1)) == "c")
    check("cold series off its slot is not due", (7, offslot) not in due)
    check("cold series past its rotation is due anyway", due.get((7, overdue)) == "c")
    check("no provider flagged as noisy", noisy == set())

    off = _cfg(tiered_refresh=False)
    due_off, _ = patch.compute_due(rows, {hotb}, off, _TODAY, _NOW)
    check("tiers off: every pair is due (1.1.x behavior)",
          len(due_off) == len({(r["m3u_account_id"], r["series_id"]) for r in rows}))

    nopromo = _cfg(provider_promotion=False)
    due_np, _ = patch.compute_due(rows, set(), nopromo, _TODAY, _NOW)
    check("promotion off: changed copy waits for its slot", (7, promo) not in due_np)

    junk = [_rel(7, promo, ler=y, lm="0"), _rel(7, offslot, ler=y, lm=str(int(_NOW * 1000)))]
    due_j, _ = patch.compute_due(junk, set(), cfg, _TODAY, _NOW)
    check("junk provider dates (zero, milliseconds) are ignored", due_j == {})


def test_promotion_noise_guard():
    print("test_promotion_noise_guard")
    cfg = _cfg()
    _, off = _slot_ids(0, 60)
    y = _NOW - _DAY
    loud = [_rel(7, s, ler=y, lm=str(int(y + 60))) for s in off[:40]]
    loud += [_rel(7, s, ler=y) for s in off[40:50]]
    due, noisy = patch.compute_due(loud, set(), cfg, _TODAY, _NOW)
    check("a provider marking most of its listing changed is ignored", noisy == {7})
    check("...so none of its copies is promoted", not any(t == "p" for t in due.values()))

    small = [_rel(9, s, ler=y, lm=str(int(y + 60))) for s in off[50:55]]
    due_s, noisy_s = patch.compute_due(small, set(), cfg, _TODAY, _NOW)
    check("a small provider's few real changes still promote",
          noisy_s == set() and all(t == "p" for t in due_s.values()) and len(due_s) == 5)


def test_tmdb_schedule_rules():
    print("test_tmdb_schedule_rules")
    A = 10
    d = lambda n: (_TODAY + _td(days=n)).isoformat()
    check("aired 3 days ago: hot", patch.tmdb_hot({"last": d(-3)}, _TODAY, A))
    check("aired 20 days ago, nothing scheduled: not hot",
          not patch.tmdb_hot({"last": d(-20)}, _TODAY, A))
    check("next episode in 2 days: hot", patch.tmdb_hot({"next": d(2)}, _TODAY, A))
    check("next episode in 10 days: not hot yet",
          not patch.tmdb_hot({"last": d(-30), "next": d(10)}, _TODAY, A))
    check("cached 'next' already passed (cache behind the premiere): still hot",
          patch.tmdb_hot({"last": d(-60), "next": d(-4)}, _TODAY, A))
    check("no dates: not hot", not patch.tmdb_hot({"st": "Ended"}, _TODAY, A))

    sane = patch.tmdb_sane
    check("decorated provider title matches TMDB title",
          sane("EN - Harbor of the Lake (US)", 2013, {"name": "Harbor of the Lake", "year": 2013}))
    check("matches on the original title too",
          sane("NF - La Torre Roja (MULTI SUB)", None, {"name": "The Red Tower", "oname": "La torre roja"}))
    check("year off by more than one: rejected",
          not sane("EN - Some Show", 2010, {"name": "Some Show", "year": 2019}))
    check("no shared words: rejected",
          not sane("EN - Garden Mysteries", None, {"name": "Space Pirates"}))
    check("a title with no usable words passes on the year",
          sane("42", 2001, {"name": "42", "year": 2001}))
    check("an error entry is never sane", not sane("Show", None, {"err": "not_found"}))

    rc = lambda e: patch.tmdb_recheck_seconds(e, _TODAY, A) / _DAY
    check("hot show re-checked every 2 days", rc({"last": d(-2)}) == 2)
    check("premiere within 14 days re-checked every 2 days", rc({"next": d(12)}) == 2)
    check("returning show weekly", rc({"st": "Returning Series", "last": d(-90)}) == 7)
    check("ended show monthly", rc({"st": "Ended", "last": d(-400)}) == 30)
    check("not found monthly", rc({"err": "not_found"}) == 30)
    check("failed lookup retried next day", rc({"err": "error"}) == 1)

    fb = lambda sig: patch.fallback_hot(sig, _TODAY, _NOW, 14)
    check("stored air date 5 days ago: hot", fb({"last_air": _TODAY - _td(days=5)}))
    check("stored air date 30 days ago: not hot", not fb({"last_air": _TODAY - _td(days=30)}))
    check("future stored air date: hot", fb({"last_air": _TODAY + _td(days=3)}))
    check("episode first seen 3 days ago: hot",
          fb({"last_air": _TODAY - _td(days=300), "last_new": _NOW - 3 * _DAY}))
    check("nothing stored: not hot", not fb({}))

    check("provider date parsed", patch._provider_lm("1790000000", _NOW) == 1790000000.0)
    check("provider date of zero ignored", patch._provider_lm("0", _NOW) is None)
    check("provider date in milliseconds ignored",
          patch._provider_lm(str(int(_NOW * 1000)), _NOW) is None)
    check("non-numeric provider date ignored", patch._provider_lm("n/a", _NOW) is None)
    check("tmdb id normalized", patch._norm_tmdb_id(" 1399 ") == "1399")
    check("'tv/' prefix accepted", patch._norm_tmdb_id("tv/1399") == "1399")
    check("junk tmdb id rejected",
          patch._norm_tmdb_id("abc") is None and patch._norm_tmdb_id("0") is None)


def test_classify_hot_prefers_tmdb():
    print("test_classify_hot_prefers_tmdb")
    cfg = _cfg()
    recent = _TODAY - _td(days=5)
    signals = {
        1: {"name": "EN - Alpha Show", "year": 2020, "tmdb_id": "11", "last_air": recent},
        2: {"name": "EN - Beta Show", "year": 2020, "tmdb_id": "22", "last_air": recent},
        3: {"name": "EN - Gamma Show", "year": 2020, "tmdb_id": "33", "last_air": recent},
        4: {"name": "EN - Delta Show", "year": 2020, "tmdb_id": None,
            "last_air": _TODAY - _td(days=200)},
    }
    items = {
        "11": {"name": "Alpha Show", "year": 2020, "last": (_TODAY - _td(days=2)).isoformat()},
        "22": {"name": "Beta Show", "year": 2020, "last": (_TODAY - _td(days=40)).isoformat(),
               "next": (_TODAY + _td(days=30)).isoformat()},
        "33": {"name": "Completely Different", "year": 1995},
    }
    hot, counts = patch.classify_hot([1, 2, 3, 4], signals, items, cfg, _TODAY, _NOW)
    check("TMDB says airing: hot", 1 in hot)
    check("TMDB says on a break: NOT hot, even with a recent stored date", 2 not in hot)
    check("TMDB entry for the wrong show: falls back to stored dates (hot)", 3 in hot)
    check("no TMDB id, old stored date: not hot", 4 not in hot)
    check("counts: 1 by tmdb, 1 by fallback, 2 known, 1 mismatch",
          counts == {"tmdb": 1, "fallback": 1, "known": 2, "mismatch": 1})
    hot_nokey, _ = patch.classify_hot([1, 2, 3, 4], signals, {}, cfg, _TODAY, _NOW)
    check("no TMDB data at all: stored dates decide", hot_nokey == {1, 2, 3})


def test_update_misses_and_priority():
    print("test_update_misses_and_priority")
    k = patch._pair_key
    prev = {k(7, 1): "h", k(7, 2): "c", k(9, 4): "n"}
    prev_epoch = _NOW - _DAY
    rows = [
        _rel(7, 1, ler=prev_epoch + 600),   # refreshed by that run
        _rel(7, 2, ler=prev_epoch - 600),   # still not refreshed
        _rel(7, 3, ler=prev_epoch - 600),   # wasn't due then
        _rel(9, 4, ler=None),               # still never refreshed
    ]
    m = patch.update_misses(prev, prev_epoch, rows, {k(7, 2): 1})
    check("refreshed pair drops out", k(7, 1) not in m)
    check("still-unrefreshed pair counts one more missed run", m.get(k(7, 2)) == 2)
    check("pair that wasn't due is not carried", k(7, 3) not in m)
    check("never-refreshed due pair is carried", m.get(k(9, 4)) == 1)
    check("no previous run recorded: nothing carried",
          patch.update_misses(prev, None, rows, {}) == {})

    due = {(7, 1): "c", (7, 2): "n", (9, 3): "h", (9, 4): "c", (7, 5): "r"}
    by_account, rank = patch.due_by_account(due, {k(7, 5): 2})
    check("per-account order: new, carried, then cold", by_account[7] == [2, 5, 1])
    check("hot before cold on the other account", by_account[9] == [3, 4])

    reset(settings={"sweep_batch_size": 1, "sweep_spacing_seconds": 0})
    patch._dispatch_refresh(by_account, 1, 0, rank=rank)
    check("chunks interleave across accounts by priority",
          ENV.enqueued == [(7, [2]), (7, [5]), (9, [3]), (7, [1]), (9, [4])])


def test_stale_means_due_and_unrefreshed():
    print("test_stale_means_due_and_unrefreshed")
    reset()
    old = _dt(2026, 10, 20, tzinfo=_utc_tz.utc)
    rows = [
        {"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": 1,
         "last_episode_refresh": old},
        {"m3u_account_id": 7, "m3u_account__name": "Provider1", "series_id": 2,
         "last_episode_refresh": old},
        {"m3u_account_id": 9, "m3u_account__name": "Provider2", "series_id": 3,
         "last_episode_refresh": None},
    ]
    check("no due set yet: every row counts (1.1.x definition)",
          len(patch._restrict_to_due([dict(r) for r in rows])) == 3)
    ENV.coresettings[patch.WATCHLIST_CORE_KEY] = {
        "series": {"1": 0, "2": 0, "3": 0},
        "due": {"at_epoch": _NOW, "pairs": {"7:1": "h"}},
        "misses": {"7:1": 1},
    }
    patch._invalidate_watchlist_cache()
    kept = patch._restrict_to_due([dict(r) for r in rows])
    check("not-due cold relation is not stale", [r["series_id"] for r in kept] == [1, 3])
    b = patch._stale_age_buckets(kept, now=_dt(2026, 11, 2, tzinfo=_utc_tz.utc))
    check("age counts runs missed, not days elapsed (missed 1 earlier run -> '2')",
          b == {"never": 1, "1": 0, "2": 1, "3+": 0})
    ENV.coresettings[patch.WATCHLIST_CORE_KEY]["due"] = {"at_epoch": _NOW, "pairs": {}}
    patch._invalidate_watchlist_cache()
    check("an EMPTY due set means nothing is due (not 'no due set')",
          [r["series_id"] for r in patch._restrict_to_due([dict(r) for r in rows])] == [3])


def test_sweep_tiered_end_to_end():
    print("test_sweep_tiered_end_to_end")
    reset(settings={"sweep_batch_size": 0})
    ENV.accounts = [{"id": 7, "server_url": "http://p7.example"},
                    {"id": 9, "server_url": "http://p9.example"}]
    ENV.auth_fail = {"http://p9.example": True}       # account 9 unreachable
    (on1,), (hotb, cold, pend, only9) = _slot_ids(1, 4)
    CLOCK.t = _NOW
    y = _NOW - _DAY
    ENV.relations = [
        dict(_rel(7, hotb, ler=y), id=1, active=True, xc=True),
        dict(_rel(7, cold, ler=y), id=2, active=True, xc=True),
        dict(_rel(7, on1, ler=_NOW - 3 * _DAY), id=3, active=True, xc=True),
        dict(_rel(7, pend, ler=y), id=4, active=True, xc=True),
        dict(_rel(9, only9, ler=y), id=5, active=True, xc=True),
    ]
    ENV.coresettings[patch.WATCHLIST_CORE_KEY] = {
        "series": {str(s): _NOW - 3600 for s in (hotb, cold, on1, pend, only9)},
        "pending_new": [str(pend), str(only9)],
    }
    saved = (patch._series_signals, patch._now_local, patch._visible_bumps)
    patch._series_signals = lambda pks: {hotb: {"last_air": _TODAY - _td(days=2)}}
    patch._now_local = lambda: _dt(2026, 11, 2, 8, 0, 0)
    patch._visible_bumps = lambda pks: {}
    try:
        summary = patch.run_sweep_impl()
    finally:
        patch._series_signals, patch._now_local, patch._visible_bumps = saved
    sent = {(a, s) for a, sids in ENV.enqueued for s in sids}
    check("hot, slot-due and new series dispatched; off-slot cold is not",
          sent == {(7, hotb), (7, on1), (7, pend)})
    check("new series dispatched first", ENV.enqueued[0] == (7, [pend, hotb, on1]))
    check("summary counts the due set by tier",
          summary["due"] == {"h": 1, "c": 1, "n": 2} and summary["due_pairs"] == 4)
    check("unreachable provider recorded by name",
          summary.get("skipped_names") == ["Provider9"])
    wl = patch.get_watchlist(force=True)
    check("due set persisted with the sweep's start time",
          wl["due"]["at_epoch"] == summary["at_epoch"]
          and patch._pair_key(9, only9) in wl["due"]["pairs"])
    check("dispatched new series leaves the queue", str(pend) not in wl["pending_new"])
    check("new series whose provider was unreachable stays queued",
          str(only9) in wl["pending_new"])
    check("no TMDB lookups queued without a key",
          not [t for t in ENV.plugin_tasks if t["name"] == patch.TMDB_TASK_PATH])

    # Next day, nothing was refreshed: yesterday's due pairs are carried.
    ENV.enqueued.clear()
    ENV.auth_fail = {}
    CLOCK.t = _NOW + _DAY
    patch._series_signals = lambda pks: {}
    patch._now_local = lambda: _dt(2026, 11, 3, 8, 0, 0)
    patch._visible_bumps = lambda pks: {}
    try:
        summary2 = patch.run_sweep_impl()
    finally:
        patch._series_signals, patch._now_local, patch._visible_bumps = saved
    wl2 = patch.get_watchlist(force=True)
    check("missed pairs are carried into the next run",
          wl2["misses"].get(patch._pair_key(7, hotb)) == 1
          and summary2["due"].get("r", 0) >= 3)


def test_sweep_survives_unreadable_provider_dates():
    print("test_sweep_survives_unreadable_provider_dates")
    reset()
    seed_relations()
    patch._observe(11)
    patch._observe(21)
    saved_values = RelQS.values

    def picky(self, *fields):
        if patch.PROVIDER_LM_FIELD in fields:
            raise RuntimeError("unsupported lookup")
        return saved_values(self, *fields)
    RelQS.values = picky
    try:
        summary = patch.run_sweep_impl()
    finally:
        RelQS.values = saved_values
    check("sweep still dispatches without the provider dates",
          sorted(a for a, _ in ENV.enqueued) == [7, 9] and summary["tasks"] == 2)


def test_sweep_queues_tmdb_when_keyed():
    print("test_sweep_queues_tmdb_when_keyed")
    reset(settings={"tmdb_api_key": "k"})
    seed_relations()
    patch._observe(11)
    saved = patch._series_signals
    patch._series_signals = lambda pks: {}
    try:
        patch.run_sweep_impl()
    finally:
        patch._series_signals = saved
    t = [t for t in ENV.plugin_tasks if t["name"] == patch.TMDB_TASK_PATH]
    check("one TMDB run queued after the sweep", len(t) == 1)
    check("...on the plugin-capable queue, after a delay",
          bool(t) and t[0]["queue"] == "dvr"
          and t[0]["countdown"] == patch.TMDB_FIRST_DELAY_SECONDS)


def test_tmdb_refresh_runs():
    print("test_tmdb_refresh_runs")
    reset(settings={"tmdb_api_key": "k"})
    CLOCK.t = _NOW
    ENV.coresettings[patch.WATCHLIST_CORE_KEY] = {"series": {"1": _NOW, "2": _NOW, "3": _NOW}}
    calls = []

    def fake_fetch(tid, key):
        calls.append(tid)
        return {"name": "S" + tid, "year": 2020, "st": "Returning Series",
                "last": "2026-01-01", "next": None}
    saved = (patch._series_signals, patch._tmdb_fetch, patch.TMDB_SPACING_SECONDS,
             patch._now_local, patch.TMDB_BATCH)
    patch._series_signals = lambda pks: {1: {"tmdb_id": "11"}, 2: {"tmdb_id": "22"},
                                         3: {"tmdb_id": None}}
    patch._tmdb_fetch = fake_fetch
    patch.TMDB_SPACING_SECONDS = 0
    patch._now_local = lambda: _dt(2026, 11, 2, 8, 0, 0)
    try:
        s1 = patch.tmdb_refresh_impl()
        items = patch._read_tmdb_cache()["items"]
        check("each watched TMDB id looked up once", sorted(calls) == ["11", "22"])
        check("entries cached with a fetch time", all("f" in items[t] for t in ("11", "22")))
        check("summary counts the lookups", s1["looked_up"] == 2 and s1["remaining"] == 0)

        calls.clear()
        s2 = patch.tmdb_refresh_impl()
        check("fresh entries are not looked up again", calls == [] and s2["looked_up"] == 0)

        # More due than one batch: re-queues itself.
        ENV.coresettings.pop(patch.TMDB_CORE_KEY, None)
        patch.TMDB_BATCH = 1
        s3 = patch.tmdb_refresh_impl()
        requeued = [t for t in ENV.plugin_tasks if t["name"] == patch.TMDB_TASK_PATH]
        check("a full batch with more due re-queues itself",
              s3["remaining"] == 1 and len(requeued) == 1)
        patch.TMDB_BATCH = saved[4]

        # Daily budget spent: nothing looked up.
        ENV.coresettings[patch.TMDB_CORE_KEY] = {"items": {}, "meta": {
            "day": "2026-11-02", "count": patch.TMDB_DAILY_CAP}}
        calls.clear()
        s4 = patch.tmdb_refresh_impl()
        check("daily lookup budget is respected", calls == [] and s4["looked_up"] == 0)

        # A rejected key stops the run and shows in status.
        ENV.coresettings.pop(patch.TMDB_CORE_KEY, None)

        def bad_key(tid, key):
            calls.append(tid)
            raise patch.TmdbAuthError("HTTP 401")
        patch._tmdb_fetch = bad_key
        calls.clear()
        s5 = patch.tmdb_refresh_impl()
        cfg = patch._load_config(force=True)
        check("rejected key stops the run after the first refusal",
              s5["stopped"] == "TMDB rejected the key" and len(calls) == 1)
        check("...and caches nothing for the refused lookup",
              patch._read_tmdb_cache()["items"] == {})
        check("status reports the bad key",
              patch.tmdb_status(cfg, patch.get_watchlist(force=True)) == "BAD KEY")

        def limited(tid, key):
            raise patch.TmdbRateLimited("HTTP 429")
        patch._tmdb_fetch = limited
        ENV.plugin_tasks.clear()
        s6 = patch.tmdb_refresh_impl()
        check("rate limit stops the run without re-queuing",
              s6["stopped"] == "rate limited"
              and not [t for t in ENV.plugin_tasks if t["name"] == patch.TMDB_TASK_PATH])
        check("a later run that isn't rejected clears the bad-key flag",
              patch._read_tmdb_cache()["meta"].get("auth_error") is None)
    finally:
        (patch._series_signals, patch._tmdb_fetch, patch.TMDB_SPACING_SECONDS,
         patch._now_local, patch.TMDB_BATCH) = saved

    reset()
    check("no key: lookups skipped", patch.tmdb_refresh_impl()["skipped"] == "no TMDB key")
    check("no key: status says off",
          patch.tmdb_status(patch._load_config(force=True), {}) == "off")

    leak = RuntimeError("HTTPSConnectionPool(host='api.example.org', port=443): Max "
                        "retries exceeded with url: /3/tv/1?api_key=SECRETKEY123 (timeout)")
    out = patch._tmdb_err(leak)
    check("TMDB error text never carries the key",
          "SECRETKEY123" not in out and "api_key" not in out)
    leak2 = ValueError("Invalid response from /3/tv/1?api_key=SECRETKEY123&language=en")
    check("...even when the URL isn't introduced by 'url'",
          "SECRETKEY123" not in patch._tmdb_err(leak2))

    due = patch.tmdb_lookups_due(
        ["1", "2", "3"],
        {"2": {"f": _NOW - 10 * _DAY, "st": "Returning Series"},
         "3": {"f": _NOW - 30 * _DAY, "st": "Returning Series"}},
        _TODAY, _NOW, 10)
    check("never-looked-up first, then the longest-unchecked", due == ["1", "3", "2"])


def test_tiering_config():
    print("test_tiering_config")
    cfg = _cfg()
    check("defaults", cfg["tiered"] is True and cfg["provider_promotion"] is True
          and cfg["hot_aired_days"] == 10 and cfg["fallback_hot_days"] == 14
          and cfg["rotation_days"] == 7 and cfg["tmdb_key"] == "")
    check("out-of-range rotation falls back to the default",
          _cfg(cold_rotation_days=0)["rotation_days"] == 7
          and _cfg(cold_rotation_days=40)["rotation_days"] == 7)
    check("string booleans parsed", _cfg(tiered_refresh="false")["tiered"] is False)
    os.environ["TMDB_API_KEY"] = "envkey"
    try:
        check("TMDB key falls back to the environment", _cfg()["tmdb_key"] == "envkey")
        check("the plugin setting wins over the environment",
              _cfg(tmdb_api_key=" mine ")["tmdb_key"] == "mine")
    finally:
        del os.environ["TMDB_API_KEY"]


def test_watchlist_expiry_by_ignored_chances():
    print("test_watchlist_expiry_by_ignored_chances")
    G = patch.CHANCE_GRACE_SECONDS
    seen = 1_000_000.0
    c1 = patch.update_chances({"1": seen}, {}, {1: seen + 5 * _DAY}, seen + 5 * _DAY + G)
    check("a bump the client had time to see counts", c1 == {"1": [1, seen + 5 * _DAY]})
    c2 = patch.update_chances({"1": seen}, c1, {1: seen + 5 * _DAY}, seen + 9 * _DAY)
    check("the same bump counts once", c2 == c1)
    c3 = patch.update_chances({"1": seen}, c2, {1: seen + 12 * _DAY}, seen + 15 * _DAY)
    check("a new bump counts again", c3["1"][0] == 2)
    check("a bump the client already synced past does not count",
          patch.update_chances({"1": seen}, {}, {1: seen - 60}, seen + 9 * _DAY) == {})
    check("a bump still inside the grace period does not count",
          patch.update_chances({"1": seen}, {}, {1: seen + 5 * _DAY},
                               seen + 5 * _DAY + G - 1) == {})
    check("no visible bump at all (failed refreshes) never counts",
          patch.update_chances({"1": seen}, {}, {}, seen + 90 * _DAY) == {})

    ttl = 30 * _DAY
    exp = patch.is_expired
    check("past TTL with 3 ignored chances: expired", exp(seen, [3, 0], seen + 31 * _DAY, ttl))
    check("past TTL with 2 ignored chances: kept", not exp(seen, [2, 0], seen + 60 * _DAY, ttl))
    check("3 ignored chances but inside TTL: kept", not exp(seen, [5, 0], seen + 20 * _DAY, ttl))
    check("past the ceiling: expired regardless",
          exp(seen, None, seen + (patch.EXPIRY_CEILING_DAYS + 1) * _DAY, ttl))
    check("a TTL longer than the ceiling raises the ceiling",
          not exp(seen, None, seen + 150 * _DAY, 200 * _DAY))

    # Through the sweep: weekly bumps the client ignores -> dropped once past
    # the TTL; a show the client re-synced is kept.
    reset(settings={"watchlist_ttl_days": 30})
    seed_relations()
    CLOCK.t = 1_000_000.0
    patch.record_series(100)
    patch.record_series(200)
    t0 = CLOCK.t
    saved = patch._visible_bumps
    try:
        for week in (1, 2, 3, 4, 5):
            CLOCK.t = t0 + week * 7 * _DAY
            bump = CLOCK.t - 3 * _DAY
            patch._visible_bumps = lambda pks, b=bump: {100: b, 200: b}
            if week == 2:
                patch.record_series(200)       # client still syncs show 200
            patch.run_sweep_impl()
            wl = patch.get_watchlist(force=True)
            if week == 3:
                check("week 3: inside the TTL nothing is dropped",
                      "100" in wl["series"] and wl["chances"].get("100", [0])[0] == 3)
            if week == 5:
                check("week 5: show ignoring every bump, past the TTL, is dropped",
                      "100" not in wl["series"])
                check("week 5: show the client re-synced is kept", "200" in wl["series"])
    finally:
        patch._visible_bumps = saved


def test_new_to_watchlist_is_queued():
    print("test_new_to_watchlist_is_queued")
    reset()
    patch.record_series(100)
    wl = patch.get_watchlist(force=True)
    check("first observation queues the series as new", wl["pending_new"] == ["100"])
    CLOCK.advance(patch.OBSERVE_DEDUPE_SECONDS + 1)
    ENV.coresettings[patch.WATCHLIST_CORE_KEY]["pending_new"] = []
    ENV.coresettings[patch.WATCHLIST_CORE_KEY]["chances"] = {"100": [2, 5.0]}
    patch._invalidate_watchlist_cache()
    patch.record_series(100)
    wl = patch.get_watchlist(force=True)
    check("a series already watched is not queued again", wl["pending_new"] == [])
    check("an observation clears its ignored-bump count", "100" not in wl["chances"])
    removed = patch.clear_watchlist()
    wl = patch.get_watchlist(force=True)
    check("clear empties every 1.2.0 field too",
          removed == 1 and wl["pending_new"] == [] and wl["chances"] == {}
          and wl["due"] is None and wl["misses"] == {})


def test_watchlist_row_old_shape_reads():
    print("test_watchlist_row_old_shape_reads")
    reset()
    ENV.coresettings[patch.WATCHLIST_CORE_KEY] = {"series": {"5": 1.0}, "last_sweep": None}
    patch._invalidate_watchlist_cache()
    wl = patch.get_watchlist(force=True)
    check("a 1.1.x row reads with empty 1.2.0 fields",
          wl["series"] == {"5": 1.0} and wl["chances"] == {} and wl["pending_new"] == []
          and wl["due"] is None and wl["misses"] == {})
    ENV.coresettings[patch.WATCHLIST_CORE_KEY].update(
        {"chances": {"5": [1, 2.0]}, "pending_new": ["5"],
         "due": {"at_epoch": 3.0, "pairs": {"7:5": "c"}}, "misses": {"7:5": 1}})
    patch._invalidate_watchlist_cache()
    wl = patch.get_watchlist(force=True)
    check("1.2.0 fields survive the row parser",
          wl["chances"] == {"5": [1, 2.0]} and wl["pending_new"] == ["5"]
          and wl["due"]["pairs"] == {"7:5": "c"} and wl["misses"] == {"7:5": 1})


def test_visible_bump_matches_core_order():
    print("test_visible_bump_matches_core_order")
    reset()
    ENV.relations = [
        {"id": 5, "series_id": 1, "m3u_account_id": 7, "m3u_account__priority": 1,
         "updated_at": 100.0},
        {"id": 6, "series_id": 1, "m3u_account_id": 8, "m3u_account__priority": 3,
         "updated_at": 50.0},
        {"id": 4, "series_id": 1, "m3u_account_id": 9, "m3u_account__priority": 3,
         "updated_at": 70.0},
        {"id": 9, "series_id": 1, "m3u_account_id": 6, "m3u_account__priority": 9,
         "updated_at": 999.0, "active": False},
    ]
    check("highest active priority, then lowest id, as core's get_series picks",
          patch._visible_bumps([1]) == {1: 70.0})


def test_status_shows_tiers_and_names():
    print("test_status_shows_tiers_and_names")
    cfg = _cfg()
    wl = {"series": {"1": 0}, "last_sweep": {
              "at": "2026-11-02 08:00:00", "accounts": 2, "tasks": 9,
              "due": {"n": 1, "h": 30, "c": 400}, "stale_total": 0,
              "skipped_accounts": [9], "skipped_names": ["Provider9"]},
          "last_retry": None}
    live = {"total": 0, "by_account": {}, "by_age": {}, "error": None}
    msg = patch.build_status_message(cfg, wl, live, 1, True, id_mode="series", tmdb="30/31")
    check("due counts by tier shown", "due n1 r0 h30 p0 c400" in msg)
    check("TMDB coverage shown", "tmdb 30/31" in msg)
    check("unreachable provider shown by name", "UNREACHABLE Provider9" in msg)
    check("default queue and TTL stay quiet", "q=" not in msg and "ttl=" not in msg)
    off = patch.build_status_message(_cfg(tiered_refresh=False), wl, live, 1, True)
    check("tiers off is spelled out", "TIERS OFF" in off)
    odd = patch.build_status_message(_cfg(watchlist_ttl_days=45, schedule_queue="alt"),
                                     wl, live, 1, True)
    check("non-default queue and TTL are shown", "q=alt" in odd and "ttl=45d" in odd)


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
    test_status_message_fits_the_ui_box()
    test_stale_cutoff_uses_last_sweep_time()
    test_sweep_records_machine_readable_time()
    test_auto_retry_config()
    test_sweep_arms_the_retry_chain()
    test_sweep_does_not_arm_when_disabled()
    test_auto_retry_chain_continues_then_stops()
    test_manifest_parity()
    test_enqueue_failure_tolerated()
    test_series_id_mode_detection()
    test_v033_series_id_is_the_series_pk()
    test_v032_series_id_is_still_a_relation_pk()
    test_stop_keeps_the_schedule_where_it_should()
    test_plugin_stop_passes_the_reason()
    test_preflight_fails_open_on_code_errors()
    test_import_time_schedule_follows_should_skip()
    test_status_shows_the_id_mode()
    test_compute_due_tiers()
    test_promotion_noise_guard()
    test_tmdb_schedule_rules()
    test_classify_hot_prefers_tmdb()
    test_update_misses_and_priority()
    test_stale_means_due_and_unrefreshed()
    test_sweep_tiered_end_to_end()
    test_sweep_survives_unreadable_provider_dates()
    test_sweep_queues_tmdb_when_keyed()
    test_tmdb_refresh_runs()
    test_tiering_config()
    test_watchlist_expiry_by_ignored_chances()
    test_new_to_watchlist_is_queued()
    test_watchlist_row_old_shape_reads()
    test_visible_bump_matches_core_order()
    test_status_shows_tiers_and_names()
    try:
        os.remove(_scratch)
    except OSError:
        pass
    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED: {_failures}")
        sys.exit(1)
    print("All checks passed.")
