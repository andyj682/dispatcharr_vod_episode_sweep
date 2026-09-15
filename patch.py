"""
Scheduled, watchlist-driven VOD episode sweep for current Dispatcharr.

Problem (recap)
---------------
Dispatcharr materialises VOD episode streams lazily and PER-RELATION (per
provider AND per category), 24h-gated, and only for the one relation a client
browses via XC `get_series_info`. `get_series` advertises just the highest-
priority relation per series, so an incremental client (e.g. an Emby .strm
generator) that syncs on `last_modified` only ever refreshes that top relation.
A new episode that first appears on a *different* relation (a second provider, or
the same provider's separate "... 4K" category) is therefore never fetched, so
it never materialises and never shows up over XC. Nothing on the Dispatcharr
side refreshes episodes on a schedule.

Fix
---
Keep a **watchlist** of the series a client actually syncs, and once a day
refresh ALL relations of each watched series:

    batch_refresh_series_episodes.delay(account_id, series_ids=[...])   # per XC account

`series_ids` forces every one of that account's relations for the series and
bypasses the 24h gate, so the whole candidate set is warmed across providers and
categories. Because that refresh also bumps each relation's `updated_at` (the
value XC surfaces as `last_modified`), the client's normal incremental sync then
notices the series and re-pulls the newly-materialised episodes.

Watchlist by observation
------------------------
The plugin wraps `xc_get_series_info` (the call a client makes per series it
syncs) and records the resolved Series PK into a durable `CoreSettings` row. This
is self-seeding (a client can't emit episodes without calling get_series_info at
least once) and self-sustaining (the daily refresh bumps last_modified, so the
client keeps calling get_series_info, which keeps re-observing). A generous TTL
ages out only series that are genuinely no longer synced.

Triggering -- two paths, deduped by a shared daily claim
--------------------------------------------------------
Dispatcharr's **default** Celery worker is a prefork pool whose consumer (main
process) never imports plugins, so a plugin `@shared_task` dispatched there is
rejected as "Received unregistered task". But the **threads-pool `dvr` worker**
imports plugins in its single (consumer) process, so it CAN run a plugin task.
So the sweep has two triggers, and whichever fires first each day wins (a shared
Redis `SET NX vodsweep:daily:<date>` claim makes the other a no-op):

  1. **Scheduled (primary):** a beat `PeriodicTask` runs the plugin `@shared_task`
     `run_sweep` at the configured hour, ROUTED TO THE `dvr` QUEUE (the only
     worker that can run it). This fires on a fixed daily timer -- independent of
     client traffic -- so it can run BEFORE the client's daily sync, which means
     new episodes surface in ONE sync instead of two. Disable via the
     "Auto-run daily sweep" setting.
  2. **Opportunistic tick (fallback):** the observation hook, on the first
     `get_series_info` at/after the hour each day, claims the day and runs the
     sweep in a background thread. Covers the case where beat/the dvr worker is
     unavailable (fails safe to the older, traffic-triggered behaviour).

Either path calls `run_sweep_impl()`, which only ever enqueues the CORE task
`batch_refresh_series_episodes` (registered on the default worker) -- so the
actual provider work always runs normally in Celery regardless of which worker
triggered it.

Everything is fail-open: observation and both triggers never affect the XC
response, and any failure leaves native behaviour untouched.

Signature-agnostic wrapper
--------------------------
"Fail-open" covers what happens *inside* the wrapper and is useless for what
happens *before it is entered*. A parameter added to core's `xc_get_series_info`
would raise `TypeError` while binding arguments -- ahead of the `_ACTIVE` guard
and the internal `try` -- and the XC router does not catch it, so an upstream
signature change would mean HTTP 500 on every series request rather than a quiet
loss of function. The wrapper therefore accepts `*args, **kwargs` and forwards
them at every call of the original, and logs once if it ever sees any.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time

logger = logging.getLogger("plugins.dispatcharr_vod_episode_sweep")

# Dispatcharr's LOGGING dictConfig names the loggers it manages (`apps`,
# `celery`, `core.*`, `django.geventpool`, root) and gives each its own handler
# with propagate=False. `plugins.*` is NOT among them, so a plugin logger has no
# handler, no level, and inherits root's EFFECTIVE level. That is fine in uWSGI
# (root at INFO), but Celery's prefork pool reconfigures root in each forked
# child and leaves it at WARNING -- which discards every plugin `logger.info()`
# at the logger, before any handler sees it, making a working plugin look
# identical to an absent one. Adopting `apps`'s level fixes that while still
# honouring DISPATCHARR_LOG_LEVEL; the NOTSET guard leaves an operator (or a
# test) that deliberately set a level in control.
if logger.level == logging.NOTSET:
    logger.setLevel(logging.getLogger("apps").getEffectiveLevel() or logging.INFO)

try:
    from celery import shared_task
except Exception:  # pragma: no cover - celery is always present at runtime
    def shared_task(*dargs, **dkwargs):
        def deco(fn):
            return fn
        if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
            return dargs[0]
        return deco


# --------------------------------------------------------------------------- #
# Constants / tunables
# --------------------------------------------------------------------------- #

# Must match the installed plugin folder name.
PLUGIN_KEY = "dispatcharr_vod_episode_sweep"

# Durable watchlist storage (its own CoreSettings row).
WATCHLIST_CORE_KEY = "dispatcharr_vod_episode_sweep_watchlist"
WATCHLIST_CORE_NAME = "Dispatcharr VOD Episode Sweep - watchlist"

# Beat PeriodicTask name + the plugin task's explicit registered name. The task
# is routed to a queue whose worker actually loads plugins (see DEFAULT_SCHEDULE_
# QUEUE) -- the prefork default worker's consumer would reject it as unregistered.
SWEEP_PERIODIC_NAME = "dispatcharr_vod_episode_sweep"
SWEEP_TASK_PATH = "dispatcharr_vod_episode_sweep.run_sweep"
# Dispatcharr's threads-pool worker (single process => its consumer imports
# plugins via worker_process_init, so it can run a plugin @shared_task).
DEFAULT_SCHEDULE_QUEUE = "dvr"

DEFAULT_SWEEP_HOUR = 3          # earliest local hour (system TZ) for the daily sweep
DEFAULT_TTL_DAYS = 30           # drop a watched series after this long unseen

# Throttle: split each account's watched series into chunks and space the chunk
# tasks out (Celery countdown) so provider get_series_info calls don't burst.
DEFAULT_BATCH_SIZE = 20         # series per background refresh task (0 = one/account)
DEFAULT_SPACING_SECONDS = 15.0  # delay added between successive chunk tasks (0 = none)

# A watched relation not episode-refreshed in this long is flagged as stale
# (likely a provider error last cycle). Sized just over the daily cadence.
STALE_HOURS = 25

# In-process: skip re-recording the same series to the DB more than once per
# this window (bounds writes during a client's burst of get_series_info calls).
OBSERVE_DEDUPE_SECONDS = 3600.0

CONFIG_TTL_SECONDS = 5.0        # in-process cache of the plugin settings read
MAX_WATCHLIST = 20000           # defensive cap on watchlist size

# Daily-tick coordination (cross-worker) key.
DAILY_PREFIX = "vodsweep:daily:"
DAILY_KEY_TTL = 129600          # 36h, so the day-key self-expires

# --------------------------------------------------------------------------- #
# Module state
# --------------------------------------------------------------------------- #

_ACTIVE = False
_orig_xc_get_series_info = None
_PATCH_TAG = "_vodsweep_patched"

# One-shot flag for the signature-drift sensor (see _log_unexpected_args_once).
_extra_args_logged = False

_pid_logged = set()

_observe_lock = threading.Lock()
_recent_observed = {}           # series_id-input -> monotonic deadline

_wl_lock = threading.Lock()
_wl_cache = None
_wl_cache_ts = 0.0

_cfg_lock = threading.Lock()
_cfg_cache = None
_cfg_cache_ts = 0.0

_daily_lock = threading.Lock()
_daily_checked_date = None      # per-worker: date we've already handled this run cycle
_ip_daily_date = None           # per-worker fallback claim when Redis is down


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _get_redis():
    try:
        from core.utils import RedisClient
        return RedisClient.get_client()
    except Exception:
        return None


def _log_pid_once(where: str) -> None:
    key = f"{where}:{os.getpid()}"
    if key not in _pid_logged:
        _pid_logged.add(key)
        logger.info("[VOD-SWEEP] active in worker pid=%s at %s", os.getpid(), where)


def _as_float(value, default):
    try:
        f = float(value)
        return f if f > 0 else default
    except (TypeError, ValueError):
        return default


def _debug_file_path() -> str:
    """Where the human-readable watchlist dump is written. Overridable in tests."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchlist_debug.json")


# --------------------------------------------------------------------------- #
# Timezone: use Dispatcharr's configured system TZ, not the container's clock
# --------------------------------------------------------------------------- #
# The daily-tick hour gate and the human-facing timestamps must reflect the
# timezone the user configured in Dispatcharr, NOT the container's process clock
# (often UTC). Otherwise "earliest sweep hour = 6" silently means 6 AM UTC.

_tz_lock = threading.Lock()
_tz_cache = None                # resolved ZoneInfo, or None to use process-local
_tz_cache_ts = 0.0
_tz_resolved = False


def _get_system_tzinfo():
    """Cached ZoneInfo for Dispatcharr's system timezone, or None (process-local)
    if it can't be resolved (no tz name, no tzdata, etc.)."""
    global _tz_cache, _tz_cache_ts, _tz_resolved
    now = time.time()
    with _tz_lock:
        if _tz_resolved and (now - _tz_cache_ts) < 300:
            return _tz_cache
    tz = None
    try:
        from core.models import CoreSettings
        tz_name = CoreSettings.get_system_time_zone()
        if tz_name:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name)
    except Exception:
        tz = None
    with _tz_lock:
        _tz_cache = tz
        _tz_cache_ts = time.time()
        _tz_resolved = True
    return tz


def _invalidate_tz_cache() -> None:
    global _tz_resolved, _tz_cache
    with _tz_lock:
        _tz_resolved = False
        _tz_cache = None


def _now_local():
    """Current datetime in Dispatcharr's system timezone (falls back to the
    process-local clock if the TZ can't be resolved)."""
    from datetime import datetime
    tz = _get_system_tzinfo()
    return datetime.now(tz) if tz is not None else datetime.now()


def _fmt_epoch(epoch) -> str:
    """Render a unix epoch as 'YYYY-MM-DD HH:MM:SS' in the system timezone."""
    from datetime import datetime
    tz = _get_system_tzinfo()
    try:
        if tz is not None:
            return datetime.fromtimestamp(float(epoch), tz).strftime("%Y-%m-%d %H:%M:%S")
        return datetime.fromtimestamp(float(epoch)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(epoch)


# --------------------------------------------------------------------------- #
# Settings (PluginConfig.settings, TTL-cached, read out-of-band)
# --------------------------------------------------------------------------- #

def _read_plugin_settings() -> dict:
    from apps.plugins.models import PluginConfig
    row = PluginConfig.objects.filter(key=PLUGIN_KEY).values("settings").first()
    if not row:
        return {}
    return row.get("settings") or {}


def _load_config(force: bool = False) -> dict:
    global _cfg_cache, _cfg_cache_ts
    now = time.time()
    with _cfg_lock:
        if not force and _cfg_cache is not None and (now - _cfg_cache_ts) < CONFIG_TTL_SECONDS:
            return _cfg_cache
    try:
        settings = _read_plugin_settings()
    except Exception as exc:
        logger.debug("[VOD-SWEEP] settings read failed (%s); using defaults", exc)
        settings = {}

    hour = settings.get("sweep_hour", DEFAULT_SWEEP_HOUR)
    try:
        hour = int(hour)
        if not (0 <= hour <= 23):
            hour = DEFAULT_SWEEP_HOUR
    except (TypeError, ValueError):
        hour = DEFAULT_SWEEP_HOUR

    try:
        batch_size = int(settings.get("sweep_batch_size", DEFAULT_BATCH_SIZE))
        if batch_size < 0:
            batch_size = DEFAULT_BATCH_SIZE
    except (TypeError, ValueError):
        batch_size = DEFAULT_BATCH_SIZE
    try:
        spacing = float(settings.get("sweep_spacing_seconds", DEFAULT_SPACING_SECONDS))
        if spacing < 0:
            spacing = DEFAULT_SPACING_SECONDS
    except (TypeError, ValueError):
        spacing = DEFAULT_SPACING_SECONDS

    scheduled = settings.get("scheduled_sweep", True)
    scheduled = scheduled if isinstance(scheduled, bool) else str(scheduled).lower() not in ("false", "0", "no", "off", "")
    queue = (settings.get("schedule_queue") or DEFAULT_SCHEDULE_QUEUE)
    queue = str(queue).strip() or DEFAULT_SCHEDULE_QUEUE

    cfg = {
        "sweep_hour": hour,
        "ttl_seconds": _as_float(settings.get("watchlist_ttl_days"), DEFAULT_TTL_DAYS) * 86400.0,
        "batch_size": batch_size,
        "spacing_seconds": spacing,
        "scheduled_sweep": scheduled,
        "schedule_queue": queue,
    }
    with _cfg_lock:
        _cfg_cache = cfg
        _cfg_cache_ts = time.time()
    return cfg


def invalidate_config_cache() -> None:
    global _cfg_cache, _cfg_cache_ts
    with _cfg_lock:
        _cfg_cache = None
        _cfg_cache_ts = 0.0
    _invalidate_tz_cache()


def is_enabled() -> bool:
    """Whether this plugin's PluginConfig row is enabled (None row -> False)."""
    try:
        from apps.plugins.models import PluginConfig
        row = PluginConfig.objects.filter(key=PLUGIN_KEY).values("enabled").first()
        return bool(row and row.get("enabled"))
    except Exception:
        return True


# --------------------------------------------------------------------------- #
# Watchlist storage (dedicated CoreSettings row)
# --------------------------------------------------------------------------- #
# Shape: {"series": {"<pk>": <last_seen_epoch>}, "last_sweep": {...}|None}

def _empty_watchlist() -> dict:
    return {"series": {}, "last_sweep": None}


def _read_watchlist_raw() -> dict:
    from core.models import CoreSettings
    row = CoreSettings.objects.filter(key=WATCHLIST_CORE_KEY).values("value").first()
    if not row:
        return _empty_watchlist()
    val = row.get("value") or {}
    if not isinstance(val, dict):
        return _empty_watchlist()
    series = val.get("series")
    if not isinstance(series, dict):
        series = {}
    return {"series": series, "last_sweep": val.get("last_sweep")}


def _mutate_watchlist(mutate) -> dict:
    """Apply mutate(data) to the watchlist row under a row lock; persist if the
    callback returns True (changed)."""
    from django.db import transaction
    from core.models import CoreSettings

    with transaction.atomic():
        row = CoreSettings.objects.select_for_update().filter(key=WATCHLIST_CORE_KEY).first()
        if row is None:
            data = _empty_watchlist()
            mutate(data)
            CoreSettings.objects.create(
                key=WATCHLIST_CORE_KEY, name=WATCHLIST_CORE_NAME, value=data,
            )
            _invalidate_watchlist_cache()
            return data
        data = row.value if isinstance(row.value, dict) else _empty_watchlist()
        if "series" not in data or not isinstance(data.get("series"), dict):
            data["series"] = {}
        changed = mutate(data)
        if changed:
            row.value = data
            row.save(update_fields=["value"])
            _invalidate_watchlist_cache()
    return data


def _invalidate_watchlist_cache() -> None:
    global _wl_cache, _wl_cache_ts
    with _wl_lock:
        _wl_cache = None
        _wl_cache_ts = 0.0


def get_watchlist(force: bool = False) -> dict:
    global _wl_cache, _wl_cache_ts
    now = time.time()
    with _wl_lock:
        if not force and _wl_cache is not None and (now - _wl_cache_ts) < CONFIG_TTL_SECONDS:
            return _wl_cache
    try:
        data = _read_watchlist_raw()
    except Exception as exc:
        logger.debug("[VOD-SWEEP] watchlist read failed (%s); treating as empty", exc)
        data = _empty_watchlist()
    with _wl_lock:
        _wl_cache = data
        _wl_cache_ts = time.time()
    return data


def record_series(series_pk) -> None:
    """Upsert a Series PK into the watchlist with last_seen=now."""
    key = str(series_pk)
    now = time.time()

    def _mut(data):
        series = data["series"]
        prev = series.get(key)
        if prev is not None and (now - float(prev)) < OBSERVE_DEDUPE_SECONDS:
            return False
        if key not in series and len(series) >= MAX_WATCHLIST:
            oldest = min(series, key=lambda k: series[k])
            series.pop(oldest, None)
        series[key] = now
        return True

    _mutate_watchlist(_mut)


def clear_watchlist() -> int:
    """Empty the watchlist. Returns the number of entries removed."""
    removed = {"n": 0}

    def _mut(data):
        removed["n"] = len(data.get("series", {}))
        if removed["n"] == 0 and data.get("last_sweep") is None:
            return False
        data["series"] = {}
        return True

    _mutate_watchlist(_mut)
    _write_debug_file()
    return removed["n"]


# --------------------------------------------------------------------------- #
# Observation: wrap xc_get_series_info
# --------------------------------------------------------------------------- #

def _resolve_series_pk(series_id):
    """series_id (an M3USeriesRelation.id) -> Series PK, for an active relation."""
    from apps.vod.models import M3USeriesRelation
    return (
        M3USeriesRelation.objects
        .filter(id=series_id, m3u_account__is_active=True)
        .values_list("series_id", flat=True)
        .first()
    )


def _observe(series_id) -> None:
    if series_id in (None, ""):
        return
    now = time.monotonic()
    with _observe_lock:
        deadline = _recent_observed.get(series_id)
        if deadline is not None and deadline > now:
            return
        _recent_observed[series_id] = now + OBSERVE_DEDUPE_SECONDS
        if len(_recent_observed) > 8192:
            for k in [k for k, d in _recent_observed.items() if d <= now]:
                _recent_observed.pop(k, None)
    try:
        series_pk = _resolve_series_pk(series_id)
        if series_pk is not None:
            record_series(series_pk)
            logger.debug("[VOD-SWEEP] watchlisted series pk=%s (via series_id=%s)",
                         series_pk, series_id)
    except Exception:
        logger.debug("[VOD-SWEEP] observe failed for series_id=%s (ignored)",
                     series_id, exc_info=True)


def _log_unexpected_args_once(args, kwargs) -> None:
    """One-shot sensor for upstream signature drift.

    Forwarding unknown parameters is what keeps the wrapper working -- but it
    also makes the change invisible, since nothing fails any more. So leave
    exactly one trace per process that a later compat check can find. Uses
    `warning` deliberately: it survives even where a pool has left root at
    WARNING, which is precisely where a plugin's INFO lines do not.
    """
    global _extra_args_logged
    if _extra_args_logged or not (args or kwargs):
        return
    _extra_args_logged = True
    logger.warning(
        "[VOD-SWEEP] xc_get_series_info was called with %s extra positional "
        "arg(s) and keyword(s) %s -- forwarded unchanged, so nothing is broken, "
        "but core's signature has changed: re-check this plugin against the "
        "current Dispatcharr release.",
        len(args), sorted(kwargs),
    )


def patched_xc_get_series_info(request, user, series_id, *args, **kwargs):
    """Observe the requested series + maybe fire the daily sweep, then return the
    native response unchanged.

    The trailing `*args, **kwargs` are load-bearing, not tidiness. If a release
    adds a parameter to core's `xc_get_series_info`, a fixed signature raises
    `TypeError` during ARGUMENT BINDING -- before this body runs, therefore
    before the `_ACTIVE` guard and before the `try` below, so neither can
    fail open. The XC router calls this inside `JsonResponse(...)` with no
    `try/except` of its own, so that `TypeError` would surface as an HTTP 500 on
    every series request and take the whole episode sync down. Accept anything,
    and forward it verbatim at EVERY call of the original -- the inactive path
    included, or disabling the feature would reintroduce the same crash.

    This wrapper is a pure observer: it always delegates and never alters what
    core returns, so forwarding is sufficient and there is nothing a new
    parameter could require us to honour on core's behalf.
    """
    if not _ACTIVE:
        return _orig_xc_get_series_info(request, user, series_id, *args, **kwargs)
    _log_pid_once("get_series_info")
    try:
        _log_unexpected_args_once(args, kwargs)
        _observe(series_id)
        _maybe_daily_sweep()
    except Exception:
        logger.debug("[VOD-SWEEP] observation hook error (ignored)", exc_info=True)
    return _orig_xc_get_series_info(request, user, series_id, *args, **kwargs)


# --------------------------------------------------------------------------- #
# Daily tick (opportunistic, Redis-locked, runs inline in the web worker)
# --------------------------------------------------------------------------- #

def _claim_daily(date_str) -> bool:
    """Atomically claim 'the sweep for <date_str>'. Returns True for exactly one
    caller per day (cross-worker via Redis; per-worker fallback if Redis down)."""
    global _ip_daily_date
    redis_client = _get_redis()
    if redis_client is not None:
        try:
            ok = redis_client.set(
                f"{DAILY_PREFIX}{date_str}", str(time.time()), nx=True, ex=DAILY_KEY_TTL
            )
            return bool(ok)
        except Exception:
            logger.debug("[VOD-SWEEP] redis daily claim failed; using in-process", exc_info=True)
    with _daily_lock:
        if _ip_daily_date == date_str:
            return False
        _ip_daily_date = date_str
        return True


def _spawn_sweep(reason="daily") -> None:
    t = threading.Thread(
        target=run_sweep_impl, name="vod-sweep", daemon=True,
    )
    t.start()
    logger.info("[VOD-SWEEP] %s sweep spawned", reason)


def _maybe_daily_sweep() -> None:
    """Cheap per-call gate: at most one Redis touch per worker per day. The first
    observation at/after the configured hour claims the day and runs the sweep."""
    global _daily_checked_date
    try:
        now = _now_local()
        today = now.strftime("%Y-%m-%d")
        hour = now.hour
        sweep_hour = int(_load_config()["sweep_hour"])
        with _daily_lock:
            if _daily_checked_date == today:
                return
            if hour < sweep_hour:
                return  # too early; re-check on a later call today (leave unmarked)
            _daily_checked_date = today
        if _claim_daily(today):
            logger.info("[VOD-SWEEP] daily tick fired (fallback) for %s (hour %s >= %s)",
                        today, hour, sweep_hour)
            _spawn_sweep(reason="tick")
    except Exception:
        logger.debug("[VOD-SWEEP] daily tick error (ignored)", exc_info=True)


@shared_task(name=SWEEP_TASK_PATH)
def run_sweep():
    """Beat entry point (primary trigger), dispatched to the plugin-capable queue.
    Dedups against the opportunistic tick via the shared daily Redis claim, so at
    most one sweep runs per day regardless of which path fired."""
    try:
        today = _now_local().strftime("%Y-%m-%d")
        if _claim_daily(today):
            logger.info("[VOD-SWEEP] scheduled sweep fired for %s", today)
            return run_sweep_impl()
        logger.info("[VOD-SWEEP] scheduled sweep for %s already handled; skipping", today)
        return {"skipped": "already ran today"}
    except Exception:
        logger.exception("[VOD-SWEEP] scheduled run_sweep error (ignored)")
        return {"skipped": "error"}


# --------------------------------------------------------------------------- #
# The sweep itself
# --------------------------------------------------------------------------- #

def _accounts_by_series(series_pks):
    """{account_id: [series_pk, ...]} for active XC accounts carrying each pk."""
    from apps.m3u.models import M3UAccount
    from apps.vod.models import M3USeriesRelation

    rels = M3USeriesRelation.objects.filter(
        series_id__in=series_pks,
        m3u_account__is_active=True,
        m3u_account__account_type=M3UAccount.Types.XC,
    ).values("m3u_account_id", "series_id")

    by_account = {}
    for r in rels:
        by_account.setdefault(r["m3u_account_id"], set()).add(r["series_id"])
    return {aid: sorted(pks) for aid, pks in by_account.items()}


def _stale_by_account(series_pks, hours=STALE_HOURS):
    """{account_name: count} of watched relations not episode-refreshed in >hours
    (None or old last_episode_refresh) -- i.e. likely provider failures last cycle
    (or not yet swept, on a first run). Uses last_episode_refresh, which -- unlike
    the episodes_fetched flag -- survives the listing scan's clobbering."""
    from datetime import timedelta
    from django.db.models import Q
    from django.utils import timezone as dj_tz
    from apps.m3u.models import M3UAccount
    from apps.vod.models import M3USeriesRelation

    cutoff = dj_tz.now() - timedelta(hours=hours)
    names = (
        M3USeriesRelation.objects.filter(
            series_id__in=series_pks,
            m3u_account__is_active=True,
            m3u_account__account_type=M3UAccount.Types.XC,
        )
        .filter(Q(last_episode_refresh__isnull=True) | Q(last_episode_refresh__lt=cutoff))
        .values_list("m3u_account__name", flat=True)
    )
    out = {}
    for name in names:
        out[name] = out.get(name, 0) + 1
    return out


def _series_names(series_pks):
    try:
        from apps.vod.models import Series
        return {
            row["id"]: row["name"]
            for row in Series.objects.filter(id__in=series_pks).values("id", "name")
        }
    except Exception:
        return {}


def _write_debug_file(last_sweep=None):
    try:
        data = get_watchlist(force=True)
        series = data.get("series", {})
        pks = [int(k) for k in series.keys() if str(k).isdigit()]
        names = _series_names(pks) if pks else {}
        entries = sorted(
            (
                {
                    "series_pk": int(k),
                    "name": names.get(int(k), "?"),
                    "last_seen_epoch": int(float(v)),
                    "last_seen": _fmt_epoch(v),
                }
                for k, v in series.items() if str(k).isdigit()
            ),
            key=lambda e: e["last_seen_epoch"],
            reverse=True,
        )
        payload = {
            "generated_at": _now_local().strftime("%Y-%m-%d %H:%M:%S"),
            "plugin": PLUGIN_KEY,
            "count": len(entries),
            "last_sweep": last_sweep if last_sweep is not None else data.get("last_sweep"),
            "watchlist": entries,
        }
        path = _debug_file_path()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        return path
    except Exception:
        logger.debug("[VOD-SWEEP] could not write debug file (ignored)", exc_info=True)
        return None


def _prune_expired(ttl_seconds):
    if ttl_seconds <= 0:
        return 0
    cutoff = time.time() - ttl_seconds
    pruned = {"n": 0}

    def _mut(data):
        series = data["series"]
        stale = [k for k, v in series.items() if float(v) < cutoff]
        for k in stale:
            series.pop(k, None)
        pruned["n"] = len(stale)
        return bool(stale)

    _mutate_watchlist(_mut)
    return pruned["n"]


def _record_sweep_result(summary) -> None:
    def _mut(data):
        data["last_sweep"] = summary
        return True
    _mutate_watchlist(_mut)


def run_sweep_impl() -> dict:
    """Refresh every relation of each watched series, per carrying XC account, by
    enqueuing the CORE batch_refresh_series_episodes task. Runs inline in the
    calling (web) worker. Fail-open; safe to call from anywhere."""
    from django.db import close_old_connections

    summary = {
        "at": _now_local().strftime("%Y-%m-%d %H:%M:%S"),
        "watched": 0, "accounts": 0, "tasks": 0, "pruned": 0,
        "spread_seconds": 0, "stale_total": 0, "stale_by_account": {}, "skipped": None,
    }
    try:
        if not is_enabled():
            summary["skipped"] = "plugin disabled"
            return summary

        cfg = _load_config(force=True)
        summary["pruned"] = _prune_expired(cfg["ttl_seconds"])

        data = get_watchlist(force=True)
        pks = [int(k) for k in data.get("series", {}).keys() if str(k).isdigit()]
        summary["watched"] = len(pks)
        if not pks:
            _record_sweep_result(summary)
            _write_debug_file(last_sweep=summary)
            logger.info("[VOD-SWEEP] sweep: watchlist empty, nothing to do")
            return summary

        # Health check: relations of watched series that didn't refresh last cycle
        # (surfaces a silently-failing provider before we fan out again).
        try:
            stale = _stale_by_account(pks)
            summary["stale_by_account"] = stale
            summary["stale_total"] = sum(stale.values())
            if summary["stale_total"]:
                logger.warning(
                    "[VOD-SWEEP] %s watched relation(s) not episode-refreshed in "
                    ">%sh (provider errors last cycle, or not yet swept): %s",
                    summary["stale_total"], STALE_HOURS, stale,
                )
        except Exception:
            logger.debug("[VOD-SWEEP] staleness audit failed (ignored)", exc_info=True)

        by_account = _accounts_by_series(pks)
        summary["accounts"] = len(by_account)

        batch_size = cfg["batch_size"]
        spacing = cfg["spacing_seconds"]

        from apps.vod.tasks import batch_refresh_series_episodes
        idx = 0  # global chunk index -> staggered countdown across all accounts
        for account_id, account_pks in by_account.items():
            if batch_size and batch_size > 0:
                chunks = [account_pks[i:i + batch_size]
                          for i in range(0, len(account_pks), batch_size)]
            else:
                chunks = [account_pks]
            for chunk in chunks:
                countdown = int(idx * spacing) if spacing > 0 else 0
                try:
                    batch_refresh_series_episodes.apply_async(
                        args=[account_id], kwargs={"series_ids": chunk},
                        countdown=countdown,
                    )
                    summary["tasks"] += 1
                    idx += 1
                except Exception as exc:
                    logger.warning("[VOD-SWEEP] enqueue failed account=%s: %s", account_id, exc)
            logger.info(
                "[VOD-SWEEP] queued %s task(s) for account=%s (%s watched series)",
                len(chunks), account_id, len(account_pks),
            )

        summary["spread_seconds"] = int((idx - 1) * spacing) if (idx and spacing > 0) else 0
        _record_sweep_result(summary)
        _write_debug_file(last_sweep=summary)
        logger.info(
            "[VOD-SWEEP] sweep done: %s series across %s account(s), %s task(s) "
            "spread over ~%ss, %s pruned, %s stale",
            summary["watched"], summary["accounts"], summary["tasks"],
            summary["spread_seconds"], summary["pruned"], summary["stale_total"],
        )
        return summary
    except Exception:
        logger.exception("[VOD-SWEEP] sweep failed (ignored)")
        return summary
    finally:
        try:
            close_old_connections()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Beat schedule (primary daily trigger, routed to the plugin-capable queue)
# --------------------------------------------------------------------------- #

def ensure_schedule() -> bool:
    """Create/update the daily beat PeriodicTask for `run_sweep`, routed to the
    plugin-capable queue -- or remove it if the scheduled sweep is disabled.
    Idempotent and fail-open. Returns True if the schedule is now present."""
    try:
        cfg = _load_config(force=True)
        if not cfg["scheduled_sweep"]:
            remove_schedule()
            logger.info("[VOD-SWEEP] auto-run daily sweep disabled (tick-only)")
            return False

        from core.scheduling import create_or_update_periodic_task
        hour = cfg["sweep_hour"]
        queue = cfg["schedule_queue"]
        task = create_or_update_periodic_task(
            task_name=SWEEP_PERIODIC_NAME,
            celery_task_path=SWEEP_TASK_PATH,
            cron_expression=f"0 {hour} * * *",
            enabled=True,
        )
        # Route to the queue whose worker actually loads plugins. Without this the
        # task lands on the default (prefork) queue and is rejected as unregistered.
        try:
            if task is not None and getattr(task, "queue", None) != queue:
                task.queue = queue
                task.save(update_fields=["queue"])
        except Exception as exc:
            logger.warning("[VOD-SWEEP] could not route schedule to queue '%s': %s", queue, exc)
        logger.info(
            "[VOD-SWEEP] scheduled daily sweep at %02d:00 (system TZ) on queue '%s'",
            hour, queue,
        )
        return True
    except Exception as exc:
        logger.warning("[VOD-SWEEP] could not create beat schedule: %s", exc)
        return False


def remove_schedule() -> bool:
    try:
        from core.scheduling import delete_periodic_task
        delete_periodic_task(SWEEP_PERIODIC_NAME)
        return True
    except Exception as exc:
        logger.debug("[VOD-SWEEP] schedule removal skipped: %s", exc)
        return False


# --------------------------------------------------------------------------- #
# Install / uninstall
# --------------------------------------------------------------------------- #

def install(manage_schedule=None) -> bool:
    """Install the observation monkeypatch (and register the plugin task, which
    happens simply by importing this module). Idempotent and reload-safe.

    manage_schedule: True -> also create/refresh the beat schedule (e.g. on
    enable); None -> only touch the schedule in the master process at import time
    (so a container restart re-establishes it without every worker racing on it).
    """
    global _orig_xc_get_series_info, _ACTIVE

    try:
        from apps.output import views as output_views
    except Exception as exc:
        logger.error("[VOD-SWEEP] could not import apps.output.views: %s", exc)
        return False

    if not hasattr(output_views, "xc_get_series_info"):
        logger.error("[VOD-SWEEP] views.xc_get_series_info missing -- not patching.")
        return False

    try:
        from apps.vod.tasks import batch_refresh_series_episodes  # noqa: F401
    except Exception as exc:
        logger.error("[VOD-SWEEP] batch_refresh_series_episodes not importable (%s).", exc)
        return False

    try:
        cur = output_views.xc_get_series_info
        if not getattr(cur, _PATCH_TAG, False):
            _orig_xc_get_series_info = cur
        setattr(patched_xc_get_series_info, _PATCH_TAG, True)
        output_views.xc_get_series_info = patched_xc_get_series_info
        _ACTIVE = True
        logger.info("[VOD-SWEEP] installed observation patch in worker pid=%s", os.getpid())
    except Exception as exc:
        logger.exception("[VOD-SWEEP] install failed: %s", exc)
        uninstall(remove_sched=False)
        return False

    # Create/refresh (or remove, per config) the beat schedule. On enable we do
    # it authoritatively; at import we do it only in the master process so a
    # restart re-establishes it without every worker racing on the same row.
    try:
        want = manage_schedule
        if want is None:
            try:
                from dispatcharr.app_initialization import should_skip_initialization
                want = not should_skip_initialization()
            except Exception:
                want = False
        if want:
            ensure_schedule()
    except Exception:
        logger.debug("[VOD-SWEEP] schedule setup skipped", exc_info=True)

    return True


def uninstall(remove_sched: bool = True) -> bool:
    global _ACTIVE
    _ACTIVE = False
    try:
        from apps.output import views as output_views
        if _orig_xc_get_series_info is not None and getattr(
            output_views.xc_get_series_info, _PATCH_TAG, False
        ):
            output_views.xc_get_series_info = _orig_xc_get_series_info
    except Exception as exc:
        logger.debug("[VOD-SWEEP] uninstall patch error: %s", exc)
    if remove_sched:
        remove_schedule()
    logger.info("[VOD-SWEEP] uninstalled patch in worker pid=%s", os.getpid())
    return True
