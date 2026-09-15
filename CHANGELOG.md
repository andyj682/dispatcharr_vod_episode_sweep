# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2026-09-15

Hardening only — **no functional change**. Both items close failure modes that
are invisible until they bite.

- **Signature-agnostic observation wrapper.** `patched_xc_get_series_info` now
  accepts `*args, **kwargs` and forwards them to core at every call site,
  including the inactive path. A parameter added to Dispatcharr's
  `xc_get_series_info` previously raised `TypeError` during argument binding —
  ahead of the plugin's own guard and `try`, so neither could fail open — and
  the XC router does not catch it, which would have meant an HTTP 500 on every
  series request rather than a quiet loss of function. Because the wrapper is a
  pure observer that always delegates, forwarding is sufficient; there is
  nothing a new parameter could require the plugin to honour on core's behalf.
  If extra parameters ever do arrive, the plugin logs one warning so the change
  leaves a trace instead of passing unnoticed.
- **Plugin logger gets its own level.** `plugins.*` is not in Dispatcharr's
  `LOGGING` config, so a plugin logger inherits root's effective level; Celery's
  prefork pool leaves root at `WARNING` in forked children, which silently
  discards plugin `INFO` records there. The logger now adopts the `apps` level
  (so `DISPATCHARR_LOG_LEVEL` still applies), guarded on `NOTSET` so a
  deliberately-set level is respected. This plugin's own code does not currently
  run in a prefork child, so this is insurance rather than a fix.

## [1.0.0] - 2026-08-27

First stable release. No functional changes from 0.4.0 — the version bump marks
the plugin as feature-complete and verified end-to-end on a live deployment.

Capabilities:
- **Self-building watchlist:** observes the series a client actually syncs (via
  `xc_get_series_info`) and records them to a durable `CoreSettings` row — no
  manual list. TTL-prunes series no longer synced.
- **Daily server-side episode sweep:** refreshes ALL of each watched series'
  provider/category relations (bypassing the 24h, browse-only gate), so new
  episodes on non-top relations — e.g. a separate 4K category — stop going
  missing over the XC API. Only the core `batch_refresh_series_episodes` task
  does the provider work.
- **Two triggers, deduped:** a beat schedule (routed to the plugins-capable
  worker) as the primary, timezone-correct daily trigger so new episodes appear
  in one client sync; an opportunistic `get_series_info` tick as a fallback. A
  shared Redis daily claim ensures at most one sweep per day.
- **Throttled** (configurable batch size + spacing) to spread provider calls,
  **fail-open** throughout, and reports a **per-account staleness summary** so a
  silently-failing provider is visible.
- Actions: status, list watched series, run sweep now, clear watchlist.
- `test_logic.py` — 54 checks. Composes with the sibling VOD plugins.

## [0.4.0] - 2026-08-26

### Added
- **Auto-run daily sweep (primary trigger).** A `django_celery_beat`
  `PeriodicTask` now runs the sweep on a fixed daily timer via a plugin
  `@shared_task` (`dispatcharr_vod_episode_sweep.run_sweep`), **routed to the
  `dvr` queue** — the threads-pool worker whose consumer actually loads plugins
  (the default prefork worker rejects plugin tasks). Because it fires on a timer
  independent of client traffic, it can run *before* the client's daily sync, so
  new episodes appear in **one** sync instead of two (the previous
  traffic-triggered design bumped `last_modified` after the triggering sync had
  already read the series list). New settings: **Auto-run daily sweep**
  (on by default) and **Schedule queue** (advanced, default `dvr`).

### Changed
- The opportunistic, traffic-triggered tick is now the **fallback** rather than
  the sole trigger; both share the same Redis daily claim
  (`SET NX vodsweep:daily:<date>`), so at most one sweep runs per day whichever
  path fires first. If beat/the `dvr` worker is unavailable, the tick still
  covers it (with the two-sync lag). The sweep hour now also drives the beat
  cron; set it a bit before your client's daily sync.

### Notes
- `test_logic.py` — 54 checks.

## [0.3.1] - 2026-08-24

### Fixed
- **Daily tick now respects Dispatcharr's configured system timezone**, not the
  container's process clock (usually UTC). Previously "Earliest sweep hour = 6"
  meant 6 AM *UTC*, so on a UTC container it silently mapped to a different local
  hour (e.g. 11 PM for a US Pacific user) — and evening syncs that landed before
  6 AM UTC never fired the tick. The hour/date gate (and the `last_sweep.at` /
  debug-file timestamps) are now computed via `CoreSettings.get_system_time_zone()`,
  cached, with a fallback to process-local time if the TZ can't be resolved.

### Notes
- `test_logic.py` — 45 checks.

## [0.3.0] - 2026-08-14

### Added
- **Sweep throttle.** New **Refresh batch size** (default 20) and **Refresh
  spacing (seconds)** (default 15) settings. The sweep now chunks each account's
  watched series and dispatches the refresh tasks with a staggered Celery
  `countdown`, spreading provider `get_series_info` calls into small spaced
  bursts instead of one flood. Set spacing to 0 to disable.
- **Per-account staleness / failure summary.** Each sweep audits watched
  relations not episode-refreshed in >25h (using `last_episode_refresh`, which
  survives the listing scan's clobbering of the `episodes_fetched` flag),
  grouped by account. A non-zero count logs a warning and is recorded as
  `stale_by_account` in `last_sweep` — so a silently-failing provider is visible.

### Notes
- Confirmed the watchlist is self-sustaining: the .strm generator gates on a
  single global `last_modified` high-water mark with no recency suppression, and
  this plugin stamps `last_modified` = wall-clock now, so swept series are always
  re-fetched and re-observed. Documented the invariant (never backdate
  `last_modified`) and why conditional-`last_modified` (efficiency) is deferred
  (it would reopen a retention gap).
- Decided against a watchlist "seed" action — a one-time generator full resync
  (`SmartSkipExisting = false`) populates the current scope exactly, and the loop
  maintains it.
- `test_logic.py` — 42 checks.

## [0.2.0] - 2026-08-14

### Changed
- **Triggering reworked to run inline instead of via a plugin Celery task.**
  Dispatcharr's default Celery worker is a prefork pool whose consumer (main
  process) never imports plugins, so a plugin `@shared_task` dispatched to it is
  rejected as `Received unregistered task` — which made both the beat schedule
  and the "Run now" `delay()` silently do nothing. Now:
  - **Run sweep now** executes `run_sweep_impl()` inline in the web worker and
    reports the summary directly.
  - The daily cadence is an **opportunistic, Redis-locked once-per-day tick**
    piggybacked on the observation hook (first `get_series_info` at/after the
    configured hour claims `vodsweep:daily:<date>` via `SET NX` and runs the
    sweep in a background thread; all other calls/workers no-op).
  - The sweep only ever enqueues the **core** `batch_refresh_series_episodes`
    task (which the default worker *does* register), so provider work runs
    normally in Celery.

### Removed
- The `django_celery_beat` schedule and the plugin `@shared_task`. Any v0.1.0
  beat `PeriodicTask` is deleted on upgrade/enable.

### Notes
- Settings field renamed to **Earliest sweep hour** (semantics: earliest local
  hour after which the daily sweep may fire, not an exact cron time).
- `test_logic.py` — 36 checks.

## [0.1.0] - 2026-08-13

### Added
- Initial release. Keeps the episode lists of synced series complete for
  incremental clients by refreshing all of each watched series' relations on a
  daily schedule.
- **Observation:** wraps `xc_get_series_info` to self-populate a durable
  watchlist (`CoreSettings` row) of the Series a client syncs — no manual list.
  In-process dedupe bounds writes during a client's request burst.
- **Scheduled sweep:** a plugin `@shared_task`
  (`dispatcharr_vod_episode_sweep.run_sweep`) registered in Celery workers via
  `worker_process_init`, scheduled daily through
  `core.scheduling.create_or_update_periodic_task`. Fans out
  `batch_refresh_series_episodes.delay(account_id, series_ids=[…])` per active XC
  account carrying the watched series (all relations, bypassing the 24h gate).
- Configurable **sweep hour** (default 03:00) and **watchlist TTL** (default
  30 days). TTL pruning drops series no longer synced.
- Actions: **status**, **list watched series**, **run sweep now**, **clear
  watchlist**. Writes a `watchlist_debug.json` in the plugin folder for quick
  inspection.
- Fail-open throughout; reload-safe monkeypatch; disabled/deleted plugin
  self-removes its schedule. Composes with the sibling VOD plugins (distinct
  hooks).
- `test_logic.py` — 34 checks, no Dispatcharr/Celery/DB required.
