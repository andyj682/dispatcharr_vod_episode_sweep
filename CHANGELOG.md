# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.1] - Unreleased

For Dispatcharr 0.34, which runs plugin action buttons in a background worker
and shows a timeout for any that take longer than about a minute while the
work carries on. Nothing changes on older releases.

### Changed

- **Run sweep now** and **Retry stale relations** are background actions on
  0.34+: the button returns at once, shows progress, and the result lands in
  the plugin's run history. Run sweep now's result is now the same compact
  line as Show status instead of the raw summary.

### Fixed

- **Only one sweep or manual retry runs at a time.** A second click while one
  is still deciding and queueing, or a click during the scheduled sweep, now
  returns "another sweep or retry is still running" instead of starting a
  duplicate that doubled the calls to every provider. The automatic retry
  passes aren't affected. If Redis is unavailable, runs go ahead unlocked, as
  before.

### Tests

- 425 → 445 checks: the run lock (a second run skipped and records nothing,
  released after success and after a failure, released only by its owner,
  fail-open without Redis, retry chain passes never blocked), progress reports
  (rising, and a failing reporter never touches the run), and the two buttons
  marked as background actions. Each new guard was mutation-checked.

## [1.2.0] - 2026-10-30

Refreshes what can actually have changed instead of everything, every day.
In practice most watched shows are not airing, so most of each day's provider
calls went to shows that could not have new episodes, while a flaky provider
left the ones that mattered stale. Works on Dispatcharr 0.32.0 and 0.33.0.

### Added

- **Prioritize refresh by airing status** (on by default). Each run refreshes
  what is *due*, in priority order: relations never refreshed and series new to
  the watchlist; anything due on an earlier run and still not refreshed; shows that
  are airing; shows whose provider reports a change; and a rotating slice of
  everything else (every show at least weekly by default, plus anything past
  its rotation). Turning it off restores the 1.1.x behavior exactly.
- **TMDB schedules (optional).** With a free TMDB key, "airing" follows each
  show's schedule: daily for 10 days after an episode airs, and from 3 days
  before the next one, so a returning show is ready for its premiere and a show
  on a break isn't refreshed daily. Lookups run in the background after the
  sweep, cached and re-checked every 2 days for airing shows, weekly for
  returning ones and monthly for ended ones; only TMDB ids are sent. TMDB ids
  that don't match the show (by first-air year and title) are ignored. Without
  a key, or for shows TMDB has nothing usable for, stored episode air dates
  decide. Falls back to Dispatcharr's `TMDB_API_KEY` environment variable.
- **Provider-change promotion.** A show whose provider listing reports a
  change since its last refresh is refreshed that day on that provider,
  whatever its airing status. This catches what no schedule shows: a new copy
  of an old show, backfilled seasons, fixed episodes. It reads what
  Dispatcharr's catalog refresh already stored, so it costs no provider calls.
  A provider that marks most of its listing changed at once is ignored for the
  run.
- **Series new to the watchlist** have all their relations refreshed on the
  next run (the client's own first sync refreshes only the top-priority copy).
- **Retry until done.** The automatic retry chain now keeps going until
  nothing is stale instead of stopping after two passes. A provider that stays
  unreachable (about 90 minutes, retried with a growing backoff) or makes no
  progress for two passes in a row is given up for the day, and the others
  carry on. A daily cap (12 passes by default) and a 20-hour limit bound it,
  and a newer sweep or Clear watchlist stops an older chain.
- **Quiet window for retries** (optional, off by default): a time range, such
  as your client's library sync, when automatic retries make no provider calls.
- **Update TMDB schedules** action, and new settings: Prioritize refresh by
  airing status, TMDB API key, Airing window, Rotation for other shows, Refresh
  when a provider reports a change, Fallback airing window, Max retry passes per
  day, Quiet window for retries. *Max retry passes per day* replaces *Retry passes*, which
  was a fixed count; the old setting is ignored.

### Changed

- **Watchlist expiry.** A series is now dropped only after the TTL **and** after
  the client has ignored 3 refreshes of it (or after 120 days unseen
  regardless). The TTL alone assumed every show was refreshed daily; with
  tiering an old show is refreshed weekly at best, and only when its top
  provider answers, so a time-only rule would have dropped shows still
  followed. A failed refresh never counts against a show.
- **"Stale" now means due and not yet refreshed.** A show that isn't due today
  never counts as stale. The age buckets count runs missed (`age(runs)`), not
  days elapsed. With no due set recorded yet, the 1.1.x definition applies.
- **Priority order** now applies to the sweep as well as retries, and across
  accounts: one provider's rotation work never queues ahead of another's new or
  airing shows. Retries also try airing shows before rotation ones.
- **Show status** adds TMDB coverage, the due set by tier and the retry
  chain's progress (`auto 4/12`, `next 10:42` or `done`, and any provider it is
  `WAITING` on or `GAVE UP` on), and names unreachable providers instead of
  showing their database id. To stay within the result toast it drops the
  dispatch spread, account counts, a retry's task count, the year, and
  default retry/queue/TTL settings.

### Tests

- 216 → 425 checks: tier assignment, the noise guard, TMDB schedule and
  sanity rules, priority and cross-account interleaving, carried-over work,
  the new stale definition, ignored-refresh expiry, TMDB lookups (budget,
  re-queue, rejected key, rate limit, key never logged), an end-to-end
  tiered sweep, and the retry chain (progress and unreachable give-up,
  backoff, cap, superseded and duplicate passes, quiet window including one
  crossing midnight, and a provider down all day end to end). Each new guard
  was mutation-checked.

## [1.1.1] - 2026-10-27

Required for Dispatcharr 0.33.0, and still works on earlier releases. Update
the plugin **before** upgrading Dispatcharr.

### Fixed

- **Dispatcharr 0.33.0: the watchlist recorded the wrong series.** 0.33.0
  changed the XC `series_id` from one provider's copy of a series (an
  `M3USeriesRelation` id) to the series itself (a `Series` id). The two id
  ranges overlap, so 1.1.0 did not fail. It quietly watched an unrelated series
  for each one the client synced, and the real ones aged out after the 30-day
  TTL. The plugin now detects the release and resolves the id the way that
  release means it. There is deliberately no "try both" fallback, since an id
  valid both ways would pick the wrong series. **Show status** reports the
  detected mode as `xc_ids=series` or `xc_ids=relation`. If 1.1.0 already ran
  on 0.33.0, run **Clear watchlist** once after updating, then have your client
  do a full resync.
- **The daily schedule could disappear after a plugin reload.** `stop()` used to
  delete the beat schedule on every call, and the re-import recreates it in
  only one web worker, so a reload handled elsewhere left no schedule until the
  next restart. `stop()` now keeps the row on reload, and on disable when
  Dispatcharr suspends and restores plugin schedules itself (0.33.0+). It still
  removes it on delete, and on disable on earlier releases.
- **The pre-flight provider check no longer fails closed on a code error.** A
  `TypeError` or `AttributeError` from core's XC client means its API changed,
  not that a provider is down. It used to skip every account, so sweeps and
  retries dispatched nothing. It now skips the check instead, with one warning.
- **Show status was cut off at the end.** The result toast shows less than the
  plugin assumed: ~420 characters fit and ~435 didn't. The message is now
  shorter, with a terser settings line, no blank lines and no repeated
  watched count. A typical day comes to ~340 characters and a bad day with 4
  providers to ~400, under a cap of 400. The auto-retry trigger now reads
  `auto, 1 left`.

### Tests

- 166 → 216 checks. New: the 0.33.0 id meaning with deliberately overlapping
  ids, per-release mode detection, stop-reason handling, code-error fail-open,
  the import-time schedule branch with `should_skip_initialization()`
  returning False, which runs in production on 0.32.0+ and was never exercised
  before, and a realistic bad day that must fit the toast unclipped. Each new
  guard was mutation-checked.

## [1.1.0] - 2026-10-05

Adds a targeted retry, and stops the sweep hammering a provider whose API is
down. Both come out of a live diagnosis where one provider's panel was
returning errors for every request while another failed a fraction of them at
random.

### Added

- **"Retry stale relations" action.** Re-refreshes only the relations currently
  flagged stale, rather than the whole watchlist. The stale set is normally a
  small slice of it, so this costs a fraction of a full sweep. Because a
  partially-failing provider misses a *different, random* slice each pass,
  repeating the retry shrinks the remainder geometrically. It reuses the
  sweep's batch-size and spacing throttle, and deliberately does not touch the
  daily claim, so it can never suppress the scheduled sweep.
- **Pre-flight provider check, used by both the sweep and the retry.** Each
  account's XC panel is probed once before fanning out; an account that is not
  answering is skipped. When a panel is down, `refresh_series_episodes`
  authenticates before every lookup, so without this a sweep produces one
  failed handshake per relation — hundreds of doomed requests at an already
  struggling provider, and hundreds of error lines.
  - The probe **fails open**: it retries, and only skips an account that fails
    every attempt. Wrongly skipping a healthy provider would cost it a whole
    day of refreshes, which is worse than attempting it and having most calls
    succeed.
  - It catches a *total* outage, not partial degradation — a provider failing
    some fraction of requests will usually pass, and should.
  - A skip is reported in `last_sweep` as `skipped_accounts` **and** logged as
    a warning. A quietly smaller sweep with a climbing stale count and no
    explanation is precisely the silent failure this plugin exists to surface.

- **Auto-retry after each sweep** (`Auto-retry stale relations`, on by default,
  with a companion `Retry passes`, default 2). After a sweep, the plugin
  re-attempts whatever is *still* stale — exactly what the manual action does —
  a bounded number of times.
  - Worth having because a provider that fails a share of its requests at
    random fails a **different** share next time. Those failures are
    independent, so each pass clears most of what the last one missed and the
    remainder shrinks quickly — a couple of passes turns a partial-failure
    backlog into a small remainder, for a small fraction of the sweep's own
    provider calls. How many passes are useful depends on your provider's
    failure rate; the status action reports the stale count so you can judge
    it.
  - Each pass waits out the previous pass's dispatch spread before auditing,
    because those tasks carry Celery countdowns and staleness means nothing
    until they have actually run — not merely been dispatched.
  - **The chain stops as soon as nothing is stale**, so a clean day costs one
    audit and no provider calls at all. It keeps going when relations are stale
    but every account was unreachable, since that is the case where waiting and
    trying again may recover. Passes are hard-capped regardless of the setting.
  - Like the scheduled sweep, the chain is dispatched to the plugin-capable
    queue; a plugin task sent to the default prefork worker is rejected
    outright. Whether a chain was armed is recorded in `last_sweep`, and each
    pass records its trigger, so "did the auto-retry actually run?" is
    answerable after the fact instead of only inferable from logs.
- **Live staleness in "Show status", with an age breakdown.** `last_sweep` is a
  snapshot taken *before* that sweep fanned out, so it can be a day old and
  describe a state that no longer exists — which makes it easy to misread the
  system entirely (a provider can have recovered hours ago with the stored
  number still showing its old backlog). Status now also audits staleness live,
  per account, and splits it by how many ~daily cycles each relation has been
  stale.
  - That age split is a **diagnostic, not decoration**. When a provider fails a
    fraction of its requests at random, the chance of the same relation missing
    N cycles running is `p**N` — so a relation stale for several cycles is very
    unlikely to be merely unlucky, and is a genuine dead-content candidate (a
    dropped title, or an `external_series_id` the provider no longer knows).
    Counting failures alone mixes those two populations together; this
    separates them.
- **Durable `last_retry` record**, so a retry is visible in status afterwards
  rather than only in the action response you happened to be looking at. It is
  stored separately from `last_sweep`: a retry is a repair, not a trigger, and
  must not overwrite the sweep's record.
  - The status message is kept deliberately compact, because the UI's result
    box clips a long one with no indication. The live audit comes first, the
    stored sweep/retry records carry totals only so their length does not grow
    with the number of accounts, and the whole message is capped with a visible
    marker so a future overrun announces itself instead of disappearing
    mid-field.

### Changed

- **"Stale" now means "not refreshed since the last sweep began", not "older
  than a fixed 25h".** The fixed window could not answer the question that
  matters — *did the last sweep refresh this?* With a roughly daily cadence a
  relation that fails at sweep time is still only ~24.5h past its previous
  success half an hour later, so it stayed invisible for another day. That made
  the auto-retry structurally unable to see the failures it exists to repair:
  it would run shortly after the sweep, find nothing, and stop. Caught in live
  testing, where a sweep left relations unrefreshed and the retry pass
  correctly reported "nothing stale" half an hour later.
  - The old window is kept as a **floor** — the cutoff is the later of the two
    — so that if sweeps stop running altogether everything still ages into
    staleness and the outage stays visible, instead of the metric freezing.
  - The sweep now records a machine-readable start time alongside its display
    timestamp, since parsing a localized string back would be fragile and
    ambiguous across DST.
  - ⚠ This changes what the stale numbers mean, so counts from before and
    after this release are not directly comparable.
- **The retry dispatches worst-stale first** — never-refreshed relations, then
  oldest. Chunks go out on an increasing countdown, so the longest-suffering
  relations are attempted earliest and are least likely to be missed if a pass
  is cut short or the provider degrades partway through.
- The staleness audit now has a single underlying query feeding both the health
  summary and the retry set, so the two definitions of "stale" cannot drift
  apart. Note the granularity: core's refresh task takes series ids, not
  relation ids, so retrying a stale series re-refreshes that series' other
  relations on the same account too.
- Provider error text is stripped of URLs before this plugin logs it.
  Dispatcharr's own XC errors embed the provider username and password in the
  failing URL; nothing logged by this plugin copies them.

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
