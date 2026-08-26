# Dispatcharr VOD Episode Sweep — design

## Problem

Dispatcharr materialises VOD **episode** streams lazily and **per-relation**
(per provider *and* per category), 24h-gated, and only for the one relation a
client browses via XC `get_series_info`. Crucially, `get_series` advertises just
**one** relation per logical series — the highest-priority account's
(`_xc_fetch_priority_distinct_relations`, `apps/output/views.py`). So an
incremental client (e.g. an Emby `.strm` generator) that syncs on `last_modified`
only ever refreshes that **top** relation.

Consequence: a new episode that first appears on a *different* relation — a
second provider, or the same provider's separate "… 4K" **category** (a distinct
relation with its own `external_series_id`) — is never fetched, so it never
materialises and never appears over XC. The XC API structurally hides the other
relations, so **no client-side XC solution can reach them**. And nothing on the
Dispatcharr side refreshes episodes on a schedule (verified: no
`CELERY_BEAT_SCHEDULE` entry; `batch_refresh_series_episodes` has no caller).

## Fix

Keep a small **watchlist** of the series a client actually syncs, and once a day
refresh **all** relations of each watched series:

```python
batch_refresh_series_episodes.delay(account_id, series_ids=[…])   # per XC account
```

`series_ids` forces every one of that account's relations for the series and
bypasses the 24h gate (`apps/vod/tasks.py::batch_refresh_series_episodes`), so
the whole candidate set is warmed across providers and categories. This is the
one thing a client can't do itself, and it must run server-side because only
server code can enumerate relations.

Because the refresh also bumps each relation's `updated_at` (the value XC
surfaces as `last_modified`), the client's **normal incremental sync** then sees
the series as changed and re-pulls the newly-materialised episodes — **no client
change required.**

## Building the watchlist by observation

Rather than maintaining a separate curated list, the plugin **observes the
client's own behaviour**: it wraps `xc_get_series_info` (the call a client makes
per series it syncs) and records the resolved **Series PK** into a durable
`CoreSettings` row.

Why this is self-seeding and self-sustaining — and free of the feedback loop
you'd worry about:

1. **Seeding is guaranteed.** A client cannot emit episodes for a series without
   calling `get_series_info` for it at least once. So every synced series is
   observed at first sync / when added.
2. **The sweep sustains observability.** The nightly all-relation refresh bumps
   `last_modified` every night, so the client keeps seeing each watched series as
   changed → keeps calling `get_series_info` → keeps re-observing it. Once on the
   list, a series stays on the list.

The only requirement is a TTL longer than the client's sync interval; the default
is **30 days**, which ages out only series that are genuinely no longer synced.

## Triggering — two paths, deduped by a shared daily claim

The prefork-worker constraint (learned the hard way, verified live): Dispatcharr's
**default** Celery worker is a **prefork** pool (`--autoscale`). Plugins are
imported into prefork *child* processes via `worker_process_init`, but the
worker's **consumer runs in the main process**, which validates the task name on
receipt and does NOT import plugins — so a plugin `@shared_task` dispatched there
is rejected (`Received unregistered task … 'dispatcharr_vod_episode_sweep.run_sweep'`
→ `KeyError`). The **threads-pool `dvr` worker** is single-process, so *its*
consumer imports plugins and CAN run a plugin task. **Core** tasks
(`batch_refresh_series_episodes`) are registered normally on the default worker.

So the sweep has **two triggers**, and a shared Redis claim
(`SET NX vodsweep:daily:<YYYY-MM-DD>`, 36h TTL) ensures at most one runs per day —
whichever fires first wins, the other no-ops:

1. **Scheduled (primary):** a beat `PeriodicTask` runs the plugin `@shared_task`
   `run_sweep` at the configured hour, **routed to the `dvr` queue** (the one
   worker that can run it, via `PeriodicTask.queue`). Because it's a fixed timer
   independent of client traffic, it can run *before* the client's daily sync —
   so new episodes surface in **one** sync instead of two. Toggle via the
   "Auto-run daily sweep" setting; the queue is overridable via "Schedule queue"
   (defaults to `dvr`).
2. **Opportunistic tick (fallback):** the observation hook, on the first
   `get_series_info` at/after the hour, claims the day and runs the sweep in a
   background thread. Covers beat/`dvr` being unavailable — fails safe to the
   traffic-triggered behaviour (which has the two-sync lag but still works). A
   per-worker in-process guard bounds it to one Redis touch per worker per day.

Either path calls `run_sweep_impl()`, which only ever enqueues the CORE
`batch_refresh_series_episodes` task (registered on the default worker), so the
provider work always runs normally regardless of which worker triggered it.
`run_sweep_impl` guards on `PluginConfig.enabled`. Manual **Run now** also calls
`run_sweep_impl()` inline in the web worker.

**Why one sync vs two:** the fallback tick is triggered *by* a sync and bumps
`last_modified` *after* that sync already read the `get_series` list, so the new
episode is only picked up on the *next* sync (two-sync lag). The scheduled
trigger runs on its own timer, so if it's set before the daily sync,
`last_modified` is already bumped when the sync reads the list — one sync. Set
the sweep hour a bit before your client's sync.

## Properties

- **Scoped:** only watched series are refreshed → bounded provider API volume,
  not the whole VOD library.
- **Background / never blocks:** observation returns the native `get_series_info`
  response unchanged; the refresh runs in Celery.
- **Fail-open:** any failure to observe, resolve, schedule, or enqueue is
  swallowed and logged; XC responses are never affected.
- **Composes:** hooks `xc_get_series_info`, a different function from
  `dispatcharr_vod_preferences` (`_get_content_and_relation`) and the reactive
  `dispatcharr_vod_episode_refresh` (`stream_vod`). No collisions.

## Throttle & health (v0.3.0)

- **Throttle.** The sweep splits each account's watched series into chunks of
  **Refresh batch size** and dispatches the chunk tasks with a staggered Celery
  `countdown` (**Refresh spacing** seconds × global chunk index), so provider
  `get_series_info` calls arrive in small spaced bursts instead of one flood.
  Set spacing to 0 to fire everything at once. This only shapes *our* provider
  load; the core `batch_refresh_series_episodes` task still does the work.
- **Health / failure summary.** Before fanning out, the sweep audits watched
  relations whose `last_episode_refresh` is older than 25h (or null) — i.e. ones
  that didn't refresh last cycle — grouped by account. It uses
  `last_episode_refresh` (a column) rather than the `episodes_fetched` flag,
  because the listing scan clobbers that flag on the majority of relations while
  the timestamp survives. A non-zero count logs a `[VOD-SWEEP] … not
  episode-refreshed in >25h …` warning and is recorded as `stale_by_account` in
  `last_sweep`, so a silently-failing provider is visible rather than leaving
  relations stuck `fetched False` unnoticed.

## Note: the self-sustaining loop and the `last_modified` invariant

The watchlist is self-sustaining without periodic re-seeding, given how the .strm
generator syncs: it gates fetches on a **single global high-water mark** of
`last_modified` across all series (not a per-series baseline) with **no recency
suppression**. Because this plugin's refresh stamps `last_modified` with
wall-clock **now** (`auto_now` `updated_at`), a swept series always jumps above
that watermark, so the generator re-fetches it, which re-fires observation and
keeps it on the watchlist. **Invariant to preserve:** never stamp `last_modified`
with a backdated value (a provider timestamp, an air date) — a global watermark
cannot see a change beneath its own ceiling, which would silently break
retention. (This is also why the deferred "conditional `last_modified`"
efficiency change is *not* free: suppressing the bump on unchanged series would
stop them being re-observed and let them age out — trading churn for a coverage
hole.)

## Debug visibility

Each sweep (and each watchlist change) writes `watchlist_debug.json` in the
plugin's own folder (under `data/plugins/dispatcharr_vod_episode_sweep/`) with
the watched series, their names, last-seen timestamps, and the last sweep's
summary. The **List watched series** action shows the same and prints the file
path. **Run sweep now** runs the sweep inline immediately. (The file is written
by a sweep — it won't exist until the first sweep or a `Clear watchlist`.)

## Relationship to the other plugins

- **`dispatcharr_vod_preferences`** picks *among* candidates; this plugin makes
  the candidate set *complete* for curated series. Complementary.
- **`dispatcharr_vod_episode_refresh`** (reactive, on play) warms candidates when
  any client plays any title; this plugin proactively warms the curated set on a
  schedule for the generator's discovery need. Complementary, not redundant.
- Retiring the generator's own periodic refresh is safe once this is live: that
  refresh could only ever hit the top relation (XC limitation), which this
  plugin supersedes.
