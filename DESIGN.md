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

## Pre-flight provider check and the stale retry (v1.1.0)

Two related additions, both prompted by watching a real provider fail.

**Why a pre-flight check earns its keep.** `refresh_series_episodes`
authenticates against the provider before *every* series lookup. So when a
panel is down, a sweep does not fail once — it fails once per relation.
Observed live: one provider's `player_api.php` returning an error for every
call produced ~800 failed handshakes in eight minutes, one per watched
relation, against a server that was already unwell. A single probe per account
before fanning out replaces all of that with one request.

The probe is **deliberately fail-open**, which is the opposite of how a gate
normally behaves and is worth stating plainly: it retries, and skips an account
only if every attempt fails. The asymmetry is the point — wrongly skipping a
healthy provider costs it a full day of refreshes, whereas wrongly sweeping an
unhealthy one just repeats what the plugin did before this existed. In doubt,
sweep.

It therefore catches a **total** outage and not partial degradation. A provider
failing some fraction of its requests at random will usually pass the probe,
and should: most of its refreshes will succeed. That case is what the retry
below is for.

A skip is recorded in `last_sweep.skipped_accounts` *and* logged as a warning,
because a silently smaller sweep — fewer accounts, a climbing stale count, no
error anywhere — is the exact failure mode this plugin was built to make
visible. A mechanism that quietly did less would be indistinguishable from one
that was working.

**Why retrying only the stale set works.** When a provider fails a fraction of
requests at random, each pass leaves a *different* slice unrefreshed. Measured
across three days on one such provider, the sets of failing series overlapped
only as much as chance predicts — i.e. failures were statistically independent
between passes. That is what makes a retry worth having: independent failures
compound, so a second pass clears most of what the first missed, and a third
most of the remainder. Had the same relations failed every time, retrying would
have been pointless and the right response would have been to rebuild the
provider's stored ids instead.

The retry reuses the sweep's chunking and spacing, so it throttles identically,
and it does not take the daily Redis claim — it is a repair tool, not a
trigger, and must never suppress the scheduled sweep.

**Staleness age separates two different problems.** Stale relations are not all
alike, and the distinction is quantitative rather than a matter of taste. If a
provider fails a proportion `p` of requests independently, the chance of one
relation missing `N` consecutive cycles is `p**N`. At an observed `p` of around
0.27 that is ~7% for two cycles and well under 1% for four — so with a backlog
in the low hundreds you would expect a handful of two-cycle stragglers by
chance and essentially none at four. A relation stale that long is therefore
almost certainly *not* unlucky: it is a title the provider dropped, or an
`external_series_id` it no longer recognises. Retrying it will never help; only
re-deriving the ids from a fresh listing scan will.

So the plugin reports stale relations bucketed by age in cycles, and the retry
dispatches the worst first. The bucket counts are the cheap way to tell a
flaky-panel backlog (nearly all one cycle, membership rotating between passes)
from genuine dead content (small, persistent, and stuck at high cycle counts) —
a far sharper separator than the raw failure count, which mixes both.

**Why status audits staleness live.** `last_sweep` is written from an audit
taken *before* that sweep fanned out, so it describes what the *previous* run
left behind and is frozen until the next one. A provider can recover, a whole
backlog can clear, and the stored figure will still show the old number for
hours. Status therefore reports a live audit alongside the historical record,
labelled as such — a stored number with no indication of its age is an
invitation to misdiagnose.

## Surviving an upstream signature change (v1.0.1)

"Fail-open" is the plugin's core safety property, but it has a precise limit
worth stating: it covers what happens *inside* the wrapper and nothing that
happens *before the wrapper is entered*. If a Dispatcharr release adds a
parameter to `xc_get_series_info`, a fixed-signature wrapper raises `TypeError`
while Python binds the arguments — ahead of the `_ACTIVE` guard and ahead of the
internal `try`, so neither can fail open. The XC router calls this handler inside
`JsonResponse(...)` with no `try/except` of its own, so that error would surface
as an **HTTP 500 on every series request**, taking the whole episode sync down
rather than quietly costing it a feature. The plugin's own enable/disable
*setting* could not rescue it either, since the failure precedes any setting
lookup; only fully disabling the plugin (which restores the original function
object) would.

So the wrapper accepts `*args, **kwargs` and forwards them verbatim at **every**
call of the original — including the inactive early-return, because otherwise
turning the feature off would reintroduce exactly the crash being guarded
against.

**Is forwarding enough here?** Yes, and the reason is specific to this wrapper's
shape rather than a general rule. Forwarding stops the crash but does not by
itself keep a plugin *correct*: a wrapper that replaces or reshapes core's
behaviour may find that a newly added parameter was something core enforced on
its behalf, which nothing then honours. This wrapper is a **pure observer** — it
records the series, optionally fires the daily tick, and returns core's own
result untouched on every path. It never substitutes its own return value and
never filters or reorders anything core produced. There is therefore nothing a
new parameter could oblige it to honour; passing it straight through leaves core
applying it exactly as before.

One consequence of hardening worth naming: forwarding makes such a change
*invisible*, because nothing fails any more. To avoid trading a loud break for
silent drift, the wrapper logs a single `warning` the first time it ever receives
parameters it does not recognise — enough to leave a trace for the next
compatibility check without adding per-request noise.

## Logging: the plugin logger sets its own level (v1.0.1)

Dispatcharr's `LOGGING` config names the loggers it manages (`apps`, `celery`,
`core.*`, `django.geventpool`, root) and gives each its own handler with
`propagate: False`. **`plugins.*` is not among them**, so a plugin logger has no
handler, no level, and inherits root's *effective* level. That is fine in uWSGI,
daphne and a Celery worker parent, where root is at INFO — but Celery's prefork
pool reconfigures root in each forked child and leaves it at **WARNING**, which
discards every plugin `INFO` record *at the logger*, before any handler sees it.
Core's own `apps.*` lines from the same process still appear, because that logger
has its own handler. The result is that a perfectly working plugin looks exactly
like an absent one.

This plugin's code does not currently run in a prefork child — the observer and
the inline sweep run in uWSGI, the scheduled task runs on the single-process
threads-pool worker, and the only thing sent to the prefork worker is core's own
`batch_refresh_series_episodes` — so this is insurance rather than a fix. It is
cheap insurance worth having, because the failure mode is invisible by
construction: you would only learn you needed it from an absence, and absences
are not noticed. The logger adopts the `apps` logger's effective level rather
than hard-coding `INFO`, so `DISPATCHARR_LOG_LEVEL` still applies, and the change
is guarded on `NOTSET` so anything that deliberately set a level keeps control.

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
