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

### What the XC `series_id` means depends on the release (v1.1.1)

Up to Dispatcharr 0.32.0, both `get_series` and `get_series_info` use the
`M3USeriesRelation` PK as the XC `series_id`, so the plugin translates it to its
Series. From 0.33.0 both use the **Series** PK, so the plugin only checks that
the series has an active relation.

Getting this wrong is silent, not a miss: relation PKs and Series PKs are
separate auto-increment ranges that overlap, so the old lookup on a new id
usually finds *some* relation, belonging to an unrelated series. That series
is what got watched, and the one the client synced aged out after the TTL.

There is no version API, so the release is **feature-detected**, once per
process: `Series` has a `language` field from 0.33.0, which shipped in the
same release as the id change. That is a proxy, not a cause, so a compat check
should confirm the two still travel together. Two deliberate choices:

- **No "try both" fallback.** An integer valid both ways would resolve to the
  wrong series in exactly the overlapping cases.
- **Unknown means observe nothing.** If the model can't be inspected, nothing
  is recorded, and detection is retried on the next call rather than cached.
  Missing a few observations is harmless; recording the wrong series is not.

`Show status` prints the detected mode (`xc_ids=series|relation|?`).

### Schedule ownership on stop (v1.1.1)

`Plugin.stop()` receives the reason in its context (`disable`, `delete`,
`reload`, on 0.32.0 and 0.33.0 alike) and only deletes the beat row where
nothing else would bring it back or remove it:

| reason  | row | why |
|---|---|---|
| `delete`  | removed | 0.32.0 core leaves plugin rows behind |
| `reload`  | kept | the re-import recreates it in only one uWSGI worker, so deleting it could leave no schedule until a restart |
| `disable` | kept on 0.33.0+, removed before | 0.33.0 core suspends and restores plugin schedules itself (detected via `PluginConfig.suspended_schedules`); deleting would leave it nothing to restore |
| other     | removed | the pre-1.1.1 behavior |

A kept row firing while the plugin is disabled is harmless: `run_sweep_impl`
checks the enabled flag first and no-ops.

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
  relations that have not been episode-refreshed since the previous sweep began
  (or never), grouped by account. It uses `last_episode_refresh` (a column)
  rather than the `episodes_fetched` flag, because the listing scan clobbers
  that flag on the majority of relations while the timestamp survives. A
  non-zero count logs a `[VOD-SWEEP] … not episode-refreshed since the previous
  sweep …` warning and is recorded as `stale_by_account` in `last_sweep`, so a
  silently-failing provider is visible rather than leaving relations stuck
  `fetched False` unnoticed.

  **Why the cutoff is the sweep time and not a fixed window.** This originally
  used a fixed 25h lookback, which cannot answer the question that actually
  matters — *did the last sweep refresh this?* With a roughly daily cadence, a
  relation that fails at 08:00 is still only ~24.5h past its previous success
  half an hour later, so it stayed invisible until the next day. That made a
  retry running shortly after a sweep structurally incapable of seeing the
  failures it exists to repair: it would audit, find nothing, and stop. The
  cutoff is now the later of the last sweep's start time and `now − 25h`. The
  sweep time makes failures visible the moment the sweep's tasks finish; the
  window is retained as a floor, because if sweeps stop running altogether the
  first cutoff would freeze and nothing would ever look stale again.

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

From v1.1.1 that includes a **code-level** error in the probe itself. A
`TypeError` or `AttributeError` means core's XC client or account API changed
(a renamed method, a new constructor argument), not that a panel is down;
counting it as "down" would skip *every* account and dispatch nothing, without
a word. Such an error now counts as "can't probe" → reachable, with one warning
per process naming it so the drift is still visible. Network and HTTP errors
remain the only things that count as down.

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

**Why the retry is automated, and why it is a chain rather than a loop.** The
daily sweep will always leave behind whatever share of requests the provider
happened to fail — every single day, unattended, with nobody watching the
number. Those relations are episodes a client cannot see for a full cycle,
which is the precise gap this plugin exists to close, so leaving the repair to
a human clicking a button only fixes it on days somebody looks.

The timing constraint shapes the implementation. A pass must not audit
staleness until the *previous* pass's refresh tasks have actually run, and
those tasks are dispatched with Celery countdowns spread over many minutes. So
each pass schedules the next with a countdown of that spread plus a settle
margin, rather than looping or sleeping — a loop would hold a worker thread for
half an hour, and auditing early would simply re-retry work still in flight.

The chain ends the moment nothing is stale, which makes the common case free:
on a healthy day the first pass performs one audit, finds nothing, and stops
without a single provider call. It deliberately does *not* stop when relations
are stale but every account was unreachable — that is exactly the situation
where waiting and trying again is the right move. Passes are hard-capped
independently of the setting so a misconfiguration cannot produce an unbounded
chain.

## Retry until done (v1.2.0)

1.1.x made a fixed two passes. On a bad provider day that left a tail for the
operator to clear by hand: five manual retries once, for a panel failing about
half its calls at random. Tiering made each day's work small enough to keep
going instead, so the chain now runs until nothing is stale, with three things
to stop it wasting calls:

- **Per-account give-up for the day.** An account unreachable at pre-flight
  three passes in a row is given up. Its retries are spaced by a growing
  backoff (30, then 60 minutes), so that takes about 90 minutes of real outage,
  not three quick passes; in between the chain doesn't probe it, and sleeps
  until it is due if nothing else is left. An account is also given up when
  two passes in a row clear under 10% of what was sent to it: logins working
  while every episode lookup fails, which the pre-flight can't see (2026-10-27,
  every `get_series_info` a 502). Two, because one pass can read low just by
  auditing before its tasks ran. A provider clearing half per pass never comes
  close. The other accounts carry on.
- **A daily cap** (default 12, at most 24) and a 20-hour age limit, so a chain
  never runs into the next day's.
- **An optional quiet window.** No pass starts inside it; one due inside it
  waits for its end without counting as a pass. Off by default, since not
  every setup has a client sync to protect.

The chain's state (passes run, what each pass sent per account, strikes,
backoffs, give-ups) is stored in the watchlist row and keyed to the sweep that
started it. Each queued pass carries that key, so a pass from a chain replaced
by a newer sweep (or by Clear watchlist) recognizes itself and stops rather
than running beside the new one, and a duplicate delivery is ignored.

The sweep's own dispatch is the chain's first observation: an account
unreachable at the sweep is already on its first strike, and what the sweep
sent is what the first pass scores. A **manual** retry ignores all of it and
tries every account, because the operator asked.

**Staleness age separates two different problems.** Stale relations are not all
alike, and the distinction is quantitative rather than a matter of taste. If a
provider fails a proportion `p` of requests independently, the chance of one
relation missing `N` consecutive cycles is `p**N`, which falls away steeply for
any plausible `p` — a couple of cycles is uncommon and several is rare. A
relation stale that long is therefore almost certainly *not* unlucky: it is a
title the provider dropped, or an `external_series_id` it no longer recognises.
Retrying it will never help; only re-deriving the ids from a fresh listing scan
will. Where the cut-off falls depends on your own provider's failure rate,
which is why the buckets are reported rather than the plugin judging for you.

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

## Tiered refresh (v1.2.0)

Refreshing every relation of every watched series daily is simple, but most
watched shows are not airing, so nearly all of that work goes to shows that
cannot have changed. Meanwhile a flaky provider leaves relations stale that the
retries cannot finish. 1.2.0 refreshes what is **due**, per (account, series)
pair, in priority order:

| Tier | Rule | Cadence |
|---|---|---|
| `n` new | a relation never refreshed, or a series new to the watchlist | next run |
| `r` carried | due on an earlier run and still not refreshed | until done |
| `h` hot | airing (below) | daily |
| `p` promoted | the provider's own `last_modified` is newer than our last refresh of that relation | that day, that account |
| `c` cold | everything else: `series_pk % R == day % R`, or the oldest refresh is older than `R + 2` days | every `R` days (default 7) |

**Three signals, in order of authority.**

1. **TMDB's schedule** (primary, optional key). Hot when an episode aired
   within `A` days (default 10, covering providers that post late) or the next
   one airs within 3. TMDB knows the schedule rather than recent history, so a
   show on a break is not hot and becomes hot just before it returns, however
   long the break. A cached "next episode" that has since passed still counts,
   so the cache lagging the premiere can't drop a show.
2. **The provider's listing date** (promotion). Dispatcharr's catalog refresh
   stores each provider's raw `get_series` entry on the relation
   (`custom_properties['basic_data']`, present on 0.32.0 and 0.33.0), including
   the provider's XC `last_modified`. It is read as a JSON key transform, so the
   rest of the blob never leaves the database. This is an **independent
   override that TMDB never vetoes**: TMDB knows when a show airs in the world,
   the provider date knows when *this provider* has it — a later release in
   another region, a late posting, a new copy of an old show, backfilled
   seasons. The two fail differently, which is why the provider date may
   override: a provider stamping its whole catalog costs one extra refresh of
   that provider for a day (exactly the pre-1.2.0 daily cost), and is caught by
   a noise guard (more than 25% of a provider's watched relations, and at
   least 20, changed at once: ignored for the run); a wrong TMDB answer costs
   missed episodes and nothing else would notice. Junk dates (before 2000,
   millisecond values, far future) are ignored.
3. **Stored dates** (fallback, for shows without usable TMDB data): hot while
   `Episode.air_date` is within `H` days (default 14) or in the future, or an
   episode was first seen (`Episode.created_at`) within `H` days. "First seen"
   is only trustworthy over short windows, because most stored episodes were
   first fetched when this plugin began refreshing everything.

**TMDB sanity check.** Provider-supplied TMDB ids are sometimes wrong. An entry
is rejected when its first-air year differs from `Series.year` by more than one,
or the titles share no significant word (provider decoration such as a language
prefix, year, region tag or "4K" is stripped first). A rejected entry sends the
series to the stored-date fallback, never to "not airing".

**Stale, redefined.** With tiers, most relations are deliberately left alone on
most days, so "not refreshed since the sweep began" would flag every cold show.
The sweep records its due set (with its start time) and the carried-over counts
alongside `last_sweep`; stale means *in that due set and not refreshed since*,
plus anything never refreshed. The age buckets count runs missed, not days. With
no due set recorded (before the first 1.2.0 sweep) the 1.1.x definition applies,
so an upgrade changes nothing until the first run.

**Priority** applies to the sweep and to retries, and chunks are interleaved
across accounts by the rank of their first series, so one account's rotation
work never queues ahead of another's new or airing shows. Under purely random
provider failure order changes no single request's odds; it pays off when a
pass is cut short.

**Unreachable providers** keep their pairs in the due set, so they read as stale
and are carried into the next run; a new series whose only provider was
unreachable stays queued as new.

**TMDB lookups** run as a plugin task on the plugin-capable queue, queued
after the sweep's dispatch so a slow TMDB never delays a refresh. Each run looks
up at most 200 ids (throttled), re-queues itself while more are due, stops at
1,500 a day, and stops at once on a rejected key (shown in status as `BAD KEY`)
or a rate limit. Entries are re-checked every 2 days for hot or soon-premiering
shows, weekly for returning ones, monthly for ended or not-found ones, and the
next day after a failed lookup. The sweep tiers on the cache from earlier runs.
Error text is scrubbed of the API key before logging, since `requests` puts the
full URL into its errors. TMDB decides **cadence only**: it never adds or
removes a watched series.

## Watchlist expiry (v1.2.0)

Membership is maintained by observation, and observation depends on bumps: a
refresh saves the relation, `updated_at` (= XC `last_modified`) moves, the
client's incremental sync notices and calls `get_series_info`, and the plugin
re-stamps the series. With a daily refresh of everything, a 30-day TTL was
never close. With tiering, three facts make a time-only rule unsafe:

- **Only one relation is visible.** `get_series` reports `last_modified` from
  the active relation with the highest account priority (lowest id first) —
  `_xc_fetch_priority_distinct_relations`. Refreshes of other copies are
  invisible to the client.
- **A failed refresh bumps nothing.** `refresh_series_episodes` only saves the
  relation when the provider call succeeds, and the catalog refresh's
  `bulk_update` leaves `updated_at` alone, so this plugin's refresh is the only
  routine bumper.
- **The client never re-syncs on a schedule** — it fetches only on change.

So a cold show whose top provider is flaky could go weeks without a visible
bump and silently age out while still followed.

The rule now asks the question the TTL was standing in for — *has the client
stopped caring?* — using the evidence for it: **the client was given a reason
to re-sync and didn't.** A *chance* is the visible `last_modified` moving past
the series' last observation, with 2 days' grace for the client to react; each
distinct bump counts once, and failed refreshes never count. Observation clears
the count. A series expires only when it has ignored 3 chances **and** is past
the TTL (so nothing expires faster than before), or after 120 days unseen
regardless (or the TTL, if longer) for a series whose top provider can never
deliver a bump. The rotation period is capped at 28 days, well under that
ceiling. The `series` map's format is unchanged; the counts live in a separate
`chances` map, so 1.1.x still reads the row.

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
retention. (This is also why suppressing bumps is *not* free: a series that is
bumped less often is re-observed less often. v1.2.0's tiered refresh does
exactly that for shows that aren't airing, which is why it ships together with
the ignored-chances expiry rule above rather than relying on the TTL.)

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
