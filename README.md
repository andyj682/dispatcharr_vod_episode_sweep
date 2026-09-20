# Dispatcharr VOD Episode Sweep (plugin)

Plugin for Dispatcharr that keeps the episode lists of the series you actually
sync **complete**, so an incremental client (e.g. an Emby `.strm` generator)
reliably learns about new episodes — including ones that first appear on a
provider/category the client can't see over the XC API.

## Why

This is specific to TV **episodes** — movies are fine. Dispatcharr's scheduled
VOD scan ingests each *movie's* provider relations directly, so a new movie (or a
separate 4K copy) appears on the next refresh without anyone browsing. *Episodes*
are the exception: Dispatcharr fetches them lazily and per relation — per provider
and per category — 24h-gated, and only for the relation a client actually browses.

Over the XC API most of that is invisible. `get_series` collapses each show to a
single entry — the highest-priority relation — merging the providers and
categories behind it, so only that relation's episodes ever surface. An episode
that debuts on a second provider, or the same provider's separate "… 4K"
category, sits on a relation no XC call ever addresses, so it's never fetched and
never appears. (A client using Dispatcharr's *native* API could force-refresh a
specific relation; nothing on the XC surface can.)

And the signal a client would sync on is unreliable:

- `last_modified` means "last refreshed," not "last changed." A refresh bumps it
  whether or not anything changed, while a genuine new episode produces no bump
  at all until something refreshes the relation it's on.
- `get_series_info` is 24h-gated: it refreshes synchronously and returns fresh
  data, but only when the gate is open — a call within 24 hours of the last
  refresh returns the cached contents unchanged.
- Dispatcharr's periodic VOD refresh is catalogue-level: it re-ingests titles and
  metadata, not episodes.

Together these set a trap for well-behaved clients. Once a client has synced
everything, it has no reason to call `get_series_info` again, so nothing
refreshes, so `last_modified` never moves, so it never calls again. The more
efficient the client, the staler its data — until episode discovery quietly stops.

This plugin:

1. **Learns which series you sync** by observing the `get_series_info` calls your
   client already makes (no manual list to maintain).
2. **Once a day, refreshes every relation** of each watched series — across all
   providers and categories, bypassing the 24h gate — so newly-added episodes
   materialise.
3. That refresh also bumps each series' `last_modified`, so your client's normal
   incremental sync notices and pulls the new episodes. **No client changes
   needed.**

Scoped to only the series you sync, so provider API volume stays bounded. Runs
in the background; never blocks or alters an XC response.

*Targets current Dispatcharr's Xtream-Codes VOD API. If the internals it patches
change, it falls back to native behavior (no sweep) rather than affecting
responses.* See `DESIGN.md` for the full rationale and `patch.py` for the code.

---

## Install

### Option A — Import via the UI (recommended)
1. Download `dispatcharr_vod_episode_sweep.zip` from the
   [latest release](https://github.com/andyj682/dispatcharr_vod_episode_sweep/releases/latest).
2. Dispatcharr UI → **Plugins** → **Import** → upload the zip.
3. Toggle the plugin **enabled** (accept the trust warning — plugins run
   server-side code).
4. **Restart the Dispatcharr container** so every uWSGI worker applies the
   observation patch (lazy-apps; see below).

### Option B — Drop-in folder (from source)
1. Copy this repo into `data/plugins/dispatcharr_vod_episode_sweep/` on the host
   (→ `/app/data/plugins/…` in the container). The folder must be named
   `dispatcharr_vod_episode_sweep` and contain `plugin.json` + `plugin.py`.
2. UI → **Plugins** → **reload**, enable, then **restart the container**.

### Why restart?
Dispatcharr runs several uWSGI workers with `lazy-apps = true`. Each imports an
*enabled* plugin's code at boot and applies the observation patch then. Enabling
without a restart only reliably patches the worker that handled the enable
request; a restart patches all of them. (Both the daily sweep and the manual
button run *inline* in these web workers and only enqueue Dispatcharr's own core
refresh task — so there's no separate Celery/beat setup to worry about.)

---

## Settings

| Field | Default | Meaning |
|---|---|---|
| **Earliest sweep hour (0-23)** | 3 | Hour the daily sweep runs, in **Dispatcharr's configured system timezone** (not the container's UTC clock). With Auto-run daily sweep on, a beat timer fires it at this hour; otherwise it fires on the first client series request at/after it. Set it a bit *before* your client's daily sync. |
| **Watchlist TTL (days)** | 30 | Forget a series if it hasn't been requested for this long. Keep it comfortably longer than your client's sync interval. |
| **Auto-run daily sweep** | on | Run the sweep on a fixed daily timer (Celery beat) rather than waiting for client traffic — so it runs *before* your sync and new episodes appear in **one** sync instead of two. Off = opportunistic (traffic-triggered) only. |
| **Auto-retry stale relations** | on | After each sweep, automatically re-refresh anything still flagged stale — the same work the Retry action does. Useful when a provider intermittently fails requests, since a later pass may succeed where the first didn't. Costs nothing on a clean day: the chain stops as soon as nothing is stale. |
| **Retry passes** | 2 | How many automatic passes to make. Each one only touches what is still stale, so passes get rapidly cheaper. 0 turns auto-retry off. |
| **Schedule queue (advanced)** | `dvr` | The Celery queue the scheduled sweep is dispatched to; must be served by a worker that loads plugins (stock Dispatcharr = the threads-pool `dvr` worker). Change only if your worker layout differs. |
| **Refresh batch size** | 20 | Series per background refresh task — smaller = smaller provider bursts. 0 = one task per account. |
| **Refresh spacing (seconds)** | 15 | Delay between successive refresh batches (Celery countdown), to spread provider calls. 0 = all at once. |

The watchlist builds itself from observed `get_series_info` calls — no manual
list. Only Xtream-Codes VOD series are affected.

---

## Actions

- **Show status** — patch active?, schedule, watchlist size, plus a **live**
  staleness audit (per account, and split by how many ~daily cycles each
  relation has been stale) alongside the stored `last_sweep` / `last_retry`
  records. Note the stored ones are snapshots taken *before* their run fanned
  out, so they describe the previous cycle; the live figure is what reflects
  now. Relations stale for 3+ cycles are unlikely to be bad luck — those are
  the dead-content candidates.
- **List watched series** — the current watchlist + the path to the debug file.
- **Run sweep now** — run the sweep inline immediately, ignoring the daily timer.
- **Retry stale relations** — re-refresh *only* the relations still flagged
  stale, instead of the whole watchlist. Much cheaper than a full sweep (the
  stale set is normally a small slice of it), and safe to repeat. Use it after
  a provider has recovered from an outage, or when a provider fails a fraction
  of its requests: because those failures are independent, each pass clears
  most of what the last one missed.
- **Clear watchlist** — forget everything (it rebuilds as clients sync).

### Debug file
Each sweep / watchlist change writes `watchlist_debug.json` in the plugin's
folder (`data/plugins/dispatcharr_vod_episode_sweep/`) with the watched series,
names, last-seen times, and last sweep summary — for quick inspection.

---

## Verifying it works

1. Enable + restart. Play/sync as usual so your client calls `get_series_info`
   for its series. Click **List watched series** — you should see them appear.
2. Click **Run sweep now**, then watch the logs:
   ```
   [VOD-SWEEP] enqueued refresh account=<A> for <N> watched series
   [VOD-SWEEP] sweep done: <N> series across <M> account(s), <M> task(s) enqueued
   ```
   followed by the native `Batch refreshing episodes …` / `Batch episode refresh
   completed` lines from the Celery worker (the core refresh task).
3. After it completes, a previously-missing episode (e.g. a 4K-category one on a
   non-top provider) should now be present via `get_series_info`, and your
   client's next incremental sync should pick it up (its `last_modified` moved).

Grep helper (adjust to your log access):
```bash
docker logs <dispatcharr-container> 2>&1 | grep -E "VOD-SWEEP|Batch (refreshing|episode)"
```

### Local logic test
```bash
python test_logic.py
```
Exercises observation/dedupe, per-account enqueue, TTL pruning, the disabled
no-op, schedule create/remove, reload-safety, and clear — with a fake clock. No
Dispatcharr, Celery, or DB required.

---

## Notes / limitations

1. **Eventual consistency.** The sweep runs daily (or on demand); new episodes
   surface on the client's next sync after a sweep, not instantly.
2. **Coverage.** Xtream-Codes VOD **series** only. Movies are refreshed eagerly
   by Dispatcharr's listing scan, so they're not needed here. Live TV/EPG/DVR
   untouched.
3. **Provider API budget.** Each watched series costs ~one `get_series_info`
   call per relation per account, once per sweep. Scoped to watched series and
   run at most daily — bounded. If a provider rate-limits you, lower **Refresh
   batch size** and/or raise **Refresh spacing** to spread the calls out.
4. **Provider health.** Each sweep logs a `stale` count — watched relations that
   weren't episode-refreshed since the previous sweep began (see the
   `[VOD-SWEEP] … not episode-refreshed …` warning and `stale_by_account` in the
   status/debug output). A persistently high count for one account means that
   provider is failing `get_series_info`, not the plugin. **Show status** also
   reports a live count, which is the one that reflects right now — the figures
   stored in `last sweep` / `last retry` are snapshots taken *before* those runs
   fanned out.
5. **Retiring a client-side periodic refresh.** If your generator has its own
   periodic re-fetch, this supersedes it (the generator could only ever hit the
   top relation). Retire it *after* confirming this plugin works live.

## Storage

- `CoreSettings` row `dispatcharr_vod_episode_sweep_watchlist` — the durable
  `{series_pk: last_seen}` map + last sweep summary.
- Redis key `vodsweep:daily:<YYYY-MM-DD>` — the once-per-day claim shared by both
  triggers so exactly one sweep runs each day (36h TTL, self-expiring).
- A `django_celery_beat` `PeriodicTask` named `dispatcharr_vod_episode_sweep`
  (daily cron at the sweep hour, routed to the `dvr` queue) → the plugin task
  `dispatcharr_vod_episode_sweep.run_sweep`. Removed when the plugin is
  disabled/deleted or Auto-run daily sweep is turned off.

The sweep runs `run_sweep_impl()` in a plugin-loaded process (the `dvr` worker
for the scheduled trigger, or a web worker for the opportunistic tick / manual
run) and only enqueues Dispatcharr's core `batch_refresh_series_episodes` task —
which does the provider work on the default worker. See `DESIGN.md` for why the
scheduled task must be routed to the `dvr` queue (prefork-worker task
registration).

---

## Acknowledgments

Designed and built by [andyj682](https://github.com/andyj682) with Claude
(Anthropic) as a pair-programming collaborator. Part of a family of VOD plugins
with
[`dispatcharr_vod_preferences`](https://github.com/andyj682/dispatcharr_vod_preferences),
[`dispatcharr_vod_concurrency_fix`](https://github.com/andyj682/dispatcharr_vod_concurrency_fix),
and
[`dispatcharr_vod_episode_refresh`](https://github.com/andyj682/dispatcharr_vod_episode_refresh).
