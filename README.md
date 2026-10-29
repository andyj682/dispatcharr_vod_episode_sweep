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
2. **Refreshes every relation** of each watched series — across all providers
   and categories, bypassing the 24h gate — so newly-added episodes
   materialize. Shows that are **airing** are refreshed daily; everything else
   on a weekly rotation, plus the day its provider reports a change (v1.2.0).
3. That refresh also bumps each series' `last_modified`, so your client's normal
   incremental sync notices and pulls the new episodes. **No client changes
   needed.**

Scoped to only the series you sync, and to the ones that can actually have
changed, so provider API volume stays small. Runs in the background; never
blocks or alters an XC response.

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
| **Watchlist TTL (days)** | 30 | A series leaves the watchlist only after this long unseen **and** after your client has ignored 3 refreshes of it — or after 120 days unseen regardless. See note 7. |
| **Auto-run daily sweep** | on | Run the sweep on a fixed daily timer (Celery beat) rather than waiting for client traffic — so it runs *before* your sync and new episodes appear in **one** sync instead of two. Off = opportunistic (traffic-triggered) only. |
| **Auto-retry stale relations** | on | After each sweep, automatically re-refresh anything still flagged stale — the same work the Retry action does. Useful when a provider intermittently fails requests, since a later pass may succeed where the first didn't. Costs nothing on a clean day: the chain stops as soon as nothing is stale. |
| **Retry passes** | 2 | How many automatic passes to make. Each one only touches what is still stale, so passes get rapidly cheaper. 0 turns auto-retry off. |
| **Refresh by airing status** | on | Refresh airing shows daily and everything else on a rotation (plus anything new, carried over, or reported changed by its provider). Off = every watched show every day, as before 1.2.0. |
| **TMDB API key (optional)** | — | A free TMDB key (v3 API key or v4 read access token). Lets the plugin follow each show's airing schedule, including pre-warming a show a few days before a new season. Only TMDB ids are sent. Falls back to Dispatcharr's `TMDB_API_KEY` environment variable; without either, stored episode air dates are used. |
| **Airing window (days)** | 10 | How long after an episode airs (per TMDB) a show stays on the daily refresh — covers providers posting late. |
| **Rotation for other shows (days)** | 7 | Shows that aren't airing are still refreshed at least this often (1–28), spread evenly across the days. |
| **Refresh when a provider reports a change** | on | Refresh a show the day its provider's listing says it changed since the last refresh, whatever its airing status — catches new copies of old shows and backfilled seasons. Ignored for a provider that marks most of its listing changed at once. |
| **Fallback airing window (days)** | 14 | For shows without usable TMDB data: daily while a stored episode air date (or a newly appeared episode) is within this many days. |
| **Schedule queue (advanced)** | `dvr` | The Celery queue the scheduled sweep is dispatched to; must be served by a worker that loads plugins (stock Dispatcharr = the threads-pool `dvr` worker). Change only if your worker layout differs. |
| **Refresh batch size** | 20 | Series per background refresh task — smaller = smaller provider bursts. 0 = one task per account. |
| **Refresh spacing (seconds)** | 15 | Delay between successive refresh batches (Celery countdown), to spread provider calls. 0 = all at once. |

The watchlist builds itself from observed `get_series_info` calls — no manual
list. Only Xtream-Codes VOD series are affected.

---

## Actions

- **Show status** — patch active?, schedule, watchlist size, TMDB coverage
  (e.g. `tmdb 410/415` = usable schedules / watched shows with a TMDB id), the
  last sweep's **due set by tier** (e.g. `due n3 r0 h20 p4 c60`: new, carried
  over, airing, provider-changed, rotation), plus a **live** staleness audit —
  per account, and split by how many runs each relation has been due and
  missed — alongside the stored `last_sweep` / `last_retry` records. The stored
  ones are snapshots taken *before* their run fanned out; the live figure is
  what reflects now. Relations missed on 3+ runs are unlikely to be bad luck —
  those are the dead-content candidates. Unreachable providers are named.
- **List watched series** — the current watchlist + the path to the debug file.
- **Run sweep now** — run the sweep inline immediately, ignoring the daily timer.
- **Retry stale relations** — re-refresh *only* the relations still flagged
  stale, instead of the whole watchlist. Much cheaper than a full sweep (the
  stale set is normally a small slice of it), and safe to repeat. Use it after
  a provider has recovered from an outage, or when a provider fails a fraction
  of its requests: because those failures are independent, each pass clears
  most of what the last one missed.
- **Update TMDB schedules** — look up airing schedules now, in the background
  (needs a TMDB key). Also happens automatically after each sweep; useful right
  after adding a key.
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
Exercises observation/dedupe, per-account enqueue, tiering and priority,
TMDB lookups, watchlist expiry, the disabled no-op, schedule create/remove,
reload-safety, and clear — with a fake clock. No Dispatcharr, Celery, or DB
required.

---

## Notes / limitations

1. **Eventual consistency.** The sweep runs daily (or on demand); new episodes
   surface on the client's next sync after a sweep, not instantly.
2. **Coverage.** Xtream-Codes VOD **series** only. Movies are refreshed eagerly
   by Dispatcharr's listing scan, so they're not needed here. Live TV/EPG/DVR
   untouched.
3. **Provider API budget.** Each refreshed series costs ~one `get_series_info`
   call per relation per account. With **Refresh by airing status** on, a day
   touches only what is due — typically a small fraction of the watchlist. If
   a provider rate-limits you, lower **Refresh batch size** and/or raise
   **Refresh spacing** to spread the calls out.
4. **Provider health.** Each sweep logs a `stale` count — relations the
   previous sweep made due that still weren't episode-refreshed (see the
   `[VOD-SWEEP] … not episode-refreshed …` warning and `stale_by_account` in the
   status/debug output). A relation that isn't due today never counts as
   stale, however long ago it was refreshed. A persistently high count for one account means that
   provider is failing `get_series_info`, not the plugin. **Show status** also
   reports a live count, which is the one that reflects right now — the figures
   stored in `last sweep` / `last retry` are snapshots taken *before* those runs
   fanned out.
5. **Retiring a client-side periodic refresh.** If your generator has its own
   periodic re-fetch, this supersedes it (the generator could only ever hit the
   top relation). Retire it *after* confirming this plugin works live.
6. **Dispatcharr 0.33.0 changed what an XC `series_id` is** (it now names the
   series itself rather than one provider's copy of it). From 1.1.1 the plugin
   detects which release it is running on and handles both; **Show status**
   reports it as `xc_ids=series` (0.33.0+) or `xc_ids=relation` (earlier).
   Update the plugin to 1.1.1 **before** upgrading Dispatcharr to 0.33.0 —
   1.1.0 on 0.33.0 silently watches the wrong series. If that already
   happened, run **Clear watchlist** once on 1.1.1, then let your client do a
   full resync so the list rebuilds. Your client may have its own steps for
   this change, since any series ids it stored also change meaning.
7. **Refresh by airing status (1.2.0).** Each run refreshes what is *due*:
   new and carried-over work first, then airing shows, then shows a provider
   reports changed, then a rotating slice of the rest. TMDB's schedule decides
   what is airing; the provider's own change date is an independent override
   (it catches what no schedule shows); stored air dates fill in where TMDB has
   nothing usable. TMDB only ever decides *how often* a show is refreshed —
   never which shows are watched. Because a show off the daily refresh is
   bumped less often, the watchlist no longer expires on time alone: a show is
   dropped only when your client has ignored 3 refreshes of it and it is past
   the TTL (or after 120 days unseen). Turn **Refresh by airing status** off to
   get the pre-1.2.0 refresh behavior back exactly.

## Storage

- `CoreSettings` row `dispatcharr_vod_episode_sweep_watchlist` — the durable
  `{series_pk: last_seen}` map, the last sweep and retry summaries, and (1.2.0)
  the last sweep's due set, carried-over counts, ignored-refresh counts and the
  new-to-the-watchlist queue. Older releases read the same row and ignore the
  new fields.
- `CoreSettings` row `dispatcharr_vod_episode_sweep_tmdb` (1.2.0) — cached TMDB
  schedules (name, first-air year, status, last/next episode dates) keyed by
  TMDB id.
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
