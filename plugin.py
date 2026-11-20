"""
Dispatcharr VOD Episode Sweep
=============================

Keeps a small watchlist of the VOD series a client actually syncs (learned by
observing XC `get_series_info` calls) and refreshes ALL relations of each watched
series once a day -- across every provider and category, bypassing the 24h,
browse-only gate. This closes the gap where an episode that first appears on a
non-top-priority relation (a second provider, or a separate "... 4K" category)
never materialises and never shows up over the XC API, so an incremental .strm
generator can't see it.

The nightly refresh also bumps each series' `last_modified`, so the client's
normal incremental sync notices the change and re-pulls the new episodes -- no
client change required. Scoped to only the series you sync, so provider API
volume stays bounded.

See patch.py for the full design. This module is the plugin entry point: it
applies the observation monkeypatch at import time (every worker) and manages the
Celery-beat schedule for the sweep task.

Author: andyj682
License: MIT
"""

import logging

logger = logging.getLogger("plugins.dispatcharr_vod_episode_sweep")

try:
    from . import patch as _patch
except Exception:  # pragma: no cover - fall back to flat import layout
    import patch as _patch

try:
    # Import-time: apply the observation patch in every worker. (No beat/plugin
    # task -- the sweep runs inline; see patch.py "Triggering".)
    _patch.install()
except Exception:  # never break app startup because of the plugin
    logger.exception("[VOD-SWEEP] auto-install on import failed")


def _format_watchlist(data):
    series = (data or {}).get("series", {})
    if not series:
        return "Watchlist is empty."
    import time
    lines = []
    for pk, ts in sorted(series.items(), key=lambda kv: kv[1], reverse=True):
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(float(ts)))
        lines.append(f"  series {pk}  (last seen {when})")
    return f"{len(series)} watched series:\n" + "\n".join(lines)


class Plugin:
    # UI title only. README / repo / zip keep the fuller "Dispatcharr VOD
    # Episode Sweep" name; "Dispatcharr" is redundant inside the Dispatcharr UI.
    name = "VOD Episode Sweep"
    version = "1.2.1"
    description = (
        "Learns which VOD series a client syncs and refreshes ALL of each watched "
        "show's provider/category relations so new episodes stop going missing. "
        "Scoped to only the series you sync; runs in the background."
    )
    author = "andyj682"
    help_url = "https://github.com/andyj682/dispatcharr_vod_episode_sweep"

    fields = [
        {
            "id": "tiered_refresh",
            "label": "Prioritize refresh by airing status",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Refresh shows that are airing every day, and everything else on "
                "a rotation (plus anything new, carried over, or reported changed "
                "by its provider). Most watched shows aren't airing, so this cuts "
                "the daily provider load sharply. Off = refresh every watched "
                "show every day, as before 1.2.0."
            ),
        },
        {
            "id": "provider_promotion",
            "label": "Refresh when a provider reports a change",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Refresh a show the day its provider's listing says it changed "
                "since the last refresh, whatever its airing status. Catches what "
                "no schedule shows: a new copy of an old show, backfilled "
                "seasons, fixed episodes. Ignored for a provider that marks most "
                "of its listing changed at once. Default on."
            ),
        },
        {
            "id": "tmdb_api_key",
            "label": "TMDB API key (optional)",
            "type": "string",
            "default": "",
            "help_text": (
                "A free TMDB key (v3 API key or v4 read access token) lets the "
                "plugin follow each show's airing schedule: refreshed daily "
                "around new episodes, and from a few days before a new season "
                "starts. Only TMDB ids are sent. Without a key, stored episode "
                "air dates are used instead. Falls back to Dispatcharr's "
                "TMDB_API_KEY environment variable if that is set."
            ),
        },
        {
            "id": "scheduled_sweep",
            "label": "Auto-run daily sweep",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Automatically run the daily sweep on a timer at the hour below. "
                "When off, it still runs -- but only when a client next requests a "
                "series after that hour, which lags new episodes by an extra sync. "
                "Leave on unless you have a reason not to."
            ),
        },
        {
            "id": "sweep_hour",
            "label": "Earliest sweep hour (0-23)",
            "type": "number",
            "default": 3,
            "help_text": (
                "Hour the daily sweep runs, in Dispatcharr's configured system "
                "timezone. With Auto-run daily sweep on (above), a timer fires the "
                "sweep at this hour; otherwise it fires on the first client series "
                "request at/after this hour. Set it a bit BEFORE your client's "
                "daily sync so new episodes are ready in one pass. Default 3."
            ),
        },
        {
            "id": "auto_retry",
            "label": "Auto-retry stale relations",
            "type": "boolean",
            "default": True,
            "help_text": (
                "After each sweep, keep re-refreshing whatever is still "
                "flagged stale until nothing is, then stop. A provider that "
                "stays down (about 90 minutes) or makes no progress is given "
                "up for the day, and the rest carry on. Does nothing on a "
                "clean day."
            ),
        },
        {
            "id": "retry_max_passes",
            "label": "Max retry passes per day",
            "type": "number",
            "default": 12,
            "help_text": (
                "Upper limit on automatic retry passes per day (0-24). The "
                "chain normally stops well before this, as soon as nothing "
                "is stale. 0 turns auto-retry off. Default 12."
            ),
        },
        {
            "id": "retry_quiet_window",
            "label": "Quiet window for retries (optional)",
            "type": "string",
            "default": "",
            "help_text": (
                "Optional time range, like 09:55-10:15, when automatic "
                "retries make no provider calls: a pass due inside it waits "
                "until it ends. Use it to keep retries out of your client's "
                "library sync. Empty = none."
            ),
        },
        {
            "id": "hot_aired_days",
            "label": "Airing window (days)",
            "type": "number",
            "default": 10,
            "help_text": (
                "How long after an episode airs (per TMDB) a show stays on the "
                "daily refresh. Covers providers that post episodes late. Default "
                "10."
            ),
        },
        {
            "id": "cold_rotation_days",
            "label": "Rotation for other shows (days)",
            "type": "number",
            "default": 7,
            "help_text": (
                "Shows that aren't airing are still refreshed at least once every "
                "this many days (1-28), spread evenly across the days. Lower = "
                "fresher, more provider calls. Default 7."
            ),
        },
        {
            "id": "fallback_hot_days",
            "label": "Fallback airing window (days)",
            "type": "number",
            "default": 14,
            "help_text": (
                "For shows without usable TMDB data: refreshed daily while a "
                "stored episode air date (or a newly appeared episode) is within "
                "this many days. Default 14."
            ),
        },
        {
            "id": "watchlist_ttl_days",
            "label": "Watchlist TTL (days)",
            "type": "number",
            "default": 30,
            "help_text": (
                "A series leaves the watchlist only after this many days unseen "
                "AND after your client has ignored 3 refreshes of it (or after "
                "120 days unseen regardless), so a show refreshed only weekly, or "
                "whose provider is having a bad week, isn't dropped while you "
                "still follow it. Default 30."
            ),
        },
        {
            "id": "sweep_batch_size",
            "label": "Refresh batch size",
            "type": "number",
            "default": 20,
            "help_text": (
                "Series per background refresh task. Smaller batches spread the "
                "provider get_series_info calls into smaller bursts (gentler on "
                "provider rate limits). 0 = one task per account. Default 20."
            ),
        },
        {
            "id": "sweep_spacing_seconds",
            "label": "Refresh spacing (seconds)",
            "type": "number",
            "default": 15,
            "help_text": (
                "Delay inserted between successive refresh batches (via a Celery "
                "countdown) so they don't all hit the providers at once. 0 = fire "
                "everything immediately. Default 15."
            ),
        },
        {
            "id": "schedule_queue",
            "label": "Schedule queue (advanced)",
            "type": "string",
            "default": "dvr",
            "help_text": (
                "Advanced: the Celery queue the scheduled sweep is dispatched to. "
                "It must be served by a worker that loads plugins -- on a stock "
                "Dispatcharr that's the threads-pool 'dvr' worker (the default "
                "prefork worker rejects plugin tasks). Change only if your worker "
                "layout differs. If the scheduled sweep never runs, the "
                "opportunistic trigger still covers it."
            ),
        },
        {
            "id": "_info",
            "label": "",
            "type": "info",
            "description": (
                "The watchlist is built automatically by observing the series "
                "your client requests over the XC API -- no manual list needed. "
                "A 'watchlist_debug.json' file is written in this plugin's folder "
                "(under data/plugins/) during each refresh. Only Xtream-Codes VOD "
                "series are affected."
            ),
        },
    ]

    actions = [
        {
            "id": "status",
            "label": "Show status",
            "description": "Report whether the patch is active, the schedule, and "
                           "the watchlist size.",
            "button_label": "Check status",
            "button_variant": "outline",
        },
        {
            "id": "list_watchlist",
            "label": "List watched series",
            "description": "Show the series currently on the watchlist and the "
                           "path to the debug JSON file.",
            "button_label": "List watchlist",
            "button_variant": "outline",
        },
        {
            "id": "refresh_now",
            "label": "Run sweep now",
            "description": "Run the episode sweep now, regardless of the daily "
                           "timer.",
            "button_label": "Run now",
            "button_variant": "filled",
            "async": True,
        },
        {
            "id": "retry_stale",
            "label": "Retry stale relations",
            "description": "Retry only relations still flagged stale.",
            "button_label": "Retry stale",
            "button_variant": "filled",
            "async": True,
        },
        {
            "id": "tmdb_refresh",
            "label": "Update TMDB schedules",
            "description": "Look up airing schedules on TMDB now.",
            "button_label": "Update TMDB",
            "button_variant": "filled",
        },
        {
            "id": "clear_watchlist",
            "label": "Clear watchlist",
            "description": "Forget every watched series. It will be rebuilt as "
                           "clients request series again.",
            "button_label": "Clear watchlist",
            "button_variant": "light",
            "button_color": "red",
            "confirm": {
                "title": "Clear the watchlist?",
                "message": "This removes every remembered series. It rebuilds "
                           "automatically as clients sync series again.",
            },
        },
    ]

    def run(self, action=None, params=None, context=None):
        context = context or {}

        if action == "enable":
            ok = _patch.install(manage_schedule=True)
            return {
                "status": "ok" if ok else "error",
                "message": "VOD episode sweep enabled (observation patched + daily "
                           "schedule set)" if ok else "Failed to enable (see logs)",
            }

        if action == "disable":
            _patch.uninstall()
            return {"status": "ok", "message": "VOD episode sweep disabled (patch reverted)"}

        if action == "status":
            import os
            cfg = _patch._load_config(force=True)
            wl = _patch.get_watchlist(force=True)
            # `last_sweep` is a snapshot taken BEFORE that sweep fanned out, so
            # it can be a day old and describe a state that no longer exists.
            # The live audit is what reflects right now. Message assembly lives
            # in patch.py so its length can be regression-tested.
            live = _patch.live_stale_summary()
            # Deliberately terse: the UI's result box truncates a long message,
            # and the per-account stale breakdown is the part worth protecting.
            return {
                "status": "ok",
                "message": _patch.build_status_message(
                    cfg, wl, live, os.getpid(), _patch._ACTIVE,
                    id_mode=_patch._xc_series_id_mode(),
                    tmdb=_patch.tmdb_status(cfg, wl),
                ),
            }

        if action == "tmdb_refresh":
            # Queued to the plugin-capable worker rather than run here: a
            # first fill is hundreds of throttled lookups, too long for a web
            # request.
            cfg = _patch._load_config(force=True)
            if not cfg.get("tmdb_key"):
                return {"status": "error",
                        "message": "No TMDB key set (plugin setting or the "
                                   "TMDB_API_KEY environment variable)."}
            if not cfg.get("tiered"):
                return {"status": "error",
                        "message": "Prioritize refresh by airing status is off, so TMDB "
                                   "schedules aren't used."}
            ok = _patch.schedule_tmdb_refresh(0, reason="manual")
            return {
                "status": "ok" if ok else "error",
                "message": ("TMDB lookups queued; they run in the background. "
                            "Show status reports coverage after the next sweep.")
                           if ok else "Could not queue TMDB lookups (see logs).",
            }

        if action == "list_watchlist":
            wl = _patch.get_watchlist(force=True)
            return {
                "status": "ok",
                "message": _format_watchlist(wl) + f"\n\nDebug file: {_patch._debug_file_path()}",
                "watchlist": wl.get("series", {}),
            }

        if action == "refresh_now":
            # Runs in whatever process runs the action: the web worker through
            # Dispatcharr 0.33, a background worker from 0.34 (where "async"
            # makes the button return at once and show progress; older
            # releases ignore the flag). Either way it only fans out the CORE
            # batch_refresh_series_episodes tasks.
            try:
                summary = _patch.run_sweep_impl(
                    progress=context.get("report_progress"))
                return {"status": "ok",
                        "message": _patch.format_run_record("Sweep ran", summary)}
            except Exception as exc:
                logger.exception("[VOD-SWEEP] refresh_now failed")
                return {"status": "error", "message": f"Sweep failed: {exc}"}

        if action == "retry_stale":
            # Same pattern as refresh_now, but scoped to the relations that
            # are actually stale.
            try:
                summary = _patch.retry_stale_impl(
                    progress=context.get("report_progress"))
                # The compact record line, not the raw dict: the dict carries
                # per-account-id maps for the chain and overran the toast.
                return {"status": "ok",
                        "message": _patch.format_run_record("Retry ran", summary)}
            except Exception as exc:
                logger.exception("[VOD-SWEEP] retry_stale failed")
                return {"status": "error", "message": f"Retry failed: {exc}"}

        if action == "clear_watchlist":
            try:
                removed = _patch.clear_watchlist()
                return {"status": "ok", "message": f"Cleared {removed} watched series."}
            except Exception as exc:
                logger.exception("[VOD-SWEEP] clear_watchlist failed")
                return {"status": "error", "message": f"Failed to clear: {exc}"}

        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        """Called by Dispatcharr on disable / delete / reload (passed as
        context["reason"]); whether the schedule row survives depends on it."""
        reason = (context or {}).get("reason") if isinstance(context, dict) else None
        _patch.stop_for_reason(reason)
        return {"status": "ok", "message": "VOD episode sweep reverted"}
