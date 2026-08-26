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
    version = "1.0.0"
    description = (
        "Learns which VOD series a client syncs and once a day refreshes ALL of "
        "each watched show's provider/category relations so new episodes stop "
        "going missing. Scoped to only the series you sync; runs in the background."
    )
    author = "andyj682"
    help_url = "https://github.com/andyj682/dispatcharr_vod_episode_sweep"

    fields = [
        {
            "id": "sweep_hour",
            "label": "Earliest sweep hour (0-23)",
            "type": "number",
            "default": 3,
            "help_text": (
                "Hour the daily sweep runs, in Dispatcharr's configured system "
                "timezone. With Auto-run daily sweep on (below), a timer fires the "
                "sweep at this hour; otherwise it fires on the first client series "
                "request at/after this hour. Set it a bit BEFORE your client's "
                "daily sync so new episodes are ready in one pass. Default 3."
            ),
        },
        {
            "id": "scheduled_sweep",
            "label": "Auto-run daily sweep",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Automatically run the daily sweep on a timer at the hour above. "
                "When off, it still runs -- but only when a client next requests a "
                "series after that hour, which lags new episodes by an extra sync. "
                "Leave on unless you have a reason not to."
            ),
        },
        {
            "id": "watchlist_ttl_days",
            "label": "Watchlist TTL (days)",
            "type": "number",
            "default": 30,
            "help_text": (
                "Drop a series from the watchlist if it hasn't been requested "
                "(via get_series_info) for this many days. Keep this comfortably "
                "longer than your client's sync interval. Default 30."
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
            last = wl.get("last_sweep")
            return {
                "status": "ok",
                "message": (
                    f"active={_patch._ACTIVE} in worker pid={os.getpid()} "
                    f"(reflects ONE worker; check logs for all pids). "
                    f"sweep_hour={cfg['sweep_hour']:02d}:00, "
                    f"scheduled={cfg['scheduled_sweep']} (queue={cfg['schedule_queue']}), "
                    f"ttl={cfg['ttl_seconds'] / 86400.0:.0f}d, "
                    f"batch={cfg['batch_size']}, spacing={cfg['spacing_seconds']:.0f}s, "
                    f"watched={len(wl.get('series', {}))}, "
                    f"last_sweep={last}"
                ),
            }

        if action == "list_watchlist":
            wl = _patch.get_watchlist(force=True)
            return {
                "status": "ok",
                "message": _format_watchlist(wl) + f"\n\nDebug file: {_patch._debug_file_path()}",
                "watchlist": wl.get("series", {}),
            }

        if action == "refresh_now":
            # Run inline in this (web) worker -- it fans out the CORE
            # batch_refresh_series_episodes tasks, which run normally in Celery.
            # We deliberately do NOT dispatch a plugin task (the default prefork
            # worker's consumer would reject it as unregistered).
            try:
                summary = _patch.run_sweep_impl()
                return {"status": "ok", "message": f"Sweep ran: {summary}"}
            except Exception as exc:
                logger.exception("[VOD-SWEEP] refresh_now failed")
                return {"status": "error", "message": f"Sweep failed: {exc}"}

        if action == "clear_watchlist":
            try:
                removed = _patch.clear_watchlist()
                return {"status": "ok", "message": f"Cleared {removed} watched series."}
            except Exception as exc:
                logger.exception("[VOD-SWEEP] clear_watchlist failed")
                return {"status": "error", "message": f"Failed to clear: {exc}"}

        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        """Called by Dispatcharr on disable / delete / reload."""
        _patch.uninstall()
        return {"status": "ok", "message": "VOD episode sweep reverted"}
