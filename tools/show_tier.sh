#!/usr/bin/env bash
# show_tier.sh -- explain how the episode sweep treats ONE series: whether it
# is on the daily ("airing") list, why or why not, what the last sweep made due
# for it, and when each provider copy was last refreshed. READ-ONLY: it changes
# nothing and makes no provider or TMDB calls. Loads the installed plugin's
# patch.py and uses its real rules with your live settings.
#
# Run this on the Dispatcharr host (wherever `docker` is). Edit the -e vars
# below, then paste the whole block.
#
#   TITLE   series name, matched case-insensitively as a substring
#   ID      numeric series id (wins if set)
#
# Change the container name "dispatcharr" if yours differs (docker ps to check).
#
# How to read the output:
#   * AIRING NOW      = would the series be on the daily list if a sweep ran
#                       now, and which rule decides (TMDB schedule, or stored
#                       episode dates when TMDB has nothing usable).
#   * LAST SWEEP      = what the last sweep actually made due, per provider:
#                       n new, r carried over, h airing, p provider change,
#                       c rotation; "not due" = skipped that day.
#   * NEXT ROTATION   = the next day it is refreshed even if not airing.
#   * last refresh    = when Dispatcharr last fetched that copy's episode list.
# If the copy was refreshed after the episode should have appeared and the
# episode is still missing, the provider simply hasn't posted it yet.

docker exec -i \
  -e TITLE="Example Show" \
  -e ID="" \
  dispatcharr python manage.py shell << 'PY'
import os, time, importlib, importlib.util
from datetime import timedelta
try:
    sw = importlib.import_module('dispatcharr_vod_episode_sweep.patch')
except Exception:
    pdir = os.environ.get('DISPATCHARR_PLUGINS_DIR') or os.environ.get('PLUGINS_DIR') or '/data/plugins'
    sp = importlib.util.spec_from_file_location('sw', os.path.join(pdir, 'dispatcharr_vod_episode_sweep', 'patch.py'))
    sw = importlib.util.module_from_spec(sp); sp.loader.exec_module(sw)

from apps.vod.models import Series

ID = (os.environ.get('ID') or '').strip()
TOK = (os.environ.get('TITLE') or '').strip()
if ID:
    found = list(Series.objects.filter(id=int(ID)))
else:
    found = list(Series.objects.filter(name__icontains=TOK).order_by('name')[:20])
if not found:
    print("No series matches %r." % (ID or TOK))

cfg = sw._load_config(force=True)
data = sw.get_watchlist(force=True)
watched = data.get("series") or {}
due = data.get("due") or {}
pairs = due.get("pairs") or {}
misses = data.get("misses") or {}
items = sw._read_tmdb_cache()["items"] if cfg["tmdb_key"] else {}
today = sw._now_local().date()
now = time.time()
rotation = max(1, int(cfg.get("rotation_days") or 7))
TIERS = {"n": "new", "r": "carried over", "h": "airing", "p": "provider change", "c": "rotation"}

print("settings: tiered=%s  TMDB key=%s  airing window=%sd  fallback window=%sd  rotation=%sd"
      % (cfg["tiered"], "yes" if cfg["tmdb_key"] else "no", cfg["hot_aired_days"],
         cfg["fallback_hot_days"], rotation))
print("last sweep: %s" % (sw._fmt_epoch(due.get("at_epoch")) if due.get("at_epoch") else "none recorded"))

for s in found:
    pk = s.id
    print("\n=== [%s] %s (%s) ===" % (pk, s.name, s.year or "?"))
    if str(pk) not in watched:
        print("  NOT ON THE WATCHLIST -- the sweep never refreshes it. (It joins once your"
              " client requests it.)")
        continue
    print("  on watchlist, last seen %s" % sw._fmt_epoch(watched[str(pk)]))
    if not cfg["tiered"]:
        print("  tiering is OFF: every watched series is refreshed every day.")

    sig = sw._series_signals([pk]).get(pk) or {}
    tid = sw._norm_tmdb_id(sig.get("tmdb_id"))
    entry = items.get(tid) if tid else None
    verdict, why = None, ""
    if not cfg["tmdb_key"]:
        why = "no TMDB key"
    elif not tid:
        why = "series has no TMDB id"
    elif not entry:
        why = "TMDB id %s not looked up yet" % tid
    elif entry.get("err"):
        why = "TMDB lookup for %s failed (%s)" % (tid, entry.get("err"))
    elif not sw.tmdb_sane(sig.get("name"), sig.get("year"), entry):
        why = "TMDB id %s looks like a different show (%s, %s)" % (tid, entry.get("name"), entry.get("year"))
    else:
        verdict = sw.tmdb_hot(entry, today, cfg["hot_aired_days"])
        print("  TMDB %s: %s, status %s, last aired %s, next airs %s"
              % (tid, entry.get("name"), entry.get("st"), entry.get("last") or "-", entry.get("next") or "-"))
        print("  AIRING NOW: %s (TMDB rule: aired within %sd, or next within %sd)"
              % ("YES -- daily" if verdict else "no", cfg["hot_aired_days"], sw.HOT_UPCOMING_DAYS))
    if verdict is None:
        fb = sw.fallback_hot(sig, today, now, cfg["fallback_hot_days"])
        print("  TMDB not used: %s" % why)
        print("  stored dates: newest episode air date %s, newest episode first seen %s"
              % (sw._to_date(sig.get("last_air")) or "-",
                 sw._fmt_epoch(sw._epoch_of(sig.get("last_new"))) if sig.get("last_new") else "-"))
        print("  AIRING NOW: %s (fallback rule: either within %sd)"
              % ("YES -- daily" if fb else "no", cfg["fallback_hot_days"]))

    slot = today.toordinal() % rotation
    ahead = (pk % rotation - slot) % rotation
    print("  NEXT ROTATION: %s" % ("today" if ahead == 0 else (today + timedelta(days=ahead)).isoformat()))

    rows = sw._relation_rows([pk])
    if not rows:
        print("  no active XC provider copies.")
    for r in sorted(rows, key=lambda r: str(r.get("m3u_account__name"))):
        key = sw._pair_key(r["m3u_account_id"], pk)
        tier = pairs.get(key)
        ler = sw._epoch_of(r.get("last_episode_refresh"))
        lm = sw._provider_lm(r.get(sw.PROVIDER_LM_FIELD), now)
        print("  - %-20s LAST SWEEP: %-22s last refresh %s | provider changed %s%s"
              % (r.get("m3u_account__name"),
                 ("%s (%s)" % (tier, TIERS.get(tier, "?"))) if tier else "not due",
                 sw._fmt_epoch(ler) if ler else "never",
                 sw._fmt_epoch(lm) if lm else "-",
                 ("  | missed %s run(s)" % misses[key]) if misses.get(key) else ""))
PY
