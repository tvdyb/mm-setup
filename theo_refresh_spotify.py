#!/usr/bin/env python3
"""Refresh KXRANKLISTSONGSPOTGLOBAL-26JUN01 and KXRANKLISTSONGSPOTUSA-26JUN01 from
the public kworb.net mirrors of the Spotify daily top-200 charts.

Why kworb (and not charts.spotify.com directly): Spotify's CSV download endpoint
(`https://charts.spotify.com/charts/view/.../download`) and JSON service
(`charts-spotify-com-service.spotify.com/...`) both require an authenticated
Spotify session. The CSV URL returns the SPA-shell HTML to anonymous clients,
and the JSON service returns 401. kworb.net republishes the same daily top-200
as plain HTML and is widely used by music-industry analytics — sufficient for a
top-1 / top-N stickiness signal.

Stickiness model (rough heuristic, NOT calibrated). The market resolves on
2026-06-01 — about a month out as of writing — so today's #1 is informative
but far from certain. We assign:
    today's #1                -> 0.40
    today's #2-#5 (split)     -> 0.30 / 4 each = 0.075
    today's #6-#10 (split)    -> 0.15 / 5 each = 0.030
    today's #11-#50 (split)   -> 0.10 / N each
    non-charting candidates   -> floor 0.005 each
Then probabilities are clipped to [0.005, 0.995] (matches the existing convention
in `theo_refresh.py`). The mass is NOT renormalized to sum to 1 — these are
independent per-strike YES probabilities, the same shape as the existing
range-theo refreshers in the parent file.

Failure mode: any HTTP / parse error returns {"ok": False, "error": "..."} and
leaves the file untouched. Caller (the cron driver) should keep the prior
strikes and not crash the bot.
"""
import argparse, datetime as dt, json, re
from pathlib import Path
import requests

THEOS_DIR = Path("/Users/wilsonw/Downloads/theos")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
HDRS = {"User-Agent": UA}

KWORB_GLOBAL = "https://kworb.net/spotify/country/global_daily.html"
KWORB_USA    = "https://kworb.net/spotify/country/us_daily.html"


def _now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


# Verified 2026-04-30 against Kalshi trade-api/v2 markets endpoint for
# KXRANKLISTSONGSPOTGLOBAL-26JUN01. The same 42 codes appear in the USA
# event with identical mappings.
ARTIST_CODES = {
    "ALE":  "Alex Warren",
    "ARI":  "Ariana Grande",
    "ASA":  "A$AP Rocky",
    "BAB":  "Baby Keem",
    "BEL":  "Bella Kay",
    "BRU":  "Bruno Mars",
    "BTS":  "BTS",
    "CHA":  "Charli xcx",
    "CHAP": "Chappell Roan",
    "DJO":  "Djo",
    "DOM":  "Dominic Fike",
    "DRA":  "Drake",
    "ELL":  "Ella Langley",
    "ESD":  "EsDeeKid",
    "FLE":  "Fleetwood Mac",
    "HAR":  "Harry Styles",
    "HUN":  "HUNTR/X",
    "JCO":  "J. Cole",
    "JOJ":  "Joji",
    "JUS":  "Justin Bieber",
    "KAT":  "KATSEYE",
    "LAN":  "Lana Del Rey",
    "LUK":  "Luke Combs",
    "MAD":  "Madonna",
    "MEG":  "Megan Moroney",
    "MIC":  "Michael Jackson",
    "MOR":  "Morgan Wallen",
    "NIC":  "Nicki Minaj",
    "NOA":  "Noah Kahan",
    "OLI":  "Olivia Dean",
    "OLII": "Olivia Rodrigo",
    "PIN":  "PinkPantheress",
    "PLA":  "Playboi Carti",
    "RAY":  "RAYE",
    "SAB":  "Sabrina Carpenter",
    "SHE":  "She & Him",
    "SOM":  "sombr",
    "TAY":  "Taylor Swift",
    "TWE":  "Twenty One Pilots",
    "WEE":  "The Weeknd",
    "YEA":  "Yeat",
    "ZAR":  "Zara Larsson",
}


def _norm(s):
    """Normalize an artist name for fuzzy matching: lowercase, strip non-alnum."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def fetch_kworb_chart(url):
    """Return list of (pos, artist_name) tuples from a kworb daily chart page.
    Some rows use rowspan for multi-track artists -> they have no <td class='np'>
    leading number; those rows are skipped, which is fine since we only need
    the best (lowest) position per artist."""
    r = requests.get(url, headers=HDRS, timeout=10)
    r.raise_for_status()
    html = r.text
    out = []
    for row in re.findall(r"<tr>(.*?)</tr>", html, re.S):
        pm = re.match(r'<td class="np"[^>]*>(\d+)</td>', row)
        if not pm:
            continue
        am = re.search(r'<a href="\.\./artist/[^"]+">([^<]+)</a>', row)
        if not am:
            continue
        out.append((int(pm.group(1)), am.group(1).strip()))
    if not out:
        raise RuntimeError(f"kworb parse: no rows found at {url}")
    if out[0][0] != 1:
        raise RuntimeError(f"kworb parse: first row pos={out[0][0]} not 1 at {url}")
    return out


def best_position_per_artist(chart):
    """Collapse (pos, artist) list -> {normalized_artist: best_pos}."""
    best = {}
    for pos, artist in chart:
        key = _norm(artist)
        if key not in best or pos < best[key]:
            best[key] = pos
    return best


def stickiness_prob(pos):
    """P(this artist is #1 on resolution date | they are at chart position `pos` today).
    Rough heuristic — see module docstring."""
    if pos is None:
        return 0.005
    if pos == 1:
        return 0.40
    if 2 <= pos <= 5:
        return 0.30 / 4
    if 6 <= pos <= 10:
        return 0.15 / 5
    if 11 <= pos <= 50:
        return 0.10 / 40
    # in chart but below top-50: tiny bump above floor
    return 0.01


def build_strikes(event_ticker, chart):
    """Return (strikes_dict, raw_strikes_dict, debug_top5)."""
    best = best_position_per_artist(chart)
    strikes = {}
    raw = {}
    for code, artist in ARTIST_CODES.items():
        tk = f"{event_ticker}-{code}"
        pos = best.get(_norm(artist))
        # Try a looser match for slash/parenthetical artist names
        if pos is None:
            for k, v in best.items():
                if k.startswith(_norm(artist)) or _norm(artist).startswith(k):
                    pos = v
                    break
        p = stickiness_prob(pos)
        raw[tk] = round(p, 6)
        strikes[tk] = round(max(0.005, min(0.995, p)), 4)
    # Debug: top-5 chart entries paired with their resolved code (or None)
    code_by_norm = {_norm(v): k for k, v in ARTIST_CODES.items()}
    top5 = []
    seen = set()
    for pos, artist in chart:
        n = _norm(artist)
        if n in seen:
            continue
        seen.add(n)
        top5.append({"pos": pos, "artist": artist, "code": code_by_norm.get(n)})
        if len(top5) == 5:
            break
    return strikes, raw, top5


def refresh_spotify_event(event, kworb_url):
    """Refresh a single Spotify rank-list event from a kworb chart URL."""
    path = THEOS_DIR / f"{event}.json"
    try:
        cur = json.loads(path.read_text())
    except Exception as e:
        return {"event": event, "ok": False, "error": f"theo read: {type(e).__name__}: {e}"}
    try:
        chart = fetch_kworb_chart(kworb_url)
    except Exception as e:
        return {"event": event, "ok": False, "error": f"kworb fetch: {type(e).__name__}: {e}"}
    try:
        strikes, raw_strikes, top5 = build_strikes(event, chart)
    except Exception as e:
        return {"event": event, "ok": False, "error": f"build_strikes: {type(e).__name__}: {e}"}
    # Sanity: at least one strike got > floor (i.e. at least one mapped artist
    # is on the chart). If not, something is wrong with the parse — refuse to
    # overwrite the prior file.
    above_floor = sum(1 for v in strikes.values() if v > 0.005)
    if above_floor == 0:
        return {"event": event, "ok": False,
                "error": f"no mapped artist found on chart (parsed {len(chart)} rows); refusing to overwrite"}
    cur["as_of"] = _now_iso()
    cur["strikes"] = strikes
    cur["raw_strikes"] = raw_strikes
    cur["chart_top5"] = top5
    base_method = cur.get("method", "").split("\n[refreshed")[0].rstrip()
    top1 = chart[0]
    cur["method"] = (base_method + f"\n[refreshed {_now_iso()[:16]}Z] "
        f"kworb {kworb_url.rsplit('/',1)[-1]}: #1={top1[1]} ({above_floor}/42 candidates on chart). "
        f"Stickiness heuristic, NOT calibrated.")
    try:
        path.write_text(json.dumps(cur, indent=2))
    except Exception as e:
        return {"event": event, "ok": False, "error": f"write: {type(e).__name__}: {e}"}
    return {"event": event, "ok": True, "top1": top1[1], "top1_pos": top1[0],
            "mapped_on_chart": above_floor, "chart_rows": len(chart)}


def refresh_kxranklistsongspotglobal(event="KXRANKLISTSONGSPOTGLOBAL-26JUN01"):
    return refresh_spotify_event(event, KWORB_GLOBAL)


def refresh_kxranklistsongspotusa(event="KXRANKLISTSONGSPOTUSA-26JUN01"):
    return refresh_spotify_event(event, KWORB_USA)


REFRESHERS = {
    "KXRANKLISTSONGSPOTGLOBAL-26JUN01":
        lambda: refresh_kxranklistsongspotglobal("KXRANKLISTSONGSPOTGLOBAL-26JUN01"),
    "KXRANKLISTSONGSPOTUSA-26JUN01":
        lambda: refresh_kxranklistsongspotusa("KXRANKLISTSONGSPOTUSA-26JUN01"),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", default=None,
                    help="Single event to refresh (default: all)")
    args = ap.parse_args()
    keys = [args.event] if args.event else list(REFRESHERS.keys())
    results = []
    for k in keys:
        if k not in REFRESHERS:
            results.append({"event": k, "ok": False, "error": "no refresher registered"})
            continue
        try:
            results.append(REFRESHERS[k]())
        except Exception as e:
            results.append({"event": k, "ok": False, "error": f"{type(e).__name__}: {e}"})
    print(json.dumps({"results": results, "as_of": _now_iso()}, indent=2))


if __name__ == "__main__":
    main()
