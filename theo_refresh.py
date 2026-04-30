#!/usr/bin/env python3
"""Refresh per-event theo JSONs from live sources.

Usage:
    python3 theo_refresh.py                          # refresh all events
    python3 theo_refresh.py --event KXAAAGASD-26APR29
"""
import argparse, datetime as dt, json, math, re, sys
from pathlib import Path
import requests

THEOS_DIR = Path("/Users/wilsonw/Downloads/theos")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
HDRS = {"User-Agent": UA}


def _norm_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _parse_strike(ticker):
    tail = str(ticker).rsplit("-", 1)[-1]
    if tail and tail[0].upper() in ("T", "P"):
        tail = tail[1:]
    try:
        return float(tail)
    except (TypeError, ValueError):
        return None


def _now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


AAA_PRICE_MIN = 2.00   # below this is implausible (US national avg has not been < $2 since 2016)
AAA_PRICE_MAX = 7.00   # above this is implausible (peak-shock ceiling — US has never crossed $5.10)
AAA_MAX_DAILY_MOVE = 0.30  # |today - yesterday| must be ≤ this; AAA daily moves > 30¢ are unheard of


def fetch_aaa():
    """Return {today, yesterday, week, month, year} from gasprices.aaa.com homepage.
    AAA's Current Avg table has rows = [today, yesterday, week, month, year], each with
    columns [Regular, Mid-Grade, Premium, Diesel, E85]. We want the first column.
    Sanity-checks raise RuntimeError on implausible scrapes so callers fall through
    to last-good theo rather than corrupt μ on a broken scrape."""
    r = requests.get("https://gasprices.aaa.com/", headers=HDRS, timeout=10)
    r.raise_for_status()
    rows = []
    for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", r.text, re.S):
        prices = re.findall(r"\$\d\.\d{3}", tr.group(1))
        if 4 <= len(prices) <= 6:
            rows.append(float(prices[0].lstrip("$")))
        if len(rows) == 5: break
    if len(rows) < 5:
        raise RuntimeError(f"AAA parse: only {len(rows)} rows found")
    today, yest = rows[0], rows[1]
    if not (AAA_PRICE_MIN <= today <= AAA_PRICE_MAX):
        raise RuntimeError(f"AAA today=${today:.3f} outside plausible range "
                           f"[${AAA_PRICE_MIN:.2f}, ${AAA_PRICE_MAX:.2f}] -- rejecting scrape")
    if not (AAA_PRICE_MIN <= yest <= AAA_PRICE_MAX):
        raise RuntimeError(f"AAA yesterday=${yest:.3f} outside plausible range -- rejecting scrape")
    if abs(today - yest) > AAA_MAX_DAILY_MOVE:
        raise RuntimeError(f"AAA daily move |today - yest| = ${abs(today - yest):.3f} > "
                           f"${AAA_MAX_DAILY_MOVE:.2f} max -- rejecting scrape (likely stale page or parse error)")
    return {"today": today, "yesterday": yest, "week": rows[2],
            "month": rows[3], "year":      rows[4]}


def fetch_bls_youth(series_id="LNS14024887"):
    """Return latest 6 numeric prints of BLS series (skips '-' / non-numeric)."""
    r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                      json={"seriesid": [series_id]}, timeout=10)
    r.raise_for_status()
    j = r.json()
    arr = j.get("Results", {}).get("series", [{}])[0].get("data", [])
    out = []
    for x in arr:
        try:
            out.append({"period": f"{x['year']}-{x['period']}", "value": float(x["value"])})
        except (TypeError, ValueError):
            continue
        if len(out) >= 6: break
    return out


def refresh_kxaaagasd(event="KXAAAGASD-26APR29"):
    """Update gas theo using live AAA. Preserves analyst-set mu_anchor (offset above today)
    so our predictive μ tracks AAA today without erasing the calibrated drift."""
    path = THEOS_DIR / f"{event}.json"
    cur  = json.loads(path.read_text())
    aaa  = fetch_aaa()
    sigma = float(cur.get("sigma_used", 0.030))
    prior_today = float(cur.get("current_value", aaa["today"]))
    prior_anchor = float(cur.get("mu_anchor", prior_today))
    # Cross-check: scraped today vs prior file's current_value. If the page
    # cached the wrong value or our parse drifted, this catches it before
    # we corrupt μ. Allow up to 15c jump per refresh — a real overnight
    # AAA move can be 7-8c, plus weekend backfill can push to ~12c.
    if abs(aaa["today"] - prior_today) > 0.15:
        raise RuntimeError(f"AAA today=${aaa['today']:.3f} jumped >${0.15:.2f} from "
                           f"prior file value ${prior_today:.3f} -- rejecting (likely stale page or parse error). "
                           f"Investigate manually before re-enabling.")
    # Drift = prior μ_anchor's offset above prior current_value. Keep that offset stable.
    drift = prior_anchor - prior_today
    mu = aaa["today"] + drift
    new_strikes = {}
    raw_strikes = {}
    for tk in cur.get("strikes", {}):
        s = _parse_strike(tk)
        if s is None: continue
        p = 1.0 - _norm_cdf((s - mu) / sigma)
        raw_strikes[tk] = round(p, 6)
        new_strikes[tk] = round(max(0.005, min(0.995, p)), 4)
    cur["current_value"] = aaa["today"]
    cur["mu_anchor"] = round(mu, 4)
    cur["as_of"] = _now_iso()
    cur["strikes"] = new_strikes
    cur["raw_strikes"] = raw_strikes
    base_method = cur.get("method", "").split("\n[refreshed")[0].rstrip()
    cur["method"] = (base_method + f"\n[refreshed {_now_iso()[:16]}Z] "
        f"AAA today=${aaa['today']:.3f} (yest ${aaa['yesterday']:.3f}); "
        f"drift=+${drift:.4f}; μ=${mu:.4f}, σ=${sigma:.3f}.")
    path.write_text(json.dumps(cur, indent=2))
    return {"event": event, "ok": True, "mu": round(mu, 4), "sigma": sigma,
            "drift": round(drift, 4), "aaa_today": aaa["today"], "aaa_yesterday": aaa["yesterday"]}


def refresh_kxyouthun(event="KXYOUTHUN-26DEC04"):
    """Refresh youth unemployment theo. If a new BLS print has appeared and is < 7.5,
    flag YES; otherwise just bump as_of and store the latest reading."""
    path = THEOS_DIR / f"{event}.json"
    cur  = json.loads(path.read_text())
    series = fetch_bls_youth()
    latest = series[0] if series else None
    cur["as_of"] = _now_iso()
    if latest is not None:
        cur["latest_bls_observation"] = latest
        if latest["value"] < 7.5:
            for tk in cur.get("strikes", {}):
                cur["strikes"][tk] = 0.995
            cur["confidence"] = "auto: latest BLS print < 7.5"
    path.write_text(json.dumps(cur, indent=2))
    return {"event": event, "ok": True, "latest": latest}


def bump_as_of(event):
    return {"event": event, "ok": False,
            "error": "no live source wired; as_of not bumped"}


def bump_static(event):
    """Touch as_of on a manually-vetted theo. Use only on slow-moving markets
    whose theo has been audited against market mid and the model is stable.
    No content changes — strikes stay as the analyst left them."""
    path = THEOS_DIR / f"{event}.json"
    cur = json.loads(path.read_text())
    cur["as_of"] = _now_iso()
    path.write_text(json.dumps(cur, indent=2))
    return {"event": event, "ok": True, "static": True}


_SPOT_CACHE_DIR = Path("/tmp/theo_refresh_spot_cache")


def fetch_yf_spot(symbol, max_age_s=15):
    """Cache to disk for max_age_s. Use yfinance (handles Yahoo's cookie/crumb)."""
    import time as _t
    _SPOT_CACHE_DIR.mkdir(exist_ok=True)
    cache = _SPOT_CACHE_DIR / f"{symbol.replace('=','_')}.txt"
    if cache.exists():
        try:
            ts_str, val_str = cache.read_text().strip().split(",", 1)
            if _t.time() - float(ts_str) < max_age_s:
                return float(val_str)
        except Exception:
            pass
    import yfinance as yf
    h = yf.Ticker(symbol).history(period="2d", interval="5m")
    closes = h["Close"].dropna()
    if len(closes) == 0:
        raise RuntimeError(f"yfinance {symbol}: no close data")
    v = float(closes.iloc[-1])
    cache.write_text(f"{_t.time()},{v}")
    return v


def refresh_kxtruev(event="KXTRUEV-26APR29"):
    """Live mu refresh for Truflation EV index. Pulls Cu/Pd/Pt spot, applies
    equal-20% basket weight against prior-day anchor in the file's `anchors` block."""
    path = THEOS_DIR / f"{event}.json"
    cur = json.loads(path.read_text())
    a = cur.get("anchors") or {}
    prior_anchor = float(a.get("prior_anchor", 1232.0))
    weights = a.get("weights") or {"HG=F": 0.20, "PA=F": 0.20, "PL=F": 0.20}
    prior = a.get("prior_close") or {"HG=F": 5.9145, "PA=F": 1461.10, "PL=F": 1942.30}
    sigma = float(cur.get("sigma_used", 10.0))
    spots = {}
    drift = 0.0
    for sym, w in weights.items():
        p0 = float(prior.get(sym, 0.0))
        if p0 <= 0:
            continue
        px = fetch_yf_spot(sym)
        spots[sym] = px
        drift += w * prior_anchor * (px / p0 - 1.0)
    mu = prior_anchor + drift
    new_strikes = {}
    raw_strikes = {}
    for tk in cur.get("strikes", {}):
        s = _parse_strike(tk)
        if s is None:
            continue
        p = 1.0 - _norm_cdf((s - mu) / sigma)
        raw_strikes[tk] = round(p, 6)
        new_strikes[tk] = round(max(0.005, min(0.995, p)), 4)
    cur["central_value"] = round(mu, 4)
    cur["as_of"] = _now_iso()
    cur["strikes"] = new_strikes
    cur["raw_strikes"] = raw_strikes
    cur["live_spots"] = {k: round(v, 4) for k, v in spots.items()}
    base_method = cur.get("method", "").split("\n[refreshed")[0].rstrip()
    cur["method"] = (base_method + f"\n[refreshed {_now_iso()[:16]}Z] "
        f"spots={ {k: round(v,3) for k,v in spots.items()} } drift={drift:+.3f} mu={mu:.3f} sigma={sigma:.2f}")
    path.write_text(json.dumps(cur, indent=2))
    return {"event": event, "ok": True, "mu": round(mu, 3),
            "sigma": sigma, "drift": round(drift, 3), "spots": spots}


REFRESHERS = {
    "KXYOUTHUN-26DEC04":            lambda: refresh_kxyouthun("KXYOUTHUN-26DEC04"),
    "KXGA1ROUND-26NOV03":           lambda: bump_static("KXGA1ROUND-26NOV03"),
    "KXVOTEHUBTRUMPUPDOWN-26APR30": lambda: bump_static("KXVOTEHUBTRUMPUPDOWN-26APR30"),
    "KXUMICHOVR-26DEC18":           lambda: bump_static("KXUMICHOVR-26DEC18"),
    "KXHORMUZWEEKLY-26MAY03":       lambda: bump_static("KXHORMUZWEEKLY-26MAY03"),
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
