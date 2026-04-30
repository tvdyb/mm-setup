"""Kalshi Rewards Tracker — local web app showing the markets you're providing
liquidity on, your share of rewards on each, and recent fills.

Reward heuristic (Kalshi doesn't expose the exact formula publicly):
  - For each side of each market, walk the bid stack from highest to lowest.
  - Only the first 300 contracts qualify for rewards (Kalshi's stated cap).
  - Each qualifying contract earns a weight proportional to its bid price (¢).
    Higher bids sit closer to the spread → higher reward weight.
  - Your share on each side = your weighted contracts / total weighted contracts.
  - $/hr estimate = your_share × per-side reward rate (default $1/hr per side,
    i.e. $2/hr per market split equally — override with --rate-per-market).

Usage:
    python3 kalshi_rewards_app.py             # serves on http://localhost:5050
    python3 kalshi_rewards_app.py --port 8080 --rate-per-market 2
"""
import argparse, base64, datetime as dt, json, math, secrets, time, threading, uuid
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KEY_ID   = "e11f3027-4745-4952-9b7a-31f9c3d1ba13"
KEY_PATH = Path("/Users/wilsonw/Downloads/write.txt")
BASE     = "https://api.elections.kalshi.com/trade-api/v2"
DEFAULT_TARGET_SIZE = 300  # fallback contract cap if a program doesn't specify one
MAX_OVER_BEST = 5  # max ¢ over best competitor bid the UI will allow
INCENTIVE_CACHE_S = 60  # programs change rarely; cache for 1 min
FOLLOWED_PATH = Path("/Users/wilsonw/Downloads/kalshi_followed_events.json")
DEFAULT_FOLLOWED = ["KXAAAGASD-26APR28"]
BLOCKED_PATH = Path("/Users/wilsonw/Downloads/kalshi_blocked_markets.json")
TRIPPED_PATH = Path("/Users/wilsonw/Downloads/kalshi_tripped_events.json")
THEOS_DIR = Path("/Users/wilsonw/Downloads/theos")
LOCAL_MUTATION_TOKEN = secrets.token_urlsafe(24)


_theos_cache = {"map": {}, "meta": {}, "mtime": 0.0}
_theos_lock = threading.Lock()


# Per-day running tallies. EV = sum over today's fills of
# (side_fair_cents − fill_px_cents) × count / 100  for buys
# (fill_px_cents − side_fair_cents) × count / 100  for sells.
# Rewards = integral of estimated $/hr over time since session start (today).
_ev_state = {"day": "", "seen_ids": set(), "ev_usd": 0.0, "fee_usd": 0.0}
_rewards_state = {"day": "", "last_t": 0.0, "rewards_usd": 0.0}
_ev_lock = threading.Lock()
_rewards_lock = threading.Lock()
REWARDS_STATE_PATH = Path("/Users/wilsonw/Downloads/kalshi_rewards_today.json")


def _utc_today():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def _load_rewards_state():
    """Restore today's accumulated rewards across server restarts. Drops the
    saved value if it's from a previous UTC day."""
    try:
        d = json.loads(REWARDS_STATE_PATH.read_text())
        if d.get("day") == _utc_today():
            _rewards_state["day"] = d["day"]
            _rewards_state["rewards_usd"] = float(d.get("rewards_usd", 0.0))
            _rewards_state["last_t"] = time.time()  # don't backfill the gap
    except FileNotFoundError:
        pass
    except Exception:
        pass


def _save_rewards_state():
    try:
        REWARDS_STATE_PATH.write_text(json.dumps({
            "day": _rewards_state["day"],
            "rewards_usd": _rewards_state["rewards_usd"],
        }))
    except Exception:
        pass


def _ev_for_fill(theo_map, ticker, side, action, count, px_cents):
    """EV in USD for one fill given current theo. side='yes'/'no',
    action='buy'/'sell', px in cents (yes-price for yes side, no-price for no side)."""
    theo_yes = theo_map.get(ticker)
    if theo_yes is None: return None
    side_fair = theo_yes * 100.0 if side == "yes" else (1.0 - theo_yes) * 100.0
    edge = (side_fair - px_cents) if action == "buy" else (px_cents - side_fair)
    return (edge * count) / 100.0


def update_ev_from_fills(fills_clean, theo_map):
    """Accumulate per-fill EV into _ev_state['ev_usd'] for today's UTC day.
    fills_clean entries: {trade_id, ticker, side, action, count, px, fee}.
    Uses trade_id to dedupe; resets totals on new UTC day."""
    today = _utc_today()
    with _ev_lock:
        if _ev_state["day"] != today:
            _ev_state["day"] = today
            _ev_state["seen_ids"] = set()
            _ev_state["ev_usd"] = 0.0
            _ev_state["fee_usd"] = 0.0
        ev_sum = _ev_state["ev_usd"]
        fee_sum = _ev_state["fee_usd"]
        seen = _ev_state["seen_ids"]
        for f in fills_clean:
            tid = f.get("trade_id") or ""
            if not tid or tid in seen: continue
            day_str = (f.get("created") or "")[:10]
            if day_str != today: continue
            ev = _ev_for_fill(theo_map, f.get("ticker", ""),
                              (f.get("side", "") or "").lower(),
                              (f.get("action", "") or "").lower(),
                              int(f.get("count", 0) or 0),
                              float(f.get("px", 0) or 0))
            if ev is None: continue
            seen.add(tid)
            ev_sum += ev
            fee_sum += float(f.get("fee", 0) or 0)
        _ev_state["ev_usd"] = ev_sum
        _ev_state["fee_usd"] = fee_sum
        return ev_sum, fee_sum, len(seen)


def update_rewards_accrual(total_per_hr):
    """Integrate the current $/hr rate over wall-clock since the previous
    tick. Resets on UTC day rollover; persists across server restarts."""
    today = _utc_today()
    now = time.time()
    with _rewards_lock:
        if _rewards_state["day"] != today:
            _rewards_state["day"] = today
            _rewards_state["last_t"] = now
            _rewards_state["rewards_usd"] = 0.0
            _save_rewards_state()
            return 0.0
        dt_h = max(0.0, (now - _rewards_state["last_t"])) / 3600.0
        # Cap dt at 5 minutes to absorb server pauses without distorting the integral
        dt_h = min(dt_h, 5.0 / 60.0)
        _rewards_state["rewards_usd"] += float(total_per_hr or 0.0) * dt_h
        _rewards_state["last_t"] = now
        _save_rewards_state()
        return _rewards_state["rewards_usd"]


_load_rewards_state()


# Overnight session tracker — independent of UTC day rollover. Tracks
# cumulative estimated incentives ($/hr integrated over wall-clock) and
# fill EV+fees since the user-set anchor. Persists across server restarts.
_overnight_state = {
    "start_ts": 0.0,
    "last_t":   0.0,
    "rewards_usd": 0.0,
    "ev_usd":   0.0,
    "fee_usd":  0.0,
    "seen_ids": set(),
    "seen_order": [],
}
_overnight_lock = threading.Lock()
OVERNIGHT_STATE_PATH = Path("/Users/wilsonw/Downloads/kalshi_overnight.json")


def _load_overnight_state():
    try:
        d = json.loads(OVERNIGHT_STATE_PATH.read_text())
        _overnight_state["start_ts"]    = float(d.get("start_ts", 0.0) or 0.0)
        _overnight_state["rewards_usd"] = float(d.get("rewards_usd", 0.0) or 0.0)
        _overnight_state["ev_usd"]      = float(d.get("ev_usd", 0.0) or 0.0)
        _overnight_state["fee_usd"]     = float(d.get("fee_usd", 0.0) or 0.0)
        ids = list(d.get("seen_ids", []) or [])
        ids = ids[-2000:]
        _overnight_state["seen_ids"]    = set(ids)
        _overnight_state["seen_order"]  = ids
        _overnight_state["last_t"]      = time.time()
    except FileNotFoundError:
        pass
    except Exception:
        pass


def _save_overnight_state():
    try:
        OVERNIGHT_STATE_PATH.write_text(json.dumps({
            "start_ts":    _overnight_state["start_ts"],
            "rewards_usd": _overnight_state["rewards_usd"],
            "ev_usd":      _overnight_state["ev_usd"],
            "fee_usd":     _overnight_state["fee_usd"],
            "seen_ids":    _overnight_state["seen_order"][-2000:],
        }))
    except Exception:
        pass


def _overnight_snapshot_locked():
    start = _overnight_state["start_ts"]
    return {
        "start_ts":    start,
        "elapsed_s":   max(0.0, time.time() - start) if start else 0.0,
        "rewards_usd": _overnight_state["rewards_usd"],
        "ev_usd":      _overnight_state["ev_usd"],
        "fee_usd":     _overnight_state["fee_usd"],
        "net_usd":     _overnight_state["rewards_usd"] + _overnight_state["ev_usd"] - _overnight_state["fee_usd"],
        "running":     bool(start),
    }


def overnight_reset():
    """Anchor a new overnight session at now. Zeros all tallies."""
    now = time.time()
    with _overnight_lock:
        _overnight_state["start_ts"]    = now
        _overnight_state["last_t"]      = now
        _overnight_state["rewards_usd"] = 0.0
        _overnight_state["ev_usd"]      = 0.0
        _overnight_state["fee_usd"]     = 0.0
        _overnight_state["seen_ids"]    = set()
        _overnight_state["seen_order"]  = []
        _save_overnight_state()
        return {"ok": True, **_overnight_snapshot_locked()}


def overnight_stop():
    with _overnight_lock:
        _overnight_state["start_ts"] = 0.0
        _save_overnight_state()
        return {"ok": True, **_overnight_snapshot_locked()}


def overnight_snapshot():
    with _overnight_lock:
        return _overnight_snapshot_locked()


def update_overnight_accrual(total_per_hr, fills_clean, theo_map):
    """Tick the overnight integrator and fold in new fills (created_time >=
    start_ts) into ev/fee tallies. No-op when not running."""
    now = time.time()
    with _overnight_lock:
        if not _overnight_state["start_ts"]:
            return _overnight_snapshot_locked()
        dt_h = max(0.0, (now - _overnight_state["last_t"])) / 3600.0
        dt_h = min(dt_h, 5.0 / 60.0)
        _overnight_state["rewards_usd"] += float(total_per_hr or 0.0) * dt_h
        _overnight_state["last_t"] = now
        seen = _overnight_state["seen_ids"]
        for f in fills_clean:
            tid = f.get("trade_id") or ""
            if not tid or tid in seen: continue
            f_ts = _parse_iso_ts(f.get("created"))
            if f_ts is None or f_ts < _overnight_state["start_ts"]: continue
            ev = _ev_for_fill(theo_map, f.get("ticker", ""),
                              (f.get("side", "") or "").lower(),
                              (f.get("action", "") or "").lower(),
                              int(f.get("count", 0) or 0),
                              float(f.get("px", 0) or 0))
            if ev is None: continue
            seen.add(tid)
            _overnight_state["seen_order"].append(tid)
            if len(_overnight_state["seen_order"]) > 2500:
                _overnight_state["seen_order"] = _overnight_state["seen_order"][-2000:]
                _overnight_state["seen_ids"] = set(_overnight_state["seen_order"])
            _overnight_state["ev_usd"]  += ev
            _overnight_state["fee_usd"] += float(f.get("fee", 0) or 0)
        _save_overnight_state()
        return _overnight_snapshot_locked()


_load_overnight_state()


def _parse_iso_ts(s):
    """Parse ISO-8601 with optional 'Z' suffix → epoch seconds, or None.
    Distinct from _parse_iso (line ~711) which returns a datetime."""
    if not s: return None
    try:
        return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _load_theos():
    """Load per-ticker YES probabilities from THEOS_DIR/*.json.
    Each file: {"event": ..., "strikes": {ticker: prob}, "confidence": ...,
                "blackouts": [{"start":ISO, "end":ISO, "reason":str}, ...]}
    Cached; reloads when any file's mtime changes."""
    if not THEOS_DIR.exists(): return {}, {}
    files = sorted(p for p in THEOS_DIR.glob("*.json") if not p.name.startswith("_"))
    latest = max((p.stat().st_mtime for p in files), default=0.0)
    with _theos_lock:
        if latest == _theos_cache["mtime"] and _theos_cache["map"]:
            return _theos_cache["map"], _theos_cache["meta"]
    m, meta = {}, {}
    for p in files:
        try:
            d = json.loads(p.read_text())
        except Exception:
            continue
        ev = d.get("event") or p.stem
        strikes = d.get("strikes") or {}
        for tk, prob in strikes.items():
            try:
                pf = float(prob)
                if pf != pf: continue
                m[str(tk).strip().upper()] = max(0.0, min(1.0, pf))
            except (TypeError, ValueError):
                continue
        blackouts = []
        for bo in (d.get("blackouts") or []):
            s_ts = _parse_iso_ts(bo.get("start"))
            e_ts = _parse_iso_ts(bo.get("end"))
            if s_ts is None or e_ts is None or e_ts <= s_ts: continue
            blackouts.append((s_ts, e_ts, str(bo.get("reason") or "")))
        try:
            band_cents = float(d.get("band_cents") or 0.0)
        except (TypeError, ValueError):
            band_cents = 0.0
        band_cents = max(0.0, min(50.0, band_cents))
        meta[ev] = {"confidence": (d.get("confidence") or "").strip().lower(),
                    "as_of": d.get("as_of"),
                    "as_of_ts": _parse_iso_ts(d.get("as_of")),
                    "underlying": d.get("underlying"),
                    "band_cents": band_cents,
                    "blackouts": blackouts}
    with _theos_lock:
        _theos_cache["map"] = m
        _theos_cache["meta"] = meta
        _theos_cache["mtime"] = latest
    return m, meta


def _event_blackout_active(meta, ev, now_ts=None):
    """Return (reason, end_ts) if `ev` is currently in a blackout window, else None."""
    if now_ts is None: now_ts = time.time()
    info = (meta or {}).get(ev) or {}
    for s, e, reason in (info.get("blackouts") or []):
        if s <= now_ts <= e:
            return reason, e
    return None


_event_models_cache = {"map": {}, "mtime": 0.0}
_event_models_lock = threading.Lock()


def _norm_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _inv_norm_cdf(p):
    """Beasley-Springer-Moro approximation of inverse standard-Normal CDF."""
    p = max(min(p, 1 - 1e-9), 1e-9)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
          1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
          6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
          3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p <= phigh:
        q = p - 0.5; r = q*q
        return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
               (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
            ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)


def _fit_event_model(strikes_dict):
    """Recover a model from {ticker: P(YES)} strike map.
    Returns ('normal', mu, sigma), ('binary', p), or None.
    Single-strike events become Bernoulli; multi-strike fit a Normal in strike-space."""
    pts = []
    for tk, p in strikes_dict.items():
        s = parse_strike(str(tk))
        if s is None:
            continue
        if 0.005 < float(p) < 0.995:
            pts.append((s, _inv_norm_cdf(1.0 - float(p))))
    if len(pts) >= 3:
        n = len(pts)
        sz = sum(z for _, z in pts); ss = sum(s for s, _ in pts)
        szz = sum(z*z for _, z in pts); ssz = sum(s*z for s, z in pts)
        denom = n*szz - sz*sz
        if abs(denom) > 1e-12:
            sigma = (n*ssz - ss*sz) / denom
            mu = (ss - sigma*sz) / n
            if sigma > 0:
                return ("normal", mu, sigma)
    if len(strikes_dict) == 1:
        ((_, p),) = list(strikes_dict.items())
        return ("binary", float(p))
    # Degenerate: every strike clipped at the same extreme (V is way outside the grid)
    probs = [float(v) for v in strikes_dict.values()]
    if probs:
        if max(probs) <= 0.01:
            return ("deterministic_low",)   # V below every strike: all NO wins
        if min(probs) >= 0.99:
            return ("deterministic_high",)  # V above every strike: all YES wins
    return None


def _load_event_models():
    """Per-event distribution model: {event_ticker: ('normal', mu, sigma) | ('binary', p)}."""
    if not THEOS_DIR.exists(): return {}
    files = sorted(p for p in THEOS_DIR.glob("*.json") if not p.name.startswith("_"))
    latest = max((p.stat().st_mtime for p in files), default=0.0)
    with _event_models_lock:
        if latest == _event_models_cache["mtime"] and _event_models_cache["map"]:
            return _event_models_cache["map"]
    out = {}
    for p in files:
        try: d = json.loads(p.read_text())
        except Exception: continue
        ev = d.get("event") or p.stem
        strikes = d.get("strikes") or {}
        m = _fit_event_model(strikes)
        if m is not None:
            out[ev] = m
    with _event_models_lock:
        _event_models_cache["map"] = out
        _event_models_cache["mtime"] = latest
    return out


def compute_portfolio_dist(rows):
    """Return {ev_usd, std_usd, percentiles, bins, per_event} for current portfolio.
    Treats events as independent. Within an event the underlying value V resolves all
    strikes jointly (perfect correlation): for V in (s_k, s_{k+1}], YES wins on strikes
    s_j with j<=k and NO wins on j>=k+1."""
    models = _load_event_models()
    by_event = defaultdict(list)
    for r in rows:
        qty = r.get("my_position") or 0
        if qty == 0: continue
        t = r.get("ticker", "")
        ev = event_ticker_of(t)
        s = parse_strike(t)
        if s is None:
            continue
        by_event[ev].append({"strike": s, "side": r["side"].lower(), "qty": qty})
    event_dists = {}
    for ev, pl in by_event.items():
        m = models.get(ev)
        if m is None: continue
        kind = m[0]
        if kind == "binary":
            p_yes = float(m[1])
            yes_pay = sum(p["qty"] for p in pl if p["side"] == "yes")
            no_pay  = sum(p["qty"] for p in pl if p["side"] == "no")
            event_dists[ev] = [(yes_pay, p_yes), (no_pay, 1.0 - p_yes)]
        elif kind == "deterministic_low":
            no_pay = sum(p["qty"] for p in pl if p["side"] == "no")
            event_dists[ev] = [(no_pay, 1.0)]
        elif kind == "deterministic_high":
            yes_pay = sum(p["qty"] for p in pl if p["side"] == "yes")
            event_dists[ev] = [(yes_pay, 1.0)]
        elif kind == "normal":
            mu, sigma = float(m[1]), float(m[2])
            strikes = sorted({p["strike"] for p in pl})
            n = len(strikes)
            if n == 0 or sigma <= 0: continue
            cdf = [_norm_cdf((s - mu) / sigma) for s in strikes]
            probs = [cdf[0]] + [cdf[i+1] - cdf[i] for i in range(n-1)] + [1.0 - cdf[-1]]
            dist = []
            for k in range(n + 1):
                payout = 0
                for p in pl:
                    j = strikes.index(p["strike"])
                    yes_wins = (k >= j + 1)
                    if (yes_wins and p["side"] == "yes") or ((not yes_wins) and p["side"] == "no"):
                        payout += p["qty"]
                dist.append((payout, probs[k]))
            event_dists[ev] = dist
    if not event_dists:
        return None
    port = [(0, 1.0)]
    for d in event_dists.values():
        out = defaultdict(float)
        for x1, p1 in port:
            for x2, p2 in d:
                out[x1 + x2] += p1 * p2
        port = sorted(out.items())
    mean = sum(x*p for x, p in port)
    var  = sum((x-mean)**2 * p for x, p in port)
    cumu = []; acc = 0.0
    for x, p in port:
        acc += p; cumu.append((x, acc))
    def pct(q):
        for x, c in cumu:
            if c >= q: return x
        return cumu[-1][0]
    xs = [x for x, _ in port]
    lo, hi = min(xs), max(xs)
    nbins = 60
    binw = max((hi - lo) / nbins, 1.0)
    bins_p = [0.0] * nbins
    for x, p in port:
        idx = min(int((x - lo) / binw), nbins - 1)
        if idx < 0: idx = 0
        bins_p[idx] += p
    bins = [{"x": lo + (i + 0.5) * binw, "p": bins_p[i]}
            for i in range(nbins) if bins_p[i] > 0]
    per_event = {}
    for ev, d in event_dists.items():
        em = sum(x*p for x, p in d)
        es = math.sqrt(sum((x-em)**2 * p for x, p in d))
        per_event[ev] = {"mean": round(em, 2), "std": round(es, 2)}
    return {
        "ev_usd":  round(mean, 2),
        "std_usd": round(math.sqrt(var), 2),
        "var_usd2": round(var, 2),
        "p5":  pct(0.05), "p25": pct(0.25), "p50": pct(0.50),
        "p75": pct(0.75), "p95": pct(0.95),
        "min": lo, "max": hi,
        "bin_width": binw,
        "bins": bins,
        "per_event": per_event,
    }


def _load_followed():
    try:
        data = json.loads(FOLLOWED_PATH.read_text())
        if isinstance(data, list): return set(str(x).strip() for x in data if x)
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return set(DEFAULT_FOLLOWED)


_followed_lock = threading.Lock()
EXTRA_EVENT_TICKERS = _load_followed()  # explicitly-followed events outside the ones I'm trading


def _save_followed():
    with _followed_lock:
        snapshot = sorted(EXTRA_EVENT_TICKERS)
    FOLLOWED_PATH.write_text(json.dumps(snapshot, indent=2))


def follow_event(ticker):
    t = (ticker or "").strip().upper()
    if not t: return {"ok": False, "error": "empty ticker"}
    with _followed_lock:
        EXTRA_EVENT_TICKERS.add(t)
    _save_followed()
    return {"ok": True, "followed": sorted(EXTRA_EVENT_TICKERS)}


def unfollow_event(ticker):
    t = (ticker or "").strip().upper()
    with _followed_lock:
        EXTRA_EVENT_TICKERS.discard(t)
    _save_followed()
    return {"ok": True, "followed": sorted(EXTRA_EVENT_TICKERS)}


def _load_blocked():
    try:
        data = json.loads(BLOCKED_PATH.read_text())
        if isinstance(data, list): return set(str(x).strip() for x in data if x)
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return set()


_blocked_lock = threading.Lock()
BLOCKED_MARKETS = _load_blocked()  # market tickers the penny bot must never trade


def _save_blocked():
    with _blocked_lock:
        snapshot = sorted(BLOCKED_MARKETS)
    BLOCKED_PATH.write_text(json.dumps(snapshot, indent=2))


def is_blocked(ticker):
    with _blocked_lock:
        return ticker in BLOCKED_MARKETS


def block_market(ticker):
    """Cancel all resting BUY orders on `ticker` (both sides) and add to blocklist."""
    t = (ticker or "").strip().upper()
    if not t: return {"ok": False, "error": "empty ticker"}
    with _blocked_lock:
        BLOCKED_MARKETS.add(t)
    _save_blocked()
    res = cancel_market_all(t)
    cancelled = res.get("count", 0)
    errors = [] if res.get("ok") else ["cancel failed"]
    return {"ok": True, "ticker": t, "cancelled": cancelled,
            "errors": errors, "blocked": sorted(BLOCKED_MARKETS)}


def unblock_market(ticker):
    t = (ticker or "").strip().upper()
    with _blocked_lock:
        BLOCKED_MARKETS.discard(t)
    _save_blocked()
    return {"ok": True, "blocked": sorted(BLOCKED_MARKETS)}


def _load_tripped():
    try:
        d = json.loads(TRIPPED_PATH.read_text())
        if isinstance(d, dict): return d
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return {}


_tripped_lock = threading.Lock()
TRIPPED_EVENTS = _load_tripped()  # {event_ticker: {ts, reason, dollars, contracts}}


def _save_tripped():
    snap = _tripped_snapshot()
    TRIPPED_PATH.write_text(json.dumps(snap, indent=2, sort_keys=True))


def _tripped_snapshot():
    with _tripped_lock:
        return dict(TRIPPED_EVENTS)


def trip_event(ev, reason, dollars=0.0, contracts=0):
    """Cancel every open order on `ev` and add it to TRIPPED_EVENTS so the
    bot stops market-making it. Persists to disk — survives restart."""
    ev = (ev or "").strip().upper()
    if not ev:
        return {"ok": False, "error": "empty event"}
    res = cancel_event_all(ev)
    with _tripped_lock:
        TRIPPED_EVENTS[ev] = {"ts": time.time(), "reason": str(reason),
                              "dollars": round(float(dollars), 2),
                              "contracts": int(contracts)}
    _save_tripped()
    return {"ok": True, "event": ev, "cancelled": res.get("count", 0),
            "tripped": dict(TRIPPED_EVENTS)}


def untrip_event(ev):
    ev = (ev or "").strip().upper()
    with _tripped_lock:
        if ev:
            TRIPPED_EVENTS.pop(ev, None)
        else:
            TRIPPED_EVENTS.clear()
    _save_tripped()
    return {"ok": True, "tripped": dict(TRIPPED_EVENTS)}


_event_title_cache = {}
_event_title_lock = threading.Lock()
_event_title_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="evtitle")


def get_event_title(event_ticker):
    """Return the human-readable event title (and sub_title), cached forever.
    Falls back to '' on any error so the slug remains visible."""
    with _event_title_lock:
        cached = _event_title_cache.get(event_ticker)
    if cached is not None:
        return cached
    title, sub = "", ""
    try:
        r = signed_request("GET", f"/events/{event_ticker}")
        if r.status_code < 400:
            ev = r.json().get("event") or {}
            title = ev.get("title") or ""
            sub   = ev.get("sub_title") or ""
    except Exception:
        pass
    val = {"title": title, "sub_title": sub}
    with _event_title_lock:
        _event_title_cache[event_ticker] = val
    return val


def search_incentivized_events(query):
    """Group active rewards programs by event ticker; substring-match on query."""
    rewards = get_reward_map_cached()
    q = (query or "").strip().upper()
    by_event = defaultdict(lambda: {"strikes": 0, "total_per_hr": 0.0, "soonest_end_h": None})
    for ticker, info in rewards.items():
        ev = event_ticker_of(ticker)
        if q and q not in ev and q not in ticker.upper(): continue
        a = by_event[ev]
        a["strikes"] += 1
        a["total_per_hr"] += info["hourly_rate"]
        if a["soonest_end_h"] is None or info["ends_in_h"] < a["soonest_end_h"]:
            a["soonest_end_h"] = info["ends_in_h"]
    with _followed_lock:
        followed = set(EXTRA_EVENT_TICKERS)
    out = [{"event": ev, "strikes": a["strikes"], "total_per_hr": a["total_per_hr"],
            "ends_in_h": a["soonest_end_h"], "followed": ev in followed}
           for ev, a in by_event.items()]
    out.sort(key=lambda e: -e["total_per_hr"])
    out = out[:50]
    titles = list(_event_title_executor.map(lambda e: get_event_title(e["event"]), out))
    for row, t in zip(out, titles):
        row["title"] = t.get("title", "")
        row["sub_title"] = t.get("sub_title", "")
    return {"results": out, "followed": sorted(followed)}

PK = serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)

# Shared session with a generous connection pool — without this, hammering
# 1000+ markets in parallel causes DNS resolver / socket exhaustion.
_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=2)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def signed_request(method, path, *, params=None, body=None):
    sig_path = "/trade-api/v2" + path
    ts = str(int(time.time() * 1000))
    msg = (ts + method + sig_path).encode()
    sig = PK.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    h = {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    last_exc = None
    last_resp = None
    for attempt in range(4):
        try:
            r = _session.request(method, BASE + path, headers=h,
                                 params=params, json=body, timeout=15)
            if r.status_code == 429 and attempt < 3:
                time.sleep(1.0 + 0.6 * attempt)
                continue
            return r
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            time.sleep(0.4 * (attempt + 1))
    if last_exc: raise last_exc
    return last_resp


def list_resting_orders():
    out, cursor = [], None
    while True:
        p = {"status": "resting", "limit": 200}
        if cursor: p["cursor"] = cursor
        r = signed_request("GET", "/portfolio/orders", params=p)
        if r.status_code >= 400: raise RuntimeError(f"orders {r.status_code}: {r.text[:200]}")
        d = r.json()
        out += d.get("orders", [])
        cursor = d.get("cursor") or None
        if not cursor: break
    return out


def list_event_markets(event_ticker):
    """All open markets (strikes) for an event."""
    out, cursor = [], None
    while True:
        p = {"event_ticker": event_ticker, "limit": 200, "status": "open"}
        if cursor: p["cursor"] = cursor
        r = signed_request("GET", "/markets", params=p)
        if r.status_code >= 400: return out
        d = r.json()
        out += d.get("markets", [])
        cursor = d.get("cursor") or None
        if not cursor: break
    return out


def event_ticker_of(market_ticker):
    parts = market_ticker.split("-")
    return "-".join(parts[:-1]) if len(parts) > 1 else market_ticker


def parse_strike(ticker):
    """KXTRUFBFST-26APR27-T89.50 -> 89.5; gas -4.245 -> 4.245."""
    last = ticker.split("-")[-1]
    if not last:
        return None
    if last[0].upper() in ("T", "P"):
        last = last[1:]
    try:
        return float(last)
    except ValueError:
        return None
    return None


def detect_arbs(rows):
    """Find pairs (YES@X, NO@Y) within the same event where X >= Y and our
    bids sum to > 100¢. These are guaranteed losers: in the (Y, X] range
    neither leg pays out, but we paid yes_px + no_px > $1."""
    by_ev = defaultdict(lambda: {"yes": [], "no": []})
    for r in rows:
        if not r.get("has_orders"): continue
        strike = parse_strike(r["ticker"])
        if strike is None: continue
        if r.get("my_top_px") is None: continue
        side = r["side"].lower()
        ev = event_ticker_of(r["ticker"])
        by_ev[ev][side].append({"ticker": r["ticker"], "strike": strike,
                                "px": r["my_top_px"], "size": r.get("my_total_sz", 0)})
    arbs = []
    for ev, sides in by_ev.items():
        for y in sides["yes"]:
            for n in sides["no"]:
                if y["strike"] >= n["strike"] and y["px"] + n["px"] > 100:
                    arbs.append({
                        "event": ev,
                        "yes_ticker": y["ticker"], "yes_strike": y["strike"],
                        "yes_px": y["px"], "yes_size": y["size"],
                        "no_ticker": n["ticker"], "no_strike": n["strike"],
                        "no_px": n["px"], "no_size": n["size"],
                        "sum": y["px"] + n["px"],
                    })
    arbs.sort(key=lambda a: -a["sum"])
    return arbs


def would_create_arb(rows, ticker, side, target_px, pending=None):
    """True iff placing a `target_px`¢ bid on (ticker, side) would form an
    arb pair with any of our existing resting bids on the conjugate side
    (read from `rows`) OR any bid we placed earlier in this same cycle
    (`pending`, list of (ticker, side, px))."""
    my_strike = parse_strike(ticker)
    if my_strike is None: return False
    ev = event_ticker_of(ticker)
    other = "no" if side == "yes" else "yes"

    other_orders = []  # (strike, px)
    for r in rows:
        if event_ticker_of(r["ticker"]) != ev: continue
        if r["side"].lower() != other: continue
        if not r.get("has_orders"): continue
        other_px = r.get("my_top_px")
        other_strike = parse_strike(r["ticker"])
        if other_px is None or other_strike is None: continue
        other_orders.append((other_strike, other_px))
    if pending:
        for (pt, ps, ppx) in pending:
            if event_ticker_of(pt) != ev or ps != other: continue
            ostrike = parse_strike(pt)
            if ostrike is None: continue
            other_orders.append((ostrike, ppx))

    for (other_strike, other_px) in other_orders:
        if side == "yes":
            yes_strike, yes_px, no_strike, no_px = my_strike, target_px, other_strike, other_px
        else:
            yes_strike, yes_px, no_strike, no_px = other_strike, other_px, my_strike, target_px
        if yes_strike >= no_strike and yes_px + no_px > 100:
            return True
    return False


def list_incentive_programs():
    """All currently-active liquidity reward programs (paginates fully)."""
    out, cursor = [], None
    while True:
        p = {"status": "active", "type": "liquidity", "limit": 1000}
        if cursor: p["cursor"] = cursor
        r = signed_request("GET", "/incentive_programs", params=p)
        if r.status_code >= 400:
            raise RuntimeError(f"incentives {r.status_code}: {r.text[:200]}")
        d = r.json()
        out += d.get("incentive_programs", []) or []
        cursor = d.get("next_cursor") or None
        if not cursor: break
    return out


def _parse_iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def build_reward_map():
    """market_ticker -> {hourly_rate, target_size, period_reward_usd, end, ends_in_h, program_id}."""
    progs = list_incentive_programs()
    out = {}
    now = dt.datetime.now(dt.timezone.utc)
    for p in progs:
        try:
            t = p["market_ticker"]
            start = _parse_iso(p["start_date"])
            end = _parse_iso(p["end_date"])
            hours = (end - start).total_seconds() / 3600.0
            if hours <= 0: continue
            period_usd = float(p.get("period_reward", 0)) / 10000.0  # centi-cents -> USD
            try:
                target = int(float(p.get("target_size_fp") or DEFAULT_TARGET_SIZE))
            except (TypeError, ValueError):
                target = DEFAULT_TARGET_SIZE
            ends_in_h = (end - now).total_seconds() / 3600.0
            # If multiple overlapping programs cover the same market, keep the
            # one ending soonest (most "current" rate).
            existing = out.get(t)
            if existing and existing["ends_in_h"] < ends_in_h:
                continue
            out[t] = {
                "hourly_rate": period_usd / hours,
                "target_size": max(1, target),
                "period_reward_usd": period_usd,
                "end": p["end_date"],
                "ends_in_h": ends_in_h,
                "program_id": p["id"],
            }
        except Exception:
            continue
    return out


# ---------- Auto-penny bot ----------

class PennyBot:
    """Periodically scans incentivized markets where my share is low and the
    spread is wide, and places small post-only penny bids at best+1¢."""
    def __init__(self):
        self.enabled = False
        self.interval = 20          # seconds between scans
        self.spread_min = 1         # ¢ — only penny if (ask - bid) > this
        self.share_max_pct = 90.0   # only penny if my share < this
        self.size = 12              # contracts per penny order
        self.cooldown_s = 0         # min seconds between prospecting pennies on same (ticker, side)
        self.defend_cooldown_s = 20 # short cooldown when defending against a jumper
        self.gap_min = 5            # ¢ — also penny if (best_bid - my_top_px) >= this,
                                    # regardless of share. Catches "I'm queued behind
                                    # a thin wall at the top while sitting at 3¢."
        self.auto_resolve_arbs = True
        self.arb_interval = 4       # seconds between arb sweeps
        self.min_edge_cents = -2    # hard floor: skip any placement where
                                    # (theo fair price - target price) is at or
                                    # below this. Rows without a theo are
                                    # exempt (no model → no rule).
        # Edge-scaled sizing: when the theo says a side is significantly
        # underpriced, multiply base self.size by these tier multipliers so
        # we capture more of the rewards. Capped by max_per_cycle and by
        # remaining rewardable capacity (target_size − my_total_sz).
        self.edge_size_multipliers = [(20, 10), (10, 5), (5, 2), (0, 1)]
        self.max_per_cycle = 200
        # Small-takes: cross the spread when conviction is high. Only
        # fires when (a) edge ≥ take_min_edge, (b) ask depth ≤
        # take_depth_max (so we don't print on a thick wall), and (c)
        # the take wouldn't push our long position past inventory_cap.
        # DISABLED 2026-04-29 for unattended overnight runs: takes are the
        # highest-EV-loss feature when a theo is wrong (we burn 15c+ per
        # take with no recovery), and we've recently caught two theos
        # mispriced 30c+. Re-enable manually when human is watching.
        self.take_enabled = False
        self.take_min_edge = 15        # ¢
        self.take_depth_max = 50       # contracts
        self.take_max_per_cycle = 30   # ¢ — cap on size of each take
        # Confidence floor for taking: never cross spread on low/medium-low
        # confidence theos even if take_enabled. Takes are unrecoverable.
        self.take_min_confidence_rank = 2   # 0=low,1=medium-low,2=medium,3=medium-high,4=high
        # Theo staleness gate: skip every phase (penny, take, sell) on a row
        # whose theo as_of is older than this many seconds. Prevents bot from
        # trading on hours-old data when a refresher silently dies.
        self.max_theo_age_s = 14400         # 4 hours
        # Expiry guard: cancel all open orders on a market when its
        # close_time is within this window. Prevents adverse fills as
        # the market approaches resolution.
        self.expiry_guard_s = 3600          # 1 hour
        # Inventory wind-down: when we hold ≥ inventory_offer_threshold
        # contracts on a side, post a passive sell at fair − wind_down_premium.
        self.sell_enabled = True
        self.inventory_cap = 100
        self.inventory_offer_threshold = 30
        self.wind_down_premium = 2     # ¢ below fair for resting sell offers
        # Floor on sell price: Kalshi reserves (100 - sell_px) × count as
        # short-sale margin at order-placement time even for covered
        # closes. At sell_px=1¢ on a 1k-contract long, that's ~$990 in
        # margin lockup for ~$10 of expected wind-down — not worth it.
        # Below this floor, just hold the lottery-ticket position to
        # expiry instead of trying to recycle inventory.
        self.sell_px_min = 5
        # Per-event dollar exposure cap (sum across all strikes/sides). Skips
        # new placements once event total my_position * fair_cents/100 ≥ cap.
        self.dollar_cap_per_event = 150.0
        # Hard fill circuit breaker (per event). When an event's *filled*
        # exposure (positions only — not resting bid notional) crosses
        # either threshold, we cancel every open order on the event and
        # add it to TRIPPED_EVENTS so every phase skips it. Persists to
        # disk so a restart doesn't undo the trip. Defaults sized 2× the
        # soft dollar_cap so the breaker is a safety net, not the primary
        # control. Reset via /api/circuit-reset.
        self.fill_breaker_enabled = True
        self.fill_breaker_dollars = 300.0
        self.fill_breaker_contracts = 2000
        # Sell-only liquidation mode. When True, skip seed/take/penny phases
        # entirely; only post aggressive sell-offers on existing inventory.
        # sell_floor_pct_of_fair sets the floor in fraction-of-fair (0.9 = no
        # lower than 90% of theo).
        self.sell_only = False
        self.sell_floor_pct_of_fair = 0.9
        # Min profit (¢) above avg fill cost when posting wind-down sells.
        # Beats out fair-minus-premium when avg cost is high enough that
        # selling at fair would book a loss.
        self.min_sell_profit_cents = 1
        # Skip placement if confidence ∈ skip_confidences AND spread ≤ skip_conf_spread.
        # Per user: "low" only — medium-low is OK to participate.
        self.skip_confidences = {"low"}
        self.skip_conf_spread = 2
        # Skip extreme-priced strikes: dollar-loss-per-fill dominates reward weight.
        self.skip_mid_lt = 8
        self.skip_mid_gt = 92
        # When defending (jumped/far_below), cancel old orders on this side
        # before placing new at higher target_px. Prevents self-stacking.
        self.cancel_on_defend = True
        self._cooldown = {}         # (ticker, side) -> last_attempt_ts
        self._sell_cooldown = {}    # (ticker, side) -> last sell-offer ts
        self._log = []              # recent {ts, msg, ok}
        self._lock = threading.Lock()
        self.last_cycle_ts = 0
        self.last_cycle_placed = 0
        self.last_cycle_scanned = 0
        self._snapshot_fn = None    # injected by main(): returns App.get_data()
        threading.Thread(target=self._loop, daemon=True).start()
        threading.Thread(target=self._arb_loop, daemon=True).start()

    def set_snapshot_fn(self, fn):
        """Wire the bot to read from the shared App snapshot instead of
        making its own /portfolio/orders + /orderbook calls every cycle."""
        self._snapshot_fn = fn

    def settings(self):
        with self._lock:
            return {
                "enabled": self.enabled, "interval": self.interval,
                "spread_min": self.spread_min, "share_max_pct": self.share_max_pct,
                "size": self.size, "cooldown_s": self.cooldown_s,
                "defend_cooldown_s": self.defend_cooldown_s,
                "gap_min": self.gap_min,
                "auto_resolve_arbs": self.auto_resolve_arbs,
                "arb_interval": self.arb_interval,
                "min_edge_cents": self.min_edge_cents,
                "max_per_cycle": self.max_per_cycle,
                "edge_size_multipliers": list(self.edge_size_multipliers),
                "take_enabled": self.take_enabled,
                "take_min_edge": self.take_min_edge,
                "take_depth_max": self.take_depth_max,
                "take_max_per_cycle": self.take_max_per_cycle,
                "take_min_confidence_rank": self.take_min_confidence_rank,
                "max_theo_age_s": self.max_theo_age_s,
                "expiry_guard_s": self.expiry_guard_s,
                "sell_enabled": self.sell_enabled,
                "inventory_cap": self.inventory_cap,
                "inventory_offer_threshold": self.inventory_offer_threshold,
                "wind_down_premium": self.wind_down_premium,
                "sell_px_min": self.sell_px_min,
                "dollar_cap_per_event": self.dollar_cap_per_event,
                "fill_breaker_enabled": self.fill_breaker_enabled,
                "fill_breaker_dollars": self.fill_breaker_dollars,
                "fill_breaker_contracts": self.fill_breaker_contracts,
                "sell_only": self.sell_only,
                "sell_floor_pct_of_fair": self.sell_floor_pct_of_fair,
                "tripped_events": _tripped_snapshot(),
                "last_cycle_ts": self.last_cycle_ts,
                "last_cycle_placed": self.last_cycle_placed,
                "last_cycle_scanned": self.last_cycle_scanned,
                "log": list(self._log[-30:]),
            }

    def update(self, **kw):
        with self._lock:
            for k, v in kw.items():
                if v is None: continue
                if k == "enabled":           self.enabled = bool(v)
                elif k == "interval":        self.interval = max(10, int(v))
                elif k == "spread_min":      self.spread_min = max(1, int(v))
                elif k == "share_max_pct":   self.share_max_pct = max(0.0, float(v))
                elif k == "size":            self.size = max(1, int(v))
                elif k == "cooldown_s":      self.cooldown_s = max(0, int(v))
                elif k == "defend_cooldown_s": self.defend_cooldown_s = max(0, int(v))
                elif k == "gap_min":         self.gap_min = max(1, int(v))
                elif k == "auto_resolve_arbs": self.auto_resolve_arbs = bool(v)
                elif k == "arb_interval":    self.arb_interval = max(2, int(v))
                elif k == "min_edge_cents":  self.min_edge_cents = int(v)
                elif k == "max_per_cycle":   self.max_per_cycle = max(1, int(v))
                elif k == "take_enabled":    self.take_enabled = bool(v)
                elif k == "take_min_edge":   self.take_min_edge = max(1, int(v))
                elif k == "take_depth_max":  self.take_depth_max = max(1, int(v))
                elif k == "take_max_per_cycle": self.take_max_per_cycle = max(1, int(v))
                elif k == "sell_enabled":    self.sell_enabled = bool(v)
                elif k == "inventory_cap":   self.inventory_cap = max(0, int(v))
                elif k == "inventory_offer_threshold": self.inventory_offer_threshold = max(1, int(v))
                elif k == "wind_down_premium": self.wind_down_premium = max(0, int(v))
                elif k == "sell_px_min":     self.sell_px_min = max(1, int(v))
                elif k == "dollar_cap_per_event": self.dollar_cap_per_event = max(0.0, float(v))
                elif k == "fill_breaker_enabled": self.fill_breaker_enabled = bool(v)
                elif k == "fill_breaker_dollars": self.fill_breaker_dollars = max(0.0, float(v))
                elif k == "sell_only":       self.sell_only = bool(v)
                elif k == "sell_floor_pct_of_fair": self.sell_floor_pct_of_fair = max(0.0, min(1.0, float(v)))
                elif k == "fill_breaker_contracts": self.fill_breaker_contracts = max(0, int(v))
                elif k == "min_sell_profit_cents": self.min_sell_profit_cents = max(0, int(v))
                elif k == "cancel_on_defend": self.cancel_on_defend = bool(v)
                elif k == "take_min_confidence_rank": self.take_min_confidence_rank = max(0, min(4, int(v)))
                elif k == "max_theo_age_s":  self.max_theo_age_s = max(60, int(v))
                elif k == "expiry_guard_s":  self.expiry_guard_s = max(0, int(v))
        return self.settings()

    def _scaled_size(self, edge_cents, row):
        """Pick order size for this row given the theo edge.

        Tiers: bigger edge → bigger multiplier on self.size. Result is
        capped by self.max_per_cycle and by remaining rewardable capacity
        on this (ticker, side), so we never overshoot the reward top-N
        target. Rows without a theo edge fall back to base self.size."""
        base = self.size
        if edge_cents is None:
            scaled = base
        else:
            mult = 1
            for thresh, m in self.edge_size_multipliers:
                if edge_cents >= thresh:
                    mult = m
                    break
            scaled = base * mult
        scaled = min(scaled, self.max_per_cycle)
        target = row.get("target_size") or 0
        mine   = row.get("my_total_sz") or 0
        remain = max(0, target - mine)
        if remain > 0:
            scaled = min(scaled, remain)
        return max(1, int(scaled))

    def _emit(self, msg, ok=True):
        with self._lock:
            self._log.append({"ts": time.time(), "msg": msg, "ok": ok})
            if len(self._log) > 200:
                self._log = self._log[-200:]

    def _loop(self):
        while True:
            try:
                if self.enabled:
                    self._cycle()
            except Exception as e:
                self._emit(f"cycle EXCEPTION {type(e).__name__}: {e}", ok=False)
            time.sleep(max(5, self.interval))

    def _arb_loop(self):
        """Independent fast sweep — runs even when auto-penny is OFF, because
        arbs against existing resting orders are always bad."""
        while True:
            try:
                if self.auto_resolve_arbs and self._snapshot_fn is not None:
                    snap = self._snapshot_fn()
                    arbs = detect_arbs(snap.get("rows") or [])
                    if arbs:
                        result = resolve_arbs_now(self._snapshot_fn)
                        for a in result.get("actions", []):
                            ok = a.get("ok", False)
                            self._emit(f"AUTO-RESOLVE {a['side'].upper()} {a['ticker']} (arb sum={a['sum']}¢, cancelled={a.get('count', 0)})", ok=ok)
            except Exception as e:
                self._emit(f"arb-loop EXCEPTION {type(e).__name__}: {e}", ok=False)
            time.sleep(max(2, self.arb_interval))

    def _cycle(self):
        # Read from the shared App snapshot instead of hitting /portfolio/orders
        # and /markets/.../orderbook ourselves — those calls already happen
        # every ~2s in the data refresh and were the source of 429s.
        if self._snapshot_fn is None: return
        snap = self._snapshot_fn()
        rows = snap.get("rows") or []
        # Track every order we place during THIS cycle so subsequent
        # placements see it. Without this, the cycle iterates rows from a
        # single (frozen) snapshot, and a YES bid placed on row 5 is invisible
        # when we evaluate the NO bid on row 47 — letting both legs of an arb
        # slip in before the next snapshot rolls over.
        pending = []
        now = time.time()
        scanned = 0; placed = 0
        # Pre-compute current $-exposure per event for budget-cap enforcement.
        # exposure = sum(my_position × best_bid_or_avg_cost / 100) per event.
        ev_exposure_usd = defaultdict(float)
        for r in rows:
            pos = r.get("my_position") or 0
            px_basis = r.get("my_avg_cost_cents")
            if px_basis is None: px_basis = r.get("best_bid") or 0
            if pos > 0:
                ev_exposure_usd[event_ticker_of(r["ticker"])] += pos * px_basis / 100.0
            for px, sz in (r.get("my_levels") or []):
                ev_exposure_usd[event_ticker_of(r["ticker"])] += int(sz or 0) * int(px or 0) / 100.0
        # Fill circuit breaker — uses FILLED positions only (not resting
        # notional) so a thick bid stack can't false-trip. When an event
        # crosses fill_breaker_dollars or fill_breaker_contracts, cancel
        # every open order on it and trip it permanently until a human
        # calls /api/circuit-reset. Defense for unattended overnight runs.
        if self.fill_breaker_enabled:
            ev_filled_usd = defaultdict(float)
            ev_filled_ct  = defaultdict(int)
            for r in rows:
                pos = r.get("my_position") or 0
                if pos <= 0: continue
                ev_f = event_ticker_of(r["ticker"])
                cost = r.get("my_avg_cost_cents") or r.get("best_bid") or 0
                ev_filled_usd[ev_f] += pos * cost / 100.0
                ev_filled_ct[ev_f]  += pos
            for ev_f, usd in ev_filled_usd.items():
                if ev_f in TRIPPED_EVENTS: continue
                ct = ev_filled_ct[ev_f]
                if usd >= self.fill_breaker_dollars or ct >= self.fill_breaker_contracts:
                    res = trip_event(ev_f, f"breaker: ${usd:.0f}/{ct}ct",
                                     dollars=usd, contracts=ct)
                    self._emit(f"FILL-BREAKER {ev_f}: ${usd:.0f}/{ct}ct "
                               f"(thr ${self.fill_breaker_dollars:.0f}/"
                               f"{self.fill_breaker_contracts}ct), cancelled "
                               f"{res.get('cancelled', 0)} order(s) — TRIPPED",
                               ok=False)
        # Blackout: pull liquidity around known discrete-vol events. We
        # cancel any open orders on a row in blackout and skip every phase
        # (no take, no sell-offer, no penny). One blanket-cancel per
        # (ticker, side) per cycle — Kalshi treats cancel of an
        # already-cancelled order as a no-op so this is idempotent.
        blackout_logged = set()
        followed_now = _load_followed()
        for r in rows:
            if r.get("blocked"): continue
            if event_ticker_of(r["ticker"]) in TRIPPED_EVENTS: continue
            best_bid, best_ask = r.get("best_bid"), r.get("best_ask")
            ticker_p2 = r["ticker"]; side_p2 = r["side"].lower()
            fair_p2 = r.get("fair_cents")
            my_pos_row = r.get("my_position", 0) or 0
            my_resting_buy_qty = r.get("my_total_sz", 0) or 0
            # Followed-list gate: only MM events explicitly in the followed
            # list. Dropped events still appear in row scope (we have
            # positions/orders there) but get treated like sell-only —
            # cancel any resting buys, skip take/seed/penny.
            ev_followed = event_ticker_of(ticker_p2) in followed_now
            if not ev_followed:
                ids_unf = (r.get("my_order_ids") or []) + (r.get("my_sell_order_ids") or [])
                if ids_unf:
                    cancelled_unf = 0
                    for oid in ids_unf:
                        cr = cancel_order(oid)
                        if cr.status_code < 300: cancelled_unf += 1
                    if cancelled_unf:
                        self._emit(f"UNFOLLOWED-CANCEL {ticker_p2} {side_p2.upper()}: cancelled {cancelled_unf} order(s) (event not in followed list)", ok=True)
                continue
            # Sell-only stale-buy reaper. Runs BEFORE blackout/expiry/staleness
            # gates so we still pull resting buys when theo is stale (those
            # gates `continue` past every phase below).
            if self.sell_only:
                buy_ids_pre = r.get("my_order_ids") or []
                if buy_ids_pre:
                    cancelled_pre = 0
                    for oid in buy_ids_pre:
                        cr = cancel_order(oid)
                        if cr.status_code < 300: cancelled_pre += 1
                    if cancelled_pre:
                        self._emit(f"SELL-ONLY-CANCEL {ticker_p2} {side_p2.upper()}: cancelled {cancelled_pre} stale buy order(s)", ok=True)
            if r.get("theo_blackout"):
                ev_b = event_ticker_of(ticker_p2)
                ids = (r.get("my_order_ids") or []) + (r.get("my_sell_order_ids") or [])
                if ids:
                    cancelled = 0
                    for oid in ids:
                        cr = cancel_order(oid)
                        if cr.status_code < 300: cancelled += 1
                    if cancelled and ev_b not in blackout_logged:
                        ends_at = r.get("theo_blackout_ends")
                        ends_str = (dt.datetime.utcfromtimestamp(ends_at).strftime("%H:%MZ")
                                    if ends_at else "?")
                        self._emit(f"BLACKOUT-CANCEL {ev_b}: {cancelled} order(s) (until {ends_str}, {r.get('theo_blackout_reason') or 'no reason'})", ok=True)
                        blackout_logged.add(ev_b)
                continue
            # Expiry guard: when a market is within self.expiry_guard_s of
            # close, cancel all my open orders on it and skip every phase.
            # Settlement-time fills can be at-the-money against a stale theo
            # for a forced loss. Same idempotent cancel pattern as blackout.
            close_ts = r.get("close_time_ts")
            if close_ts is not None and (close_ts - now) <= self.expiry_guard_s:
                ev_e = event_ticker_of(ticker_p2)
                ids = (r.get("my_order_ids") or []) + (r.get("my_sell_order_ids") or [])
                if ids:
                    cancelled = 0
                    for oid in ids:
                        cr = cancel_order(oid)
                        if cr.status_code < 300: cancelled += 1
                    if cancelled:
                        mins_left = max(0, int((close_ts - now) / 60))
                        self._emit(f"EXPIRY-GUARD {ticker_p2} {side_p2.upper()}: cancelled {cancelled} order(s) ({mins_left}m to close)", ok=True)
                continue
            # Theo staleness gate: if our theo is older than self.max_theo_age_s,
            # something upstream broke (refresher dead, file not updating).
            # Skip every phase rather than trade on stale prices.
            as_of_ts = r.get("theo_as_of_ts")
            if r.get("theo_yes_prob") is not None and as_of_ts is None:
                ev_s = event_ticker_of(ticker_p2)
                key = ("stale_missing", ev_s, int(now / 3600))
                if key not in self._cooldown:
                    self._cooldown[key] = now
                    self._emit(f"STALE-THEO {ev_s}: missing/malformed as_of, skipping", ok=False)
                continue
            if as_of_ts is not None and (now - as_of_ts) > self.max_theo_age_s:
                # Don't spam logs — only emit once per (event, hour).
                ev_s = event_ticker_of(ticker_p2)
                hour_bucket = int(now / 3600)
                key = ("stale", ev_s, hour_bucket)
                if key not in self._cooldown:
                    self._cooldown[key] = now
                    age_h = (now - as_of_ts) / 3600.0
                    self._emit(f"STALE-THEO {ev_s}: theo as_of {age_h:.1f}h old > {self.max_theo_age_s/3600:.1f}h max, skipping", ok=False)
                continue
            # Hard inventory cap: if we already hold ≥ cap on this (ticker, side),
            # don't add more risk via takes or new bids. Wind-down (Phase 2b)
            # still runs below to actually shed the position.
            inventory_full = my_pos_row >= self.inventory_cap

            # Phase 2a — Small take when ask is thin and theo says it's
            # underpriced. Crosses the spread (post_only=False). Capped by
            # ask depth, take_max_per_cycle, and remaining inventory_cap
            # headroom so we never grow position past the cap.
            if (self.take_enabled and not self.sell_only and fair_p2 is not None and best_ask is not None):
                take_edge = fair_p2 - best_ask
                ask_depth = r.get("best_ask_size") or 0
                my_pos = r.get("my_position", 0) or 0
                take_headroom = self.inventory_cap - my_pos - my_resting_buy_qty
                # Confidence gate on takes: a wrong theo + a take = unrecoverable
                # loss (we crossed the spread). Only cross on theos we trust.
                conf_take = (r.get("theo_confidence") or "").lower()
                conf_rank = {"low": 0, "medium-low": 1, "medium": 2,
                             "medium-high": 3, "high": 4}.get(conf_take, 0)
                if (take_edge >= self.take_min_edge
                        and conf_rank >= self.take_min_confidence_rank
                        and 0 < ask_depth <= self.take_depth_max
                        and take_headroom > 0
                        and ev_exposure_usd[event_ticker_of(ticker_p2)] < self.dollar_cap_per_event):
                    with self._lock:
                        last_t = self._cooldown.get((ticker_p2, side_p2, "take"), 0)
                    if now - last_t >= self.defend_cooldown_s:
                        ev_remaining_usd = max(0.0, self.dollar_cap_per_event - ev_exposure_usd[event_ticker_of(ticker_p2)])
                        cap_by_budget = int(ev_remaining_usd * 100.0 / max(1, best_ask))
                        take_n = min(ask_depth, self.take_max_per_cycle, take_headroom, cap_by_budget)
                        if take_n >= 1 and not would_create_arb(rows, ticker_p2, side_p2, best_ask, pending=pending):
                            with self._lock:
                                self._cooldown[(ticker_p2, side_p2, "take")] = now
                            resp = place_buy(ticker_p2, side_p2, take_n, best_ask, post_only=False)
                            if resp.status_code < 300:
                                placed += 1
                                pending.append((ticker_p2, side_p2, best_ask))
                                ev_exposure_usd[event_ticker_of(ticker_p2)] += take_n * best_ask / 100.0
                                self._emit(f"TAKE {side_p2.upper()} {ticker_p2} {best_ask}¢×{take_n} (edge=+{take_edge:.0f}¢, depth={ask_depth}, pos {my_pos}→{my_pos+take_n})", ok=True)
                            else:
                                self._emit(f"TAKE FAIL {side_p2.upper()} {ticker_p2} {best_ask}¢×{take_n}: {resp.status_code} {resp.text[:80]}", ok=False)
                            time.sleep(0.25)

            # Phase 2b — Inventory wind-down. Posts a passive sell at:
            #   max(avg_cost + min_sell_profit_cents, fair − wind_down_premium)
            # so we never realize a loss when we have a position. For
            # low-confidence theos we use a lower position threshold so we
            # exit faster.
            conf = (r.get("theo_confidence") or "").lower()
            sell_threshold = self.inventory_offer_threshold
            if conf in ("low", "medium-low") and not self.sell_only:
                sell_threshold = max(5, self.inventory_offer_threshold // 3)
            sell_ok = self.sell_enabled and (fair_p2 is not None or r.get("my_avg_cost_cents") is not None)
            if sell_ok:
                my_pos2 = r.get("my_position", 0) or 0
                avg_c = r.get("my_avg_cost_cents")
                existing_sell_sz = r.get("my_sell_total_sz", 0) or 0
                if my_pos2 >= sell_threshold and existing_sell_sz < my_pos2:
                    if self.sell_only:
                        # Aggressive liquidation: post within ±1¢ of best_ask
                        # (top of offer book). Floor at ceil(0.9*fair) — avg
                        # cost is allowed to lose money. If the floor is more
                        # than 1¢ above best_ask, skip (can't be near top of
                        # book without violating the floor).
                        best_ask_p2 = r.get("best_ask")
                        if best_ask_p2 is None: continue
                        floor_pct = (self.sell_floor_pct_of_fair * fair_p2) if fair_p2 is not None else None
                        floor = int(-(-floor_pct // 1)) if floor_pct is not None else 1
                        if floor > best_ask_p2 + 1:
                            continue
                        sell_px = max(best_ask_p2 - 1, floor)
                        sell_px = max(1, min(99, int(sell_px)))
                    else:
                        floor_profit = (avg_c + self.min_sell_profit_cents) if avg_c is not None else None
                        fair_target  = (fair_p2 - self.wind_down_premium) if fair_p2 is not None else None
                        candidates = [v for v in (floor_profit, fair_target) if v is not None]
                        sell_px = int(round(max(candidates))) if candidates else None
                        if sell_px is None: continue
                        if floor_profit is not None and floor_profit > 99:
                            continue
                        sell_px = max(1, min(99, sell_px))
                    # Skip dust-priced sells: at sell_px < sell_px_min,
                    # Kalshi's (100-px) margin lockup ≫ realized profit
                    # so we'd just churn the API for nothing.
                    already_offered = any(int(px or 0) >= sell_px for px, _ in (r.get("my_sell_levels") or []))
                    if sell_px >= self.sell_px_min and not already_offered and (best_bid is None or sell_px > best_bid):
                        with self._lock:
                            last_s = self._sell_cooldown.get((ticker_p2, side_p2), 0)
                        if now - last_s >= self.cooldown_s:
                            with self._lock:
                                self._sell_cooldown[(ticker_p2, side_p2)] = now
                            sell_n = max(0, my_pos2 - existing_sell_sz)
                            resp = place_sell(ticker_p2, side_p2, sell_n, sell_px)
                            if resp.status_code < 300:
                                placed += 1
                                fair_str = f"fair={fair_p2:.0f}¢" if fair_p2 is not None else "no fair"
                                avg_str  = f"avg={avg_c:.1f}¢" if avg_c is not None else "no avg"
                                self._emit(f"SELL-OFFER {side_p2.upper()} {ticker_p2} {sell_px}¢×{sell_n} ({fair_str}, {avg_str}, conf={conf or 'none'})", ok=True)
                            else:
                                self._emit(f"SELL-OFFER FAIL {side_p2.upper()} {ticker_p2} {sell_px}¢×{sell_n}: {resp.status_code} {resp.text[:80]}", ok=False)
                            time.sleep(0.25)
            if self.sell_only:
                # Skip buy/seed/penny phases entirely — pure liquidation mode.
                # (Stale buy-cancel runs at the top of the loop pre-gates.)
                continue

            # Inventory cap gates everything below (seed + regular penny).
            # Phase 2b sell-offer above already ran — that's how we wind down.
            # Also cancel any resting buy orders on this side; otherwise
            # they'd keep filling and pushing position further past the cap.
            if inventory_full:
                for oid in (r.get("my_order_ids") or []):
                    cr = cancel_order(oid)
                    if cr.status_code < 300:
                        self._emit(f"CAP-CANCEL {side_p2.upper()} {ticker_p2} (pos={my_pos_row}≥{self.inventory_cap})", ok=True)
                continue

            # Seeding empty books at 1¢×300 was the worst dollar-fill source —
            # one wrong-side resolution = $300 gone. Replaced with a tiny
            # foothold seed (1¢ × 25) only when we have a theo and edge is
            # comfortably positive. Skip otherwise.
            if best_bid is None:
                if r.get("my_top_px") is not None: continue
                if best_ask is not None and best_ask <= 1: continue
                fair = r.get("fair_cents")
                if fair is None: continue              # no theo → no seeding
                edge = fair - 1
                if edge < self.take_min_edge: continue # require ≥ 15¢ edge to seed
                ticker = r["ticker"]; side = r["side"].lower()
                ev_seed = event_ticker_of(ticker)
                seed_headroom = self.inventory_cap - my_pos_row - my_resting_buy_qty
                ev_remaining_usd = max(0.0, self.dollar_cap_per_event - ev_exposure_usd[ev_seed])
                cap_by_budget = int(ev_remaining_usd * 100.0)
                seed_n = min(25, self.max_per_cycle, seed_headroom, cap_by_budget)
                if seed_n < 1: continue
                with self._lock:
                    last = self._cooldown.get((ticker, side), 0)
                if now - last < self.cooldown_s: continue
                if would_create_arb(rows, ticker, side, 1, pending=pending):
                    continue
                with self._lock:
                    self._cooldown[(ticker, side)] = now
                scanned += 1
                resp = place_buy(ticker, side, seed_n, 1)
                if resp.status_code < 300:
                    placed += 1
                    pending.append((ticker, side, 1))
                    ev_exposure_usd[ev_seed] += seed_n / 100.0
                    self._emit(f"SEED {side.upper()} {ticker} 1¢×{seed_n} (edge={edge:+.0f}¢, fair {fair:.0f}¢)", ok=True)
                else:
                    self._emit(f"SEED FAIL {side.upper()} {ticker} 1¢×{seed_n}: {resp.status_code} {resp.text[:80]}", ok=False)
                time.sleep(0.25)
                continue

            if best_ask is None: continue
            spread = best_ask - best_bid
            my_top = r.get("my_top_px")
            share_pct = r.get("share_pct", 0)

            # Reasons we'd want to act on this row:
            jumped    = (my_top is not None) and (best_bid > my_top)
            far_below = (my_top is not None) and (best_bid - my_top >= self.gap_min)
            low_share = share_pct < self.share_max_pct and r.get("has_orders")
            no_orders = (my_top is None)
            # Stack-room trigger: if we have rewardable capacity left
            # (my_total_sz < target_size), evaluate the row regardless of
            # the share threshold. The theo edge gate downstream filters
            # out placements that wouldn't be in-edge.
            target_sz = r.get("target_size") or 0
            mine_sz   = r.get("my_total_sz") or 0
            under_target = target_sz > 0 and mine_sz < target_sz
            is_defense = jumped or far_below or low_share
            if not (is_defense or no_orders or under_target): continue

            # --- Selective participation gates ---
            ticker_g = r["ticker"]; side_g = r["side"].lower()
            ev_g = event_ticker_of(ticker_g)
            conf_g = (r.get("theo_confidence") or "").lower()
            mid_g = (best_bid + best_ask) / 2.0
            # 1) Low-confidence + tight spread = stay out (informed flow only).
            if conf_g in self.skip_confidences and spread <= self.skip_conf_spread:
                continue
            # 2) Extreme strikes — dollar-loss-per-fill dominates reward weight.
            if mid_g < self.skip_mid_lt or mid_g > self.skip_mid_gt:
                continue
            # 3) Per-event $-budget cap: stop adding when our total exposure on
            #    this event already meets cap.
            if ev_exposure_usd[ev_g] >= self.dollar_cap_per_event:
                continue
            # Note: spread_min is no longer a hard gate. Every row in the
            # dashboard is on a market we explicitly chose to participate in
            # (followed event or one we already trade), so we always try to
            # place. The bump formula below falls back to joining-at-best on
            # tight spreads where we can't legally outbid without crossing.
            scanned += 1

            # Bump strategy: default join-at-best-bid (no penny-up on tight
            # spreads). When defending and the spread is wide we bump 1/8 of
            # the spread; otherwise we sit at best_bid. Stack-mode never bumps.
            stack_only = under_target and not (is_defense or no_orders)
            if spread <= 1 or stack_only or not is_defense:
                bump = 0
            else:
                bump = max(1, spread // 8)
            target_px = best_bid + bump

            # Offset optimizer: if the queue at best_bid is so deep that we
            # can't get any of our size into the rewardable top-N (target_size)
            # by joining at best_bid, sit one tick below — small reward share
            # if best_bid drains, but near-zero fill probability.
            target_sz_g = r.get("target_size") or 0
            bb_size = r.get("best_bid_size") or 0
            if (bump == 0 and target_sz_g > 0 and bb_size >= 2 * target_sz_g
                    and target_px - 1 >= 1 and target_px - 1 < best_ask):
                # Only step back if we'd otherwise be queue-buried with no
                # share, AND we don't already have orders at best_bid.
                if my_top is None or my_top < target_px:
                    target_px -= 1
                    self._emit(f"OFFSET-BACK {side_g.upper()} {ticker_g} → {target_px}¢ (bb_size={bb_size} ≥ 2×{target_sz_g})", ok=True)
            # No-cross safety: never place a bid at/above the best ask.
            # Combined with post_only=True on the order, this guarantees
            # we cannot take liquidity. Both checks are required because
            # the orderbook can shift between snapshot and submission.
            if target_px >= best_ask: continue
            # If we already lead at/above target_px, only re-place when
            # there's still rewardable capacity (otherwise we'd just be
            # resending the same order). Stack-mode passes through here.
            if my_top is not None and my_top >= target_px and not under_target:
                continue

            ticker = r["ticker"]; side = r["side"].lower()
            # Anti-arb gate: never place a bid that combined with an existing
            # opposite-side bid (resting OR placed earlier this cycle) would
            # form an arb pair — sum > 100¢ across YES_X + NO_Y where X >= Y.
            if would_create_arb(rows, ticker, side, target_px, pending=pending):
                self._emit(f"skip {side.upper()} {ticker} {target_px}¢: would create arb", ok=False)
                continue
            # Theo-edge gate: skip if buying at target_px would be ≥
            # |min_edge_cents| above the model's fair price.
            fair = r.get("fair_cents")
            if fair is not None:
                edge = fair - target_px
                if edge <= self.min_edge_cents:
                    self._emit(f"skip {side.upper()} {ticker} {target_px}¢: edge {edge:+.0f}¢ ≤ {self.min_edge_cents}¢ (fair {fair:.0f}¢)", ok=False)
                    continue
            cooldown = self.defend_cooldown_s if (is_defense or stack_only) else self.cooldown_s
            with self._lock:
                last = self._cooldown.get((ticker, side), 0)
            if now - last < cooldown: continue
            with self._lock:
                self._cooldown[(ticker, side)] = now

            if is_defense:   tag = "DEFEND"
            elif stack_only: tag = "STACK"
            else:            tag = "penny"
            edge_for_size = (fair - target_px) if fair is not None else None
            # Anti-self-stacking: when defending (jumping price), cancel my
            # existing orders on this side first. Without this, old orders
            # at lower prices remain resting → my_total_sz balloons past
            # target_size and dollar exposure compounds across levels.
            effective_resting_qty = my_resting_buy_qty
            if self.cancel_on_defend and is_defense and (r.get("my_order_ids") or []):
                cancelled = 0
                failed = 0
                for oid in r.get("my_order_ids") or []:
                    cr = cancel_order(oid)
                    if cr.status_code < 300:
                        cancelled += 1
                    else:
                        failed += 1
                if cancelled:
                    self._emit(f"PRE-DEFEND-CANCEL {side.upper()} {ticker}: cancelled {cancelled} stale orders", ok=True)
                if failed:
                    self._emit(f"PRE-DEFEND-CANCEL FAIL {side.upper()} {ticker}: {failed} cancel(s) failed; skip replace", ok=False)
                    continue
                effective_resting_qty = 0
            order_size_n = self._scaled_size(edge_for_size, r)
            inv_headroom = self.inventory_cap - my_pos_row - effective_resting_qty
            if inv_headroom <= 0:
                continue
            order_size_n = min(order_size_n, inv_headroom)
            # Cap order size by per-event remaining $-budget so a single
            # placement can't blow through the event cap.
            ev_remaining_usd = max(0.0, self.dollar_cap_per_event - ev_exposure_usd[ev_g])
            if target_px > 0:
                cap_by_budget = int(ev_remaining_usd * 100.0 / target_px)
                if cap_by_budget < 1:
                    continue
                order_size_n = min(order_size_n, cap_by_budget)
            if order_size_n < 1: continue
            resp = place_buy(ticker, side, order_size_n, target_px)
            if resp.status_code < 300:
                placed += 1
                pending.append((ticker, side, target_px))
                ev_exposure_usd[ev_g] += order_size_n * target_px / 100.0
                edge_str = f", edge={edge_for_size:+.0f}¢" if edge_for_size is not None else ""
                self._emit(f"{tag} {side.upper()} {ticker} {target_px}¢×{order_size_n} (spread={spread}¢, share={share_pct:.1f}%, my_top={my_top}{edge_str}, ev$={ev_exposure_usd[ev_g]:.0f})", ok=True)
            else:
                self._emit(f"{tag} FAIL {side.upper()} {ticker} {target_px}¢×{order_size_n}: {resp.status_code} {resp.text[:80]}", ok=False)
            time.sleep(0.25)
        with self._lock:
            self.last_cycle_ts = time.time(); self.last_cycle_scanned = scanned; self.last_cycle_placed = placed


PENNY = PennyBot()


# Incentive-map cache (programs change rarely)
_inc_lock = threading.Lock()
_inc_cache = {"map": None, "ts": 0.0}

def get_reward_map_cached():
    with _inc_lock:
        now = time.time()
        if _inc_cache["map"] is not None and now - _inc_cache["ts"] < INCENTIVE_CACHE_S:
            return _inc_cache["map"]
        m = build_reward_map()
        _inc_cache["map"] = m
        _inc_cache["ts"] = now
        return m


def get_orderbook(ticker):
    r = signed_request("GET", f"/markets/{ticker}/orderbook")
    if r.status_code >= 400: return {"yes": [], "no": []}
    raw = r.json().get("orderbook_fp") or r.json().get("orderbook") or {}
    out = {"yes": [], "no": []}
    for src, dst in (("yes_dollars", "yes"), ("no_dollars", "no"), ("yes", "yes"), ("no", "no")):
        for entry in raw.get(src, []) or []:
            try:
                px = round(float(entry[0]) * 100) if isinstance(entry[0], str) else int(entry[0])
                sz = float(entry[1])
            except (TypeError, ValueError, IndexError):
                continue
            out[dst].append((px, sz))
    out["yes"].sort(key=lambda e: -e[0])
    out["no"].sort(key=lambda e: -e[0])
    return out


def get_fills(limit=30):
    r = signed_request("GET", "/portfolio/fills", params={"limit": limit})
    if r.status_code >= 400: return []
    return r.json().get("fills", []) or []


def get_fills_today(max_pages=10, page_limit=200):
    """Paginate fills for the current UTC day. Stops once a page's oldest
    fill is older than today, so on a steady stream the second-page hit
    is rare."""
    today = _utc_today()
    out = []
    cursor = None
    for _ in range(max_pages):
        params = {"limit": page_limit}
        if cursor: params["cursor"] = cursor
        r = signed_request("GET", "/portfolio/fills", params=params)
        if r.status_code >= 400: break
        j = r.json()
        page = j.get("fills") or []
        if not page: break
        for f in page:
            day_str = (f.get("created_time") or "")[:10]
            if day_str == today:
                out.append(f)
        # Page is reverse-chrono; stop once the oldest entry is before today.
        oldest_day = (page[-1].get("created_time") or "")[:10]
        if oldest_day < today: break
        cursor = j.get("cursor") or ""
        if not cursor: break
    return out


def cancel_order(order_id):
    return signed_request("DELETE", f"/portfolio/orders/{order_id}")


def place_buy(ticker, side, count, price_cents, post_only=True):
    body = {
        "action": "buy",
        "client_order_id": str(uuid.uuid4()),
        "count": int(count),
        "side": side,
        "ticker": ticker,
        "type": "limit",
    }
    if post_only:
        body["post_only"] = True
    if side == "yes":
        body["yes_price"] = int(price_cents)
    else:
        body["no_price"] = int(price_cents)
    return signed_request("POST", "/portfolio/orders", body=body)


def place_sell(ticker, side, count, price_cents, post_only=True):
    """Post a sell offer on `side` (must already hold the contracts).
    `price_cents` is the price for the contracts being sold (same units
    as place_buy: YES sells use yes_price, NO sells use no_price)."""
    body = {
        "action": "sell",
        "client_order_id": str(uuid.uuid4()),
        "count": int(count),
        "side": side,
        "ticker": ticker,
        "type": "limit",
    }
    if post_only:
        body["post_only"] = True
    if side == "yes":
        body["yes_price"] = int(price_cents)
    else:
        body["no_price"] = int(price_cents)
    return signed_request("POST", "/portfolio/orders", body=body)


def list_positions():
    """Return list of position dicts from /portfolio/positions. Each entry
    has fields including `ticker` and `market_position` — a signed int
    where >0 = long YES, <0 = long NO."""
    out, cursor = [], None
    while True:
        p = {"limit": 200}
        if cursor: p["cursor"] = cursor
        r = signed_request("GET", "/portfolio/positions", params=p)
        if r.status_code >= 400:
            return out
        d = r.json()
        out += d.get("market_positions", []) or d.get("positions", []) or []
        cursor = d.get("cursor") or None
        if not cursor: break
    return out


def positions_by_ticker():
    """ticker -> per-side long qty and average cost in cents."""
    pos_map = {}
    for p in list_positions():
        tkr = p.get("ticker")
        if not tkr:
            continue
        raw = p.get("position_fp")
        if raw is None: raw = p.get("position")
        if raw is None: raw = p.get("market_position")
        try:
            mp = int(round(float(raw or 0)))
        except (TypeError, ValueError):
            mp = 0
        try:
            exp_usd = float(p.get("market_exposure_dollars") or 0)
            avg_cents = (exp_usd * 100.0 / abs(mp)) if mp else None
        except (TypeError, ValueError):
            avg_cents = None
        pos_map[tkr] = {"yes": max(0, mp), "no": max(0, -mp), "avg_cents": avg_cents}
    return pos_map


def side_position(pos_map, ticker, side):
    return int((pos_map.get(ticker) or {}).get(side, 0) or 0)


def resting_buy_qty(orders, ticker, side):
    return sum(order_size(o) for o in orders
               if o.get("ticker") == ticker and o.get("side") == side
               and o.get("action") == "buy")


def event_exposure_usd(pos_map, orders=None):
    exposure = defaultdict(float)
    for ticker, sides in pos_map.items():
        avg_cents = sides.get("avg_cents")
        if avg_cents is None:
            continue
        ev = event_ticker_of(ticker)
        exposure[ev] += (int(sides.get("yes", 0) or 0) +
                         int(sides.get("no", 0) or 0)) * avg_cents / 100.0
    for o in orders or []:
        if o.get("action") != "buy":
            continue
        exposure[event_ticker_of(o.get("ticker", ""))] += order_size(o) * order_price_cents(o) / 100.0
    return exposure


def cancel_orders_strict(orders):
    results = []
    for o in orders:
        r = cancel_order(o["order_id"])
        results.append({"id": o["order_id"][:8], "status": r.status_code})
        time.sleep(0.15)
    return results, all(x["status"] < 300 for x in results)


def cancel_side(ticker, side):
    """Cancel all resting BUY orders for a given (ticker, side)."""
    if side not in ("yes", "no"):
        return {"ok": False, "error": f"bad side: {side}"}
    orders = list_resting_orders()
    targets = [o for o in orders
               if o.get("ticker") == ticker and o.get("side") == side
               and o.get("action") == "buy"]
    if not targets:
        return {"ok": False, "error": "no resting buy orders for that market/side"}
    results, ok = cancel_orders_strict(targets)
    return {"ok": ok, "ticker": ticker, "side": side,
            "cancelled": results, "count": len(results)}


def cancel_market_all(ticker):
    orders = list_resting_orders()
    targets = [o for o in orders if o.get("ticker") == ticker]
    if not targets:
        return {"ok": True, "ticker": ticker, "cancelled": [], "count": 0}
    results, ok = cancel_orders_strict(targets)
    return {"ok": ok, "ticker": ticker, "cancelled": results, "count": len(results)}


def cancel_event_all(ev):
    """Cancel every resting order whose market belongs to event `ev`."""
    orders = list_resting_orders()
    targets = [o for o in orders if event_ticker_of(o.get("ticker") or "") == ev]
    if not targets:
        return {"ok": True, "event": ev, "cancelled": [], "count": 0}
    results, ok = cancel_orders_strict(targets)
    return {"ok": ok, "event": ev, "cancelled": results, "count": len(results)}


def resolve_arbs_now(snapshot_fn):
    """For every detected arb pair, cancel the leg where we have less capital
    at risk (smaller px*size). Returns count cancelled."""
    snap = snapshot_fn()
    arbs = detect_arbs(snap.get("rows") or [])
    cancelled = set()
    actions = []
    for a in arbs:
        yes_risk = a["yes_px"] * max(1, a["yes_size"])
        no_risk = a["no_px"] * max(1, a["no_size"])
        if yes_risk <= no_risk:
            t, s = a["yes_ticker"], "yes"
        else:
            t, s = a["no_ticker"], "no"
        if (t, s) in cancelled: continue
        cancelled.add((t, s))
        r = cancel_side(t, s)
        actions.append({"event": a["event"], "ticker": t, "side": s,
                        "ok": bool(r.get("ok")), "count": r.get("count", 0),
                        "sum": a["sum"]})
        time.sleep(0.12)
    return {"ok": True, "arbs_found": len(arbs), "legs_cancelled": len(actions),
            "actions": actions}


def cancel_zero_reward_orders():
    """Cancel every individual resting BUY order that isn't earning rewards
    right now. An order earns nothing if either:
      - the market has no active liquidity-rewards program, OR
      - the cumulative size at strictly-better prices is >= the program's
        target_size (its contracts fall outside the rewardable top)."""
    rewards = get_reward_map_cached()
    orders = list_resting_orders()
    buys = [o for o in orders if o.get("action") == "buy"]
    tickers = list({o["ticker"] for o in buys})
    books = fetch_orderbooks_parallel(tickers)

    losers = []
    for o in buys:
        info = rewards.get(o["ticker"])
        if not info:
            losers.append(o)
            continue
        levels = books.get(o["ticker"], {}).get(o["side"], [])
        ahead = cumulative_strictly_better(levels, order_price_cents(o))
        if ahead >= info["target_size"]:
            losers.append(o)

    results = []
    for o in losers:
        r = cancel_order(o["order_id"])
        results.append({
            "ticker": o["ticker"], "side": o["side"],
            "id": o["order_id"][:8], "px": order_price_cents(o),
            "status": r.status_code,
        })
        time.sleep(0.12)
    return {"ok": True, "scanned": len(buys), "cancelled": len(results), "results": results}


def cancel_bad_edge_orders(threshold_cents=-1):
    """Cancel every resting BUY whose price is worse than the model's fair
    by more than `threshold_cents`. Default -1¢: kill any order where
    (fair - my_px) < -1, i.e. priced ≥ 1¢ above fair. Orders on tickers
    with no theo are exempt (no model → no rule)."""
    theo_map, _ = _load_theos()
    orders = list_resting_orders()
    buys = [o for o in orders if o.get("action") == "buy"]
    losers = []
    for o in buys:
        tkr = (o.get("ticker") or "").strip().upper()
        prob = theo_map.get(tkr)
        if prob is None:
            continue
        side = (o.get("side") or "").lower()
        side_prob = prob if side == "yes" else (1.0 - prob)
        fair = side_prob * 100.0
        my_px = order_price_cents(o)
        edge = fair - my_px
        if edge < threshold_cents:
            losers.append((o, fair, my_px, edge))
    results = []
    for o, fair, my_px, edge in losers:
        r = cancel_order(o["order_id"])
        results.append({
            "ticker": o["ticker"], "side": o["side"],
            "id": o["order_id"][:8], "px": my_px,
            "fair": round(fair, 1), "edge": round(edge, 1),
            "status": r.status_code,
        })
        time.sleep(0.12)
    return {"ok": True, "scanned": len(buys), "cancelled": len(results),
            "threshold": threshold_cents, "results": results}


DEFAULT_NEW_ORDER_SIZE = 300  # contracts for placing on a fresh strike


def guarded_buy_size(ticker, side, price_cents, requested_count, *,
                     orders, pos_map, exposure, theo_map, theo_meta, rewards):
    ev = event_ticker_of(ticker)
    if ticker not in theo_map:
        return 0, "no theo"
    as_of_ts = (theo_meta.get(ev) or {}).get("as_of_ts")
    if as_of_ts is None:
        return 0, "missing/malformed theo as_of"
    age = time.time() - as_of_ts
    if age > PENNY.max_theo_age_s:
        return 0, f"stale theo ({age/3600.0:.1f}h)"
    if _event_blackout_active(theo_meta, ev):
        return 0, "theo blackout"
    info = rewards.get(ticker) or {}
    close_ts = _parse_iso_ts(info.get("end"))
    if close_ts is not None and (close_ts - time.time()) <= PENNY.expiry_guard_s:
        return 0, "inside expiry guard"
    current_pos = side_position(pos_map, ticker, side)
    resting_qty = resting_buy_qty(orders, ticker, side)
    inv_headroom = PENNY.inventory_cap - current_pos - resting_qty
    if inv_headroom <= 0:
        return 0, "inventory cap"
    ev_remaining_usd = max(0.0, PENNY.dollar_cap_per_event - exposure[ev])
    cap_by_budget = int(ev_remaining_usd * 100.0 / max(1, int(price_cents)))
    n = min(int(requested_count), PENNY.max_per_cycle, inv_headroom, cap_by_budget)
    if n < 1:
        return 0, "dollar cap"
    return n, ""


def place_bulk_untraded(event_ticker, side, over_best):
    """Place a fresh order on every market in `event_ticker` where we have no
    resting BUY on `side`. Price = best_bid + over_best (capped no-cross)."""
    if side not in ("yes", "no"):
        return {"ok": False, "error": "bad side"}
    over = int(over_best)
    if not (1 <= over <= MAX_OVER_BEST):
        return {"ok": False, "error": f"over_best must be 1..{MAX_OVER_BEST}"}

    mkts = list_event_markets(event_ticker)
    if not mkts:
        return {"ok": False, "error": "no open markets in event"}
    orders = list_resting_orders()
    have = {(o["ticker"], o["side"]) for o in orders if o.get("action") == "buy"}
    targets = [m["ticker"] for m in mkts if (m["ticker"], side) not in have]
    if not targets:
        return {"ok": False, "error": "already on every strike for that side"}

    books = fetch_orderbooks_parallel(targets)
    rewards = get_reward_map_cached()
    theo_map, theo_meta = _load_theos()
    pos_map = positions_by_ticker()
    exposure = event_exposure_usd(pos_map, orders)
    other = "no" if side == "yes" else "yes"
    placed, results = 0, []
    for t in targets:
        book = books.get(t, {})
        levels = book.get(side, [])
        best = levels[0][0] if levels else 0
        other_levels = book.get(other, [])
        ask = 100 - other_levels[0][0] if other_levels else 100
        target_px = best + over
        if target_px >= ask:
            target_px = ask - 1
        if target_px < 1:
            results.append({"ticker": t, "skipped": True, "reason": "no valid price"})
            continue
        n, reason = guarded_buy_size(t, side, target_px, DEFAULT_NEW_ORDER_SIZE,
                                     orders=orders, pos_map=pos_map, exposure=exposure,
                                     theo_map=theo_map, theo_meta=theo_meta, rewards=rewards)
        if n < 1:
            results.append({"ticker": t, "skipped": True, "reason": reason})
            continue
        r = place_buy(t, side, n, target_px)
        ok = r.status_code < 300
        if ok:
            placed += 1
            exposure[event_ticker_of(t)] += n * target_px / 100.0
            orders.append({"ticker": t, "side": side, "action": "buy",
                           "remaining_count": n,
                           "yes_price_dollars": str(target_px / 100.0),
                           "no_price_dollars": str(target_px / 100.0)})
        results.append({"ticker": t, "px": target_px, "status": r.status_code,
                        "count": n,
                        "err": (r.text[:120] if not ok else None)})
        time.sleep(0.15)

    return {"ok": True, "event": event_ticker, "side": side, "over_best": over,
            "placed": placed, "candidates": len(targets), "results": results}


def move_orders(ticker, side, new_price_cents):
    """Cancel all resting BUY orders for (ticker, side) and replace with one
    consolidated order at new_price_cents. If none exist, place a fresh order
    of size DEFAULT_NEW_ORDER_SIZE."""
    if side not in ("yes", "no"):
        return {"ok": False, "error": f"bad side: {side}"}
    if not (1 <= int(new_price_cents) <= 99):
        return {"ok": False, "error": f"bad price: {new_price_cents}"}

    orders = list_resting_orders()
    targets = [o for o in orders
               if o.get("ticker") == ticker and o.get("side") == side
               and o.get("action") == "buy"]

    if targets:
        total_count = sum(order_size(o) for o in targets)
    else:
        total_count = DEFAULT_NEW_ORDER_SIZE
    if total_count <= 0:
        return {"ok": False, "error": "zero size"}

    # No-cross safety: ensure new bid doesn't cross the opposite-side ask
    book = get_orderbook(ticker)
    other = "no" if side == "yes" else "yes"
    other_levels = book.get(other, [])
    top_other_bid = other_levels[0][0] if other_levels else 0
    ask = 100 - top_other_bid if other_levels else 100
    if int(new_price_cents) >= ask:
        return {"ok": False, "error": f"price {new_price_cents}¢ would cross (best {side} ask = {ask}¢)"}

    orders = list_resting_orders()
    rewards = get_reward_map_cached()
    theo_map, theo_meta = _load_theos()
    pos_map = positions_by_ticker()
    exposure = event_exposure_usd(pos_map, orders)
    target_ids = {o.get("order_id") for o in targets}
    n, reason = guarded_buy_size(ticker, side, int(new_price_cents), total_count,
                                 orders=[o for o in orders if o.get("order_id") not in target_ids],
                                 pos_map=pos_map, exposure=exposure,
                                 theo_map=theo_map, theo_meta=theo_meta, rewards=rewards)
    if n < 1:
        return {"ok": False, "error": reason}

    cancel_results, cancels_ok = cancel_orders_strict(targets)
    if not cancels_ok:
        return {"ok": False, "error": "cancel failed; replacement skipped",
                "cancelled": cancel_results}

    place_r = place_buy(ticker, side, n, int(new_price_cents))
    place_status = place_r.status_code
    place_body = {}
    try:
        place_body = place_r.json()
    except Exception:
        place_body = {"text": place_r.text[:200]}

    return {
        "ok": place_status < 300,
        "ticker": ticker,
        "side": side,
        "count": n,
        "price_cents": int(new_price_cents),
        "cancelled": cancel_results,
        "place_status": place_status,
        "place_body": place_body,
    }


def order_price_cents(o):
    side = o["side"]
    field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    return round(float(o.get(field, "0")) * 100)


def order_size(o):
    try: return int(float(o.get("remaining_count_fp", o.get("remaining_count", 0))))
    except (TypeError, ValueError): return 0


def expand_levels_to_contracts(levels, cap):
    out = []
    for px, sz in levels:
        n = int(sz)
        for _ in range(n):
            if len(out) >= cap: return out
            out.append(px)
    return out


def compute_reward_share(book_side_levels, my_orders_on_side, cap):
    qualifying = expand_levels_to_contracts(book_side_levels, cap=cap)
    total_weight = sum(qualifying)

    my_at_price = defaultdict(int)
    for o in my_orders_on_side:
        my_at_price[order_price_cents(o)] += order_size(o)

    my_in_top = 0
    my_weight = 0
    seen = 0
    for px, sz in book_side_levels:
        sz_int = int(sz)
        room = cap - seen
        if room <= 0: break
        contracts_here = min(sz_int, room)
        my_here = min(my_at_price.get(px, 0), contracts_here)
        my_in_top += my_here
        my_weight += my_here * px
        seen += contracts_here

    return my_weight, total_weight, my_in_top, len(qualifying)


def cumulative_strictly_better(book_side_levels, my_cents):
    return int(sum(sz for px, sz in book_side_levels if px > my_cents))


# Cache: tickers we've fetched orderbooks for in this snapshot (parallelized)
def fetch_orderbooks_parallel(tickers):
    out = {}
    threads = []
    lock = threading.Lock()
    sem = threading.Semaphore(16)
    def worker(t):
        with sem:
            try:
                book = get_orderbook(t)
            except Exception:
                book = {"yes": [], "no": []}
        with lock:
            out[t] = book
    for t in tickers:
        th = threading.Thread(target=worker, args=(t,), daemon=True)
        th.start(); threads.append(th)
    for th in threads: th.join(timeout=20)
    return out


def build_summary():
    started = time.time()
    rewards = get_reward_map_cached()
    incentivized = set(rewards.keys())

    orders = list_resting_orders()
    by_ts = defaultdict(list)  # (ticker, side) -> [orders]
    sell_by_ts = defaultdict(list)
    my_event_tickers = set()
    for o in orders:
        if o.get("action") == "buy":
            by_ts[(o["ticker"], o["side"])].append(o)
            my_event_tickers.add(event_ticker_of(o["ticker"]))
        elif o.get("action") == "sell":
            sell_by_ts[(o["ticker"], o["side"])].append(o)
            my_event_tickers.add(event_ticker_of(o["ticker"]))

    # Scope: events I'm trading + any explicitly-followed events, × all strikes
    # in them, then filter to strikes that currently have an active rewards
    # program. Plus my own resting orders so they never disappear from view.
    candidate = set(t for t, _ in by_ts.keys()) | set(t for t, _ in sell_by_ts.keys())
    with _followed_lock:
        followed_snapshot = set(EXTRA_EVENT_TICKERS)
    with _blocked_lock:
        blocked_snapshot = set(BLOCKED_MARKETS)
    events_to_scan = my_event_tickers | followed_snapshot
    if events_to_scan:
        ev_lock = threading.Lock()
        ev_sem = threading.Semaphore(6)
        def fetch_ev(ev):
            with ev_sem:
                mkts = list_event_markets(ev)
            with ev_lock:
                for m in mkts: candidate.add(m["ticker"])
        ts = [threading.Thread(target=fetch_ev, args=(ev,), daemon=True) for ev in events_to_scan]
        for t in ts: t.start()
        for t in ts: t.join(timeout=12)

    my_orders_set = set(t for t, _ in by_ts.keys()) | set(t for t, _ in sell_by_ts.keys())
    # Filled positions per ticker — fetch first so any market with a long
    # position is always visible in rows (even if we have no resting order
    # there yet). Without this, the wind-down phase in PennyBot can't see
    # most of our inventory.
    try:
        pos_map = positions_by_ticker()
    except Exception:
        pos_map = {}
    pos_tickers = set(pos_map.keys())
    all_tickers = sorted((candidate & incentivized) | my_orders_set | pos_tickers)
    books = fetch_orderbooks_parallel(all_tickers)
    theo_map, _theo_meta = _load_theos()

    rows = []
    total_per_hr = 0.0
    total_pool_per_hr = 0.0
    for ticker in all_tickers:
        info = rewards.get(ticker)
        market_rate = info["hourly_rate"] if info else 0.0
        target = info["target_size"] if info else DEFAULT_TARGET_SIZE
        rate_per_side = market_rate / 2.0
        total_pool_per_hr += market_rate
        book = books.get(ticker, {"yes": [], "no": []})
        for side in ("yes", "no"):
            side_orders = by_ts.get((ticker, side), [])
            sell_orders = sell_by_ts.get((ticker, side), [])
            book_side = book.get(side, [])
            other_side = book.get("no" if side == "yes" else "yes", [])
            my_w, tot_w, my_in_top, tot_in_top = compute_reward_share(book_side, side_orders, cap=target)
            share = (my_w / tot_w) if tot_w else 0.0
            best = book_side[0][0] if book_side else None
            best_size = book_side[0][1] if book_side else None
            top_my_px = max((order_price_cents(o) for o in side_orders), default=None)
            ahead = cumulative_strictly_better(book_side, top_my_px) if top_my_px is not None else None
            best_ask = (100 - other_side[0][0]) if other_side else None
            best_ask_size = other_side[0][1] if other_side else None
            book_top = [[int(px), int(sz)] for px, sz in book_side[:5]]
            est_per_hr = share * rate_per_side
            total_per_hr += est_per_hr
            theo_yes = theo_map.get(ticker)
            if theo_yes is not None:
                side_prob = theo_yes if side == "yes" else (1.0 - theo_yes)
                # Range/band theos: when an event publishes band_cents (half-
                # width of the no-quote zone around fair), we subtract it from
                # side fair so the edge gate refuses to buy inside the band.
                # This is how uncertainty propagates through to placement —
                # we'll only post when the market is below fair_lo (or above
                # fair_hi on the other side, equivalently).
                _band = (_theo_meta.get(event_ticker_of(ticker)) or {}).get("band_cents") or 0.0
                fair_cents = max(0.0, side_prob * 100.0 - float(_band))
                edge_vs_bid = (fair_cents - best) if best is not None else None
                edge_vs_ask = (fair_cents - best_ask) if best_ask is not None else None
            else:
                side_prob = None
                fair_cents = None
                edge_vs_bid = None
                edge_vs_ask = None
            rows.append({
                "ticker": ticker,
                "side": side.upper(),
                "best_bid": best,
                "best_bid_size": best_size,
                "best_ask": best_ask,
                "best_ask_size": best_ask_size,
                "book_top": book_top,
                "my_top_px": top_my_px,
                "my_total_sz": sum(order_size(o) for o in side_orders),
                "my_levels": [(order_price_cents(o), order_size(o)) for o in
                              sorted(side_orders, key=lambda o: -order_price_cents(o))],
                "my_order_ids": [o["order_id"] for o in side_orders if o.get("order_id")],
                "my_sell_order_ids": [o["order_id"] for o in sell_orders if o.get("order_id")],
                "my_sell_total_sz": sum(order_size(o) for o in sell_orders),
                "my_sell_levels": [(order_price_cents(o), order_size(o)) for o in
                                   sorted(sell_orders, key=lambda o: -order_price_cents(o))],
                "ahead": ahead,
                "in_top_300": (ahead is not None and ahead < target),
                "my_in_top": my_in_top,
                "tot_in_top": tot_in_top,
                "share_pct": share * 100.0,
                "est_per_hr": est_per_hr,
                "has_orders": bool(side_orders),
                "market_rate_per_hr": market_rate,
                "target_size": target,
                "ends_in_h": info["ends_in_h"] if info else None,
                "close_time_ts": _parse_iso_ts(info.get("end")) if info else None,
                "blocked": ticker in blocked_snapshot,
                "theo_yes_prob": theo_yes,
                "side_theo_prob": side_prob,
                "fair_cents": fair_cents,
                "edge_vs_bid": edge_vs_bid,
                "edge_vs_ask": edge_vs_ask,
                "my_position": (pos_map.get(ticker) or {}).get(side, 0),
                "my_avg_cost_cents": (pos_map.get(ticker) or {}).get("avg_cents"),
                "theo_confidence": (_theo_meta.get(event_ticker_of(ticker)) or {}).get("confidence") or "",
                "theo_as_of_ts": (_theo_meta.get(event_ticker_of(ticker)) or {}).get("as_of_ts"),
                "theo_band_cents": (_theo_meta.get(event_ticker_of(ticker)) or {}).get("band_cents") or 0.0,
                "theo_blackout": (_blackout_now := _event_blackout_active(_theo_meta, event_ticker_of(ticker))) is not None,
                "theo_blackout_reason": (_blackout_now[0] if _blackout_now else ""),
                "theo_blackout_ends": (_blackout_now[1] if _blackout_now else None),
            })

    # Sort: rows where I have orders first (by est $/hr desc), then untraded
    # strikes by ticker.
    rows.sort(key=lambda r: (-1 if r["has_orders"] else 0, -r["est_per_hr"], r["ticker"], r["side"]))

    # Per-event aggregates for the "join untraded" panel.
    ev_agg = defaultdict(lambda: {"strikes": set(), "untraded_yes": 0, "untraded_no": 0,
                                  "active_yes": 0, "active_no": 0, "est_per_hr": 0.0})
    for r in rows:
        ev = event_ticker_of(r["ticker"])
        a = ev_agg[ev]
        a["strikes"].add(r["ticker"])
        a["est_per_hr"] += r["est_per_hr"]
        side_l = r["side"].lower()
        key = ("active_" if r["has_orders"] else "untraded_") + side_l
        a[key] += 1
    events = [{
        "event": ev, "strikes": len(a["strikes"]),
        "untraded_yes": a["untraded_yes"], "untraded_no": a["untraded_no"],
        "active_yes": a["active_yes"], "active_no": a["active_no"],
        "est_per_hr": a["est_per_hr"],
    } for ev, a in ev_agg.items()]
    events.sort(key=lambda e: -e["est_per_hr"])

    # Fills — pull all of today for accurate EV; UI only renders the most recent.
    fills_today = get_fills_today()
    fills_clean = []
    for f in fills_today:
        try:
            cnt = int(float(f.get("count_fp", 0)))
        except (TypeError, ValueError):
            cnt = 0
        side = f.get("side", "")
        px = round(float(f.get("yes_price_dollars" if side == "yes" else "no_price_dollars", "0")) * 100)
        fills_clean.append({
            "trade_id": f.get("trade_id") or "",
            "ts": f.get("ts", 0),
            "created": f.get("created_time", ""),
            "ticker": f.get("ticker") or f.get("market_ticker") or "",
            "side": side.upper(),
            "action": f.get("action", ""),
            "count": cnt,
            "px": px,
            "fee": float(f.get("fee_cost", "0") or 0),
            "is_taker": bool(f.get("is_taker")),
        })
    fills_clean.sort(key=lambda x: x.get("created", ""), reverse=True)

    # EV + rewards day-tallies
    ev_input = [{**fc, "side": fc["side"].lower(), "action": (fc["action"] or "").lower()}
                for fc in fills_clean]
    ev_today, fee_today, ev_fill_count = update_ev_from_fills(ev_input, theo_map)
    rewards_today = update_rewards_accrual(total_per_hr)
    overnight = update_overnight_accrual(total_per_hr, ev_input, theo_map)

    # Balance
    balance = None
    try:
        rb = signed_request("GET", "/portfolio/balance")
        if rb.status_code < 400:
            j = rb.json()
            balance = {"balance_cents": j.get("balance"), "portfolio_cents": j.get("portfolio_value")}
    except Exception:
        pass

    arbs = detect_arbs(rows)
    portfolio_dist = compute_portfolio_dist(rows)
    with _theo_refresh_lock:
        theo_refresh = {
            "last_run": _theo_refresh_state["last_run"],
            "results":  _theo_refresh_state["last_results"],
            "error":    _theo_refresh_state["last_error"],
        }

    return {
        "as_of": time.strftime("%H:%M:%S"),
        "elapsed_s": round(time.time() - started, 2),
        "total_orders": len(orders),
        "total_markets": len(all_tickers),
        "total_pool_per_hr": total_pool_per_hr,
        "total_per_hr": total_per_hr,
        "total_per_min": total_per_hr / 60.0,
        "balance": balance,
        "rows": rows,
        "fills": fills_clean[:30],
        "ev_today_usd": ev_today,
        "ev_today_fees_usd": fee_today,
        "ev_today_fills": ev_fill_count,
        "rewards_today_usd": rewards_today,
        "overnight": overnight,
        "events": events,
        "arbs": arbs,
        "portfolio_dist": portfolio_dist,
        "theo_refresh": theo_refresh,
        "max_over_best": MAX_OVER_BEST,
    }


PAGE = """<!doctype html>
<html><head><meta charset="utf-8">
<title>Kalshi Rewards Tracker</title>
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background:#0b0f15; color:#e6edf3; margin:0; padding:18px; }
  h1 { font-size:18px; margin:0 0 4px; }
  h2 { font-size:14px; margin:18px 0 8px; color:#8b949e; text-transform:uppercase; letter-spacing:.1em; }
  .meta { color:#8b949e; font-size:12px; margin-bottom:12px; }
  table { border-collapse:collapse; width:100%; font-size:12px; }
  th, td { text-align:left; padding:5px 9px; border-bottom:1px solid #21262d; white-space:nowrap; }
  th { background:#161b22; color:#8b949e; font-weight:600; position:sticky; top:0; }
  tr:hover { background:#0d1117; }
  tr.zero td:first-child { border-left:3px solid #f85149; }
  tr.zero { background:#170c0c; }
  tr.untraded td { color:#6e7681; }
  tr.untraded td:first-child { border-left:3px solid #21262d; }
  tr.untraded a.mkt { color:#388bfd; }
  tr.blocked { background:#1a0f1a; opacity:0.55; }
  tr.blocked td:first-child { border-left:3px solid #db61a2; }
  .yes { color:#3fb950; }
  .no  { color:#f85149; }
  .green { color:#3fb950; }
  .yellow { color:#d29922; }
  .red { color:#f85149; }
  .dim { color:#6e7681; }
  .bar { display:inline-block; background:#1f6feb; height:6px; vertical-align:middle; margin-left:6px; border-radius:2px; }
  .levels { font-family: ui-monospace, monospace; font-size:11px; color:#8b949e; }
  .pill { display:inline-block; padding:1px 6px; border-radius:8px; font-size:11px; }
  .pill-good { background:#0e2f1c; color:#3fb950; }
  .pill-bad  { background:#311b1b; color:#f85149; }
  .summary { display:flex; gap:14px; flex-wrap:wrap; margin-bottom:12px; }
  .card { background:#161b22; padding:9px 14px; border-radius:6px; min-width:120px; }
  .card .lbl { font-size:11px; color:#8b949e; text-transform:uppercase; letter-spacing:.08em; }
  .card .val { font-size:18px; font-weight:600; }
  .row-grid { display:grid; grid-template-columns: 2fr 1fr; gap:24px; margin-top:6px; }
  @media (max-width: 1100px) { .row-grid { grid-template-columns: 1fr; } }
  .pulse { animation: pulse 1.2s ease-in-out; }
  @keyframes pulse { 0% { background:#1c2333; } 100% { background:transparent; } }
  #live { font-size:11px; padding:2px 8px; border-radius:10px; background:#0e2f1c; color:#3fb950; }
  #live.stale { background:#311b1b; color:#f85149; }
  button.act { background:#21262d; color:#e6edf3; border:1px solid #30363d; border-radius:4px;
               padding:2px 7px; font-size:11px; cursor:pointer; margin-right:2px; }
  button.act:hover { background:#30363d; }
  button.act:disabled { opacity:.4; cursor:not-allowed; }
  button.act.primary { background:#1f6feb; border-color:#1f6feb; }
  button.act.primary:hover { background:#388bfd; }
  button.act.danger { background:#3a1818; border-color:#552020; color:#f85149; }
  button.act.danger:hover { background:#5a2424; }
  button.act.bulk { background:#3a2a18; border-color:#553e20; color:#d29922; padding:5px 10px; font-size:12px; }
  button.act.bulk:hover { background:#5a4124; }
  a.mkt { color:#58a6ff; text-decoration:none; }
  a.mkt:hover { text-decoration:underline; }
  .book { font-family: ui-monospace, monospace; font-size:11px; }
  .book .lvl { color:#c9d1d9; margin-right:6px; }
  .book .top { color:#3fb950; font-weight:600; }
  .book .ask { color:#f85149; margin-left:8px; padding-left:8px; border-left:1px solid #30363d; }
  .events-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); gap:10px; margin-bottom:10px; }
  .ev-card { background:#161b22; border:1px solid #21262d; padding:10px 12px; border-radius:6px; }
  .ev-card .ev-name { font-weight:600; font-size:12px; color:#c9d1d9; }
  .ev-card .ev-stats { font-size:11px; color:#8b949e; margin:3px 0 6px; }
  .ev-card .ev-row { display:flex; align-items:center; gap:6px; font-size:11px; margin-top:4px; }
  .ev-card .ev-lbl { color:#8b949e; min-width:120px; }
  #penny-panel { background:#161b22; border:1px solid #21262d; padding:10px 14px; border-radius:6px;
                 margin-bottom:14px; display:flex; gap:18px; align-items:center; flex-wrap:wrap; }
  #penny-panel.on { border-color:#3fb950; }
  #penny-panel .pp-name { font-weight:600; color:#c9d1d9; }
  #penny-panel input { width:54px; background:#0d1117; color:#e6edf3; border:1px solid #30363d;
                       border-radius:4px; padding:3px 6px; font-size:12px; }
  #penny-panel label { font-size:11px; color:#8b949e; display:flex; align-items:center; gap:5px; }
  .toggle { display:inline-block; padding:5px 12px; border-radius:6px; cursor:pointer; font-weight:600;
            font-size:12px; border:1px solid; }
  .toggle.off { background:#21262d; border-color:#30363d; color:#8b949e; }
  .toggle.on  { background:#0e2f1c; border-color:#3fb950; color:#3fb950; }
  #penny-log { font-family: ui-monospace, monospace; font-size:11px; max-height:120px;
               overflow-y:auto; background:#0d1117; border:1px solid #21262d; border-radius:4px;
               padding:6px 8px; margin-top:6px; }
  #penny-log .ok { color:#3fb950; }
  #penny-log .err { color:#f85149; }
  #penny-log .ts { color:#6e7681; margin-right:6px; }
  #search-panel { background:#161b22; border:1px solid #21262d; padding:10px 14px;
                  border-radius:6px; margin-bottom:14px; }
  #search-q { width:340px; background:#0d1117; color:#e6edf3; border:1px solid #30363d;
              border-radius:4px; padding:6px 10px; font-size:13px; }
  #search-results { margin-top:10px; max-height:280px; overflow-y:auto; }
  #search-results .sr-row { display:flex; align-items:center; gap:10px; padding:5px 0;
                            border-bottom:1px solid #21262d; font-size:12px; }
  #search-results .sr-row:last-child { border-bottom:none; }
  #search-results .sr-ev { font-weight:600; color:#c9d1d9; min-width:230px; }
  #search-results .sr-stats { color:#8b949e; flex:1; }
  #search-results .sr-rate { color:#3fb950; min-width:90px; text-align:right; }
  #arb-panel { background:#311b1b; border:1px solid #f85149; padding:10px 14px;
               border-radius:6px; margin-bottom:14px; }
  #arb-panel .ah { font-weight:600; color:#f85149; margin-bottom:6px; }
  #arb-panel .a-row { font-family: ui-monospace, monospace; font-size:11px;
                      padding:3px 0; color:#e6edf3; }
  #arb-panel.ok { background:#0e2f1c; border-color:#3fb950; }
  #arb-panel.ok .ah { color:#3fb950; }
  input.px { width:42px; background:#0d1117; color:#e6edf3; border:1px solid #30363d;
             border-radius:4px; padding:2px 4px; font-size:11px; }
  #toast { position:fixed; bottom:20px; right:20px; background:#161b22; border:1px solid #30363d;
           padding:10px 14px; border-radius:6px; font-size:12px; max-width:380px;
           box-shadow:0 4px 12px rgba(0,0,0,.5); display:none; z-index:100; }
  #toast.show { display:block; }
  #toast.ok { border-color:#3fb950; }
  #toast.err { border-color:#f85149; }
</style></head>
<body>
<h1>Kalshi Liquidity Rewards Tracker <span id="live">live</span></h1>
<div class="meta">
  Refreshing every <span id="refresh-ms">2000</span>ms •
  As of <span id="as-of">—</span> •
  built in <span id="elapsed">—</span>s
</div>

<div id="day-tally" style="display:flex; gap:24px; margin:8px 0 14px; font-size:13px;
     background:#161b22; border:1px solid #21262d; border-radius:6px; padding:10px 14px;">
  <div><span class="dim">EV today</span>
       <strong id="ev-today" style="margin-left:6px;">$0.00</strong>
       <span class="dim" id="ev-today-meta" style="margin-left:6px; font-size:11px;"></span></div>
  <div><span class="dim">MM rewards today</span>
       <strong id="rewards-today" style="margin-left:6px;">$0.00</strong>
       <span class="dim" style="margin-left:6px; font-size:11px;">(integrates current $/hr since session start)</span></div>
  <div><span class="dim">Net</span>
       <strong id="net-today" style="margin-left:6px;">$0.00</strong></div>
</div>

<div id="overnight-panel" style="display:flex; gap:24px; align-items:baseline; flex-wrap:wrap; margin:8px 0 14px; font-size:13px;
     background:#161b22; border:1px solid #21262d; border-radius:6px; padding:10px 14px;">
  <div><span class="dim">Overnight</span>
       <strong id="ov-state" style="margin-left:6px;">idle</strong>
       <span class="dim" id="ov-elapsed" style="margin-left:6px; font-size:11px;"></span></div>
  <div><span class="dim">incentives</span>
       <strong id="ov-rewards" style="margin-left:6px;">$0.00</strong></div>
  <div><span class="dim">fill EV</span>
       <strong id="ov-ev" style="margin-left:6px;">$0.00</strong>
       <span class="dim" id="ov-fees" style="margin-left:6px; font-size:11px;"></span></div>
  <div><span class="dim">net</span>
       <strong id="ov-net" style="margin-left:6px;">$0.00</strong></div>
  <div style="margin-left:auto;">
       <button onclick="overnightStart()" style="padding:4px 10px; font-size:12px;">Start / Reset</button>
       <button onclick="overnightStop()" style="padding:4px 10px; font-size:12px;">Stop</button></div>
</div>

<div id="port-dist" style="margin:8px 0 14px; background:#161b22; border:1px solid #21262d; border-radius:6px; padding:10px 14px;">
  <div style="display:flex; gap:24px; align-items:baseline; flex-wrap:wrap; font-size:13px;">
    <div><span class="dim">Portfolio EV</span>
         <strong id="port-ev" style="margin-left:6px;">—</strong></div>
    <div><span class="dim">σ</span>
         <strong id="port-std" style="margin-left:6px;">—</strong></div>
    <div><span class="dim">P5 / P50 / P95</span>
         <strong id="port-pcts" style="margin-left:6px;">—</strong></div>
    <div><span class="dim">range</span>
         <span id="port-range" style="margin-left:6px;">—</span></div>
    <div style="margin-left:auto; display:flex; gap:10px; align-items:center;">
      <span class="dim" id="theo-refresh-status" style="font-size:11px;">theos: —</span>
      <button id="theo-refresh-btn" onclick="refreshTheos()"
              style="padding:4px 10px; font-size:11px; background:#1f6feb; color:#fff; border:0; border-radius:4px; cursor:pointer;">
        Refresh theos
      </button>
    </div>
  </div>
  <div style="margin-top:4px;"><span class="dim" id="port-events" style="font-size:11px;"></span></div>
  <svg id="port-svg" width="100%" height="160" style="display:block; margin-top:8px;"></svg>
</div>

<div class="summary" id="summary"></div>

<h2>Auto-penny · post-only top-of-book on wide-spread incentivized markets</h2>
<div id="penny-panel">
  <span class="pp-name">Auto-penny</span>
  <span id="penny-toggle" class="toggle off" onclick="togglePenny()">OFF</span>
  <label>spread &gt; <input type="number" id="pp-spread" min="1" max="99" value="1">¢</label>
  <label>my share &lt; <input type="number" id="pp-share" min="0" max="100" step="0.5" value="90">%</label>
  <label>or gap &gt; <input type="number" id="pp-gap" min="1" max="99" value="5">¢</label>
  <label>size <input type="number" id="pp-size" min="1" max="500" value="12"></label>
  <label>every <input type="number" id="pp-interval" min="10" max="600" value="20">s</label>
  <label>cooldown <input type="number" id="pp-cooldown" min="0" max="3600" value="0">s</label>
  <label>defend cd <input type="number" id="pp-defcd" min="0" max="3600" value="20">s</label>
  <button class="act primary" onclick="savePenny()">Save</button>
  <span style="border-left:1px solid #30363d; height:20px;"></span>
  <span id="arb-toggle" class="toggle on" onclick="toggleArbResolver()">ANTI-ARB ON</span>
  <label>arb sweep <input type="number" id="pp-arbint" min="2" max="60" value="4">s</label>
  <span id="penny-stats" class="dim" style="font-size:11px;"></span>
</div>
<div id="penny-log"></div>

<div id="arb-panel" style="display:none;"></div>

<h2>Add markets with rewards</h2>
<div id="search-panel">
  <input type="text" id="search-q" placeholder="Search event tickers (e.g. KXTRUF, GAS, BFST)..." autocomplete="off">
  <span id="search-stats" class="dim" style="font-size:11px; margin-left:8px;"></span>
  <div id="search-results"></div>
</div>

<h2>Events · join untraded strikes</h2>
<div class="events-grid" id="events-panel"></div>

<div class="row-grid">
  <div>
    <h2>Markets · sorted by est. $/hr
      <button class="act bulk" onclick="cancelZero()" style="margin-left:14px;">
        Kill every order earning $0
      </button>
      <button class="act bulk" onclick="cancelBadEdge()" style="margin-left:8px;">
        Kill orders &gt; 1¢ worse than theo
      </button>
    </h2>
    <table>
      <thead><tr>
        <th>Market · side</th>
        <th>My px</th>
        <th>Size</th>
        <th>Spread</th>
        <th title="Model fair price for this side (cents) and edge vs best bid">Theo / Edge</th>
        <th>Ahead</th>
        <th>In top</th>
        <th>Mine / In top</th>
        <th>Share</th>
        <th>$/hr</th>
        <th>$/min</th>
        <th>Pool $/hr</th>
        <th>Ends</th>
        <th>Levels</th>
        <th>Action</th>
      </tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>

  <div>
    <h2>Recent fills</h2>
    <table>
      <thead><tr>
        <th>Time</th><th>Market · side</th><th>Action</th><th>×</th><th>@</th><th>Fee</th><th>T?</th>
      </tr></thead>
      <tbody id="fills"></tbody>
    </table>
  </div>
</div>

<p class="meta" id="footer-note"></p>

<div id="toast"></div>

<script>
const MUTATION_TOKEN = "__MUTATION_TOKEN__";
function apiFetch(url, opts={}) {
  opts.headers = Object.assign({}, opts.headers || {}, {"X-Local-Token": MUTATION_TOKEN});
  return fetch(url, opts);
}
const REFRESH_MS = 2000;
const MAX_OVER_BEST = __MAX_OVER_BEST__;
document.getElementById("refresh-ms").textContent = REFRESH_MS;
let lastFillIds = new Set();

function fmtUSD(x) {
  if (x === null || x === undefined) return "—";
  return "$" + x.toFixed(x < 1 ? 4 : 2);
}
function fmtPct(x) {
  return (x).toFixed(1) + "%";
}
function shareColor(p) {
  if (p >= 30) return "green";
  if (p >= 5) return "yellow";
  return "red";
}
function theoCell(r) {
  if (r.fair_cents === null || r.fair_cents === undefined) {
    return '<span class="dim">—</span>';
  }
  const fair = Math.round(r.fair_cents);
  const edge = r.edge_vs_bid;
  const edgeRound = (edge === null || edge === undefined) ? null : Math.round(edge);
  let edgeStr = '';
  if (edgeRound !== null) {
    const sign = edgeRound > 0 ? '+' : '';
    const cls = edgeRound >= 5 ? 'green' : (edgeRound >= 1 ? 'yellow' : (edgeRound <= -5 ? 'red' : 'dim'));
    edgeStr = ` <span class="${cls}">${sign}${edgeRound}¢</span>`;
  }
  return `<strong>${fair}¢</strong>${edgeStr}`;
}
function relTime(iso) {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (isNaN(t)) return iso;
  const s = Math.round((Date.now() - t)/1000);
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.round(s/60) + "m ago";
  return Math.round(s/3600) + "h ago";
}
function shortTicker(t) {
  // KXTRUFBFST-26APR27-T89.40 -> KXTRUFBFST · T89.40
  const parts = t.split("-");
  const last = parts[parts.length-1];
  const head = parts[0];
  return head + " · " + last;
}
function kalshiUrl(ticker) {
  // Kalshi's event URLs require a slug we don't have. The series page (first
  // segment lowercased) is the most reliable landing — user clicks through
  // to the right event from there.
  const series = ticker.split("-")[0].toLowerCase();
  return "https://kalshi.com/markets/" + series;
}

function renderSummary(d) {
  const c = (lbl, val, cls="") => `<div class="card"><div class="lbl">${lbl}</div><div class="val ${cls}">${val}</div></div>`;
  const bal = d.balance ? "$" + (d.balance.balance_cents/100).toFixed(2) : "—";
  const port = d.balance ? "$" + (d.balance.portfolio_cents/100).toFixed(2) : "—";
  const total_hr = d.total_per_hr;
  const total_min = d.total_per_min;
  const pool_hr = d.total_pool_per_hr || 0;
  const captured = pool_hr > 0 ? (100 * total_hr / pool_hr) : 0;
  document.getElementById("summary").innerHTML =
    c("My est.", "$" + total_hr.toFixed(2) + "/hr", "green") +
    c("Per minute", "$" + total_min.toFixed(3) + "/min", "green") +
    c("% of pool", captured.toFixed(1) + "%", "yellow") +
    c("Pool total", "$" + pool_hr.toFixed(2) + "/hr") +
    c("Markets", d.total_markets) +
    c("Resting orders", d.total_orders) +
    c("Balance", bal) +
    c("Portfolio", port);
}

function renderBook(r) {
  const top = r.book_top || [];
  if (!top.length) return '<span class="dim">empty</span>';
  const cells = top.slice(0, 3).map(([px, sz], i) =>
    `<span class="lvl ${i===0?'top':''}">${px}¢×${sz}</span>`
  ).join("");
  const ask = (r.best_ask !== null && r.best_ask !== undefined)
    ? `<span class="ask">ask ${r.best_ask}¢</span>` : "";
  return cells + ask;
}

function renderEvents(events) {
  if (!events || !events.length) {
    document.getElementById("events-panel").innerHTML = '<div class="dim">No events.</div>';
    return;
  }
  const html = events.map(e => {
    const sideRow = (side) => {
      const untraded = side === "yes" ? e.untraded_yes : e.untraded_no;
      const active   = side === "yes" ? e.active_yes   : e.active_no;
      const cls = side === "yes" ? "yes" : "no";
      const dis = untraded === 0 ? "disabled" : "";
      const btns = Array.from({length: MAX_OVER_BEST}, (_, i) =>
        `<button class="act" ${dis} onclick="placeBulk('${e.event}','${side}',${i+1})">+${i+1}</button>`
      ).join("");
      return `<div class="ev-row">
        <span class="ev-lbl"><span class="${cls}">${side.toUpperCase()}</span>: ${active} active, <strong>${untraded}</strong> untraded</span>
        ${btns}
      </div>`;
    };
    return `<div class="ev-card">
      <div class="ev-name">${e.event}</div>
      <div class="ev-stats">${e.strikes} strikes · est $${e.est_per_hr.toFixed(2)}/hr</div>
      ${sideRow("yes")}
      ${sideRow("no")}
    </div>`;
  }).join("");
  document.getElementById("events-panel").innerHTML = html;
}

async function fetchPenny() {
  try {
    const r = await fetch("/api/penny");
    const j = await r.json();
    renderPenny(j);
  } catch (e) { /* silent */ }
}

function renderPenny(s) {
  const tog = document.getElementById("penny-toggle");
  const panel = document.getElementById("penny-panel");
  if (s.enabled) { tog.className = "toggle on"; tog.textContent = "ON"; panel.classList.add("on"); }
  else { tog.className = "toggle off"; tog.textContent = "OFF"; panel.classList.remove("on"); }
  if (document.activeElement.tagName !== "INPUT") {
    document.getElementById("pp-spread").value = s.spread_min;
    document.getElementById("pp-share").value = s.share_max_pct;
    document.getElementById("pp-gap").value = s.gap_min;
    document.getElementById("pp-size").value = s.size;
    document.getElementById("pp-interval").value = s.interval;
    document.getElementById("pp-cooldown").value = s.cooldown_s;
    document.getElementById("pp-defcd").value = s.defend_cooldown_s;
    document.getElementById("pp-arbint").value = s.arb_interval;
  }
  const arbTog = document.getElementById("arb-toggle");
  if (s.auto_resolve_arbs) { arbTog.className = "toggle on"; arbTog.textContent = "ANTI-ARB ON"; }
  else { arbTog.className = "toggle off"; arbTog.textContent = "ANTI-ARB OFF"; }
  const last = s.last_cycle_ts ? new Date(s.last_cycle_ts*1000).toLocaleTimeString() : "—";
  document.getElementById("penny-stats").textContent =
    `last cycle ${last} · scanned ${s.last_cycle_scanned} · placed ${s.last_cycle_placed}`;
  const html = (s.log || []).slice().reverse().map(l => {
    const t = new Date(l.ts*1000).toLocaleTimeString();
    return `<div class="${l.ok?'ok':'err'}"><span class="ts">${t}</span>${l.msg}</div>`;
  }).join("");
  document.getElementById("penny-log").innerHTML = html || '<span class="dim">no actions yet</span>';
}

async function toggleArbResolver() {
  const cur = document.getElementById("arb-toggle").textContent.includes("ON");
  const r = await apiFetch("/api/penny", {method: "POST", headers:{"Content-Type":"application/json"},
                                       body: JSON.stringify({auto_resolve_arbs: !cur})});
  const j = await r.json();
  renderPenny(j);
  showToast("Anti-arb " + (j.auto_resolve_arbs ? "ON" : "OFF"), true);
}

async function togglePenny() {
  const cur = document.getElementById("penny-toggle").textContent === "ON";
  const r = await apiFetch("/api/penny", {method: "POST", headers:{"Content-Type":"application/json"},
                                       body: JSON.stringify({enabled: !cur})});
  const j = await r.json();
  renderPenny(j);
  showToast("Auto-penny " + (j.enabled ? "ON" : "OFF"), true);
}

async function savePenny() {
  const body = {
    spread_min: parseInt(document.getElementById("pp-spread").value, 10),
    share_max_pct: parseFloat(document.getElementById("pp-share").value),
    gap_min: parseInt(document.getElementById("pp-gap").value, 10),
    size: parseInt(document.getElementById("pp-size").value, 10),
    interval: parseInt(document.getElementById("pp-interval").value, 10),
    cooldown_s: parseInt(document.getElementById("pp-cooldown").value, 10),
    defend_cooldown_s: parseInt(document.getElementById("pp-defcd").value, 10),
    arb_interval: parseInt(document.getElementById("pp-arbint").value, 10),
  };
  const r = await apiFetch("/api/penny", {method: "POST", headers:{"Content-Type":"application/json"},
                                       body: JSON.stringify(body)});
  const j = await r.json();
  renderPenny(j);
  showToast("Auto-penny settings saved", true);
}

async function placeBulk(eventTicker, side, overBest) {
  if (!confirm(`Place 300-contract orders on every untraded ${side.toUpperCase()} strike of ${eventTicker} at best+${overBest}¢?`)) return;
  showToast(`Placing ${side.toUpperCase()} on untraded strikes of ${eventTicker} ...`, true);
  try {
    const r = await apiFetch("/api/place-bulk", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({event: eventTicker, side, over_best: overBest}),
    });
    const j = await r.json();
    if (j.ok) {
      showToast(`Placed ${j.placed} of ${j.candidates} ${side.toUpperCase()} orders on ${eventTicker}`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || "unknown"), false);
    }
  } catch (e) {
    showToast("ERROR: " + e.message, false);
  }
}

function bestCompetitorPx(r) {
  // Best price strictly above ours (i.e. someone is bidding higher than us).
  // If no one is ahead, fall back to best_bid (which is our own top).
  if (r.ahead > 0 && r.best_bid !== null && r.best_bid > r.my_top_px) return r.best_bid;
  return r.best_bid;
}

function renderRows(rows) {
  const html = rows.map((r, idx) => {
    const sideCls = r.side === "YES" ? "yes" : "no";
    const inTop = r.in_top_300
      ? '<span class="pill pill-good">YES</span>'
      : '<span class="pill pill-bad">NO</span>';
    const sc = shareColor(r.share_pct);
    const barW = Math.max(1, Math.min(120, Math.round(r.share_pct * 1.2)));
    const levels = r.my_levels.map(([px,sz]) => px+"¢×"+sz).join(", ");
    const tickEsc = r.ticker.replace(/'/g, "\\'");
    const sideLow = r.side.toLowerCase();
    const compBest = bestCompetitorPx(r);
    let quickBtns = "";
    for (let n = 1; n <= MAX_OVER_BEST; n++) {
      const target = (compBest !== null) ? compBest + n : null;
      const dis = (target === null || target > 99) ? "disabled" : "";
      quickBtns += `<button class="act" ${dis} onclick="moveOrder('${tickEsc}','${sideLow}',${target||0})">+${n}</button>`;
    }
    const defaultPx = (compBest !== null) ? compBest + 1 : (r.my_top_px || 1);
    const hasOrders = r.has_orders;
    const zeroReward = hasOrders && r.my_in_top === 0;
    let rowCls = "";
    if (r.blocked) rowCls = 'class="blocked"';
    else if (zeroReward) rowCls = 'class="zero"';
    else if (!hasOrders) rowCls = 'class="untraded"';
    const myPxDisp = r.my_top_px === null ? "—" : (r.my_top_px + "¢");
    const aheadDisp = r.ahead === null ? "—" : r.ahead;
    const cancelBtn = hasOrders
      ? `<button class="act danger" onclick="cancelOrder('${tickEsc}','${sideLow}')">✕</button>`
      : "";
    const blockBtn = r.blocked
      ? `<button class="act" title="Unblock market" onclick="unblockMarket('${tickEsc}')">▶ Unblock</button>`
      : `<button class="act danger" title="Cancel all orders & block penny bot from this market" onclick="blockMarket('${tickEsc}')">⛔ Block</button>`;
    const moveLabel = hasOrders ? "Move" : "Place";
    const endsDisp = r.ends_in_h !== null && r.ends_in_h !== undefined
      ? (r.ends_in_h < 1 ? Math.round(r.ends_in_h*60)+"m"
         : r.ends_in_h < 24 ? r.ends_in_h.toFixed(1)+"h"
         : (r.ends_in_h/24).toFixed(1)+"d") : "—";
    return `<tr ${rowCls}>
      <td><a class="mkt" href="${kalshiUrl(r.ticker)}" target="_blank" rel="noopener">${shortTicker(r.ticker)}↗</a> <span class="${sideCls}"><strong>${r.side}</strong></span></td>
      <td>${myPxDisp}</td>
      <td>${r.my_total_sz}</td>
      <td>${(r.best_bid !== null && r.best_ask !== null) ? ((r.best_ask - r.best_bid) + "¢") : '<span class="dim">—</span>'}</td>
      <td>${theoCell(r)}</td>
      <td>${aheadDisp}</td>
      <td>${hasOrders ? inTop : "<span class='dim'>—</span>"}</td>
      <td>${r.my_in_top} / ${r.tot_in_top}</td>
      <td class="${sc}"><strong>${fmtPct(r.share_pct)}</strong> <span class="bar" style="width:${barW}px"></span></td>
      <td class="green">$${r.est_per_hr.toFixed(3)}</td>
      <td class="dim">$${(r.est_per_hr/60).toFixed(4)}</td>
      <td class="dim">$${r.market_rate_per_hr.toFixed(2)}</td>
      <td class="dim">${endsDisp}</td>
      <td class="levels">${levels}</td>
      <td>
        ${quickBtns}
        <input class="px" id="px-${idx}" type="number" min="1" max="99" value="${defaultPx}">
        <button class="act primary" onclick="moveOrderFromInput('${tickEsc}','${sideLow}','px-${idx}')">${moveLabel}</button>
        ${cancelBtn}
        ${blockBtn}
      </td>
    </tr>`;
  }).join("");
  document.getElementById("rows").innerHTML = html ||
    '<tr><td colspan="15" class="dim">No incentivized markets right now.</td></tr>';
}

function shortTickerLink(t) { return `<a class="mkt" href="${kalshiUrl(t)}" target="_blank" rel="noopener">${shortTicker(t)}↗</a>`; }

function showToast(msg, ok) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.className = "show " + (ok ? "ok" : "err");
  clearTimeout(window._toastTimer);
  window._toastTimer = setTimeout(() => { t.className = ""; }, 4500);
}

async function moveOrder(ticker, side, priceCents) {
  if (!priceCents || priceCents < 1 || priceCents > 99) {
    showToast("invalid price " + priceCents, false); return;
  }
  showToast(`Moving ${side.toUpperCase()} ${ticker} → ${priceCents}¢ ...`, true);
  try {
    const r = await apiFetch("/api/move", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ticker, side, price_cents: priceCents}),
    });
    const j = await r.json();
    if (j.ok) {
      showToast(`Moved ${side.toUpperCase()} ${ticker.split("-").pop()} → ${priceCents}¢ (${j.count} contracts)`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || JSON.stringify(j.place_body) || "unknown"), false);
    }
  } catch (e) {
    showToast("ERROR: " + e.message, false);
  }
}

function moveOrderFromInput(ticker, side, inputId) {
  const v = parseInt(document.getElementById(inputId).value, 10);
  moveOrder(ticker, side, v);
}

async function cancelOrder(ticker, side) {
  if (!confirm(`Cancel all ${side.toUpperCase()} orders on ${ticker}?`)) return;
  showToast(`Cancelling ${side.toUpperCase()} ${ticker} ...`, true);
  try {
    const r = await apiFetch("/api/cancel", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({ticker, side}),
    });
    const j = await r.json();
    if (j.ok) {
      showToast(`Cancelled ${j.count} order(s) on ${side.toUpperCase()} ${ticker.split("-").pop()}`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || "unknown"), false);
    }
  } catch (e) {
    showToast("ERROR: " + e.message, false);
  }
}

async function cancelZero() {
  if (!confirm("Kill every individual resting order whose price level is past the top 300?")) return;
  showToast("Scanning every resting order...", true);
  try {
    const r = await apiFetch("/api/cancel-zero", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: "{}",
    });
    const j = await r.json();
    if (j.ok) {
      showToast(`Cancelled ${j.cancelled} of ${j.scanned} resting orders.`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || "unknown"), false);
    }
  } catch (e) {
    showToast("ERROR: " + e.message, false);
  }
}

async function cancelBadEdge() {
  if (!confirm("Kill every order priced more than 1¢ above the model's fair value?")) return;
  showToast("Scanning every resting order vs theo...", true);
  try {
    const r = await apiFetch("/api/cancel-bad-edge", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({threshold_cents: -1}),
    });
    const j = await r.json();
    if (j.ok) {
      showToast(`Cancelled ${j.cancelled} of ${j.scanned} resting orders (edge < -1¢ vs theo).`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || "unknown"), false);
    }
  } catch (e) {
    showToast("ERROR: " + e.message, false);
  }
}

function renderFills(fills) {
  const html = fills.map(f => {
    const isNew = !lastFillIds.has(f.created + f.ticker + f.px);
    const sideCls = f.side === "YES" ? "yes" : "no";
    const actionCls = f.action === "buy" ? "green" : "red";
    return `<tr class="${isNew ? 'pulse' : ''}">
      <td class="dim">${relTime(f.created)}</td>
      <td>${shortTickerLink(f.ticker)} <span class="${sideCls}"><strong>${f.side}</strong></span></td>
      <td class="${actionCls}">${f.action}</td>
      <td>${f.count}</td>
      <td>${f.px}¢</td>
      <td class="dim">$${f.fee.toFixed(2)}</td>
      <td class="dim">${f.is_taker ? "T" : "M"}</td>
    </tr>`;
  }).join("");
  document.getElementById("fills").innerHTML = html ||
    '<tr><td colspan="7" class="dim">No fills yet.</td></tr>';
  lastFillIds = new Set(fills.map(f => f.created + f.ticker + f.px));
}

function renderDayTally(d) {
  const ev = (typeof d.ev_today_usd === "number") ? d.ev_today_usd : 0;
  const fees = (typeof d.ev_today_fees_usd === "number") ? d.ev_today_fees_usd : 0;
  const rew = (typeof d.rewards_today_usd === "number") ? d.rewards_today_usd : 0;
  const fills = d.ev_today_fills || 0;
  const net = ev - fees + rew;
  const fmt = (x) => (x >= 0 ? "+$" : "-$") + Math.abs(x).toFixed(2);
  const cls = (x) => x >= 0 ? "green" : "red";
  const evEl = document.getElementById("ev-today");
  evEl.textContent = fmt(ev);
  evEl.className = cls(ev);
  document.getElementById("ev-today-meta").textContent =
    `(${fills} fills · −$${fees.toFixed(2)} fees)`;
  const rewEl = document.getElementById("rewards-today");
  rewEl.textContent = "+$" + rew.toFixed(2);
  rewEl.className = "green";
  const netEl = document.getElementById("net-today");
  netEl.textContent = fmt(net);
  netEl.className = cls(net);
}

function renderOvernight(ov) {
  if (!ov) return;
  const fmt = (x) => (x >= 0 ? "+$" : "-$") + Math.abs(x).toFixed(2);
  const cls = (x) => x >= 0 ? "green" : "red";
  document.getElementById("ov-state").textContent = ov.running ? "running" : "idle";
  document.getElementById("ov-state").className = ov.running ? "green" : "dim";
  let elap = "";
  if (ov.running) {
    const s = Math.max(0, ov.elapsed_s|0);
    const h = (s/3600)|0, m = ((s%3600)/60)|0;
    elap = `(${h}h ${m}m)`;
  }
  document.getElementById("ov-elapsed").textContent = elap;
  const r = +ov.rewards_usd || 0, e = +ov.ev_usd || 0, f = +ov.fee_usd || 0, n = +ov.net_usd || 0;
  const rw = document.getElementById("ov-rewards"); rw.textContent = "+$" + r.toFixed(2); rw.className = "green";
  const ev = document.getElementById("ov-ev"); ev.textContent = fmt(e); ev.className = cls(e);
  document.getElementById("ov-fees").textContent = f ? `(−$${f.toFixed(2)} fees)` : "";
  const nt = document.getElementById("ov-net"); nt.textContent = fmt(n); nt.className = cls(n);
}

async function overnightStart() {
  if (!confirm("Reset overnight tracker to now? Zeros rewards/EV/fees tallies.")) return;
  await apiFetch("/api/overnight/reset", {method:"POST", headers:{"Content-Type":"application/json"}, body:"{}"});
  refresh();
}
async function overnightStop() {
  await apiFetch("/api/overnight/stop", {method:"POST", headers:{"Content-Type":"application/json"}, body:"{}"});
  refresh();
}

function renderPortfolioDist(pd) {
  const evEl = document.getElementById("port-ev");
  const stdEl = document.getElementById("port-std");
  const pctsEl = document.getElementById("port-pcts");
  const rangeEl = document.getElementById("port-range");
  const evtsEl = document.getElementById("port-events");
  const svg = document.getElementById("port-svg");
  if (!pd) {
    evEl.textContent = "—"; stdEl.textContent = "—";
    pctsEl.textContent = "—"; rangeEl.textContent = "—";
    evtsEl.textContent = "no theos"; svg.innerHTML = ""; return;
  }
  const fmt = (x) => "$" + x.toLocaleString("en-US", {maximumFractionDigits: 0});
  evEl.textContent = fmt(pd.ev_usd);
  stdEl.textContent = fmt(pd.std_usd);
  pctsEl.textContent = `${fmt(pd.p5)} / ${fmt(pd.p50)} / ${fmt(pd.p95)}`;
  rangeEl.textContent = `${fmt(pd.min)} … ${fmt(pd.max)}`;
  evtsEl.textContent = "per-event std: " + Object.entries(pd.per_event)
    .map(([ev, s]) => `${ev.replace(/^KX/, "")}=${fmt(s.std)}`).join("  ");
  // Render histogram as SVG bars
  const bins = pd.bins || [];
  if (!bins.length) { svg.innerHTML = ""; return; }
  const W = svg.clientWidth || 800, H = 160;
  const padL = 50, padR = 12, padT = 10, padB = 22;
  const xMin = pd.min, xMax = pd.max;
  const xRange = Math.max(xMax - xMin, 1);
  const pMax = Math.max(...bins.map(b => b.p));
  const xPx = (x) => padL + ((x - xMin) / xRange) * (W - padL - padR);
  const yPx = (p) => padT + (1 - p / pMax) * (H - padT - padB);
  const bw = pd.bin_width;
  let parts = [];
  // shaded P5-P95 region
  const x5 = xPx(pd.p5), x95 = xPx(pd.p95);
  parts.push(`<rect x="${x5}" y="${padT}" width="${x95-x5}" height="${H-padT-padB}" fill="#1f6feb" opacity="0.08"/>`);
  // bars
  for (const b of bins) {
    const x = xPx(b.x - bw/2);
    const w = Math.max(xPx(b.x + bw/2) - x - 0.5, 0.5);
    const y = yPx(b.p);
    parts.push(`<rect x="${x}" y="${y}" width="${w}" height="${H-padB-y}" fill="#3fb950" opacity="0.85"/>`);
  }
  // EV vertical line
  const xEv = xPx(pd.ev_usd);
  parts.push(`<line x1="${xEv}" y1="${padT}" x2="${xEv}" y2="${H-padB}" stroke="#f85149" stroke-width="1.5" stroke-dasharray="3 3"/>`);
  parts.push(`<text x="${xEv+4}" y="${padT+10}" fill="#f85149" font-size="10">EV ${fmt(pd.ev_usd)}</text>`);
  // x-axis ticks at min, p5, p50, p95, max
  for (const [v, label] of [[pd.min, "min"], [pd.p5, "p5"], [pd.p50, "p50"], [pd.p95, "p95"], [pd.max, "max"]]) {
    const x = xPx(v);
    parts.push(`<line x1="${x}" y1="${H-padB}" x2="${x}" y2="${H-padB+4}" stroke="#8b949e"/>`);
    parts.push(`<text x="${x}" y="${H-6}" fill="#8b949e" font-size="10" text-anchor="middle">${fmt(v)}</text>`);
  }
  // y-axis label
  parts.push(`<text x="6" y="${padT+8}" fill="#8b949e" font-size="10">prob</text>`);
  svg.innerHTML = parts.join("");
}

async function refreshTheos() {
  const btn = document.getElementById("theo-refresh-btn");
  if (!btn) return;
  btn.disabled = true; btn.textContent = "Refreshing...";
  try {
    const r = await apiFetch("/api/refresh-theos", {method: "POST"});
    const d = await r.json();
    const okCount = (d.results || []).filter(x => x.ok).length;
    const tot = (d.results || []).length;
    btn.textContent = `Refreshed ${okCount}/${tot}`;
    setTimeout(() => { btn.textContent = "Refresh theos"; btn.disabled = false; }, 1500);
  } catch (e) {
    btn.textContent = "Failed"; btn.disabled = false;
  }
}

function renderTheoRefreshStatus(tr) {
  const el = document.getElementById("theo-refresh-status");
  if (!el) return;
  if (!tr || !tr.last_run) { el.textContent = "theos: never refreshed"; return; }
  const ageS = Math.floor(Date.now() / 1000 - tr.last_run);
  const ageStr = ageS < 60 ? `${ageS}s` : `${Math.floor(ageS/60)}m${ageS%60}s`;
  const okCount = (tr.results || []).filter(x => x.ok).length;
  const tot = (tr.results || []).length;
  el.textContent = `theos: ${okCount}/${tot} ok · ${ageStr} ago`;
}

function setLive(ok) {
  const el = document.getElementById("live");
  el.classList.toggle("stale", !ok);
  el.textContent = ok ? "live" : "stale";
}

async function tick() {
  try {
    const r = await fetch("/api/data");
    if (!r.ok) throw new Error("HTTP " + r.status);
    const d = await r.json();
    document.getElementById("as-of").textContent = d.as_of;
    document.getElementById("elapsed").textContent = d.elapsed_s;
    document.getElementById("footer-note").textContent =
      `Reward share = your weighted contracts in the top N / total weighted (weight = bid price ¢, N = each program's target_size). ` +
      `Per-market $/hr is from the live Kalshi /incentive_programs endpoint, split equally between yes/no. ` +
      `T = taker fill, M = maker fill.`;
    renderSummary(d);
    renderDayTally(d);
    renderOvernight(d.overnight);
    renderPortfolioDist(d.portfolio_dist);
    renderTheoRefreshStatus(d.theo_refresh);
    renderArbs(d.arbs || []);
    renderEvents(d.events);
    renderRows(d.rows);
    renderFills(d.fills);
    setLive(true);
  } catch (e) {
    setLive(false);
    console.error(e);
  }
}
tick(); setInterval(tick, REFRESH_MS);
fetchPenny(); setInterval(fetchPenny, 3000);

let _searchTimer = null;
function renderArbs(arbs) {
  const p = document.getElementById("arb-panel");
  if (!arbs.length) { p.style.display = "none"; return; }
  p.style.display = "block";
  p.classList.remove("ok");
  const rows = arbs.map(a => {
    const yLink = `<a class="mkt" href="${kalshiUrl(a.yes_ticker)}" target="_blank" rel="noopener">${a.yes_ticker.split("-").pop()}</a>`;
    const nLink = `<a class="mkt" href="${kalshiUrl(a.no_ticker)}" target="_blank" rel="noopener">${a.no_ticker.split("-").pop()}</a>`;
    return `<div class="a-row">${a.event} · YES ${yLink}@${a.yes_px}¢ + NO ${nLink}@${a.no_px}¢ = <strong>${a.sum}¢</strong> (loss ${a.sum-100}¢ in ${a.no_strike}–${a.yes_strike} range)</div>`;
  }).join("");
  p.innerHTML = `<div class="ah">⚠ ${arbs.length} arb pair${arbs.length===1?'':'s'} resting · auto-fill bait
    <button class="act danger" style="margin-left:12px;" onclick="resolveArbs()">Cancel one leg of each</button></div>
    ${rows}`;
}
async function resolveArbs() {
  if (!confirm("Cancel the smaller leg of every detected arb pair?")) return;
  showToast("Resolving arbs...", true);
  try {
    const r = await apiFetch("/api/resolve-arbs", {method:"POST", headers:{"Content-Type":"application/json"}, body: "{}"});
    const j = await r.json();
    if (j.ok) {
      showToast(`Cancelled ${j.legs_cancelled} leg(s) across ${j.arbs_found} arb(s).`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || "unknown"), false);
    }
  } catch (e) { showToast("ERROR: " + e.message, false); }
}

async function runSearch() {
  const q = document.getElementById("search-q").value;
  try {
    const r = await fetch("/api/search?q=" + encodeURIComponent(q));
    const j = await r.json();
    renderSearch(j);
  } catch (e) { console.error(e); }
}
function renderSearch(j) {
  const results = j.results || [];
  const followed = new Set(j.followed || []);
  document.getElementById("search-stats").textContent =
    `${results.length} match · following ${followed.size}`;
  const html = results.map(r => {
    const ends = r.ends_in_h !== null && r.ends_in_h !== undefined
      ? (r.ends_in_h < 1 ? Math.round(r.ends_in_h*60)+"m"
         : r.ends_in_h < 24 ? r.ends_in_h.toFixed(1)+"h"
         : (r.ends_in_h/24).toFixed(1)+"d") : "—";
    const ev = r.event.replace(/'/g, "\\'");
    const btn = r.followed
      ? `<button class="act danger" onclick="unfollow('${ev}')">Unfollow</button>`
      : `<button class="act primary" onclick="follow('${ev}')">+ Follow</button>`;
    const esc = s => (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const title = r.title ? esc(r.title) : esc(r.event);
    const sub   = r.title ? `<span class="sr-slug" style="font-size:11px;color:#9aa;">${esc(r.event)}</span>` : '';
    return `<div class="sr-row">
      <span class="sr-ev"><div>${title}</div>${sub}</span>
      <span class="sr-stats">${r.strikes} strike${r.strikes===1?'':'s'} · ends ${ends}</span>
      <span class="sr-rate">$${r.total_per_hr.toFixed(2)}/hr</span>
      ${btn}
    </div>`;
  }).join("");
  document.getElementById("search-results").innerHTML = html ||
    '<div class="dim" style="padding:6px 0;">No incentivized events match.</div>';
}
async function follow(ev) {
  const r = await apiFetch("/api/follow", {method:"POST", headers:{"Content-Type":"application/json"},
                                        body: JSON.stringify({event: ev})});
  const j = await r.json();
  if (j.ok) { showToast("Now following " + ev, true); runSearch(); tick(); }
  else { showToast("FAIL: " + (j.error || "unknown"), false); }
}
async function unfollow(ev) {
  const r = await apiFetch("/api/unfollow", {method:"POST", headers:{"Content-Type":"application/json"},
                                          body: JSON.stringify({event: ev})});
  const j = await r.json();
  if (j.ok) { showToast("Unfollowed " + ev, true); runSearch(); tick(); }
}
async function blockMarket(ticker) {
  if (!confirm(`Block ${ticker}?\nThis cancels all your orders on it AND stops the penny bot from placing on it.`)) return;
  showToast(`Blocking ${ticker}...`, true);
  try {
    const r = await apiFetch("/api/block", {method:"POST", headers:{"Content-Type":"application/json"},
                                         body: JSON.stringify({ticker})});
    const j = await r.json();
    if (j.ok) {
      const errs = (j.errors && j.errors.length) ? ` (errors: ${j.errors.join("; ")})` : "";
      showToast(`Blocked ${ticker} · cancelled ${j.cancelled} order(s)${errs}`, true);
      tick();
    } else {
      showToast("FAIL: " + (j.error || "unknown"), false);
    }
  } catch (e) { showToast("ERROR: " + e.message, false); }
}
async function unblockMarket(ticker) {
  showToast(`Unblocking ${ticker}...`, true);
  try {
    const r = await apiFetch("/api/unblock", {method:"POST", headers:{"Content-Type":"application/json"},
                                           body: JSON.stringify({ticker})});
    const j = await r.json();
    if (j.ok) { showToast(`Unblocked ${ticker}`, true); tick(); }
    else { showToast("FAIL: " + (j.error || "unknown"), false); }
  } catch (e) { showToast("ERROR: " + e.message, false); }
}
document.getElementById("search-q").addEventListener("input", () => {
  clearTimeout(_searchTimer);
  _searchTimer = setTimeout(runSearch, 200);
});
runSearch();
</script>
</body></html>
"""


class App:
    def __init__(self):
        self._cache = None
        self._cache_ts = 0
        self._lock = threading.Lock()
        self._build_lock = threading.Lock()
        self._refresher_started = False

    def _build_and_store(self):
        """Build a fresh snapshot. Single-flight via self._build_lock so concurrent
        callers don't pile up build_summary()."""
        with self._build_lock:
            try:
                d = build_summary()
            except Exception as e:
                # Don't poison the cache on transient errors; keep serving the
                # last good snapshot. Log to stderr.
                import traceback, sys
                print(f"build_summary failed: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                return None
        with self._lock:
            self._cache = d
            self._cache_ts = time.time()
        return d

    def get_data(self):
        # Always serve the cached snapshot if we have one — this keeps /api/data
        # latency at <5ms regardless of how slow build_summary() runs.
        with self._lock:
            cached = self._cache
        if cached is not None:
            return cached
        # Cold start: no cache yet. Build synchronously so the first hit returns
        # real data instead of an empty payload.
        d = self._build_and_store()
        return d if d is not None else {"as_of": time.strftime("%H:%M:%S"),
                                        "elapsed_s": 0, "rows": [],
                                        "events": [], "fills": [], "arbs": [],
                                        "total_orders": 0, "total_markets": 0,
                                        "total_pool_per_hr": 0,
                                        "total_per_hr": 0, "total_per_min": 0,
                                        "balance": {"balance_cents": 0, "portfolio_cents": 0}}

    def invalidate(self):
        # Drop the cache and immediately rebuild (background) so the next /api/data
        # gets a warm cache rather than triggering a sync rebuild.
        with self._lock:
            self._cache = None
            self._cache_ts = 0
        threading.Thread(target=self._build_and_store, daemon=True).start()

    def start_refresher(self, interval_s=1.5):
        if self._refresher_started: return
        self._refresher_started = True
        def loop():
            while True:
                try:
                    self._build_and_store()
                except Exception:
                    pass
                time.sleep(interval_s)
        threading.Thread(target=loop, daemon=True).start()


def make_handler(app):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            try:
                if self.path in ("/", "/index.html"):
                    page = (PAGE.replace("__MAX_OVER_BEST__", str(MAX_OVER_BEST))
                                .replace("__MUTATION_TOKEN__", LOCAL_MUTATION_TOKEN))
                    self._send(200, page.encode(), "text/html; charset=utf-8")
                elif self.path == "/api/data":
                    d = app.get_data()
                    self._send(200, json.dumps(d).encode(), "application/json")
                elif self.path == "/api/penny":
                    self._send(200, json.dumps(PENNY.settings()).encode(), "application/json")
                elif self.path.startswith("/api/search"):
                    from urllib.parse import urlparse, parse_qs
                    qs = parse_qs(urlparse(self.path).query)
                    q = (qs.get("q", [""])[0] or "")
                    out = search_incentivized_events(q)
                    self._send(200, json.dumps(out).encode(), "application/json")
                else:
                    self._send(404, b"not found", "text/plain")
            except Exception as e:
                msg = f"error: {type(e).__name__}: {e}".encode()
                self._send(500, msg, "text/plain")

        def do_POST(self):
            try:
                if self.headers.get("X-Local-Token") != LOCAL_MUTATION_TOKEN:
                    self._send(403, json.dumps({"ok": False, "error": "bad local token"}).encode(), "application/json")
                    return
                length = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(length) if length else b"{}"
                req = json.loads(raw.decode() or "{}")

                if self.path == "/api/move":
                    result = move_orders(
                        req.get("ticker", ""),
                        req.get("side", "").lower(),
                        int(req.get("price_cents", 0)),
                    )
                elif self.path == "/api/cancel":
                    result = cancel_side(
                        req.get("ticker", ""),
                        req.get("side", "").lower(),
                    )
                elif self.path == "/api/cancel-zero":
                    result = cancel_zero_reward_orders()
                elif self.path == "/api/cancel-bad-edge":
                    thr = req.get("threshold_cents")
                    if thr is None: thr = -1
                    result = cancel_bad_edge_orders(int(thr))
                elif self.path == "/api/place-bulk":
                    result = {"ok": False, "error": "place-bulk disabled"}
                elif self.path == "/api/penny":
                    PENNY.update(**req)
                    result = {"ok": True, **PENNY.settings()}
                elif self.path == "/api/follow":
                    result = follow_event(req.get("event", ""))
                elif self.path == "/api/unfollow":
                    result = unfollow_event(req.get("event", ""))
                elif self.path == "/api/block":
                    result = block_market(req.get("ticker", ""))
                elif self.path == "/api/unblock":
                    result = unblock_market(req.get("ticker", ""))
                elif self.path == "/api/circuit-reset":
                    result = untrip_event(req.get("event", ""))
                elif self.path == "/api/circuit-trip":
                    result = trip_event(req.get("event", ""),
                                        req.get("reason", "manual trip"))
                elif self.path == "/api/resolve-arbs":
                    result = resolve_arbs_now(app.get_data)
                elif self.path == "/api/refresh-theos":
                    result = refresh_theos_now()
                elif self.path == "/api/overnight/reset":
                    result = overnight_reset()
                elif self.path == "/api/overnight/stop":
                    result = overnight_stop()
                else:
                    self._send(404, b"not found", "text/plain"); return

                if result.get("ok"):
                    app.invalidate()
                code = 200 if result.get("ok") else 400
                self._send(code, json.dumps(result).encode(), "application/json")
            except Exception as e:
                msg = json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}).encode()
                self._send(500, msg, "application/json")

        def log_message(self, fmt, *args): pass
    return Handler


_THEO_REFRESH_SCRIPT = Path(__file__).resolve().parent / "theo_refresh.py"
_theo_refresh_state = {"last_run": 0.0, "last_results": [], "last_error": ""}
_theo_refresh_lock = threading.Lock()


def refresh_theos_now(timeout_s=30):
    """Run theo_refresh.py and return parsed results."""
    import subprocess, sys as _sys
    if not _THEO_REFRESH_SCRIPT.exists():
        return {"ok": False, "error": f"refresher not found at {_THEO_REFRESH_SCRIPT}"}
    try:
        proc = subprocess.run([_sys.executable, str(_THEO_REFRESH_SCRIPT)],
                              capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "refresher timeout"}
    parsed = None
    try:
        parsed = json.loads(proc.stdout) if proc.stdout.strip() else None
    except Exception:
        pass
    with _theo_refresh_lock:
        _theo_refresh_state["last_run"] = time.time()
        _theo_refresh_state["last_results"] = (parsed or {}).get("results", [])
        _theo_refresh_state["last_error"] = proc.stderr[-500:] if proc.returncode else ""
    return {"ok": proc.returncode == 0, "results": (parsed or {}).get("results", []),
            "stderr": proc.stderr[-500:]}


def start_theo_refresh_loop(interval_s=60):
    def loop():
        while True:
            try: refresh_theos_now()
            except Exception: pass
            time.sleep(interval_s)
    threading.Thread(target=loop, daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--theo-refresh-s", type=int, default=60,
                    help="Auto-refresh theos every N seconds (0 to disable)")
    args = ap.parse_args()

    app = App()
    PENNY.set_snapshot_fn(app.get_data)
    app.start_refresher(interval_s=1.5)
    if args.theo_refresh_s > 0:
        start_theo_refresh_loop(args.theo_refresh_s)
        print(f"Theo auto-refresh: every {args.theo_refresh_s}s")
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(app))
    token_path = Path.home() / ".kalshi_local_token"
    try:
        token_path.write_text(LOCAL_MUTATION_TOKEN)
        token_path.chmod(0o600)
    except Exception as e:
        print(f"warn: could not write {token_path}: {e}")
    print(f"Serving on http://localhost:{args.port}  (live per-market reward rates)")
    print(f"Local mutation token written to {token_path} "
          f"(use: curl -H \"X-Local-Token: $(cat {token_path})\" ...)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
