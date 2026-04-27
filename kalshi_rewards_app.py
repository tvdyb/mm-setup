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
import argparse, base64, datetime as dt, json, os, time, threading, uuid
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from collections import defaultdict
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KEY_ID   = os.environ.get("KALSHI_KEY_ID", "")
KEY_PATH = Path(os.environ.get("KALSHI_KEY_PATH", "private_key.pem"))
BASE     = "https://api.elections.kalshi.com/trade-api/v2"
DEFAULT_TARGET_SIZE = 300  # fallback contract cap if a program doesn't specify one
MAX_OVER_BEST = 5  # max ¢ over best competitor bid the UI will allow
INCENTIVE_CACHE_S = 60  # programs change rarely; cache for 1 min
FOLLOWED_PATH = Path(os.environ.get("KALSHI_FOLLOWED_PATH", "kalshi_followed_events.json"))
DEFAULT_FOLLOWED = []
BLOCKED_PATH = Path(os.environ.get("KALSHI_BLOCKED_PATH", "kalshi_blocked_markets.json"))


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
    cancelled = 0
    errors = []
    for side in ("yes", "no"):
        res = cancel_side(t, side)
        if res.get("ok"):
            cancelled += res.get("count", 0)
        elif "no resting" not in (res.get("error") or ""):
            errors.append(f"{side}: {res.get('error')}")
    return {"ok": True, "ticker": t, "cancelled": cancelled,
            "errors": errors, "blocked": sorted(BLOCKED_MARKETS)}


def unblock_market(ticker):
    t = (ticker or "").strip().upper()
    with _blocked_lock:
        BLOCKED_MARKETS.discard(t)
    _save_blocked()
    return {"ok": True, "blocked": sorted(BLOCKED_MARKETS)}


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
    return {"results": out[:50], "followed": sorted(followed)}

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
    """KXTRUFBFST-26APR27-T89.50 -> 89.5; non-strike tickers -> None."""
    last = ticker.split("-")[-1]
    if last and last[0].upper() == "T":
        try: return float(last[1:])
        except ValueError: return None
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
        self.interval = 30          # seconds between scans
        self.spread_min = 15        # ¢ — only penny if (ask - bid) > this
        self.share_max_pct = 5.0    # only penny if my share < this
        self.size = 30              # contracts per penny order
        self.cooldown_s = 240       # min seconds between prospecting pennies on same (ticker, side)
        self.defend_cooldown_s = 20 # short cooldown when defending against a jumper
        self.gap_min = 5            # ¢ — also penny if (best_bid - my_top_px) >= this,
                                    # regardless of share. Catches "I'm queued behind
                                    # a thin wall at the top while sitting at 3¢."
        self.auto_resolve_arbs = True
        self.arb_interval = 4       # seconds between arb sweeps
        self._cooldown = {}         # (ticker, side) -> last_attempt_ts
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
        return self.settings()

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
        for r in rows:
            if r.get("blocked"): continue
            best_bid, best_ask = r.get("best_bid"), r.get("best_ask")
            if best_bid is None or best_ask is None: continue
            spread = best_ask - best_bid
            my_top = r.get("my_top_px")
            share_pct = r.get("share_pct", 0)

            # Reasons we'd want to act on this row:
            jumped    = (my_top is not None) and (best_bid > my_top)
            far_below = (my_top is not None) and (best_bid - my_top >= self.gap_min)
            low_share = share_pct < self.share_max_pct and r.get("has_orders")
            no_orders = (my_top is None)
            is_defense = jumped or far_below or low_share
            if not (is_defense or no_orders): continue
            # Note: spread_min is no longer a hard gate. Every row in the
            # dashboard is on a market we explicitly chose to participate in
            # (followed event or one we already trade), so we always try to
            # place. The bump formula below falls back to joining-at-best on
            # tight spreads where we can't legally outbid without crossing.
            scanned += 1

            # Bid 1/8 of the spread above best (rounded down). On a 1¢ spread,
            # bump down to 0 — join at best_bid via post_only rather than skip.
            if spread <= 1:
                bump = 0
            else:
                bump = max(1, spread // 8)
            target_px = best_bid + bump
            # No-cross safety: never place a bid at/above the best ask.
            # Combined with post_only=True on the order, this guarantees
            # we cannot take liquidity. Both checks are required because
            # the orderbook can shift between snapshot and submission.
            if target_px >= best_ask: continue
            if my_top is not None and my_top >= target_px: continue

            ticker = r["ticker"]; side = r["side"].lower()
            # Anti-arb gate: never place a bid that combined with an existing
            # opposite-side bid (resting OR placed earlier this cycle) would
            # form an arb pair — sum > 100¢ across YES_X + NO_Y where X >= Y.
            if would_create_arb(rows, ticker, side, target_px, pending=pending):
                self._emit(f"skip {side.upper()} {ticker} {target_px}¢: would create arb", ok=False)
                continue
            cooldown = self.defend_cooldown_s if is_defense else self.cooldown_s
            with self._lock:
                last = self._cooldown.get((ticker, side), 0)
            if now - last < cooldown: continue
            with self._lock:
                self._cooldown[(ticker, side)] = now

            tag = "DEFEND" if is_defense else "penny"
            resp = place_buy(ticker, side, self.size, target_px)
            if resp.status_code < 300:
                placed += 1
                pending.append((ticker, side, target_px))
                self._emit(f"{tag} {side.upper()} {ticker} {target_px}¢×{self.size} (spread={spread}¢, share={share_pct:.1f}%, my_top={my_top})", ok=True)
            else:
                self._emit(f"{tag} FAIL {side.upper()} {ticker} {target_px}¢: {resp.status_code} {resp.text[:80]}", ok=False)
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


def cancel_order(order_id):
    return signed_request("DELETE", f"/portfolio/orders/{order_id}")


def place_buy(ticker, side, count, price_cents):
    body = {
        "action": "buy",
        "client_order_id": str(uuid.uuid4()),
        "count": int(count),
        "side": side,
        "ticker": ticker,
        "type": "limit",
        "post_only": True,
    }
    if side == "yes":
        body["yes_price"] = int(price_cents)
    else:
        body["no_price"] = int(price_cents)
    return signed_request("POST", "/portfolio/orders", body=body)


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
    results = []
    for o in targets:
        r = cancel_order(o["order_id"])
        results.append({"id": o["order_id"][:8], "status": r.status_code})
        time.sleep(0.15)
    ok = all(x["status"] < 300 for x in results)
    return {"ok": ok, "ticker": ticker, "side": side,
            "cancelled": results, "count": len(results)}


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


DEFAULT_NEW_ORDER_SIZE = 300  # contracts for placing on a fresh strike


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
        r = place_buy(t, side, DEFAULT_NEW_ORDER_SIZE, target_px)
        ok = r.status_code < 300
        if ok: placed += 1
        results.append({"ticker": t, "px": target_px, "status": r.status_code,
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

    cancel_results = []
    for o in targets:
        r = cancel_order(o["order_id"])
        cancel_results.append({"id": o["order_id"][:8], "status": r.status_code})
        time.sleep(0.15)

    place_r = place_buy(ticker, side, total_count, int(new_price_cents))
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
        "count": total_count,
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
    my_event_tickers = set()
    for o in orders:
        if o.get("action") == "buy":
            by_ts[(o["ticker"], o["side"])].append(o)
            my_event_tickers.add(event_ticker_of(o["ticker"]))

    # Scope: events I'm trading + any explicitly-followed events, × all strikes
    # in them, then filter to strikes that currently have an active rewards
    # program. Plus my own resting orders so they never disappear from view.
    candidate = set(t for t, _ in by_ts.keys())
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

    my_orders_set = set(t for t, _ in by_ts.keys())
    all_tickers = sorted((candidate & incentivized) | my_orders_set)
    books = fetch_orderbooks_parallel(all_tickers)

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
            book_side = book.get(side, [])
            other_side = book.get("no" if side == "yes" else "yes", [])
            my_w, tot_w, my_in_top, tot_in_top = compute_reward_share(book_side, side_orders, cap=target)
            share = (my_w / tot_w) if tot_w else 0.0
            best = book_side[0][0] if book_side else None
            top_my_px = max((order_price_cents(o) for o in side_orders), default=None)
            ahead = cumulative_strictly_better(book_side, top_my_px) if top_my_px is not None else None
            best_ask = (100 - other_side[0][0]) if other_side else None
            book_top = [[int(px), int(sz)] for px, sz in book_side[:5]]
            est_per_hr = share * rate_per_side
            total_per_hr += est_per_hr
            rows.append({
                "ticker": ticker,
                "side": side.upper(),
                "best_bid": best,
                "best_ask": best_ask,
                "book_top": book_top,
                "my_top_px": top_my_px,
                "my_total_sz": sum(order_size(o) for o in side_orders),
                "my_levels": [(order_price_cents(o), order_size(o)) for o in
                              sorted(side_orders, key=lambda o: -order_price_cents(o))],
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
                "blocked": ticker in blocked_snapshot,
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

    # Fills
    fills = get_fills(limit=30)
    fills_clean = []
    for f in fills:
        try:
            cnt = int(float(f.get("count_fp", 0)))
        except (TypeError, ValueError):
            cnt = 0
        side = f.get("side", "")
        px = round(float(f.get("yes_price_dollars" if side == "yes" else "no_price_dollars", "0")) * 100)
        fills_clean.append({
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
        "fills": fills_clean,
        "events": events,
        "arbs": arbs,
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

<div class="summary" id="summary"></div>

<h2>Auto-penny · post-only top-of-book on wide-spread incentivized markets</h2>
<div id="penny-panel">
  <span class="pp-name">Auto-penny</span>
  <span id="penny-toggle" class="toggle off" onclick="togglePenny()">OFF</span>
  <label>spread &gt; <input type="number" id="pp-spread" min="1" max="99" value="15">¢</label>
  <label>my share &lt; <input type="number" id="pp-share" min="0" max="100" step="0.5" value="5">%</label>
  <label>or gap &gt; <input type="number" id="pp-gap" min="1" max="99" value="5">¢</label>
  <label>size <input type="number" id="pp-size" min="1" max="500" value="30"></label>
  <label>every <input type="number" id="pp-interval" min="10" max="600" value="30">s</label>
  <label>cooldown <input type="number" id="pp-cooldown" min="0" max="3600" value="240">s</label>
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
    </h2>
    <table>
      <thead><tr>
        <th>Market · side</th>
        <th>My px</th>
        <th>Size</th>
        <th>Spread</th>
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
  const r = await fetch("/api/penny", {method: "POST", headers:{"Content-Type":"application/json"},
                                       body: JSON.stringify({auto_resolve_arbs: !cur})});
  const j = await r.json();
  renderPenny(j);
  showToast("Anti-arb " + (j.auto_resolve_arbs ? "ON" : "OFF"), true);
}

async function togglePenny() {
  const cur = document.getElementById("penny-toggle").textContent === "ON";
  const r = await fetch("/api/penny", {method: "POST", headers:{"Content-Type":"application/json"},
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
  const r = await fetch("/api/penny", {method: "POST", headers:{"Content-Type":"application/json"},
                                       body: JSON.stringify(body)});
  const j = await r.json();
  renderPenny(j);
  showToast("Auto-penny settings saved", true);
}

async function placeBulk(eventTicker, side, overBest) {
  if (!confirm(`Place 300-contract orders on every untraded ${side.toUpperCase()} strike of ${eventTicker} at best+${overBest}¢?`)) return;
  showToast(`Placing ${side.toUpperCase()} on untraded strikes of ${eventTicker} ...`, true);
  try {
    const r = await fetch("/api/place-bulk", {
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
    '<tr><td colspan="14" class="dim">No incentivized markets right now.</td></tr>';
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
    const r = await fetch("/api/move", {
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
    const r = await fetch("/api/cancel", {
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
    const r = await fetch("/api/cancel-zero", {
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
    const r = await fetch("/api/resolve-arbs", {method:"POST", headers:{"Content-Type":"application/json"}, body: "{}"});
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
    return `<div class="sr-row">
      <span class="sr-ev">${r.event}</span>
      <span class="sr-stats">${r.strikes} strike${r.strikes===1?'':'s'} · ends ${ends}</span>
      <span class="sr-rate">$${r.total_per_hr.toFixed(2)}/hr</span>
      ${btn}
    </div>`;
  }).join("");
  document.getElementById("search-results").innerHTML = html ||
    '<div class="dim" style="padding:6px 0;">No incentivized events match.</div>';
}
async function follow(ev) {
  const r = await fetch("/api/follow", {method:"POST", headers:{"Content-Type":"application/json"},
                                        body: JSON.stringify({event: ev})});
  const j = await r.json();
  if (j.ok) { showToast("Now following " + ev, true); runSearch(); tick(); }
  else { showToast("FAIL: " + (j.error || "unknown"), false); }
}
async function unfollow(ev) {
  const r = await fetch("/api/unfollow", {method:"POST", headers:{"Content-Type":"application/json"},
                                          body: JSON.stringify({event: ev})});
  const j = await r.json();
  if (j.ok) { showToast("Unfollowed " + ev, true); runSearch(); tick(); }
}
async function blockMarket(ticker) {
  if (!confirm(`Block ${ticker}?\nThis cancels all your orders on it AND stops the penny bot from placing on it.`)) return;
  showToast(`Blocking ${ticker}...`, true);
  try {
    const r = await fetch("/api/block", {method:"POST", headers:{"Content-Type":"application/json"},
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
    const r = await fetch("/api/unblock", {method:"POST", headers:{"Content-Type":"application/json"},
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

    def get_data(self):
        with self._lock:
            now = time.time()
            if self._cache and now - self._cache_ts < 1.5:
                return self._cache
            d = build_summary()
            self._cache = d
            self._cache_ts = now
            return d

    def invalidate(self):
        with self._lock:
            self._cache = None
            self._cache_ts = 0


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
                    page = PAGE.replace("__MAX_OVER_BEST__", str(MAX_OVER_BEST))
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
                elif self.path == "/api/place-bulk":
                    result = place_bulk_untraded(
                        req.get("event", ""),
                        req.get("side", "").lower(),
                        int(req.get("over_best", 1)),
                    )
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
                elif self.path == "/api/resolve-arbs":
                    result = resolve_arbs_now(app.get_data)
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5050)
    args = ap.parse_args()

    app = App()
    PENNY.set_snapshot_fn(app.get_data)
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(app))
    print(f"Serving on http://localhost:{args.port}  (live per-market reward rates)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
