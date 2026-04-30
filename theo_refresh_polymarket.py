#!/usr/bin/env python3
"""Polymarket-mirrored theo refreshers for selected Kalshi events.

Sister module to theo_refresh.py. Each refresher pulls a live mid from the
Polymarket Gamma API (https://gamma-api.polymarket.com/) for a market that
mirrors a Kalshi event, and writes the new probabilities into the existing
theo JSON at /Users/wilsonw/Downloads/theos/{event_ticker}.json.

Probe results (2026-04-30, while drafting this module):

    KXAPRPOTUS-26MAY01      -- NO clean mirror.
        Polymarket has an event 'trump-approval-rating-on-may-1' with bucket
        markets, but its resolution source is *Silver Bulletin*, while the
        Kalshi event resolves to *RealClearPolitics*. RCP and SB run
        ~1.5-3.5pp apart historically (per the Kalshi theo's own notes:
        RCP 40.7 vs SB 39.0 vs FPO 37.0 on 2026-04-28). Mirroring SB-priced
        buckets onto RCP-priced strikes would be wrong. Polymarket bucket
        edges (0.5pp wide: <38.0, 38.0-38.4, 38.5-38.9, 39.0-39.4,
        39.5-39.9, >=40.0) also do not align with the Kalshi 0.3pp strikes
        centered at 39.9, 40.0, 40.3, 40.6, 40.9, 41.2, 41.5, 41.6.

    KXGA1ROUND-26NOV03      -- NO clean mirror.
        Polymarket has D-wins / R-wins binaries for the Georgia Senate
        ('will-the-democrats-win-the-georgia-senate-race-in-2026' and
        'will-the-republicans-win-...'), but no market on whether the GA
        2026 Senate or any other GA statewide race goes to a runoff /
        clears 50% in the first round. Kalshi is a 9-strike multi-office
        first-round market.

    KXHORMUZWEEKLY-26MAY03  -- NO clean mirror.
        No Polymarket market on IMF PortWatch Hormuz weekly transit counts.
        Iran-related markets on Polymarket are framed as US-Iran nuclear
        deal / regime fall / NPT withdrawal -- none track ship traffic.

    KXMIDTERMMOV-{state}    -- NO clean mirror (all 16).
        Polymarket has only the binary 'will the {party} win the {state}
        senate race in 2026' for each state. Kalshi MIDTERMMOV is a
        multi-strike margin distribution (P13, P15, ..., P34, etc., where
        the suffix is a positive margin in pp for the favored party).
        Knowing P(party wins) gives only P(margin > 0); it does not pin
        down the mu/sigma needed to populate the Kalshi P-strikes. Mapping
        would require inventing sigma -- fabricated, so skipped.

Therefore REFRESHERS is empty in this module. The Polymarket helper
(`fetch_polymarket_market`) and the per-event stub functions are kept so
the wiring is ready if Polymarket adds a clean equivalent later.
"""
import datetime as dt
import json
import math
from pathlib import Path

import requests

THEOS_DIR = Path("/Users/wilsonw/Downloads/theos")
GAMMA_BASE = "https://gamma-api.polymarket.com"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
HDRS = {"User-Agent": UA}
HTTP_TIMEOUT = 10


def _now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _norm_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def fetch_polymarket_market(slug):
    """Return (mid, best_bid, best_ask, last_trade, yes_price, raw) for a
    binary Polymarket market identified by slug. Mid = midpoint of bestBid
    and bestAsk; falls back to outcomePrices[0] (the YES price) when the
    book is one-sided. Raises RuntimeError on parse / HTTP failure.
    """
    r = requests.get(f"{GAMMA_BASE}/markets",
                     params={"slug": slug}, headers=HDRS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        raise RuntimeError(f"polymarket: no market for slug {slug!r}")
    m = arr[0]
    if m.get("closed") or m.get("archived"):
        raise RuntimeError(f"polymarket: market {slug!r} is closed/archived")
    try:
        bid = float(m.get("bestBid")) if m.get("bestBid") is not None else None
        ask = float(m.get("bestAsk")) if m.get("bestAsk") is not None else None
    except (TypeError, ValueError):
        bid = ask = None
    last = m.get("lastTradePrice")
    try:
        last = float(last) if last is not None else None
    except (TypeError, ValueError):
        last = None
    yes = None
    op = m.get("outcomePrices")
    if isinstance(op, str):
        try:
            op = json.loads(op)
        except (TypeError, ValueError):
            op = None
    if isinstance(op, list) and len(op) >= 1:
        try:
            yes = float(op[0])
        except (TypeError, ValueError):
            yes = None
    if bid is not None and ask is not None and 0 < bid <= ask < 1:
        mid = 0.5 * (bid + ask)
    elif yes is not None:
        mid = yes
    elif last is not None:
        mid = last
    else:
        raise RuntimeError(f"polymarket: no usable price for {slug!r}")
    return {
        "mid": mid,
        "bid": bid,
        "ask": ask,
        "last": last,
        "yes": yes,
        "slug": slug,
    }


def fetch_polymarket_event(slug):
    """Return the event dict (incl. its sub-markets) for a Polymarket event
    slug. Used for multi-bucket events like 'trump-approval-rating-on-may-1'.
    """
    r = requests.get(f"{GAMMA_BASE}/events",
                     params={"slug": slug}, headers=HDRS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        raise RuntimeError(f"polymarket: no event for slug {slug!r}")
    return arr[0]


# ---------------------------------------------------------------------------
# Per-event stubs. None of these have a clean Polymarket mirror as of
# 2026-04-30, so they all return ok=False with an explanatory error rather
# than mutating the theo file. They are kept so the parent file can wire
# them up symmetrically once Polymarket adds matching markets.
# ---------------------------------------------------------------------------


def refresh_kxaprpotus(event="KXAPRPOTUS-26MAY01"):
    """Trump RCP approval on May 1 (Kalshi) vs Trump SB approval on May 1
    (Polymarket event 'trump-approval-rating-on-may-1'). Resolution sources
    differ by ~1.5-3.5pp and the bucket grids do not align -- not a mirror.
    """
    return {"event": event, "ok": False, "polymarket_mid": None,
            "error": "no clean mirror: Polymarket resolves to Silver Bulletin, "
                     "Kalshi resolves to RealClearPolitics (~1.5-3.5pp gap); "
                     "bucket grids also differ (0.5pp vs 0.3pp)."}


def refresh_kxga1round(event="KXGA1ROUND-26NOV03"):
    """Georgia 2026 statewide first-round (>50%, no runoff) -- 9 office
    strikes (SEN, GOV, LTGOV, AG, SOS, COM, COI, COL, SUP). Polymarket has
    only D-wins / R-wins binaries for the GA Senate, no first-round /
    runoff market for any GA office.
    """
    return {"event": event, "ok": False, "polymarket_mid": None,
            "error": "no clean mirror: Polymarket has no GA first-round / "
                     "runoff market for any of the 9 statewide offices."}


def refresh_kxhormuzweekly(event="KXHORMUZWEEKLY-26MAY03"):
    """Strait of Hormuz weekly transit count (IMF PortWatch). Polymarket
    has Iran-deal / regime / NPT markets but nothing on Hormuz ship traffic.
    """
    return {"event": event, "ok": False, "polymarket_mid": None,
            "error": "no clean mirror: no Polymarket market on Hormuz "
                     "transit counts (PortWatch)."}


# 16 MIDTERMMOV state events -- all share the same "no clean mirror" reason:
# Polymarket has only binary winner markets, Kalshi is multi-strike margin.
_MIDTERMMOV_STATES = [
    "ALSENR", "ARSENR", "COSEND", "DESEND", "IDSENR", "ILSEND",
    "KSSENR", "KYSENR", "LASENR", "MASEND", "MNSEND", "MSSENR",
    "NJSEND", "NMSEND", "OKSENR", "ORSEND",
]


def refresh_kxmidtermmov(event):
    """Per-state Senate margin-of-victory event. Polymarket only carries the
    binary 'will the {party} win the {state} senate race in 2026'; mapping
    that onto the Kalshi P{margin} strike vector requires assuming a sigma
    (fabrication), so we skip.
    """
    return {"event": event, "ok": False, "polymarket_mid": None,
            "error": "no clean mirror: Polymarket has only binary winner; "
                     "Kalshi is multi-strike margin (sigma cannot be "
                     "inferred from a single binary price)."}


# ---------------------------------------------------------------------------
# REFRESHERS dict -- empty because none of the requested events have a
# clean Polymarket mirror as of 2026-04-30. The parent file can still
# import the stubs above for diagnostic / status reporting.
# ---------------------------------------------------------------------------

REFRESHERS = {}


# Stubs exposed for callers that want a structured "skipped because..."
# response per event. These are NOT wired into REFRESHERS (so running
# theo_refresh.py won't touch the theo files) but they do return the
# {event, ok, error} shape if you want to log them.
SKIP_STUBS = {
    "KXAPRPOTUS-26MAY01":      lambda: refresh_kxaprpotus("KXAPRPOTUS-26MAY01"),
    "KXGA1ROUND-26NOV03":      lambda: refresh_kxga1round("KXGA1ROUND-26NOV03"),
    "KXHORMUZWEEKLY-26MAY03":  lambda: refresh_kxhormuzweekly("KXHORMUZWEEKLY-26MAY03"),
    **{f"KXMIDTERMMOV-{s}":    (lambda s=s: refresh_kxmidtermmov(f"KXMIDTERMMOV-{s}"))
       for s in _MIDTERMMOV_STATES},
}


def main():
    """Diagnostic entry point: prints the skip reason for each requested
    event. Does NOT mutate any theo files (REFRESHERS is empty).
    """
    results = []
    for k, fn in SKIP_STUBS.items():
        try:
            results.append(fn())
        except Exception as e:
            results.append({"event": k, "ok": False,
                            "error": f"{type(e).__name__}: {e}"})
    print(json.dumps({"results": results, "as_of": _now_iso()}, indent=2))


if __name__ == "__main__":
    main()
