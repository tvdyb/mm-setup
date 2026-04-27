"""
Place resting BUY orders @ 1¢ on YES and NO for every sub-market in the
KXTRUFBFST-26APR27 (Truflation Breakfast Commodity Index, 2026-04-27) event.

Defaults to --dry-run. Pass --live to actually submit.
"""

import argparse
import base64
import datetime as dt
import json
import os
import sys
import time
import uuid
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KEY_PATH = Path(os.environ.get("KALSHI_KEY_PATH", "private_key.pem"))
BASE = "https://api.elections.kalshi.com/trade-api/v2"
EVENT_TICKER = "KXTRUFBFST-26APR27"
COUNT = 300
PRICE_CENTS = 1


def load_key():
    return serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)


def sign(private_key, ts_ms: str, method: str, path: str) -> str:
    msg = (ts_ms + method.upper() + path).encode()
    sig = private_key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode()


def auth_headers(private_key, method: str, path_for_sig: str) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sign(private_key, ts, method, path_for_sig),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
        "accept": "application/json",
    }


def request(private_key, method: str, path: str, *, params=None, body=None):
    # Signature path = the request path WITHOUT query string, but INCLUDING the /trade-api/v2 prefix.
    path_for_sig = "/trade-api/v2" + path
    headers = auth_headers(private_key, method, path_for_sig)
    url = BASE + path
    r = requests.request(method, url, headers=headers, params=params,
                         json=body if body is not None else None, timeout=15)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")
    return r.json() if r.text else {}


def list_markets(pk):
    out = []
    cursor = None
    while True:
        params = {"event_ticker": EVENT_TICKER, "limit": 200, "status": "open"}
        if cursor:
            params["cursor"] = cursor
        # Path used for signature should not include query — sign just the path.
        data = request(pk, "GET", "/markets", params=params)
        out.extend(data.get("markets", []))
        cursor = data.get("cursor") or None
        if not cursor:
            break
    return out


def list_resting_orders(pk, tickers):
    """Return dict {(ticker, side): True} for existing resting/open BUY orders."""
    existing = {}
    cursor = None
    while True:
        params = {"status": "resting", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = request(pk, "GET", "/portfolio/orders", params=params)
        for o in data.get("orders", []):
            if o.get("action") != "buy":
                continue
            t = o.get("ticker")
            if t in tickers:
                existing[(t, o.get("side"))] = o
        cursor = data.get("cursor") or None
        if not cursor:
            break
    return existing


def place_order(pk, ticker: str, side: str):
    body = {
        "action": "buy",
        "client_order_id": str(uuid.uuid4()),
        "count": COUNT,
        "side": side,                # "yes" or "no"
        "ticker": ticker,
        "type": "limit",
        "post_only": True,
        "yes_price": PRICE_CENTS if side == "yes" else None,
        "no_price":  PRICE_CENTS if side == "no" else None,
    }
    body = {k: v for k, v in body.items() if v is not None}
    return request(pk, "POST", "/portfolio/orders", body=body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="actually submit (default is dry-run)")
    args = ap.parse_args()

    pk = load_key()
    print(f"Fetching markets in event {EVENT_TICKER}...")
    markets = list_markets(pk)
    if not markets:
        print("No open markets found.")
        sys.exit(1)

    tickers = {m["ticker"] for m in markets}
    print(f"  {len(markets)} open market(s):")
    for m in markets:
        print(f"    {m['ticker']:40s}  {m.get('title','')[:60]}")

    print("\nChecking existing resting BUY orders...")
    existing = list_resting_orders(pk, tickers)
    if existing:
        for (t, s), o in existing.items():
            print(f"    SKIP {t} {s}: existing buy id={o.get('order_id','?')[:8]} "
                  f"px={o.get('yes_price') or o.get('no_price')} "
                  f"remaining={o.get('remaining_count')}")
    else:
        print("    (none)")

    plan = []
    for m in markets:
        for side in ("yes", "no"):
            if (m["ticker"], side) in existing:
                continue
            plan.append((m["ticker"], side))

    print(f"\nPlanned NEW orders: {len(plan)}  ({COUNT} contracts @ {PRICE_CENTS}¢ each, post_only GTC)")
    for t, s in plan:
        print(f"    BUY {s.upper():3s}  {t}   {COUNT} @ {PRICE_CENTS}¢   max=${COUNT*PRICE_CENTS/100:.2f}")
    print(f"\nTotal max capital at risk: ${len(plan)*COUNT*PRICE_CENTS/100:.2f}")

    if not args.live:
        print("\nDRY RUN — no orders submitted. Re-run with --live to submit.")
        return

    print("\nSubmitting...")
    results = []
    for t, s in plan:
        try:
            resp = place_order(pk, t, s)
            oid = resp.get("order", {}).get("order_id", "?")
            print(f"    OK   BUY {s.upper():3s} {t}  id={oid}")
            results.append({"ticker": t, "side": s, "order_id": oid, "status": "ok"})
        except Exception as e:
            print(f"    FAIL BUY {s.upper():3s} {t}  {e}")
            results.append({"ticker": t, "side": s, "error": str(e), "status": "fail"})
    out = Path(f"kalshi_breakfast_orders_{dt.datetime.now():%Y%m%d_%H%M%S}.json")
    out.write_text(json.dumps(results, indent=2))
    print(f"\nLog: {out}")


if __name__ == "__main__":
    main()
