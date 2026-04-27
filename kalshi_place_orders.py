"""Place resting BUY YES + BUY NO orders at a given price on every market in a Kalshi event.
Skips sides where a resting BUY already exists. Defaults to dry-run."""
import argparse, base64, json, os, time, uuid, datetime as dt
from pathlib import Path
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KEY_PATH = Path(os.environ.get("KALSHI_KEY_PATH", "private_key.pem"))
BASE = "https://api.elections.kalshi.com/trade-api/v2"
THROTTLE_S = 0.2


def load_key(): return serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)


def request(pk, method, path, *, params=None, body=None):
    sig_path = "/trade-api/v2" + path
    ts = str(int(time.time()*1000))
    msg = (ts + method + sig_path).encode()
    s = pk.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    h = {"KALSHI-ACCESS-KEY":KEY_ID, "KALSHI-ACCESS-SIGNATURE":base64.b64encode(s).decode(),
         "KALSHI-ACCESS-TIMESTAMP":ts, "accept":"application/json", "Content-Type":"application/json"}
    return requests.request(method, BASE+path, headers=h, params=params, json=body, timeout=15)


def list_markets(pk, event_ticker):
    out = []; cursor = None
    while True:
        p = {"event_ticker": event_ticker, "limit": 200, "status": "open"}
        if cursor: p["cursor"] = cursor
        r = request(pk, "GET", "/markets", params=p)
        if r.status_code >= 400: raise RuntimeError(r.text)
        d = r.json()
        out += d.get("markets", [])
        cursor = d.get("cursor") or None
        if not cursor: break
    return out


def list_resting_buys(pk, ticker_set):
    existing = {}
    cursor = None
    while True:
        p = {"status": "resting", "limit": 200}
        if cursor: p["cursor"] = cursor
        r = request(pk, "GET", "/portfolio/orders", params=p)
        if r.status_code >= 400: raise RuntimeError(r.text)
        d = r.json()
        for o in d.get("orders", []):
            if o.get("action") != "buy": continue
            if o.get("ticker") in ticker_set:
                existing[(o["ticker"], o["side"])] = o
        cursor = d.get("cursor") or None
        if not cursor: break
    return existing


def place(pk, ticker, side, count, price_cents):
    body = {"action":"buy","client_order_id":str(uuid.uuid4()),"count":count,
            "side":side,"ticker":ticker,"type":"limit","post_only":True}
    if side == "yes": body["yes_price"] = price_cents
    else:             body["no_price"]  = price_cents
    return request(pk, "POST", "/portfolio/orders", body=body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("event", help="event ticker, e.g. KXTRUFDEAD-26APR27")
    ap.add_argument("--price", type=int, required=True, help="bid price in cents")
    ap.add_argument("--count", type=int, default=300)
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    pk = load_key()
    print(f"Fetching markets in event {args.event}...")
    mkts = list_markets(pk, args.event)
    if not mkts:
        print("No open markets found."); return
    mkts.sort(key=lambda m: m["ticker"])
    tickers = {m["ticker"] for m in mkts}
    print(f"  {len(mkts)} open market(s):")
    for m in mkts:
        print(f"    {m['ticker']:40s}  {m.get('title','')[:60]}")

    print("\nChecking existing resting BUY orders...")
    existing = list_resting_buys(pk, tickers)
    for (t, s), o in existing.items():
        px = float(o.get("yes_price_dollars" if s=="yes" else "no_price_dollars","0"))
        print(f"    SKIP {t} {s}  px={px}")

    plan = []
    for m in mkts:
        for side in ("yes","no"):
            if (m["ticker"], side) in existing: continue
            plan.append((m["ticker"], side))

    print(f"\nPlanned NEW orders: {len(plan)}  ({args.count} contracts @ {args.price}¢, post_only)")
    for t, s in plan:
        print(f"    BUY {s.upper():3s}  {t}   {args.count} @ {args.price}¢   max=${args.count*args.price/100:.2f}")
    print(f"\nTotal max capital at risk: ${len(plan)*args.count*args.price/100:.2f}")

    if not args.live:
        print("\nDRY RUN — re-run with --live to submit."); return

    print("\nSubmitting...")
    log = []
    for t, s in plan:
        for attempt in range(3):
            time.sleep(THROTTLE_S)
            r = place(pk, t, s, args.count, args.price)
            if r.status_code < 300:
                oid = r.json().get("order",{}).get("order_id","?")
                print(f"    OK   BUY {s.upper():3s} {t}  id={oid[:8]}")
                log.append({"ticker":t,"side":s,"order_id":oid,"status":"ok"})
                break
            elif r.status_code == 429:
                print(f"    429 on {t} {s}, backing off..."); time.sleep(2.0)
            else:
                print(f"    FAIL BUY {s.upper():3s} {t}  {r.status_code} {r.text[:140]}")
                log.append({"ticker":t,"side":s,"status":"fail","err":r.text})
                break
    p = Path(f"kalshi_place_{args.event}_{dt.datetime.now():%Y%m%d_%H%M%S}.json")
    p.write_text(json.dumps(log, indent=2))
    print(f"\nLog: {p}")


if __name__ == "__main__":
    main()
