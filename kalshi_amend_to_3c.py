"""Cancel all 1¢ resting BUY orders on KXTRUFBFST-26APR27-* and replace at 3¢.
Throttled at ~5 req/s to avoid Kalshi 429 limits. Defaults to dry-run."""
import argparse, base64, json, os, time, uuid, datetime as dt
from pathlib import Path
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KEY_PATH = Path(os.environ.get("KALSHI_KEY_PATH", "private_key.pem"))
BASE = "https://api.elections.kalshi.com/trade-api/v2"
EVENT_PREFIX = "KXTRUFBFST-26APR27"
NEW_PRICE_CENTS = 3
OLD_PRICE_CENTS = 1
THROTTLE_S = 0.2


def load_key(): return serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)


def request(pk, method, path, *, params=None, body=None):
    sig_path = "/trade-api/v2" + path
    ts = str(int(time.time()*1000))
    msg = (ts + method + sig_path).encode()
    s = pk.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    h = {"KALSHI-ACCESS-KEY":KEY_ID, "KALSHI-ACCESS-SIGNATURE":base64.b64encode(s).decode(),
         "KALSHI-ACCESS-TIMESTAMP":ts, "accept":"application/json", "Content-Type":"application/json"}
    r = requests.request(method, BASE+path, headers=h, params=params, json=body, timeout=15)
    return r


def list_orders(pk):
    out = []; cursor=None
    while True:
        p = {"status":"resting","limit":200}
        if cursor: p["cursor"] = cursor
        r = request(pk, "GET", "/portfolio/orders", params=p)
        if r.status_code >= 400: raise RuntimeError(r.text)
        d = r.json()
        out += d.get("orders", [])
        cursor = d.get("cursor") or None
        if not cursor: break
    return out


def cancel(pk, order_id):
    return request(pk, "DELETE", f"/portfolio/orders/{order_id}")


def place(pk, ticker, side, count, price_cents):
    body = {"action":"buy","client_order_id":str(uuid.uuid4()),"count":count,
            "side":side,"ticker":ticker,"type":"limit","post_only":True}
    if side == "yes": body["yes_price"] = price_cents
    else:             body["no_price"]  = price_cents
    return request(pk, "POST", "/portfolio/orders", body=body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()
    pk = load_key()

    print("Fetching resting orders...")
    orders = list_orders(pk)
    targets = []
    for o in orders:
        if not o.get("ticker","").startswith(EVENT_PREFIX): continue
        if o.get("action") != "buy": continue
        side = o.get("side")
        px_field = "yes_price_dollars" if side == "yes" else "no_price_dollars"
        px = float(o.get(px_field, "0"))
        if abs(px - OLD_PRICE_CENTS/100) > 1e-6:
            print(f"  SKIP {o['ticker']} {side}  px={px} (not at {OLD_PRICE_CENTS}¢)")
            continue
        targets.append(o)

    print(f"\nWill cancel+replace {len(targets)} order(s) at {OLD_PRICE_CENTS}¢ -> {NEW_PRICE_CENTS}¢:")
    for o in sorted(targets, key=lambda x: (x["ticker"], x["side"])):
        cnt = int(float(o["remaining_count_fp"]))
        print(f"  {o['ticker']:32s} {o['side']:3s}  cnt={cnt:>4d}  id={o['order_id'][:8]}  new max=${cnt*NEW_PRICE_CENTS/100:.2f}")
    total = sum(int(float(o["remaining_count_fp"])) for o in targets) * NEW_PRICE_CENTS / 100
    print(f"\nTotal new capital tied up: ${total:.2f}")

    if not args.live:
        print("\nDRY RUN — re-run with --live.")
        return

    log = []
    print("\nStep 1: cancelling old orders...")
    for o in targets:
        for attempt in range(3):
            time.sleep(THROTTLE_S)
            r = cancel(pk, o["order_id"])
            if r.status_code < 300:
                print(f"  CANCELLED {o['ticker']:32s} {o['side']:3s}  id={o['order_id'][:8]}")
                log.append({"step":"cancel","ticker":o["ticker"],"side":o["side"],"id":o["order_id"],"status":"ok"})
                break
            elif r.status_code == 429:
                print(f"  429 on {o['order_id'][:8]}, backing off...")
                time.sleep(2.0)
            else:
                print(f"  FAIL cancel {o['ticker']} {o['side']}: {r.status_code} {r.text[:140]}")
                log.append({"step":"cancel","ticker":o["ticker"],"side":o["side"],"id":o["order_id"],"status":"fail","err":r.text})
                break

    print("\nStep 2: placing new orders at 3¢...")
    for o in targets:
        cnt = int(float(o["remaining_count_fp"]))
        for attempt in range(3):
            time.sleep(THROTTLE_S)
            r = place(pk, o["ticker"], o["side"], cnt, NEW_PRICE_CENTS)
            if r.status_code < 300:
                new_id = r.json().get("order",{}).get("order_id","?")
                print(f"  PLACED   {o['ticker']:32s} {o['side']:3s}  cnt={cnt}  new_id={new_id[:8]}")
                log.append({"step":"place","ticker":o["ticker"],"side":o["side"],"new_id":new_id,"status":"ok"})
                break
            elif r.status_code == 429:
                print(f"  429 on place {o['ticker']} {o['side']}, backing off...")
                time.sleep(2.0)
            else:
                print(f"  FAIL place {o['ticker']} {o['side']}: {r.status_code} {r.text[:140]}")
                log.append({"step":"place","ticker":o["ticker"],"side":o["side"],"status":"fail","err":r.text})
                break

    p = Path(f"kalshi_amend_{dt.datetime.now():%Y%m%d_%H%M%S}.json")
    p.write_text(json.dumps(log, indent=2))
    print(f"\nLog: {p}")


if __name__ == "__main__":
    main()
