"""Kalshi resting-order monitor.

Rule: an order qualifies as "in the top 300 on that side" if cumulative bid
size at STRICTLY better prices on the same side (yes/no) is < 300 contracts.
Orders failing the rule are cancelled and the user is notified.

Run modes:
  --once           single scan, then exit
  --dry-run        report only, don't cancel
  default          loop forever every --interval seconds (default 60)
"""
import argparse, base64, json, os, time, datetime as dt, subprocess, sys
from pathlib import Path
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KEY_PATH = Path(os.environ.get("KALSHI_KEY_PATH", "private_key.pem"))
BASE = "https://api.elections.kalshi.com/trade-api/v2"
TOP_N = 300
LOG_PATH = Path(os.environ.get("KALSHI_MONITOR_LOG", "kalshi_reward_monitor.log"))
THROTTLE_S = 0.15


def load_key(): return serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)


def request(pk, method, path, *, params=None, body=None):
    sig_path = "/trade-api/v2" + path
    ts = str(int(time.time() * 1000))
    msg = (ts + method + sig_path).encode()
    s = pk.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                                 salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    h = {"KALSHI-ACCESS-KEY": KEY_ID, "KALSHI-ACCESS-SIGNATURE": base64.b64encode(s).decode(),
         "KALSHI-ACCESS-TIMESTAMP": ts, "accept": "application/json", "Content-Type": "application/json"}
    return requests.request(method, BASE + path, headers=h, params=params, json=body, timeout=15)


def list_resting_orders(pk):
    out, cursor = [], None
    while True:
        p = {"status": "resting", "limit": 200}
        if cursor: p["cursor"] = cursor
        r = request(pk, "GET", "/portfolio/orders", params=p)
        if r.status_code >= 400: raise RuntimeError(r.text)
        d = r.json()
        out += d.get("orders", [])
        cursor = d.get("cursor") or None
        if not cursor: break
    return out


def get_orderbook(pk, ticker):
    r = request(pk, "GET", f"/markets/{ticker}/orderbook")
    if r.status_code >= 400:
        return {}
    return r.json().get("orderbook") or {}


def cancel(pk, order_id):
    return request(pk, "DELETE", f"/portfolio/orders/{order_id}")


def my_bid_cents(o):
    """Bid price in cents (integer) for this BUY order."""
    side = o["side"]
    px = o.get("yes_price_dollars" if side == "yes" else "no_price_dollars", "0")
    return round(float(px) * 100)


def cumulative_better_size(book, side, my_cents):
    """Sum of contracts at prices STRICTLY > my_cents on the given side.
    Kalshi orderbook returns yes/no arrays of [price_cents, size]."""
    levels = book.get(side, []) or []
    total = 0
    for entry in levels:
        try:
            px, size = int(entry[0]), float(entry[1])
        except (TypeError, ValueError, IndexError):
            continue
        if px > my_cents:
            total += size
    return int(total)


def notify(title, msg):
    """macOS notification + stderr."""
    sys.stderr.write(f"  >> {title}: {msg}\n")
    try:
        subprocess.run(
            ["osascript", "-e", f'display notification "{msg}" with title "{title}" sound name "Pop"'],
            check=False, timeout=3)
    except Exception:
        pass


def log(line):
    ts = dt.datetime.now().isoformat(timespec="seconds")
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a") as f:
        f.write(f"{ts}  {line}\n")
    print(f"{ts}  {line}", flush=True)


def scan_once(pk, dry_run: bool):
    orders = list_resting_orders(pk)
    if not orders:
        log("No resting orders.")
        return 0, 0, 0

    books = {}  # cache one fetch per ticker per cycle
    kept = removed = errors = 0
    cancelled_summary = []

    for o in orders:
        if o.get("action") != "buy":  # this rule is about resting BUYs
            kept += 1
            continue
        ticker = o["ticker"]; side = o["side"]
        my_cents = my_bid_cents(o)
        if ticker not in books:
            books[ticker] = get_orderbook(pk, ticker)
            time.sleep(THROTTLE_S)
        ahead = cumulative_better_size(books[ticker], side, my_cents)
        in_top = ahead < TOP_N

        if in_top:
            kept += 1
            continue

        # Out of top 300 -> cancel
        if dry_run:
            log(f"  WOULD-CANCEL  {ticker:32s} {side:3s}  px={my_cents}¢  ahead={ahead}  id={o['order_id'][:8]}")
            removed += 1
            cancelled_summary.append((ticker, side, my_cents, ahead))
        else:
            r = cancel(pk, o["order_id"])
            time.sleep(THROTTLE_S)
            if r.status_code < 300:
                log(f"  CANCELLED     {ticker:32s} {side:3s}  px={my_cents}¢  ahead={ahead}  id={o['order_id'][:8]}")
                removed += 1
                cancelled_summary.append((ticker, side, my_cents, ahead))
            else:
                log(f"  CANCEL-FAIL   {ticker:32s} {side:3s}  {r.status_code} {r.text[:120]}")
                errors += 1

    log(f"scan: total={len(orders)}  kept={kept}  removed={removed}  errors={errors}  dry_run={dry_run}")
    if cancelled_summary:
        title = "Kalshi monitor: cancellations" if not dry_run else "Kalshi monitor: would cancel"
        body = f"{len(cancelled_summary)} order(s) out of top {TOP_N}"
        notify(title, body)
    return kept, removed, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="single scan then exit")
    ap.add_argument("--dry-run", action="store_true", help="report but don't cancel")
    ap.add_argument("--interval", type=int, default=60, help="seconds between scans")
    args = ap.parse_args()
    pk = load_key()

    log(f"==== monitor starting (dry_run={args.dry_run}, interval={args.interval}s, top_n={TOP_N}) ====")

    if args.once:
        scan_once(pk, args.dry_run)
        return

    while True:
        try:
            scan_once(pk, args.dry_run)
        except Exception as e:
            log(f"  EXCEPTION  {type(e).__name__}: {e}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
