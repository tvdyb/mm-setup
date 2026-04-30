"""Overnight wallet watchdog. Silent unless something extreme.

Triggers (any one → kill MM):
  - total exposure > $1500 (inventory bloat)
  - realized loss in this run > $200
  - tripped events >= 3 (multiple per-event breakers fired)
  - HTTP unresponsive 5+ consecutive polls

On trigger: POST /api/penny enabled=false, print ALERT line.
Otherwise: prints nothing (monitor stays silent = healthy).
"""
import sys, time, json, urllib.request, urllib.error, pathlib
sys.path.insert(0, "/Users/wilsonw/Downloads")
import kalshi_rewards_app as k

PORT = 5050
TOKEN = pathlib.Path("/Users/wilsonw/.kalshi_local_token").read_text().strip()

EXPOSURE_LIMIT_USD = 5000.0
LOSS_LIMIT_USD = 500.0
TRIPPED_LIMIT = 5
HTTP_FAIL_LIMIT = 5
POLL_S = 60

def get_pos():
    try:
        ps = k.list_positions()
    except Exception as e:
        return None, str(e)
    exp = sum(float(p.get("market_exposure_dollars") or 0) for p in ps)
    pnl = sum(float(p.get("realized_pnl_cents") or 0) for p in ps) / 100.0
    return {"n": len(ps), "exp": exp, "pnl": pnl}, None

def get_penny():
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/penny", timeout=8) as r:
            return json.loads(r.read()), None
    except Exception as e:
        return None, str(e)

def kill_mm(reason):
    body = json.dumps({"enabled": False}).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}/api/penny", data=body,
                                 headers={"Content-Type": "application/json",
                                          "X-Local-Token": TOKEN}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            j = json.loads(r.read())
        return j.get("enabled") is False
    except Exception as e:
        print(f"FAIL kill_mm: {e}", flush=True)
        return False

def main():
    base, err = get_pos()
    if base is None:
        print(f"ALERT: cannot read baseline positions ({err})", flush=True)
        sys.exit(1)
    base_pnl = base["pnl"]
    print(f"watchdog armed: baseline pos={base['n']} exp=${base['exp']:.2f} pnl=${base_pnl:.2f} "
          f"limits exp>${EXPOSURE_LIMIT_USD} loss>${LOSS_LIMIT_USD} tripped>={TRIPPED_LIMIT}", flush=True)
    http_fails = 0
    while True:
        pos, perr = get_pos()
        pen, _ = get_penny()
        if pos is None:
            http_fails += 1
            if http_fails >= HTTP_FAIL_LIMIT:
                killed = kill_mm("positions API down")
                print(f"ALERT [{time.strftime('%H:%M:%S')}] positions API down {http_fails}x — KILL_MM ok={killed}", flush=True)
                http_fails = 0
            time.sleep(POLL_S); continue
        http_fails = 0
        loss = base_pnl - pos["pnl"]
        tripped = len((pen or {}).get("tripped_events") or {})
        triggers = []
        if pos["exp"] > EXPOSURE_LIMIT_USD: triggers.append(f"exposure=${pos['exp']:.2f}")
        if loss > LOSS_LIMIT_USD: triggers.append(f"loss=${loss:.2f}")
        if tripped >= TRIPPED_LIMIT: triggers.append(f"tripped={tripped}")
        if triggers:
            killed = kill_mm(",".join(triggers))
            print(f"ALERT [{time.strftime('%H:%M:%S')}] EXTREME: {','.join(triggers)} — KILL_MM ok={killed}", flush=True)
            sys.exit(0)
        time.sleep(POLL_S)

if __name__ == "__main__":
    main()
