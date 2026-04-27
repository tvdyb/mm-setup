# mm-setup — Kalshi liquidity-rewards market making

A small Python toolkit for providing liquidity on
[Kalshi](https://kalshi.com) reward-incentivized markets and capturing the
maker rebate. Built and iterated on live during the April 2026 Truflation /
gas / eggs / EV reward programs.

The core piece is `kalshi_rewards_app.py` — a single-file local web app that
shows every reward-incentivized market you're (or could be) participating in,
your share of the reward pool on each side, and a configurable auto-pennying
bot that defends top-of-book against jumps and prevents creating arbs in the
process. The other scripts are smaller standalone utilities for one-shot
placement, monitoring, and price amendments.

> **Strategy in one line:** post deep, post quietly, sit in the top 300
> contracts on each side of as many incentivized markets as possible, and
> defend rank only when actually displaced. Rewards beat spread.

---

## Why this exists

Kalshi pays maker rebates (`/incentive_programs`) on a per-market, per-side,
per-hour basis. The exact formula is undocumented, but reverse-engineered:

- Walk the bid stack from highest price down.
- Only the **first 300 contracts** on each side qualify for rewards.
- Each qualifying contract earns weight roughly proportional to its bid price
  in cents — so a 90¢ resting bid is worth more reward weight than a 5¢ bid,
  even though both are in the top 300.
- Your share of that side's hourly $ pool ≈ your weighted contracts / total
  weighted contracts in the top 300.

The implication for a market maker: you don't need to be at top of book,
you only need to be **in the top 300 cumulative size** on your side, and your
weight scales with price — so deeper markets reward higher bids more. The
edge isn't in the spread you cross, it's in the rebate you collect for
sitting there.

The risks are real:

1. **Adverse fills.** A 90¢ YES that gets hit means you took on a
   95%-priced trade against someone with information. Don't post sizes you
   can't eat.
2. **Creating arbs.** Two markets in the same event (e.g.
   `KXTRUFEGGS-26APR27-T2.85` YES and `KXTRUFEGGS-26APR27-T2.86` NO) where
   `bid_yes_strike_low + bid_no_strike_high > 100¢` is a guaranteed-loss
   pair. Surprisingly common when each side's pennying logic runs
   independently. The app prevents this both pre-trade (would-create-arb
   check during pennying) and post-trade (continuous arb sweeper that
   cancels the cheaper leg).
3. **Getting jumped silently.** A 1¢ overbid on a single contract drops you
   from rank 1 to rank 2 — costing measurable reward share over an hour.
   The app's `DEFEND` path watches `my_top_px` vs `best_bid` every cycle and
   re-pennies when displaced.

---

## Components

### `kalshi_rewards_app.py` — the main app

Single-file `http.server`-based web app. Run it and open
`http://localhost:5050/`.

What it shows per (market × side):

| Column | Meaning |
| ------ | ------- |
| Market · side | Linked ticker, YES/NO badge |
| My px | Highest price you have a resting bid at |
| Size | Total contracts you have resting on that side |
| Spread | Best ask − best bid in cents |
| Ahead | Cumulative size at strictly better prices than yours (rank measure) |
| In top | Whether ≥1 of your contracts is in the rewards-paying top 300 |
| Mine / In top | Your contracts and total contracts in the top 300 |
| Share | Your reward weight ÷ total reward weight on that side |
| $/hr, $/min | Estimated reward run-rate based on share × pool |
| Pool $/hr | Kalshi's published hourly $ for this program |
| Ends | Time until program closes |
| Levels | Your stacked resting prices |
| Action | Move/cancel buttons + ⛔ Block button |

Server-side every ~1.5s the app re-snapshots:

- `/portfolio/orders?status=resting` — your live stack
- `/incentive_programs` (cached 60s) — which markets pay rewards now
- `/markets/{ticker}/orderbook` — fetched in parallel under a 16-way semaphore
- Joins by event ticker so untraded strikes in events you already participate
  in show up too. `KALSHI_FOLLOWED_PATH` adds extra events to scan.

#### Auto-pennying bot

A background thread (`PennyBot`) wakes every `interval` seconds and decides,
per row, whether to place. The decision tree:

```
if row is blocked (per-market blocklist):
    skip
if best_bid is None or best_ask is None:
    skip
spread = best_ask - best_bid

# Reasons to act:
jumped     = my_top_px is not None and best_bid > my_top_px
far_below  = my_top_px is not None and (best_bid - my_top_px) >= gap_min
low_share  = share_pct < share_max_pct and has_orders
no_orders  = my_top_px is None
is_defense = jumped or far_below or low_share

if not (is_defense or no_orders):
    skip

# Where to bid:
bump      = max(1, spread // 8) if spread > 1 else 0   # join-at-best on 1¢ markets
target_px = best_bid + bump
if target_px >= best_ask: skip                          # never cross
if my_top_px >= target_px: skip                         # already at/above

# Don't create an arb against my own resting / pending orders:
if would_create_arb(rows, ticker, side, target_px, pending=pending):
    skip and emit("would create arb")

# Cooldown — defense path uses a much shorter window:
cooldown_used = defend_cooldown_s if is_defense else cooldown_s
if now - last_placement[(ticker, side)] < cooldown_used:
    skip

place_buy(ticker, side, size, target_px, post_only=True)
pending.append((ticker, side, target_px))   # so subsequent rows in this cycle see it
```

Key design decisions and *why*:

- **Bump = `spread // 8`**, not `+1`. On wider markets pennying by 1¢ is
  pointless — you sit just behind the wall. On 1¢-spread markets `spread // 8
  == 0` falls back to "join at best" (legal because we then reject if
  `target ≥ best_ask`).
- **No `spread_min` hard gate.** An earlier version skipped tight markets
  entirely; this gave up rewards on EGGS strikes (1¢ spread, 90+% reward
  share if you join). Defense triggers (`jumped`, `far_below`, `low_share`)
  override any spread heuristic.
- **Two cooldowns.** `cooldown_s` (default 240s) caps churn from re-pennying
  the same passive book. `defend_cooldown_s` (default 20s) is for active
  displacement — when someone jumps you, you can't sit on a 4-minute
  cooldown.
- **Intra-cycle `pending` list.** The cycle iterates rows from a single
  frozen snapshot. Without tracking placements made earlier in the same
  cycle, a YES bid placed on row 5 is invisible to the arb-prevention check
  on row 47, and both legs of an arb pair slip in before the next snapshot
  rolls. The `pending` list closes that race.
- **Independent arb sweeper.** `_arb_loop` runs every `arb_interval` seconds
  even when auto-pennying is off, because adverse arbs can also appear when
  the *book around you* moves. It cancels the leg with the lower
  $-at-risk (`px × size`).
- **Per-market block.** ⛔ Block on any row cancels both YES and NO orders
  on that ticker and persists the ticker to a blocklist file the bot reads
  on every cycle. Useful when a market has gone bad (e.g. clear directional
  signal arriving) and you want to keep pennying everything else.

#### Anti-arb: detection and prevention

For each event, build (YES, NO) pairs across strikes. An arb pair exists
whenever:

```
YES@strike_lo  AND  NO@strike_hi  with  strike_lo ≤ strike_hi
                                  AND  bid_yes + bid_no > 100¢
```

Why: in the price range `(strike_lo, strike_hi]`, both legs would settle in
the money for the *opposite* side of what we hold. With `bid_yes + bid_no >
100¢`, even before fees we lose money in that range with positive
probability.

Two layers:

1. **Pre-trade** (`would_create_arb`): every penny attempt simulates the new
   bid against current book + within-cycle pending placements and rejects
   the place if it would form an arb pair.
2. **Post-trade** (`detect_arbs` → `resolve_arbs_now`): independent sweeper
   running every few seconds catches arbs created when *someone else's*
   posting moved the book. Cancels the cheaper leg.

#### Other UI controls

- **Followed events search panel** — text-search live reward programs and
  click to add the event to your tracked set.
- **Per-market block / unblock** — ⛔ button on every row.
- **Resolve arbs now** — manual one-shot equivalent of the sweeper.
- **Cancel zero-reward orders** — bulk cancel anything currently sitting
  outside the top 300.

### `kalshi_reward_monitor.py` — standalone top-300 enforcer

Loops over all your resting BUY orders and cancels any whose
strict-better-than-mine cumulative size has passed 300. Runs as a separate
process (`--once`, `--dry-run`, or default loop) and pops a macOS
notification when it cancels something. Predates the rewards-app bot — kept
because it works without the web UI and is good as a belt-and-suspenders.

### `kalshi_place_orders.py` — bulk seed an event

Place BUY YES + BUY NO at a given price on every open market in an event,
skipping sides where you already have a resting BUY. Defaults to dry-run.
Used at the start of a session to lay down the initial passive stack.

```bash
python3 kalshi_place_orders.py KXTRUFEGGS-26APR27 --price 1 --count 300
python3 kalshi_place_orders.py KXTRUFEGGS-26APR27 --price 1 --count 300 --live
```

### `kalshi_amend_to_3c.py` — bulk price-shift one event

Cancel all 1¢ resting BUYs on a given event prefix and replace at 3¢.
Hard-coded prefix and prices because it was written for a single specific
amendment session. Easy to tweak.

### `kalshi_breakfast_orders.py` — historical seed for KXTRUFBFST

The original one-off used to seed the breakfast event at 1¢ before any of
the generic tooling existed. Kept for reference — `kalshi_place_orders.py`
supersedes it.

---

## Setup

```bash
git clone https://github.com/tvdyb/mm-setup
cd mm-setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. On Kalshi: Settings → API Keys → "+ Create" → save the UUID and download
#    the private key file (PEM). Move the PEM into this directory.
mv ~/Downloads/your-downloaded-key.pem ./private_key.pem
chmod 600 ./private_key.pem

# 2. Fill in env vars
cp .env.example .env
$EDITOR .env
set -a; source .env; set +a   # or use direnv / a tool of your choice

# 3. Run
python3 kalshi_rewards_app.py
# open http://localhost:5050
```

The auto-pennying bot starts disabled. Toggle it from the UI's penny panel
once you've reviewed the markets the dashboard is showing.

### Environment variables

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `KALSHI_KEY_ID` | *(required)* | UUID printed in Kalshi's API key UI |
| `KALSHI_KEY_PATH` | `./private_key.pem` | Path to the matching RSA private key |
| `KALSHI_FOLLOWED_PATH` | `./kalshi_followed_events.json` | Persisted set of explicitly-followed event tickers |
| `KALSHI_BLOCKED_PATH` | `./kalshi_blocked_markets.json` | Persisted blocklist of market tickers the penny bot must skip |
| `KALSHI_MONITOR_LOG` | `./kalshi_reward_monitor.log` | Where the standalone monitor writes |

---

## Operational notes

- **Rate limits.** Kalshi enforces ~10 req/s; bursts past that return 429.
  All the scripts throttle to ~5 req/s and back off on 429 with up to 3
  retries. The web app uses a 16-way parallel semaphore on orderbook fetches
  with a 32-connection pool — anything wider triggers DNS / socket
  exhaustion in `requests`.
- **Snapshot staleness.** The app keeps a single ~1.5s server-side snapshot
  shared between the web UI poller and the penny bot. The penny bot reads
  from this snapshot rather than re-hitting the API on its own — this is
  what makes it possible to run a 30-second pennying cycle without going
  near the rate limit.
- **`post_only=True`.** Every order placed through this stack is post-only.
  Crossing the spread *negates* the maker rebate for that fill, so an
  accidental cross is a strict loss vs. just not posting.
- **Event-ticker derivation.** `parts = market.split("-"); event = "-".join(parts[:-1])`.
  Works for every Kalshi market I've used; no separate API call required.
- **Strike parsing.** Last segment starts with `T` followed by a decimal:
  `KXTRUFBFST-26APR27-T89.50` → strike `89.5`. Used by the arb detector to
  match YES/NO pairs across strikes.

## What's not here

- No directional signal. This is purely a maker-rebate harvester. If you
  have a view on the underlying, this isn't the right tool.
- No persistent fill / PnL accounting. The app shows recent fills via
  `/portfolio/fills` but does not reconcile to a database — read your
  Kalshi statements for actual PnL.
- No paper-trading mode for the bot. The dry-run flags on the standalone
  scripts work, but the web app's bot has no simulator. Use `enabled=false`
  + watch the log to evaluate behavior.

## License

Personal project, no license. Don't run it against an account you can't
afford to debug.
