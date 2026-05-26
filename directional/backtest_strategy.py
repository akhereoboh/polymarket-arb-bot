"""
backtest_strategy.py

Replays the directional bot's signal logic against historical PolyBackTest data
and simulates real FAK fills against historical order books.

Outputs:
  - Signal accuracy (would the direction prediction have won?)
  - Fill rate at each entry-time bucket (T-300, T-180, T-120, T-60)
  - Simulated PnL with realistic fill prices
  - Breakdown by timeframe (5m vs 15m)

Usage:
  python3 backtest_strategy.py

Reads from /root/polymarket-arb-bot/directional/.env:
  POLYBACKTEST_API_KEY=pdm_...

Writes results to:
  ./backtest_results.csv     — per-signal records
  ./backtest_summary.txt     — aggregate stats
"""

import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


# ── Config ─────────────────────────────────────────────────────────────
load_dotenv('/root/polymarket-arb-bot/directional/.env')

API_KEY = os.getenv('POLYBACKTEST_API_KEY', '').strip()
if not API_KEY:
    print('ERROR: POLYBACKTEST_API_KEY not set in .env')
    sys.exit(1)

API_BASE = 'https://api.polybacktest.com'
HEADERS = {'Authorization': f'Bearer {API_KEY}'}

# Bot strategy parameters (match what's actually live)
MIN_MOVE_PCT       = float(os.getenv('MIN_MOVE_PCT', '0.05'))
HARD_FILL_CAP      = float(os.getenv('HARD_FILL_CAP', '0.80'))
TRADE_AMOUNT       = float(os.getenv('TRADE_AMOUNT', '20'))
FAK_BUFFER_REGULAR = 0.05
EARLY_FAK_BUFFER   = float(os.getenv('EARLY_FAK_BUFFER', '0.15'))

# Entry-window offsets to test (seconds before market close)
ENTRY_OFFSETS_5M  = [60, 90, 120]                # 1, 1.5, 2 min before close
ENTRY_OFFSETS_15M = [60, 120, 180, 240, 300]     # 1-5 min before close

# Output files
RESULTS_CSV = './backtest_results.csv'
SUMMARY_TXT = './backtest_summary.txt'


# ── API helpers ────────────────────────────────────────────────────────
def api_get(path: str, params: dict = None, retries: int = 3) -> dict:
    """GET with retry on 429, returns None on error."""
    url = f'{API_BASE}{path}'
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params or {}, timeout=15)
            if r.status_code == 429:
                wait = int(r.headers.get('X-RateLimit-Reset', '5'))
                time.sleep(min(wait, 30))
                continue
            if r.status_code == 402:
                # Plan limit hit — caller decides whether to stop
                return {'_plan_limit': True}
            if r.status_code == 404:
                return {'_not_found': True}
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == retries - 1:
                print(f'  ! API error after retries: {e}')
                return None
            time.sleep(2 ** attempt)
    return None


def list_resolved_markets(coin: str, mtype: str) -> list[dict]:
    """Pull all resolved markets of a given type (free plan caps at 50)."""
    markets = []
    cursor = None
    while True:
        params = {
            'type': mtype,
            'resolved': 'true',
            'limit': 100,
        }
        if cursor:
            params['cursor'] = cursor

        resp = api_get(f'/v3/{coin}/markets', params=params)
        if not resp or '_plan_limit' in resp or '_not_found' in resp:
            break

        batch = resp.get('data', [])
        markets.extend(batch)

        pagination = resp.get('pagination', {})
        if not pagination.get('has_more'):
            break
        cursor = pagination.get('next_cursor')
        if not cursor:
            break
        time.sleep(0.1)  # polite spacing

    return markets


def get_snapshot_at(coin: str, market_id: str, timestamp_ms: int,
                    include_orderbook: bool = True) -> dict | None:
    """Fetch closest snapshot to a wall-clock moment, with full order book."""
    path = f'/v3/{coin}/markets/{market_id}/snapshots/at/{timestamp_ms}'
    params = {'include_orderbook': 'true'} if include_orderbook else {}
    resp = api_get(path, params=params)
    if not resp or '_not_found' in resp:
        return None
    return resp.get('data', {}).get('snapshot')


# ── Strategy logic — simplified replay ─────────────────────────────────
def simulate_signal_at(snapshot: dict, market: dict, coin: str) -> dict:
    """
    Replay the bot's check_signal logic against a historical snapshot.

    Returns dict with: direction, signal_strength_pct, crowd_price, crowd_agrees
    or None if no signal at this moment.
    """
    coin_price = snapshot.get(f'{coin}_price')
    if not coin_price:
        return None

    price_start = market.get(f'{coin}_price_start')
    if not price_start:
        return None

    # Equivalent to check_signal's CL move test
    move_pct = (coin_price - price_start) / price_start * 100

    if abs(move_pct) < MIN_MOVE_PCT:
        return {'fires': False, 'reason': 'below_min_move'}

    direction = 'up' if move_pct > 0 else 'down'
    crowd_price = (snapshot.get('price_up') if direction == 'up'
                   else snapshot.get('price_down'))

    if crowd_price is None:
        return {'fires': False, 'reason': 'no_crowd_price'}

    # Bot rejects if crowd strongly disagrees
    if crowd_price < 0.35:
        return {'fires': False, 'reason': 'crowd_disagrees'}

    return {
        'fires':         True,
        'direction':     direction,
        'move_pct':      move_pct,
        'crowd_price':   crowd_price,
        'crowd_agrees':  crowd_price > 0.55,
    }


def simulate_fak_fill(snapshot: dict, signal: dict, shares: int,
                      early_mode: bool) -> dict:
    """
    Simulate a FAK against the historical order book.
    Returns: dict with filled (bool), shares_filled, avg_price, max_price
    """
    book = snapshot.get('orderbook_up') if signal['direction'] == 'up' \
           else snapshot.get('orderbook_down')

    if not book or not book.get('asks'):
        return {
            'filled':        False,
            'shares_filled': 0,
            'avg_price':     0,
            'max_price':     0,
            'reason':        'no_book',
        }

    raw_price = signal['crowd_price']
    fak_buffer = EARLY_FAK_BUFFER if early_mode else FAK_BUFFER_REGULAR
    max_price = round(min(raw_price + fak_buffer, HARD_FILL_CAP), 2)

    asks = sorted(book['asks'], key=lambda x: float(x['price']))
    available = [a for a in asks if float(a['price']) <= max_price]

    if not available:
        return {
            'filled':        False,
            'shares_filled': 0,
            'avg_price':     0,
            'max_price':     max_price,
            'reason':        'no_liquidity_at_ceiling',
        }

    # Walk the book: greedily fill from cheapest asks up to our share count
    shares_remaining = shares
    cost = 0
    for ask in available:
        ask_price = float(ask['price'])
        ask_size = float(ask['size'])
        take = min(shares_remaining, ask_size)
        cost += take * ask_price
        shares_remaining -= take
        if shares_remaining <= 0:
            break

    shares_filled = shares - shares_remaining
    if shares_filled == 0:
        return {
            'filled':        False,
            'shares_filled': 0,
            'avg_price':     0,
            'max_price':     max_price,
            'reason':        'walked_book_no_match',
        }

    avg_price = cost / shares_filled
    return {
        'filled':        True,
        'shares_filled': shares_filled,
        'avg_price':     avg_price,
        'max_price':     max_price,
        'partial':       shares_filled < shares,
    }


def compute_pnl(signal: dict, fill: dict, market: dict) -> float:
    """If fill succeeded, compute PnL based on actual market winner."""
    if not fill['filled']:
        return 0.0

    winner = market.get('winner')
    if winner is None:
        return 0.0

    won = (signal['direction'] == winner)
    cost = fill['shares_filled'] * fill['avg_price']
    payout = fill['shares_filled'] * 1.0 if won else 0.0
    return round(payout - cost, 4)


# ── Main backtest loop ─────────────────────────────────────────────────
def backtest_market(market: dict, coin: str) -> list[dict]:
    """
    For one market, test signals at each entry-window offset.
    Returns list of result rows.
    """
    rows = []
    mtype = market['market_type']
    market_id = market['market_id']
    end_time = market.get('end_time')
    if not end_time:
        return rows

    # Parse end_time to epoch ms
    end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
    end_ms = int(end_dt.timestamp() * 1000)

    offsets = ENTRY_OFFSETS_5M if mtype == '5m' else ENTRY_OFFSETS_15M
    is_15m = (mtype == '15m')

    for offset_sec in offsets:
        target_ms = end_ms - (offset_sec * 1000)

        snapshot = get_snapshot_at(coin, market_id, target_ms,
                                   include_orderbook=True)
        if not snapshot:
            continue

        signal = simulate_signal_at(snapshot, market, coin)
        if not signal:
            continue

        # Determine early-entry eligibility (only for 15m markets at T>=180s)
        early_mode = (is_15m and offset_sec >= 180 and signal.get('fires'))

        if not signal['fires']:
            rows.append({
                'market_id':     market_id,
                'slug':          market.get('slug', ''),
                'timeframe':     mtype,
                'offset_sec':    offset_sec,
                'fires':         False,
                'skip_reason':   signal.get('reason', 'unknown'),
                'direction':     '',
                'move_pct':      '',
                'crowd_price':   '',
                'filled':        False,
                'shares_filled': 0,
                'avg_price':     0,
                'max_price':     0,
                'cost':          0,
                'pnl':           0,
                'winner':        market.get('winner', ''),
                'won':           '',
            })
            continue

        # Position sizing matches bot: confidence-weighted, min 5 shares
        amount = TRADE_AMOUNT * (0.6 if signal['crowd_agrees'] else 0.3)
        intended_shares = max(5, int(amount / signal['crowd_price']))

        fill = simulate_fak_fill(snapshot, signal, intended_shares,
                                 early_mode=early_mode)
        pnl = compute_pnl(signal, fill, market)

        rows.append({
            'market_id':     market_id,
            'slug':          market.get('slug', ''),
            'timeframe':     mtype,
            'offset_sec':    offset_sec,
            'fires':         True,
            'skip_reason':   '',
            'direction':     signal['direction'],
            'move_pct':      round(signal['move_pct'], 4),
            'crowd_price':   round(signal['crowd_price'], 4),
            'filled':        fill['filled'],
            'shares_filled': fill['shares_filled'],
            'avg_price':     round(fill['avg_price'], 4),
            'max_price':     fill['max_price'],
            'cost':          round(fill['shares_filled'] * fill['avg_price'], 4),
            'pnl':           pnl,
            'winner':        market.get('winner', ''),
            'won':           (signal['direction'] == market.get('winner')),
            'early_mode':    early_mode,
        })

    return rows


def write_results(all_rows: list[dict]) -> None:
    """Write per-signal CSV and aggregate summary."""
    if not all_rows:
        print('No results to write.')
        return

    fieldnames = list(all_rows[0].keys())
    with open(RESULTS_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f'\nWrote {len(all_rows)} rows to {RESULTS_CSV}')

    # Aggregate summary
    fired = [r for r in all_rows if r['fires']]
    filled = [r for r in fired if r['filled']]

    lines = []
    lines.append('=' * 70)
    lines.append('BACKTEST SUMMARY')
    lines.append('=' * 70)
    lines.append(f'Total signal checks: {len(all_rows)}')
    lines.append(f'Signals that fired:  {len(fired)} ({len(fired)/max(len(all_rows),1)*100:.1f}%)')
    lines.append(f'Signals that filled: {len(filled)} ({len(filled)/max(len(fired),1)*100:.1f}% of fired)')

    if filled:
        wins = [r for r in filled if r['won']]
        total_pnl = sum(r['pnl'] for r in filled)
        total_cost = sum(r['cost'] for r in filled)

        lines.append('')
        lines.append('Filled trades:')
        lines.append(f'  Wins:        {len(wins)}/{len(filled)} = {len(wins)/len(filled)*100:.1f}%')
        lines.append(f'  Total spent: ${total_cost:.2f}')
        lines.append(f'  Total PnL:   ${total_pnl:+.2f}')
        lines.append(f'  Avg per trade: ${total_pnl/len(filled):+.4f}')
        lines.append(f'  ROI:         {total_pnl/max(total_cost,0.01)*100:+.2f}%')

        # By timeframe
        lines.append('')
        lines.append('By timeframe:')
        for tf in ['5m', '15m']:
            tf_filled = [r for r in filled if r['timeframe'] == tf]
            if tf_filled:
                tf_wins = [r for r in tf_filled if r['won']]
                tf_pnl = sum(r['pnl'] for r in tf_filled)
                lines.append(f'  {tf}: {len(tf_wins)}/{len(tf_filled)} wins = '
                             f'{len(tf_wins)/len(tf_filled)*100:.1f}% | PnL ${tf_pnl:+.2f}')

        # By entry offset
        lines.append('')
        lines.append('By entry offset (seconds before close):')
        by_offset = defaultdict(list)
        for r in filled:
            by_offset[r['offset_sec']].append(r)
        for offset in sorted(by_offset.keys()):
            rows = by_offset[offset]
            wins = [r for r in rows if r['won']]
            pnl = sum(r['pnl'] for r in rows)
            lines.append(f'  T-{offset}s: {len(wins)}/{len(rows)} wins = '
                         f'{len(wins)/len(rows)*100:.1f}% | PnL ${pnl:+.2f}')

        # Skip reasons
        lines.append('')
        lines.append('Skip / no-fill reasons:')
        skip_counts = defaultdict(int)
        for r in fired:
            if not r['filled']:
                skip_counts['fired_but_no_fill'] += 1
        for r in all_rows:
            if not r['fires']:
                skip_counts[r['skip_reason']] += 1
        for reason, count in sorted(skip_counts.items(), key=lambda x: -x[1]):
            lines.append(f'  {reason}: {count}')

    text = '\n'.join(lines)
    with open(SUMMARY_TXT, 'w') as f:
        f.write(text + '\n')
    print('\n' + text)


# ── Entrypoint ────────────────────────────────────────────────────────
def main():
    print(f'PolyBackTest Backtest')
    print(f'Strategy params: MIN_MOVE_PCT={MIN_MOVE_PCT}, HARD_FILL_CAP={HARD_FILL_CAP}')
    print(f'  TRADE_AMOUNT=${TRADE_AMOUNT}, EARLY_FAK_BUFFER={EARLY_FAK_BUFFER}')
    print()

    all_rows = []
    coin = 'btc'

    for mtype in ['5m', '15m']:
        print(f'\n=== Fetching resolved {mtype} markets for {coin.upper()} ===')
        markets = list_resolved_markets(coin, mtype)
        print(f'Got {len(markets)} markets')

        if not markets:
            continue

        # Run backtest on each market
        for i, market in enumerate(markets):
            if (i + 1) % 10 == 0:
                print(f'  Processed {i+1}/{len(markets)} markets...')

            rows = backtest_market(market, coin)
            all_rows.extend(rows)
            time.sleep(0.05)  # polite spacing

    write_results(all_rows)


if __name__ == '__main__':
    main()
