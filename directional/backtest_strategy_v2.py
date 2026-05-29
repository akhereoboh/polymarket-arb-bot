"""
backtest_strategy_v2.py

Enhanced backtest that records signal evaluations at EVERY threshold,
so we can post-hoc analyze multiple MIN_MOVE_PCT values without re-fetching.

Key change from v1: instead of skipping rows with "below_min_move",
record the actual move_pct so we can filter at any threshold later.

Output:
  ./backtest_results_v2.csv     — per-snapshot records with raw move_pct
  ./backtest_summary_v2.txt     — multi-threshold comparison

Usage:
  python3 backtest_strategy_v2.py
"""

import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


# Config
load_dotenv('/root/polymarket-arb-bot/directional/.env')

API_KEY = os.getenv('POLYBACKTEST_API_KEY', '').strip()
if not API_KEY:
    print('ERROR: POLYBACKTEST_API_KEY not set in .env')
    sys.exit(1)

API_BASE = 'https://api.polybacktest.com'
HEADERS = {'Authorization': f'Bearer {API_KEY}'}

HARD_FILL_CAP = float(os.getenv('HARD_FILL_CAP', '0.90'))
TRADE_AMOUNT = float(os.getenv('TRADE_AMOUNT', '4.0'))
FAK_BUFFER_REGULAR = 0.05
EARLY_FAK_BUFFER = float(os.getenv('EARLY_FAK_BUFFER', '0.15'))

# Thresholds to test in post-hoc analysis
TEST_THRESHOLDS = [0.025, 0.035, 0.05, 0.07, 0.10]

ENTRY_OFFSETS_5M = [60, 90, 120]
ENTRY_OFFSETS_15M = [60, 120, 180, 240, 300]

RESULTS_CSV_TEMPLATE = './backtest_results_v2_{coin}.csv'
SUMMARY_TXT_TEMPLATE = './backtest_summary_v2_{coin}.txt'


def api_get(path, params=None, retries=3):
    url = f'{API_BASE}{path}'
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params or {}, timeout=15)
            if r.status_code == 429:
                wait = int(r.headers.get('X-RateLimit-Reset', '5'))
                time.sleep(min(wait, 30))
                continue
            if r.status_code in (402, 404):
                return {'_skip': True}
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == retries - 1:
                return None
            time.sleep(2 ** attempt)
    return None


def list_resolved_markets(coin, mtype):
    markets = []
    cursor = None
    while True:
        params = {'type': mtype, 'resolved': 'true', 'limit': 100}
        if cursor:
            params['cursor'] = cursor
        resp = api_get(f'/v3/{coin}/markets', params=params)
        if not resp or '_skip' in resp:
            break
        markets.extend(resp.get('data', []))
        pag = resp.get('pagination', {})
        if not pag.get('has_more'):
            break
        cursor = pag.get('next_cursor')
        if not cursor:
            break
        time.sleep(0.1)
    return markets


def get_snapshot_at(coin, market_id, ts_ms):
    path = f'/v3/{coin}/markets/{market_id}/snapshots/at/{ts_ms}'
    resp = api_get(path, params={'include_orderbook': 'true'})
    if not resp or '_skip' in resp:
        return None
    return resp.get('data', {}).get('snapshot')


def simulate_fak_fill(snapshot, direction, crowd_price, shares, early_mode):
    book = snapshot.get('orderbook_up') if direction == 'up' else snapshot.get('orderbook_down')
    if not book or not book.get('asks'):
        return {'filled': False, 'shares_filled': 0, 'avg_price': 0, 'max_price': 0, 'reason': 'no_book'}

    fak_buffer = EARLY_FAK_BUFFER if early_mode else FAK_BUFFER_REGULAR
    max_price = round(min(crowd_price + fak_buffer, HARD_FILL_CAP), 2)
    asks = sorted(book['asks'], key=lambda x: float(x['price']))
    available = [a for a in asks if float(a['price']) <= max_price]

    if not available:
        return {'filled': False, 'shares_filled': 0, 'avg_price': 0, 'max_price': max_price, 'reason': 'no_liq'}

    remaining = shares
    cost = 0
    for ask in available:
        ask_price = float(ask['price'])
        take = min(remaining, float(ask['size']))
        cost += take * ask_price
        remaining -= take
        if remaining <= 0:
            break

    filled_shares = shares - remaining
    if filled_shares == 0:
        return {'filled': False, 'shares_filled': 0, 'avg_price': 0, 'max_price': max_price, 'reason': 'walked_empty'}

    return {
        'filled': True,
        'shares_filled': filled_shares,
        'avg_price': cost / filled_shares,
        'max_price': max_price,
    }


def backtest_market(market, coin):
    rows = []
    mtype = market['market_type']
    market_id = market['market_id']
    end_time = market.get('end_time')
    if not end_time:
        return rows

    end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
    end_ms = int(end_dt.timestamp() * 1000)

    price_start = market.get(f'{coin}_price_start')
    winner = market.get('winner')

    if price_start is None or winner is None:
        return rows

    offsets = ENTRY_OFFSETS_5M if mtype == '5m' else ENTRY_OFFSETS_15M
    is_15m = (mtype == '15m')

    for offset_sec in offsets:
        target_ms = end_ms - (offset_sec * 1000)
        snapshot = get_snapshot_at(coin, market_id, target_ms)
        if not snapshot:
            continue

        coin_price = snapshot.get(f'{coin}_price')
        if not coin_price:
            continue

        move_pct = (coin_price - price_start) / price_start * 100
        direction = 'up' if move_pct > 0 else 'down'

        crowd_price_raw = snapshot.get('price_up') if direction == 'up' else snapshot.get('price_down')
        if crowd_price_raw is None:
            continue
        crowd_price = float(crowd_price_raw)

        # Simulate fill for THIS direction (irrespective of any threshold)
        early_mode = (is_15m and offset_sec >= 180)
        amount = TRADE_AMOUNT * (0.6 if crowd_price > 0.55 else 0.3)
        intended_shares = max(5, int(amount / max(crowd_price, 0.01)))

        # Only attempt fill if crowd_price isn't crazy (avoid 0.99 burns)
        # We'll filter at threshold level later
        fill = simulate_fak_fill(snapshot, direction, crowd_price, intended_shares, early_mode)

        won = (direction.lower() == winner.lower())
        if fill['filled']:
            cost = fill['shares_filled'] * fill['avg_price']
            pnl = (fill['shares_filled'] if won else 0) - cost
        else:
            cost = 0
            pnl = 0

        rows.append({
            'market_id':     market_id,
            'slug':          market.get('slug', ''),
            'timeframe':     mtype,
            'offset_sec':    offset_sec,
            'move_pct':      round(move_pct, 4),
            'abs_move_pct':  round(abs(move_pct), 4),
            'direction':     direction,
            'crowd_price':   round(crowd_price, 4),
            'crowd_agrees':  crowd_price > 0.55,
            'filled':        fill['filled'],
            'shares_filled': fill['shares_filled'],
            'avg_price':     round(fill['avg_price'], 4),
            'max_price':     fill['max_price'],
            'cost':          round(cost, 4),
            'pnl':           round(pnl, 4),
            'winner':        winner,
            'won':           won,
            'early_mode':    early_mode,
        })

    return rows


def write_results(all_rows, coin):
    if not all_rows:
        print('No results.')
        return

    results_csv = RESULTS_CSV_TEMPLATE.format(coin=coin)
    summary_txt = SUMMARY_TXT_TEMPLATE.format(coin=coin)

    fields = list(all_rows[0].keys())
    with open(results_csv, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)
    print(f'\nWrote {len(all_rows)} rows to {results_csv}')

    # Post-hoc analysis at multiple thresholds
    lines = []
    lines.append('=' * 75)
    lines.append(f'MULTI-THRESHOLD BACKTEST COMPARISON — {coin.upper()}')
    lines.append('=' * 75)
    lines.append(f'Total snapshot evaluations: {len(all_rows)}')
    lines.append('')

    lines.append(f'{"Threshold":<12} {"Fires":<8} {"Filled":<8} {"Win%":<8} {"PnL":<12} {"ROI%":<8}')
    lines.append('-' * 60)

    for thresh in TEST_THRESHOLDS:
        fired = [r for r in all_rows if r['abs_move_pct'] >= thresh and r['crowd_price'] >= 0.35]
        filled = [r for r in fired if r['filled']]

        if filled:
            wins = sum(1 for r in filled if r['won'])
            total_cost = sum(r['cost'] for r in filled)
            total_pnl = sum(r['pnl'] for r in filled)
            roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
            lines.append(f'{thresh:<12} {len(fired):<8} {len(filled):<8} '
                        f'{wins/len(filled)*100:.1f}%   ${total_pnl:+.2f}      {roi:+.1f}%')
        else:
            lines.append(f'{thresh:<12} {len(fired):<8} 0        --       --          --')

    lines.append('')
    lines.append('Notes:')
    lines.append('  - "Fires" = signals matching threshold AND crowd_price >= 0.35')
    lines.append(f'  - "Filled" = orders that found liquidity at <= HARD_FILL_CAP ({HARD_FILL_CAP})')
    lines.append('  - PnL assumes TRADE_AMOUNT, current FAK ceiling, real historical order books')

    # Best threshold by total PnL
    lines.append('')
    lines.append('Best threshold by total PnL:')
    best = None
    for thresh in TEST_THRESHOLDS:
        fired = [r for r in all_rows if r['abs_move_pct'] >= thresh and r['crowd_price'] >= 0.35]
        filled = [r for r in fired if r['filled']]
        pnl = sum(r['pnl'] for r in filled)
        if best is None or pnl > best[1]:
            best = (thresh, pnl, len(filled))
    if best:
        lines.append(f'  Threshold {best[0]}: ${best[1]:+.2f} on {best[2]} fills')

    text = '\n'.join(lines)
    with open(summary_txt, 'w') as f:
        f.write(text + '\n')
    print('\n' + text)



def main():
    import argparse
    parser = argparse.ArgumentParser(description='Multi-threshold backtest for Polymarket up/down markets')
    parser.add_argument('--coin', default='btc', choices=['btc', 'eth', 'sol'],
                        help='Which coin to backtest (default: btc)')
    args = parser.parse_args()
    coin = args.coin.lower()

    print(f'PolyBackTest v2 — multi-threshold backtest')
    print(f'COIN: {coin.upper()}')
    print(f'HARD_FILL_CAP={HARD_FILL_CAP}, TRADE_AMOUNT=${TRADE_AMOUNT}')
    print(f'Testing thresholds: {TEST_THRESHOLDS}')

    all_rows = []
    for mtype in ['5m', '15m']:
        print(f'\n=== Fetching resolved {coin.upper()} {mtype} markets ===')
        markets = list_resolved_markets(coin, mtype)
        print(f'Got {len(markets)} markets')
        for i, m in enumerate(markets):
            if (i + 1) % 25 == 0:
                print(f'  {i+1}/{len(markets)}...')
            all_rows.extend(backtest_market(m, coin))
            time.sleep(0.05)

    write_results(all_rows, coin)


if __name__ == '__main__':
    main()
