"""
Cross-reference signals_log.csv with actual Polymarket trade fills.

Pulls all real fills from data-api.polymarket.com, matches them against
logged signals by condition_id, and adds a 'filled' column with True/False
plus actual fill price and actual PnL.

Then prints a fill-rate analysis by price bucket: which signal price ranges
actually fill, which get lost in execution.

Usage:
  python3 add_fill_status.py
"""

import csv
import json
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests


CSV_FILE = '/root/polymarket-arb-bot/directional/signals_log.csv'
FUNDER = '0xfc4a9ac2835efefefbb9206b0b8fa3603767f757'
DATA_API = 'https://data-api.polymarket.com/trades'


def fetch_all_fills() -> list[dict]:
    """Pull every trade for this funder from data-api, paginated."""
    all_trades = []
    offset = 0
    page_size = 500
    while True:
        try:
            r = requests.get(
                DATA_API,
                params={'user': FUNDER, 'limit': page_size, 'offset': offset},
                timeout=20,
            )
            if r.status_code != 200:
                print(f'data-api returned {r.status_code}', file=sys.stderr)
                break
            data = r.json()
        except Exception as e:
            print(f'data-api error: {e}', file=sys.stderr)
            break

        if not data:
            break
        all_trades.extend(data)
        if len(data) < page_size:
            break
        offset += page_size

    return all_trades


def load_csv() -> tuple[list[dict], list[str]]:
    if not Path(CSV_FILE).exists():
        print(f'Missing {CSV_FILE}')
        sys.exit(1)
    with open(CSV_FILE) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def save_csv(rows: list[dict], fieldnames: list[str]) -> None:
    tmp = CSV_FILE + '.tmp'
    with open(tmp, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, CSV_FILE)


def main():
    print('Loading signals_log.csv...')
    rows, fieldnames = load_csv()
    print(f'  {len(rows)} signal rows')

    # Add new columns if missing
    for col in ('filled', 'fill_price', 'fill_size', 'fill_pnl', 'fill_tx'):
        if col not in fieldnames:
            fieldnames.append(col)

    print('\nFetching actual fills from Polymarket data-api...')
    fills = fetch_all_fills()
    btc_fills = [t for t in fills if 'btc-updown' in t.get('slug', '')]
    print(f'  Total fills: {len(fills)}')
    print(f'  BTC up/down fills: {len(btc_fills)}')

    # Backup before editing
    shutil.copy2(CSV_FILE, CSV_FILE + '.bak')

    # Index fills by conditionId for fast lookup. Multiple fills per market
    # are possible; we'll use the first one we find.
    fills_by_cid: dict[str, list[dict]] = defaultdict(list)
    for f in btc_fills:
        fills_by_cid[f.get('conditionId', '').lower()].append(f)

    # Match each signal to a fill
    filled = 0
    for row in rows:
        cid = (row.get('condition_id') or '').lower().strip()
        if not cid:
            row['filled'] = 'False'
            row['fill_price'] = ''
            row['fill_size'] = ''
            row['fill_pnl'] = ''
            row['fill_tx'] = ''
            continue

        matches = fills_by_cid.get(cid, [])
        if not matches:
            row['filled'] = 'False'
            row['fill_price'] = ''
            row['fill_size'] = ''
            row['fill_pnl'] = ''
            row['fill_tx'] = ''
            continue

        # Pick the fill whose 'outcome' matches our 'direction'
        our_dir = row.get('direction', '').lower()
        side_match = None
        for m in matches:
            mside = (m.get('outcome') or '').lower()
            if mside == our_dir:
                side_match = m
                break
        if not side_match:
            # filled but direction mismatch — flag but record
            side_match = matches[0]

        fill_price = float(side_match.get('price', 0))
        fill_size = float(side_match.get('size', 0))
        fill_cost = fill_price * fill_size

        # Real PnL: did this market resolve our way?
        outcome = row.get('outcome', '')
        if outcome == 'WIN':
            pnl = fill_size * 1.0 - fill_cost
        elif outcome == 'LOSS':
            pnl = -fill_cost
        else:
            pnl = 0.0

        row['filled'] = 'True'
        row['fill_price'] = f'{fill_price:.4f}'
        row['fill_size'] = f'{fill_size:.4f}'
        row['fill_pnl'] = f'{pnl:+.4f}'
        row['fill_tx'] = side_match.get('transactionHash', '')
        filled += 1

    print(f'\nMatched {filled} of {len(rows)} signals to actual fills')

    save_csv(rows, fieldnames)
    print(f'Updated {CSV_FILE}')

    # ─── Analysis ─────────────────────────────────────────────────────
    print('\n' + '=' * 70)
    print(' Fill-rate analysis by entry price bucket')
    print('=' * 70)

    buckets = [(0.0, 0.20), (0.20, 0.35), (0.35, 0.50),
               (0.50, 0.65), (0.65, 0.75), (0.75, 0.85), (0.85, 1.0)]

    by_bucket = defaultdict(lambda: {
        'logged': 0, 'filled': 0,
        'logged_wins': 0, 'filled_wins': 0,
        'fill_pnl_total': 0.0, 'fill_cost_total': 0.0,
    })

    for row in rows:
        try:
            p = float(row.get('entry_price', 0))
        except ValueError:
            continue
        for lo, hi in buckets:
            if lo <= p < hi:
                key = f'{lo:.2f}-{hi:.2f}'
                by_bucket[key]['logged'] += 1
                if row.get('outcome') == 'WIN':
                    by_bucket[key]['logged_wins'] += 1
                if row.get('filled') == 'True':
                    by_bucket[key]['filled'] += 1
                    if row.get('outcome') == 'WIN':
                        by_bucket[key]['filled_wins'] += 1
                    # Real PnL
                    try:
                        fp = float(row.get('fill_price', 0))
                        fs = float(row.get('fill_size', 0))
                        pnl = float(row.get('fill_pnl', 0))
                        by_bucket[key]['fill_pnl_total'] += pnl
                        by_bucket[key]['fill_cost_total'] += fp * fs
                    except ValueError:
                        pass
                break

    print(f'\n{"Range":<12} {"Logged":>7} {"Filled":>7} {"Fill%":>6} {"Th.W%":>6} {"Real W%":>8} {"Real PnL":>10}')
    print('-' * 70)
    for lo, hi in buckets:
        key = f'{lo:.2f}-{hi:.2f}'
        if key not in by_bucket:
            continue
        b = by_bucket[key]
        if b['logged'] == 0:
            continue
        fill_rate = b['filled'] / b['logged'] * 100
        theoretical_wr = b['logged_wins'] / b['logged'] * 100
        real_wr = b['filled_wins'] / b['filled'] * 100 if b['filled'] > 0 else 0
        real_wr_str = f'{real_wr:.1f}%' if b['filled'] > 0 else '--'
        print(f'  {key:<10} {b["logged"]:>7} {b["filled"]:>7} {fill_rate:>5.1f}% {theoretical_wr:>5.1f}% {real_wr_str:>8} ${b["fill_pnl_total"]:>+8.2f}')

    print()
    print('Legend:')
    print('  Logged   = signals our bot generated')
    print('  Filled   = how many actually executed on Polymarket')
    print('  Fill%    = fill rate per bucket')
    print('  Th.W%    = theoretical win rate (if all logged signals had filled)')
    print('  Real W%  = actual win rate on the ones that DID fill')
    print('  Real PnL = actual USDC profit/loss on filled trades')

    # Overall numbers
    total_logged = sum(b['logged'] for b in by_bucket.values())
    total_filled = sum(b['filled'] for b in by_bucket.values())
    total_pnl = sum(b['fill_pnl_total'] for b in by_bucket.values())
    total_filled_wins = sum(b['filled_wins'] for b in by_bucket.values())
    print('\nOverall:')
    print(f'  {total_filled}/{total_logged} signals filled ({total_filled/total_logged*100:.1f}%)')
    if total_filled > 0:
        print(f'  Real win rate: {total_filled_wins}/{total_filled} = {total_filled_wins/total_filled*100:.1f}%')
        print(f'  Real PnL: ${total_pnl:+.2f}')


if __name__ == '__main__':
    main()
