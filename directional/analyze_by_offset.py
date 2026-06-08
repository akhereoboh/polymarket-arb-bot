"""
analyze_by_offset.py

Reads existing backtest_results_v2_<coin>.csv files and breaks down
performance by entry offset, to answer:
  - Is T-60s better than T-180s?
  - Does the answer differ for 5m vs 15m markets?
  - At what threshold does each offset become profitable?

Usage:
  python3 analyze_by_offset.py --coin btc
  python3 analyze_by_offset.py --coin btc --threshold 0.05
"""
import argparse
import csv
import os
from collections import defaultdict


def analyze(coin, threshold):
    path = f'./backtest_results_v2_{coin}.csv'
    if not os.path.exists(path):
        print(f'No results file at {path} — run backtest first.')
        return

    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({
                'timeframe': r['timeframe'],
                'offset_sec': int(r['offset_sec']),
                'abs_move_pct': float(r['abs_move_pct']),
                'crowd_price': float(r['crowd_price']),
                'filled': r['filled'].lower() == 'true',
                'won': r['won'].lower() == 'true',
                'pnl': float(r['pnl']),
                'cost': float(r['cost']),
            })

    print(f'\n═══ {coin.upper()} — Performance by Entry Offset ═══')
    print(f'Filter: abs_move_pct >= {threshold}% AND crowd_price >= 0.35')
    print(f'Total rows in file: {len(rows)}')
    print()

    # Group by (timeframe, offset_sec)
    grouped = defaultdict(list)
    for r in rows:
        if r['abs_move_pct'] >= threshold and r['crowd_price'] >= 0.35:
            grouped[(r['timeframe'], r['offset_sec'])].append(r)

    print(f'{"Timeframe":10s} {"Offset":>7s} {"Signals":>8s} {"Filled":>7s} {"W":>5s} {"L":>5s} {"WR":>7s} {"PnL":>10s} {"ROI":>7s}')
    print('-' * 80)

    for key in sorted(grouped.keys()):
        tf, offset = key
        signals = grouped[key]
        filled = [r for r in signals if r['filled']]
        if not filled:
            print(f'{tf:10s} T-{offset:>4d}  {len(signals):>8d} {0:>7d}     -     -      -          -        -')
            continue
        wins = sum(1 for r in filled if r['won'])
        losses = len(filled) - wins
        wr = wins / len(filled) * 100
        pnl = sum(r['pnl'] for r in filled)
        cost = sum(r['cost'] for r in filled)
        roi = pnl / cost * 100 if cost > 0 else 0
        print(f'{tf:10s} T-{offset:>4d}  {len(signals):>8d} {len(filled):>7d} {wins:>5d} {losses:>5d} {wr:>6.1f}% ${pnl:>+7.2f} {roi:>+6.1f}%')

    # Summary: best offset overall
    print()
    print('═══ Best offset by net PnL ═══')
    offset_totals = defaultdict(lambda: {'pnl': 0, 'fills': 0, 'wins': 0, 'losses': 0})
    for (tf, offset), signals in grouped.items():
        filled = [r for r in signals if r['filled']]
        offset_totals[offset]['pnl'] += sum(r['pnl'] for r in filled)
        offset_totals[offset]['fills'] += len(filled)
        offset_totals[offset]['wins'] += sum(1 for r in filled if r['won'])
        offset_totals[offset]['losses'] += sum(1 for r in filled if not r['won'])

    best = sorted(offset_totals.items(), key=lambda x: -x[1]['pnl'])
    for offset, s in best:
        wr = s['wins']/(s['wins']+s['losses'])*100 if (s['wins']+s['losses']) > 0 else 0
        print(f'  T-{offset:>4d}s: ${s["pnl"]:>+7.2f} on {s["fills"]} fills ({s["wins"]}W/{s["losses"]}L = {wr:.1f}% WR)')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--coin', default='btc', choices=['btc', 'eth', 'sol', 'doge', 'xrp'])
    p.add_argument('--threshold', type=float, default=0.05,
                   help='Min abs_move_pct to include (default 0.05)')
    args = p.parse_args()
    analyze(args.coin.lower(), args.threshold)


if __name__ == '__main__':
    main()
