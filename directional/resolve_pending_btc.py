"""
resolve_pending_btc.py

Reads PENDING bot.py BTC trades from signals_log.csv and resolves them
by looking up actual market outcomes on Polymarket's gamma API.

For each PENDING row:
  1. Query gamma-api.polymarket.com/markets?slug=<slug>
  2. Read 'outcomePrices' (e.g. [1.0, 0.0] means UP won)
  3. Compute won/loss and PnL based on the row's direction/entry/shares
  4. Write back to CSV

Usage:
  python3 resolve_pending_btc.py
  python3 resolve_pending_btc.py --dry-run   # show what would change without writing
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from collections import defaultdict

CSV_PATH = '/root/polymarket-arb-bot/directional/signals_log.csv'
BACKUP_PATH = '/root/polymarket-arb-bot/directional/signals_log.csv.before_resolver_backup'
GAMMA_BASE = 'https://gamma-api.polymarket.com'


def fetch_market(slug):
    """Look up a market by slug. Returns dict or None."""
    url = f'{GAMMA_BASE}/markets?slug={slug}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'resolver/1.0'})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f'  HTTP error {e.code} for {slug}: {e.reason}')
        return None
    except Exception as e:
        print(f'  Error fetching {slug}: {e}')
        return None


def parse_outcome(market):
    """
    Returns (up_won: bool, final_up_price: float, final_down_price: float) or None.
    Only returns a value if the market is resolved.
    """
    if not market:
        return None
    
    # Check if market is closed/resolved
    closed = market.get('closed', False)
    
    # outcomePrices is a JSON string like '["1", "0"]' (UP, DOWN)
    op = market.get('outcomePrices')
    if not op:
        return None
    
    try:
        if isinstance(op, str):
            op = json.loads(op)
    except:
        return None
    
    if not isinstance(op, list) or len(op) < 2:
        return None
    
    try:
        up_price = float(op[0])
        down_price = float(op[1])
    except (ValueError, TypeError):
        return None
    
    # Only consider resolved if prices are clearly 1/0 (not partial)
    if not closed and abs(up_price - down_price) < 0.95:
        return None  # not yet resolved
    
    up_won = up_price > down_price
    return (up_won, up_price, down_price)


def compute_pnl(direction, entry, shares, won):
    """PnL for a sim trade. entry is the crowd_price paid per share."""
    try:
        entry = float(entry)
        shares = int(float(shares))
    except (ValueError, TypeError):
        return 0.0
    if shares == 0:
        return 0.0
    cost = shares * entry
    proceeds = shares * 1.0 if won else 0.0
    return round(proceeds - cost, 4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change without writing')
    parser.add_argument('--limit', type=int, default=0,
                        help='Process only N rows (0 = all)')
    args = parser.parse_args()

    print(f'Reading {CSV_PATH}')
    
    with open(CSV_PATH, newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    print(f'Total rows in CSV: {len(rows)}')
    
    # Find candidate rows: PENDING outcome, has slug
    candidates = []
    for i, r in enumerate(rows):
        outcome = (r.get('outcome') or '').strip().upper()
        slug = (r.get('slug') or '').strip()
        direction = (r.get('direction') or '').strip().upper()
        if outcome == 'PENDING' and slug and direction in ('UP', 'DOWN'):
            candidates.append((i, r))
    
    print(f'Candidates to resolve: {len(candidates)}')
    if not candidates:
        print('Nothing to do.')
        return
    
    if args.limit > 0:
        candidates = candidates[:args.limit]
        print(f'Limiting to first {args.limit}')
    
    # Cache by slug — many rows share the same market (different signal times)
    market_cache = {}
    
    resolved_count = 0
    still_pending = 0
    skipped = 0
    
    stats = defaultdict(lambda: {'W': 0, 'L': 0})
    
    for n, (idx, r) in enumerate(candidates):
        slug = r['slug'].strip()
        direction = r['direction'].strip().upper()
        entry = r.get('entry_price', '0')
        shares = r.get('shares', '0')
        tf = r.get('timeframe', '?')
        
        if (n + 1) % 25 == 0:
            print(f'  Progress: {n+1}/{len(candidates)}...')
        
        # Use cache
        if slug not in market_cache:
            market = fetch_market(slug)
            market_cache[slug] = market
            time.sleep(0.1)  # gentle on gamma API
        else:
            market = market_cache[slug]
        
        outcome_data = parse_outcome(market)
        if outcome_data is None:
            still_pending += 1
            continue
        
        up_won, final_up, final_down = outcome_data
        won = (direction == 'UP' and up_won) or (direction == 'DOWN' and not up_won)
        pnl = compute_pnl(direction, entry, shares, won)
        
        # Update row
        rows[idx]['outcome'] = 'WIN' if won else 'LOSS'
        rows[idx]['resolution_cl_price'] = f'{final_up}/{final_down}'  # UP_final/DOWN_final
        rows[idx]['fill_pnl'] = str(pnl)
        rows[idx]['up_won'] = 'True' if up_won else 'False'
        
        resolved_count += 1
        stats[tf]['W' if won else 'L'] += 1
    
    print()
    print('═══ RESOLUTION SUMMARY ═══')
    print(f'Resolved: {resolved_count}')
    print(f'Still pending: {still_pending}')
    print(f'Skipped: {skipped}')
    print()
    
    if stats:
        print('Per-timeframe breakdown:')
        for tf in sorted(stats.keys()):
            s = stats[tf]
            total = s['W'] + s['L']
            wr = (s['W']/total*100) if total > 0 else 0
            print(f'  {tf}: {s["W"]}W / {s["L"]}L = {wr:.1f}% WR ({total} resolved)')
    
    if args.dry_run:
        print('\nDRY RUN — no changes written to CSV')
        return
    
    if resolved_count == 0:
        print('No updates to write.')
        return
    
    # Backup the original CSV before overwriting
    import shutil
    shutil.copy(CSV_PATH, BACKUP_PATH)
    print(f'\nBacked up original to {BACKUP_PATH}')
    
    with open(CSV_PATH, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    
    print(f'Wrote {len(rows)} rows back to {CSV_PATH}')
    print(f'{resolved_count} rows updated with outcome.')


if __name__ == '__main__':
    main()