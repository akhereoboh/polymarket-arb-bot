"""
resolve_pending_btc_v2.py — fixed version using /events endpoint

Reads PENDING bot.py BTC trades from signals_log.csv and resolves them
by looking up actual market outcomes on Polymarket's gamma /events API.

Usage:
  python3 resolve_pending_btc_v2.py --dry-run --limit 50  # test first
  python3 resolve_pending_btc_v2.py                       # do it for real
"""
import argparse
import csv
import json
import os
import shutil
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict

CSV_PATH = '/root/polymarket-arb-bot/directional/signals_log.csv'
BACKUP_PATH = '/root/polymarket-arb-bot/directional/signals_log.csv.before_resolver_backup'
GAMMA_BASE = 'https://gamma-api.polymarket.com'


def fetch_event(slug):
    """Query /events endpoint. Returns event dict or None."""
    url = f'{GAMMA_BASE}/events?slug={slug}'
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
        print(f'  HTTP {e.code} for {slug}')
        return None
    except Exception as e:
        print(f'  Error for {slug}: {e}')
        return None


def parse_outcome(event):
    """
    Returns (up_won: bool, final_up: float, final_down: float) or None if unresolved.
    
    Event structure:
      event['closed'] = True (resolved)
      event['markets'][0]['outcomePrices'] = '["0", "1"]'  (UP_final, DOWN_final)
      event['markets'][0]['outcomes'] = '["Up", "Down"]'   (order confirmation)
    """
    if not event:
        return None
    
    if not event.get('closed'):
        return None  # not yet resolved
    
    markets = event.get('markets')
    if not markets or not isinstance(markets, list):
        return None
    
    m = markets[0]
    op_str = m.get('outcomePrices')
    if not op_str:
        return None
    
    try:
        # outcomePrices is a JSON string like '["0", "1"]'
        op = json.loads(op_str) if isinstance(op_str, str) else op_str
    except:
        return None
    
    if not isinstance(op, list) or len(op) < 2:
        return None
    
    try:
        up_price = float(op[0])
        down_price = float(op[1])
    except (ValueError, TypeError):
        return None
    
    # Must be definitively resolved (1/0 or 0/1, not 0.5/0.5)
    if abs(up_price - down_price) < 0.95:
        return None
    
    up_won = up_price > down_price
    return (up_won, up_price, down_price)


def compute_pnl(entry, shares, won):
    """PnL for a sim trade. entry is price paid per share."""
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
    parser.add_argument('--sim-only', action='store_true', default=True,
                        help='Only resolve dry_run=True rows (default: True)')
    args = parser.parse_args()

    print(f'Reading {CSV_PATH}')
    
    with open(CSV_PATH, newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    print(f'Total rows in CSV: {len(rows)}')
    
    candidates = []
    for i, r in enumerate(rows):
        outcome = (r.get('outcome') or '').strip().upper()
        slug = (r.get('slug') or '').strip()
        direction = (r.get('direction') or '').strip().upper()
        dry_run_str = (r.get('dry_run') or '').strip().lower()
        is_dry = (dry_run_str == 'true')
        
        # Only resolve PENDING rows with valid slug + direction
        if outcome != 'PENDING':
            continue
        if not slug or direction not in ('UP', 'DOWN'):
            continue
        # Sim-only filter
        if args.sim_only and not is_dry:
            continue
        
        candidates.append((i, r))
    
    print(f'Candidates to resolve: {len(candidates)} (sim-only={args.sim_only})')
    if not candidates:
        print('Nothing to do.')
        return
    
    if args.limit > 0:
        candidates = candidates[:args.limit]
        print(f'Limiting to first {args.limit}')
    
    # Cache by slug
    event_cache = {}
    
    resolved_count = 0
    still_pending = 0
    
    stats = defaultdict(lambda: {'W': 0, 'L': 0, 'pnl': 0.0,
                                  '5m_W': 0, '5m_L': 0, '15m_W': 0, '15m_L': 0,
                                  'up_W': 0, 'up_L': 0, 'down_W': 0, 'down_L': 0})
    
    for n, (idx, r) in enumerate(candidates):
        slug = r['slug'].strip()
        direction = r['direction'].strip().upper()
        entry = r.get('entry_price', '0')
        shares = r.get('shares', '0')
        tf = r.get('timeframe', '?')
        
        if (n + 1) % 50 == 0:
            print(f'  Progress: {n+1}/{len(candidates)}...')
        
        if slug not in event_cache:
            event = fetch_event(slug)
            event_cache[slug] = event
            time.sleep(0.1)  # gentle on API
        else:
            event = event_cache[slug]
        
        outcome_data = parse_outcome(event)
        if outcome_data is None:
            still_pending += 1
            continue
        
        up_won, final_up, final_down = outcome_data
        won = (direction == 'UP' and up_won) or (direction == 'DOWN' and not up_won)
        pnl = compute_pnl(entry, shares, won)
        
        rows[idx]['outcome'] = 'WIN' if won else 'LOSS'
        rows[idx]['resolution_cl_price'] = f'{final_up}/{final_down}'
        rows[idx]['fill_pnl'] = str(pnl)
        rows[idx]['up_won'] = 'True' if up_won else 'False'
        
        resolved_count += 1
        bucket = stats['BTC']
        if won: bucket['W'] += 1
        else: bucket['L'] += 1
        bucket['pnl'] += pnl
        if tf == '5m':
            if won: bucket['5m_W'] += 1
            else: bucket['5m_L'] += 1
        elif tf == '15m':
            if won: bucket['15m_W'] += 1
            else: bucket['15m_L'] += 1
        if direction == 'UP':
            if won: bucket['up_W'] += 1
            else: bucket['up_L'] += 1
        else:
            if won: bucket['down_W'] += 1
            else: bucket['down_L'] += 1
    
    print()
    print('═══ RESOLUTION SUMMARY ═══')
    print(f'Resolved: {resolved_count}')
    print(f'Still pending: {still_pending}')
    print()
    
    if resolved_count > 0:
        s = stats['BTC']
        total = s['W'] + s['L']
        wr = (s['W']/total*100) if total > 0 else 0
        print(f'BTC: {s["W"]}W / {s["L"]}L = {wr:.1f}% WR | PnL ${s["pnl"]:+.2f}')
        
        tf5 = s['5m_W'] + s['5m_L']
        tf15 = s['15m_W'] + s['15m_L']
        wr5 = (s['5m_W']/tf5*100) if tf5 > 0 else 0
        wr15 = (s['15m_W']/tf15*100) if tf15 > 0 else 0
        print(f'  5m: {s["5m_W"]}/{s["5m_L"]} = {wr5:.1f}% ({tf5} resolved)')
        print(f' 15m: {s["15m_W"]}/{s["15m_L"]} = {wr15:.1f}% ({tf15} resolved)')
        print()
        up = s['up_W'] + s['up_L']
        down = s['down_W'] + s['down_L']
        up_wr = (s['up_W']/up*100) if up > 0 else 0
        down_wr = (s['down_W']/down*100) if down > 0 else 0
        print(f'  UP: {s["up_W"]}/{s["up_L"]} = {up_wr:.1f}%')
        print(f'DOWN: {s["down_W"]}/{s["down_L"]} = {down_wr:.1f}%')
    
    if args.dry_run:
        print('\nDRY RUN — no changes written to CSV')
        return
    
    if resolved_count == 0:
        print('No updates to write.')
        return
    
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