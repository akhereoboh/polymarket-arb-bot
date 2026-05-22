"""
Resolver + analyzer for skipped_signals.csv.

Reads the CSV the bot writes when it skips a 'Market too one-sided' trade,
queries Polymarket gamma to resolve PENDING entries by condition_id, updates
the CSV in place, then prints the would-have analysis.

Why this exists separately from analyze_skipped_history.py:
  - That script parses journalctl logs (lossy, needs fuzzy title matching)
  - This script reads structured CSV the bot writes with condition_id at skip time
  - Resolution is much more reliable

Usage:
  python3 resolve_skipped_signals.py                  # resolve + analyze
  python3 resolve_skipped_signals.py --no-resolve     # analyze only
  python3 resolve_skipped_signals.py --since 2026-05-22  # filter by date
"""

import argparse
import csv
import json
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests


CSV_FILE = '/root/polymarket-arb-bot/directional/skipped_signals.csv'
GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets'


# ── gamma ───────────────────────────────────────────────────────────────

def fetch_market_state(condition_id: str) -> dict | None:
    """
    Look up a single market by condition_id and extract resolution.
    Returns dict with closed, outcome_prices, up_won, or None on failure.
    """
    try:
        r = requests.get(
            GAMMA_MARKETS,
            params={'condition_ids': condition_id},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        print(f'  gamma error for {condition_id[:16]}...: {e}')
        return None

    if not data or not isinstance(data, list):
        return None

    market = data[0]
    if not market.get('closed'):
        return None

    prices_raw = market.get('outcomePrices', '[]')
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        prices = [float(p) for p in prices]
    except Exception:
        return None

    if len(prices) < 2:
        return None

    # Resolved: one outcome is ~1.0 and the other is ~0.0
    resolved = (
        any(abs(p - 1.0) < 0.01 for p in prices)
        and any(abs(p - 0.0) < 0.01 for p in prices)
    )
    if not resolved:
        return None

    return {
        'closed': True,
        'outcome_prices': prices,
        'up_won': prices[0] >= 0.99,
    }


# ── CSV I/O ─────────────────────────────────────────────────────────────

def load_csv() -> tuple[list[dict], list[str]]:
    if not Path(CSV_FILE).exists():
        print(f'No file at {CSV_FILE} — has the bot logged any skipped signals yet?')
        sys.exit(1)
    with open(CSV_FILE) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    return rows, fieldnames


def save_csv(rows: list[dict], fieldnames: list[str]) -> None:
    tmp = CSV_FILE + '.tmp'
    with open(tmp, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, CSV_FILE)


def backup_csv() -> None:
    bak = CSV_FILE + '.bak'
    if not Path(bak).exists() or (
        Path(CSV_FILE).stat().st_mtime > Path(bak).stat().st_mtime
    ):
        shutil.copy2(CSV_FILE, bak)
        print(f'Backed up to {bak}')


# ── resolution ──────────────────────────────────────────────────────────

def resolve_pending(rows: list[dict]) -> int:
    """For each PENDING row whose market has closed, query gamma and update."""
    updated = 0
    now = datetime.now(timezone.utc)

    pending = [r for r in rows if r.get('outcome') == 'PENDING']
    if not pending:
        print('No PENDING rows to resolve.')
        return 0

    print(f'Found {len(pending)} PENDING rows. Resolving via gamma...')

    for i, row in enumerate(pending, 1):
        if i % 25 == 0:
            print(f'  ({i}/{len(pending)})...')

        # Skip if market hasn't closed yet
        try:
            end_dt = datetime.strptime(row['end_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        if end_dt > now:
            continue

        cid = row.get('condition_id', '').strip()
        if not cid:
            continue

        state = fetch_market_state(cid)
        if not state:
            continue

        up_won = state['up_won']
        direction = row['direction'].lower()
        would_have_won = (direction == 'up' and up_won) or (direction == 'down' and not up_won)

        row['outcome'] = 'RESOLVED'
        row['would_have_won'] = 'True' if would_have_won else 'False'
        updated += 1

    return updated


# ── analysis ────────────────────────────────────────────────────────────

def pnl_for_row(row: dict, trade_amount: float = 20.0) -> float:
    """
    Reconstruct what the PnL would have been at $20 stake.
    Mirrors calc_position_size in bot.py.
    """
    try:
        entry_price = float(row['skipped_price'])
        confidence = float(row.get('confidence', 0))
    except (ValueError, KeyError):
        return 0.0

    # Replicate calc_position_size
    if confidence >= 0.15:
        amount = trade_amount
    elif confidence >= 0.10:
        amount = trade_amount * 0.6
    else:
        amount = trade_amount * 0.3

    shares = max(5, int(amount / entry_price))
    cost = shares * entry_price

    if row.get('would_have_won') == 'True':
        return shares * 1.0 - cost
    elif row.get('would_have_won') == 'False':
        return -cost
    return 0.0


def session_for_hour_et(h: int) -> str:
    if 1 <= h < 6:   return 'asian'
    if 6 <= h < 8:   return 'eu_overlap'
    if 8 <= h < 11:  return 'us_open'
    if 11 <= h < 16: return 'us_main'
    if 16 <= h < 20: return 'us_late'
    return 'off_hours'


def hour_et_from_utc_str(utc_str: str) -> int | None:
    try:
        dt = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        return (dt.hour - 4) % 24  # EDT
    except Exception:
        return None


def analyze(rows: list[dict], since: str | None = None) -> None:
    if since:
        try:
            cutoff = datetime.strptime(since, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            rows = [
                r for r in rows
                if datetime.strptime(r.get('timestamp', '1970-01-01 00:00:00'),
                                     '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc) >= cutoff
            ]
        except ValueError:
            print(f'Invalid --since date format. Use YYYY-MM-DD.')
            return

    resolved = [r for r in rows if r.get('outcome') == 'RESOLVED']
    pending = [r for r in rows if r.get('outcome') == 'PENDING']

    print()
    print('=' * 64)
    print(' Skipped One-Sided Signals — Would-Have Analysis')
    print('=' * 64)
    print(f'Total skipped signals: {len(rows)}')
    print(f'Resolved:              {len(resolved)}')
    print(f'Pending:               {len(pending)}')

    if not resolved:
        print('\nNothing resolved yet. Run again later as markets close.')
        return

    wins = [r for r in resolved if r.get('would_have_won') == 'True']
    losses = [r for r in resolved if r.get('would_have_won') == 'False']
    total_pnl = sum(pnl_for_row(r) for r in resolved)
    total_cost = sum(abs(pnl_for_row(r)) if r.get('would_have_won') == 'False'
                     else (float(r.get('skipped_price', 0)) * max(5, int(20 / float(r.get('skipped_price', 1)))))
                     for r in resolved)
    actual_rate = len(wins) / len(resolved) * 100
    avg_entry = sum(float(r['skipped_price']) for r in resolved) / len(resolved)
    breakeven = avg_entry * 100

    print(f'\nWould have won:   {len(wins)} ({actual_rate:.1f}%)')
    print(f'Would have lost:  {len(losses)} ({len(losses)/len(resolved)*100:.1f}%)')
    print(f'Total PnL:        ${total_pnl:+.2f}')
    print(f'Avg entry price:  {avg_entry:.3f}')
    print(f'Breakeven rate:   {breakeven:.1f}%')
    edge = actual_rate - breakeven
    verdict = 'PROFITABLE' if edge > 0 else 'UNPROFITABLE'
    print(f'Edge:             {edge:+.1f}pp  → {verdict} on average')

    # By price bucket
    print('\n--- By skipped entry price ---')
    buckets = [(0.85, 0.90), (0.90, 0.93), (0.93, 0.96), (0.96, 1.00)]
    by_bucket = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
    for r in resolved:
        try:
            p = float(r['skipped_price'])
        except (ValueError, KeyError):
            continue
        for lo, hi in buckets:
            if lo <= p < hi:
                key = f'{lo:.2f}-{hi:.2f}'
                if r.get('would_have_won') == 'True':
                    by_bucket[key]['wins'] += 1
                else:
                    by_bucket[key]['losses'] += 1
                by_bucket[key]['pnl'] += pnl_for_row(r)
                break
    print(f'{"Range":12s} {"n":>4s} {"wins":>5s} {"loss":>5s} {"win%":>7s} {"be%":>6s} {"PnL":>9s}')
    for lo, hi in buckets:
        key = f'{lo:.2f}-{hi:.2f}'
        if key not in by_bucket:
            continue
        v = by_bucket[key]
        tot = v['wins'] + v['losses']
        wr = v['wins'] / tot * 100
        be = ((lo + hi) / 2) * 100
        print(f'  {key:8s}   {tot:4d}  {v["wins"]:4d}  {v["losses"]:4d}  {wr:6.1f}  {be:5.1f}  ${v["pnl"]:+8.2f}')

    # By confidence
    print('\n--- By confidence ---')
    cbuckets = [(0.0, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 99)]
    by_conf = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
    for r in resolved:
        try:
            c = float(r['confidence'])
        except (ValueError, KeyError):
            continue
        for lo, hi in cbuckets:
            if lo <= c < hi:
                key = f'{lo:.2f}-{hi:.2f}' if hi < 99 else f'{lo:.2f}+'
                if r.get('would_have_won') == 'True':
                    by_conf[key]['wins'] += 1
                else:
                    by_conf[key]['losses'] += 1
                by_conf[key]['pnl'] += pnl_for_row(r)
                break
    for lo, hi in cbuckets:
        key = f'{lo:.2f}-{hi:.2f}' if hi < 99 else f'{lo:.2f}+'
        if key not in by_conf:
            continue
        v = by_conf[key]
        tot = v['wins'] + v['losses']
        wr = v['wins'] / tot * 100
        print(f'  {key:8s}   {tot:4d}  {v["wins"]:4d}  {v["losses"]:4d}  {wr:6.1f}  ${v["pnl"]:+8.2f}')

    # By timeframe
    print('\n--- By timeframe ---')
    for tf in ('5m', '15m'):
        tf_resolved = [r for r in resolved if r.get('timeframe') == tf]
        if not tf_resolved:
            continue
        tf_wins = sum(1 for r in tf_resolved if r.get('would_have_won') == 'True')
        tf_pnl = sum(pnl_for_row(r) for r in tf_resolved)
        print(f'  {tf}: n={len(tf_resolved):3d}  wins={tf_wins:3d} ({tf_wins/len(tf_resolved)*100:5.1f}%)  PnL=${tf_pnl:+8.2f}')

    # By direction
    print('\n--- By direction ---')
    for d in ('UP', 'DOWN'):
        d_resolved = [r for r in resolved if r.get('direction') == d]
        if not d_resolved:
            continue
        d_wins = sum(1 for r in d_resolved if r.get('would_have_won') == 'True')
        d_pnl = sum(pnl_for_row(r) for r in d_resolved)
        print(f'  {d:5s}: n={len(d_resolved):3d}  wins={d_wins:3d} ({d_wins/len(d_resolved)*100:5.1f}%)  PnL=${d_pnl:+8.2f}')

    # By session
    print('\n--- By session (ET) ---')
    by_sess = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
    for r in resolved:
        h = hour_et_from_utc_str(r.get('timestamp', ''))
        if h is None:
            continue
        s = session_for_hour_et(h)
        if r.get('would_have_won') == 'True':
            by_sess[s]['wins'] += 1
        else:
            by_sess[s]['losses'] += 1
        by_sess[s]['pnl'] += pnl_for_row(r)
    order = ['asian', 'eu_overlap', 'us_open', 'us_main', 'us_late', 'off_hours']
    for s in order:
        if s not in by_sess:
            continue
        v = by_sess[s]
        tot = v['wins'] + v['losses']
        wr = v['wins'] / tot * 100
        print(f'  {s:12s} n={tot:3d}  wins={v["wins"]:3d} ({wr:5.1f}%)  PnL=${v["pnl"]:+8.2f}')


# ── main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-resolve', action='store_true',
                        help='Skip gamma resolution, analyze existing data only')
    parser.add_argument('--since', default=None,
                        help='Only analyze trades since YYYY-MM-DD')
    args = parser.parse_args()

    rows, fieldnames = load_csv()

    if not args.no_resolve:
        backup_csv()
        n = resolve_pending(rows)
        if n > 0:
            save_csv(rows, fieldnames)
            print(f'Resolved {n} pending rows. CSV updated.')
        else:
            print('Nothing new to resolve.')

    analyze(rows, since=args.since)


if __name__ == '__main__':
    main()
