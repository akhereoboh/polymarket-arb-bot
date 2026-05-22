"""
Parse historical journalctl logs for 'Market too one-sided' skips,
then resolve each via Polymarket gamma to see if the signal would have won.

Usage:
  # Parse last 7 days (default)
  python3 analyze_skipped_history.py

  # Parse a specific window
  python3 analyze_skipped_history.py --since "3 days ago"

  # Use a saved log file instead of journalctl
  python3 analyze_skipped_history.py --logfile /tmp/journal.txt

Output: /tmp/skipped_analysis.csv plus stdout summary.
"""

import argparse
import csv
import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests


GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events'
GAMMA_MARKETS = 'https://gamma-api.polymarket.com/markets'

OUTPUT_CSV = '/tmp/skipped_analysis.csv'

# Cache event lookups across the run so we don't re-query gamma
_event_cache: dict[str, dict] = {}


# ── log fetching ────────────────────────────────────────────────────────

def fetch_journalctl(since: str = '7 days ago') -> str:
    """Pull bot logs from journalctl."""
    try:
        result = subprocess.run(
            ['journalctl', '-u', 'polybot-directional',
             '--since', since, '--no-pager', '-o', 'short-iso'],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            print(f'journalctl failed: {result.stderr}', file=sys.stderr)
            sys.exit(1)
        return result.stdout
    except Exception as e:
        print(f'Could not run journalctl: {e}', file=sys.stderr)
        sys.exit(1)


# ── log parsing ─────────────────────────────────────────────────────────

# Pattern for a Signal block followed by skip. We look for these markers in
# sequence:
#   [Signal] [TIMEFRAME] TRUNCATED_TITLE | NNs left
#   CL: $X (Y%) | BN: $X (Y%)
#   Direction: UP/DOWN | Confidence: N%
#   → Market too one-sided (PRICE) — poor risk/reward, skipping
SIGNAL_LINE = re.compile(
    r'\[Signal\]\s+\[(?P<tf>\d+m)\]\s+(?P<title>.+?)\s+\|\s+(?P<seconds>\d+(?:\.\d+)?)s\s+left'
)
CL_BN_LINE = re.compile(
    r'CL:\s+\$([\d,]+\.\d+)\s+\(([+-]?\d+\.\d+)%\)\s+\|\s+'
    r'BN:\s+\$([\d,]+\.\d+)\s+\(([+-]?\d+\.\d+)%\)'
)
DIRECTION_LINE = re.compile(
    r'Direction:\s+(?P<dir>UP|DOWN)\s+\|\s+Confidence:\s+(?P<conf>[\d.]+)%'
)
ONE_SIDED_SKIP = re.compile(
    r'Market too one-sided \((?P<price>[\d.]+)\)'
)


def parse_log(log_text: str) -> list[dict]:
    """
    Walk the log line-by-line. When we see a [Signal] line, collect the
    next few lines until we either hit a skip marker or another [Signal].
    Returns list of skip events.
    """
    events = []
    lines = log_text.splitlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        sig_match = SIGNAL_LINE.search(line)
        if not sig_match:
            i += 1
            continue

        # Extract timestamp from this line (journalctl ISO format)
        ts = _extract_timestamp(line)

        # Found a signal. Collect the next ~6 lines.
        block = lines[i:i+8]
        i += 1

        # Inside the block, look for CL/BN, Direction, and the one-sided skip
        cl_bn = None
        direction = None
        confidence = None
        skipped_price = None
        was_one_sided = False

        for bl in block:
            m = CL_BN_LINE.search(bl)
            if m and cl_bn is None:
                cl_bn = {
                    'cl_price': float(m.group(1).replace(',', '')),
                    'cl_pct': float(m.group(2)),
                    'bn_price': float(m.group(3).replace(',', '')),
                    'bn_pct': float(m.group(4)),
                }
                continue
            m = DIRECTION_LINE.search(bl)
            if m and direction is None:
                direction = m.group('dir').lower()
                confidence = float(m.group('conf'))
                continue
            m = ONE_SIDED_SKIP.search(bl)
            if m:
                skipped_price = float(m.group('price'))
                was_one_sided = True
                break

        if not was_one_sided:
            continue
        if not cl_bn or not direction:
            continue

        events.append({
            'timestamp': ts,
            'timeframe': sig_match.group('tf'),
            'title_truncated': sig_match.group('title').strip(),
            'seconds_left': float(sig_match.group('seconds')),
            'direction': direction,
            'confidence': confidence,
            'cl_pct': cl_bn['cl_pct'],
            'bn_pct': cl_bn['bn_pct'],
            'cl_price': cl_bn['cl_price'],
            'bn_price': cl_bn['bn_price'],
            'skipped_price': skipped_price,
        })

    return events


def _extract_timestamp(line: str) -> str:
    """Best-effort grab of the ISO timestamp at the start of a journalctl line."""
    parts = line.split(maxsplit=1)
    return parts[0] if parts else ''


# ── deduplication ───────────────────────────────────────────────────────

def dedupe_events(events: list[dict]) -> list[dict]:
    """
    The bot re-evaluates each market every 5s, so a single signal may appear
    in the log 5-15 times. Keep only the LATEST one per (title, timeframe)
    since that's closest to market close and represents the firmest signal.
    """
    by_key = {}
    for e in events:
        key = (e['title_truncated'], e['timeframe'])
        # Keep the entry with the smallest seconds_left (latest in the window)
        if key not in by_key or e['seconds_left'] < by_key[key]['seconds_left']:
            by_key[key] = e
    return list(by_key.values())


# ── gamma resolution ────────────────────────────────────────────────────

def reconstruct_title_filter(title_truncated: str) -> tuple[str | None, str | None]:
    """
    From a truncated title like 'Bitcoin Up or Down - May 22, 3:00AM-3:05',
    extract a date string we can use to query gamma's date-bounded events list.

    Returns (date_str_YYYY_MM_DD, time_marker) or (None, None) if can't parse.
    """
    m = re.search(r'-\s+([A-Za-z]+\s+\d+),', title_truncated)
    if not m:
        return None, None
    month_day = m.group(1)
    # We need year too. Assume current year — works for backtest windows of <1yr.
    year = datetime.now(timezone.utc).year
    try:
        dt = datetime.strptime(f'{year} {month_day}', '%Y %B %d')
        return dt.strftime('%Y-%m-%d'), None
    except ValueError:
        return None, None


def find_matching_event(title_truncated: str, timeframe: str, signal_ts: str) -> dict | None:
    """
    Query gamma for closed events on the relevant date, then fuzzy-match by
    title prefix. Cache by date+truncated_title to avoid re-fetching.
    """
    cache_key = title_truncated + '|' + timeframe
    if cache_key in _event_cache:
        return _event_cache[cache_key]

    date_str, _ = reconstruct_title_filter(title_truncated)
    if not date_str:
        return None

    # Pull all closed events for the day
    try:
        r = requests.get(
            GAMMA_EVENTS,
            params={
                'closed': 'true',
                'limit': '500',
                'end_date_min': f'{date_str}T00:00:00Z',
                'end_date_max': f'{date_str}T23:59:59Z',
            },
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=20,
        )
        events = r.json() if r.status_code == 200 else []
    except Exception as e:
        print(f'  gamma events fetch error: {e}')
        return None

    # Filter to BTC up/down events matching our timeframe
    candidates = [
        ev for ev in events
        if 'btc-updown' in ev.get('slug', '')
        and f'btc-updown-{timeframe}' in ev.get('slug', '')
    ]

    # Fuzzy match by title prefix — the log's truncated title should be a prefix
    # of the gamma event title.
    matched = None
    for ev in candidates:
        full_title = ev.get('title', '')
        if full_title.startswith(title_truncated):
            matched = ev
            break

    _event_cache[cache_key] = matched
    return matched


def resolve_event(event: dict) -> dict | None:
    """
    Given a gamma event, extract the resolution result.
    Returns dict with: closed, outcome_prices, condition_id, up_token, down_token
    """
    if not event.get('closed'):
        return None
    markets = event.get('markets', [])
    if not markets:
        return None
    m = markets[0]

    prices_raw = m.get('outcomePrices', '[]')
    try:
        prices = [float(p) for p in json.loads(prices_raw)] if isinstance(prices_raw, str) else [float(p) for p in prices_raw]
    except Exception:
        prices = []

    if len(prices) < 2:
        return None
    if not (any(abs(p - 1.0) < 0.01 for p in prices) and any(abs(p - 0.0) < 0.01 for p in prices)):
        return None  # not actually resolved yet

    return {
        'condition_id': m.get('conditionId'),
        'up_won': prices[0] >= 0.99,
        'outcome_prices': prices,
    }


# ── analysis ────────────────────────────────────────────────────────────

def analyze(events: list[dict]) -> None:
    """Resolve each skipped event and report what would have happened."""
    print(f'\nResolving {len(events)} skipped one-sided events via gamma...')
    resolved = []

    for i, e in enumerate(events, 1):
        if i % 10 == 0:
            print(f'  ({i}/{len(events)})...')

        event = find_matching_event(e['title_truncated'], e['timeframe'], e['timestamp'])
        if not event:
            continue

        resolution = resolve_event(event)
        if not resolution:
            continue

        # Determine win/loss for OUR signal direction
        up_won = resolution['up_won']
        our_direction = e['direction']
        would_have_won = (our_direction == 'up' and up_won) or (our_direction == 'down' and not up_won)

        # Per-share PnL at the skipped entry price (assuming we'd have bought $20 worth)
        entry_price = e['skipped_price']
        shares = max(5, int(20 / entry_price))  # mirror calc_position_size
        cost = shares * entry_price
        if would_have_won:
            proceeds = shares * 1.0
            pnl = proceeds - cost
        else:
            proceeds = 0.0
            pnl = -cost

        resolved.append({
            **e,
            'condition_id': resolution['condition_id'],
            'up_won': up_won,
            'would_have_won': would_have_won,
            'shares': shares,
            'cost': round(cost, 4),
            'proceeds': round(proceeds, 4),
            'pnl': round(pnl, 4),
        })

    print(f'\nResolved: {len(resolved)} / {len(events)}\n')
    print('=' * 60)
    print(' Skipped One-Sided Signals — Would-Have Analysis')
    print('=' * 60)

    if not resolved:
        print('Nothing to analyze. Could not resolve any events.')
        return

    wins = [r for r in resolved if r['would_have_won']]
    losses = [r for r in resolved if not r['would_have_won']]
    total_pnl = sum(r['pnl'] for r in resolved)
    total_cost = sum(r['cost'] for r in resolved)

    print(f'Total skipped:    {len(resolved)}')
    print(f'Would have won:   {len(wins)} ({len(wins)/len(resolved)*100:.1f}%)')
    print(f'Would have lost:  {len(losses)} ({len(losses)/len(resolved)*100:.1f}%)')
    print(f'Total PnL:        ${total_pnl:+.2f}')
    print(f'Total cost:       ${total_cost:.2f}')
    if total_cost > 0:
        print(f'ROI:              {total_pnl/total_cost*100:+.1f}%')

    # Break-even win rate analysis
    avg_entry = sum(r['skipped_price'] for r in resolved) / len(resolved)
    breakeven = avg_entry * 100
    actual = len(wins) / len(resolved) * 100
    print(f'\nAvg entry price:        {avg_entry:.3f}')
    print(f'Break-even win rate:    {breakeven:.1f}%')
    print(f'Actual would-be rate:   {actual:.1f}%')
    edge = actual - breakeven
    if edge > 0:
        print(f'Edge over breakeven:    +{edge:.1f}pp  → PROFITABLE on average')
    else:
        print(f'Edge over breakeven:    {edge:.1f}pp  → UNPROFITABLE on average')

    # By price bucket
    print('\n--- By skipped entry price ---')
    buckets = [(0.85, 0.90), (0.90, 0.93), (0.93, 0.96), (0.96, 1.00)]
    by_bucket = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
    for r in resolved:
        for lo, hi in buckets:
            if lo <= r['skipped_price'] < hi:
                key = f'{lo:.2f}-{hi:.2f}'
                if r['would_have_won']:
                    by_bucket[key]['wins'] += 1
                else:
                    by_bucket[key]['losses'] += 1
                by_bucket[key]['pnl'] += r['pnl']
                break
    for lo, hi in buckets:
        key = f'{lo:.2f}-{hi:.2f}'
        if key not in by_bucket:
            continue
        v = by_bucket[key]
        tot = v['wins'] + v['losses']
        wr = v['wins'] / tot * 100
        be = ((lo + hi) / 2) * 100
        verdict = 'WIN' if wr > be else 'LOSS'
        print(f'  {key}  n={tot:3d}  win%={wr:5.1f}  breakeven={be:5.1f}  PnL=${v["pnl"]:+.2f}  ({verdict})')

    # By timeframe
    print('\n--- By timeframe ---')
    for tf in ('5m', '15m'):
        tf_resolved = [r for r in resolved if r['timeframe'] == tf]
        if not tf_resolved:
            continue
        tf_wins = sum(1 for r in tf_resolved if r['would_have_won'])
        tf_pnl = sum(r['pnl'] for r in tf_resolved)
        print(f'  {tf}:  n={len(tf_resolved)}  wins={tf_wins} ({tf_wins/len(tf_resolved)*100:.1f}%)  PnL=${tf_pnl:+.2f}')

    # Save CSV
    if resolved:
        fields = list(resolved[0].keys())
        with open(OUTPUT_CSV, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(resolved)
        print(f'\nDetailed rows saved to {OUTPUT_CSV}')


# ── main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--since', default='7 days ago',
                        help='journalctl --since value (default: 7 days ago)')
    parser.add_argument('--logfile', default=None,
                        help='Read from a file instead of journalctl')
    args = parser.parse_args()

    if args.logfile:
        with open(args.logfile) as f:
            log_text = f.read()
    else:
        log_text = fetch_journalctl(args.since)

    print(f'Parsing logs ({len(log_text):,} chars)...')
    raw_events = parse_log(log_text)
    print(f'Found {len(raw_events)} raw one-sided skip events (with repeats)')

    events = dedupe_events(raw_events)
    print(f'After dedup: {len(events)} unique market+timeframe events')

    if not events:
        print('No skipped one-sided events found. Nothing to analyze.')
        return

    analyze(events)


if __name__ == '__main__':
    main()
