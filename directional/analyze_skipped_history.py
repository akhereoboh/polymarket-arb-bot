"""
Parse historical journalctl logs for 'Market too one-sided' skips,
then resolve each via Polymarket gamma using the slug-based lookup.
 
Resolution strategy:
  1. Parse skipped one-sided events from journalctl
  2. For each event, compute the market slug from its title's time window:
       btc-updown-{tf}-{unix_ts_of_window_open}
  3. Query gamma /events?slug={slug} for the resolved market
  4. Read outcomePrices ([1.0,0.0] = UP won, [0.0,1.0] = DOWN won)
  5. Compare with our signal direction → win/loss
 
Usage:
  python3 analyze_skipped_history.py --since "3 days ago"
"""
 
import argparse
import csv
import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
 
import requests
 
 
GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events'
OUTPUT_CSV = '/tmp/skipped_analysis.csv'
 
# Polymarket markets resolve in ET. EDT is UTC-4 (active in May 2026).
# EST is UTC-5. The bot is currently running during EDT.
ET_UTC_OFFSET_HOURS = 4  # EDT
 
# Cache to avoid re-querying the same slug
_slug_cache: dict[str, dict | None] = {}
 
 
# ── log fetching ────────────────────────────────────────────────────────
 
def fetch_journalctl(since: str = '7 days ago') -> str:
    try:
        result = subprocess.run(
            ['journalctl', '-u', 'polybot-directional',
             '--since', since, '--no-pager', '-o', 'short-iso'],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            print(f'journalctl failed: {result.stderr}', file=sys.stderr)
            sys.exit(1)
        return result.stdout
    except Exception as e:
        print(f'Could not run journalctl: {e}', file=sys.stderr)
        sys.exit(1)
 
 
# ── log parsing ─────────────────────────────────────────────────────────
 
# Bot's [Signal] line — handles BOTH formats:
#   New format: [Signal] [5m] TITLE | NNs left
#   Old format: [Signal] TITLE | NNs left   (no timeframe tag)
SIGNAL_RE = re.compile(
    r'\[Signal\]\s+(?:\[(?P<tf>\d+m)\]\s+)?(?P<title>Bitcoin Up or Down[^|]+?)\s+\|\s+(?P<seconds>\d+(?:\.\d+)?)s\s+left'
)
CL_BN_RE = re.compile(
    r'CL:\s+\$([\d,]+\.\d+)\s+\(([+-]?\d+\.\d+)%\)\s+\|\s+'
    r'BN:\s+\$([\d,]+\.\d+)\s+\(([+-]?\d+\.\d+)%\)'
)
DIRECTION_RE = re.compile(
    r'Direction:\s+(?P<dir>UP|DOWN)\s+\|\s+Confidence:\s+(?P<conf>[\d.]+)%'
)
ONE_SIDED_RE = re.compile(
    r'Market too one-sided \((?P<price>[\d.]+)\)'
)
 
 
def parse_log(log_text: str) -> list[dict]:
    events = []
    lines = log_text.splitlines()
 
    i = 0
    while i < len(lines):
        line = lines[i]
        sig = SIGNAL_RE.search(line)
        if not sig:
            i += 1
            continue
 
        # Collect the next 8 lines as the signal block
        block = lines[i:i+8]
        i += 1
 
        cl_bn = None
        direction = None
        confidence = None
        skipped_price = None
 
        for bl in block:
            if cl_bn is None:
                m = CL_BN_RE.search(bl)
                if m:
                    cl_bn = {
                        'cl_pct': float(m.group(2)),
                        'bn_pct': float(m.group(4)),
                    }
                    continue
            if direction is None:
                m = DIRECTION_RE.search(bl)
                if m:
                    direction = m.group('dir').lower()
                    confidence = float(m.group('conf'))
                    continue
            m = ONE_SIDED_RE.search(bl)
            if m:
                skipped_price = float(m.group('price'))
                break
 
        if skipped_price is None or not cl_bn or not direction:
            continue
 
        # Try to infer timeframe from the title if it wasn't in the brackets
        title = sig.group('title').strip()
        tf = sig.group('tf')
        if not tf:
            # Parse the title's time range to infer 5m vs 15m
            tf = infer_timeframe(title)
 
        events.append({
            'log_timestamp': line.split(maxsplit=1)[0] if line.split() else '',
            'timeframe': tf,
            'title': title,
            'seconds_left': float(sig.group('seconds')),
            'direction': direction,
            'confidence': confidence,
            'cl_pct': cl_bn['cl_pct'],
            'bn_pct': cl_bn['bn_pct'],
            'skipped_price': skipped_price,
        })
 
    return events
 
 
def infer_timeframe(title: str) -> str | None:
    """
    From 'Bitcoin Up or Down - May 22, 1:00AM-1:05AM ET' infer '5m' or '15m'.
    Returns None if we can't parse.
    """
    m = re.search(
        r'(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)',
        title
    )
    if not m:
        return None
    h1, m1 = int(m.group(1)), int(m.group(2))
    h2, m2 = int(m.group(4)), int(m.group(5))
    # Convert both to minutes-of-day (within 12h cycle is fine for diff)
    diff = (h2 * 60 + m2) - (h1 * 60 + m1)
    if diff == 5:
        return '5m'
    if diff == 15:
        return '15m'
    # Wrap around midnight: e.g., 11:55PM-12:00AM
    if diff < 0:
        diff += 12 * 60
    if diff == 5:
        return '5m'
    if diff == 15:
        return '15m'
    return None
 
 
# ── slug construction ───────────────────────────────────────────────────
 
# Match a title like "Bitcoin Up or Down - May 21, 7:45PM-7:50PM ET"
TITLE_TIME_RE = re.compile(
    r'(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+'
    r'(?P<h1>\d{1,2}):(?P<m1>\d{2})(?P<ap1>AM|PM)'
    r'[-–](?P<h2>\d{1,2}):(?P<m2>\d{2})(?P<ap2>AM|PM)'
)
 
 
def title_to_open_unix(title: str, year: int = 2026) -> int | None:
    """
    From a market title, return the Unix timestamp of the window-open time
    in UTC. The slug uses this exact timestamp.
    """
    m = TITLE_TIME_RE.search(title)
    if not m:
        return None
 
    month_name = m.group('month')
    day = int(m.group('day'))
    h1 = int(m.group('h1'))
    m1 = int(m.group('m1'))
    ap1 = m.group('ap1')
 
    # Convert to 24h ET
    if ap1 == 'PM' and h1 != 12:
        h1 += 12
    elif ap1 == 'AM' and h1 == 12:
        h1 = 0
 
    try:
        dt_et = datetime.strptime(
            f'{year} {month_name} {day} {h1:02d}:{m1:02d}',
            '%Y %B %d %H:%M'
        )
    except ValueError:
        return None
 
    # Convert ET → UTC
    dt_utc = dt_et + timedelta(hours=ET_UTC_OFFSET_HOURS)
    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return int(dt_utc.timestamp())
 
 
def build_slug(title: str, timeframe: str) -> str | None:
    """Build the Polymarket slug: btc-updown-{tf}-{unix_ts_window_open}."""
    if timeframe not in ('5m', '15m'):
        return None
    ts = title_to_open_unix(title)
    if ts is None:
        return None
    return f'btc-updown-{timeframe}-{ts}'
 
 
# ── gamma resolution ────────────────────────────────────────────────────
 
def resolve_via_gamma(slug: str) -> dict | None:
    """
    Query gamma for the market and extract resolution.
 
    Returns: {closed, up_won, outcome_prices, condition_id} or None on failure.
    """
    if slug in _slug_cache:
        return _slug_cache[slug]
 
    try:
        r = requests.get(
            GAMMA_EVENTS,
            params={'slug': slug, 'closed': 'true'},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15,
        )
        if r.status_code != 200:
            _slug_cache[slug] = None
            return None
        data = r.json()
    except Exception as e:
        print(f'  gamma error for {slug}: {e}')
        _slug_cache[slug] = None
        return None
 
    if not data:
        _slug_cache[slug] = None
        return None
 
    event = data[0]
    if not event.get('closed'):
        _slug_cache[slug] = None
        return None
 
    markets = event.get('markets', [])
    if not markets:
        _slug_cache[slug] = None
        return None
 
    market = markets[0]
    prices_raw = market.get('outcomePrices', '[]')
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        prices = [float(p) for p in prices]
    except Exception:
        _slug_cache[slug] = None
        return None
 
    if len(prices) < 2:
        _slug_cache[slug] = None
        return None
 
    # Resolved markets show 1.0/0.0. If not, treat as unresolved.
    resolved = (
        any(abs(p - 1.0) < 0.01 for p in prices)
        and any(abs(p - 0.0) < 0.01 for p in prices)
    )
    if not resolved:
        _slug_cache[slug] = None
        return None
 
    result = {
        'closed': True,
        'condition_id': market.get('conditionId'),
        'outcome_prices': prices,
        # Outcomes order is [UP, DOWN] per Polymarket convention
        'up_won': prices[0] >= 0.99,
    }
    _slug_cache[slug] = result
    return result
 
 
# ── dedupe ──────────────────────────────────────────────────────────────
 
def dedupe(events: list[dict]) -> list[dict]:
    """Keep latest in window (smallest seconds_left) per unique (title, timeframe)."""
    by_key = {}
    for e in events:
        key = (e['title'], e.get('timeframe'))
        if key not in by_key or e['seconds_left'] < by_key[key]['seconds_left']:
            by_key[key] = e
    return list(by_key.values())
 
 
# ── analysis ────────────────────────────────────────────────────────────
 
def analyze(events: list[dict]) -> None:
    print(f'\nResolving {len(events)} skipped one-sided events via gamma...')
    resolved = []
    no_slug = 0
    no_gamma = 0
 
    for i, e in enumerate(events, 1):
        if i % 20 == 0:
            print(f'  ({i}/{len(events)})...')
 
        slug = build_slug(e['title'], e.get('timeframe'))
        if not slug:
            no_slug += 1
            continue
 
        gamma_result = resolve_via_gamma(slug)
        if not gamma_result:
            no_gamma += 1
            continue
 
        up_won = gamma_result['up_won']
        our_dir = e['direction']
        would_have_won = (our_dir == 'up' and up_won) or (our_dir == 'down' and not up_won)
 
        # PnL math at $20 stake (matching bot's TRADE_AMOUNT default)
        entry_price = e['skipped_price']
        shares = max(5, int(20 / entry_price))
        cost = shares * entry_price
        proceeds = shares * 1.0 if would_have_won else 0.0
        pnl = proceeds - cost
 
        resolved.append({
            'log_timestamp': e['log_timestamp'],
            'slug': slug,
            'condition_id': gamma_result['condition_id'],
            'timeframe': e.get('timeframe'),
            'title': e['title'],
            'direction': e['direction'].upper(),
            'confidence': e['confidence'],
            'cl_pct': e['cl_pct'],
            'bn_pct': e['bn_pct'],
            'skipped_price': entry_price,
            'up_won': up_won,
            'would_have_won': would_have_won,
            'shares': shares,
            'cost': round(cost, 4),
            'proceeds': round(proceeds, 4),
            'pnl': round(pnl, 4),
        })
 
    print(f'\nResolved: {len(resolved)} / {len(events)}')
    print(f'  Failed to build slug: {no_slug}')
    print(f'  Failed gamma lookup:  {no_gamma}')
 
    print('\n' + '=' * 64)
    print(' Skipped One-Sided Signals — Would-Have-Been Analysis')
    print('=' * 64)
 
    if not resolved:
        return
 
    wins = [r for r in resolved if r['would_have_won']]
    losses = [r for r in resolved if not r['would_have_won']]
    total_pnl = sum(r['pnl'] for r in resolved)
    total_cost = sum(r['cost'] for r in resolved)
    avg_entry = sum(r['skipped_price'] for r in resolved) / len(resolved)
    actual_rate = len(wins) / len(resolved) * 100
    breakeven = avg_entry * 100
 
    print(f'Total skipped:    {len(resolved)}')
    print(f'Would have won:   {len(wins)} ({actual_rate:.1f}%)')
    print(f'Would have lost:  {len(losses)} ({len(losses)/len(resolved)*100:.1f}%)')
    print(f'Total PnL:        ${total_pnl:+.2f}')
    print(f'Total cost:       ${total_cost:.2f}')
    if total_cost > 0:
        print(f'ROI:              {total_pnl/total_cost*100:+.1f}%')
    print(f'\nAvg entry price:        {avg_entry:.3f}')
    print(f'Break-even win rate:    {breakeven:.1f}%')
    print(f'Actual would-be rate:   {actual_rate:.1f}%')
    edge = actual_rate - breakeven
    verdict = 'PROFITABLE' if edge > 0 else 'UNPROFITABLE'
    print(f'Edge over breakeven:    {edge:+.1f}pp  → {verdict} on average')
 
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
    print(f'{"Range":12s} {"n":>4s} {"wins":>5s} {"loss":>5s} {"win%":>7s} {"be%":>6s} {"PnL":>9s}  verdict')
    for lo, hi in buckets:
        key = f'{lo:.2f}-{hi:.2f}'
        if key not in by_bucket:
            continue
        v = by_bucket[key]
        tot = v['wins'] + v['losses']
        wr = v['wins'] / tot * 100
        be = ((lo + hi) / 2) * 100
        verdict = 'PROFITABLE' if wr > be else 'UNPROFITABLE'
        print(f'  {key:8s}   {tot:4d}  {v["wins"]:4d}  {v["losses"]:4d}  {wr:6.1f}  {be:5.1f}  ${v["pnl"]:+8.2f}  {verdict}')
 
    # By timeframe
    print('\n--- By timeframe ---')
    for tf in ('5m', '15m'):
        tf_resolved = [r for r in resolved if r['timeframe'] == tf]
        if not tf_resolved:
            continue
        tf_wins = sum(1 for r in tf_resolved if r['would_have_won'])
        tf_pnl = sum(r['pnl'] for r in tf_resolved)
        print(f'  {tf}: n={len(tf_resolved):3d}  wins={tf_wins:3d} ({tf_wins/len(tf_resolved)*100:5.1f}%)  PnL=${tf_pnl:+8.2f}')
 
    # By direction
    print('\n--- By direction ---')
    for d in ('UP', 'DOWN'):
        d_resolved = [r for r in resolved if r['direction'] == d]
        if not d_resolved:
            continue
        d_wins = sum(1 for r in d_resolved if r['would_have_won'])
        d_pnl = sum(r['pnl'] for r in d_resolved)
        print(f'  {d:5s}: n={len(d_resolved):3d}  wins={d_wins:3d} ({d_wins/len(d_resolved)*100:5.1f}%)  PnL=${d_pnl:+8.2f}')
 
    # Save CSV
    if resolved:
        fields = list(resolved[0].keys())
        with open(OUTPUT_CSV, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(resolved)
        print(f'\nSaved {len(resolved)} rows to {OUTPUT_CSV}')
 
 
# ── main ────────────────────────────────────────────────────────────────
 
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--since', default='7 days ago')
    parser.add_argument('--logfile', default=None)
    args = parser.parse_args()
 
    if args.logfile:
        with open(args.logfile) as f:
            log_text = f.read()
    else:
        log_text = fetch_journalctl(args.since)
 
    print(f'Parsing {len(log_text):,} chars of logs...')
    raw = parse_log(log_text)
    print(f'Found {len(raw)} raw one-sided skip events (with repeats)')
 
    events = dedupe(raw)
    print(f'After dedup: {len(events)} unique markets')
 
    if not events:
        return
 
    analyze(events)
 
 
if __name__ == '__main__':
    main()