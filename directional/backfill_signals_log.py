"""
Backfill signals_log.csv with condition_id, slug, timeframe — then resolve
PENDING outcomes via gamma using the condition_id.

This handles existing rows that were logged before bot.py started capturing
condition_id. For each row:
  1. Parse title to compute window-open Unix timestamp
  2. Infer timeframe from title's time range (5m or 15m)
  3. Build slug as btc-updown-{tf}-{open_ts}
  4. Query gamma for the market (slug filter is exact, no fuzzy match)
  5. Read condition_id from response
  6. If market is closed/resolved, read outcomePrices and determine win/loss
  7. Write everything back to the CSV

Usage:
  python3 backfill_signals_log.py

Output: updates /root/polymarket-arb-bot/directional/signals_log.csv in place.
Backup written to signals_log.csv.bak before any writes.
"""

import csv
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests


CSV_FILE = '/root/polymarket-arb-bot/directional/signals_log.csv'
GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events'

# Polymarket settles in ET. May 2026 is EDT (UTC-4).
ET_UTC_OFFSET_HOURS = 4

# Cache by slug to avoid hitting gamma twice for the same market
_slug_cache: dict[str, dict | None] = {}


# ── title parsing ───────────────────────────────────────────────────────

TITLE_TIME_RE = re.compile(
    r'(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+'
    r'(?P<h1>\d{1,2}):(?P<m1>\d{2})(?P<ap1>AM|PM)'
    r'[-–](?P<h2>\d{1,2}):(?P<m2>\d{2})(?P<ap2>AM|PM)'
)


def infer_timeframe(title: str) -> str | None:
    m = TITLE_TIME_RE.search(title)
    if not m:
        return None
    h1, m1, ap1 = int(m.group('h1')), int(m.group('m1')), m.group('ap1')
    h2, m2, ap2 = int(m.group('h2')), int(m.group('m2')), m.group('ap2')

    # Convert to 24h
    if ap1 == 'PM' and h1 != 12: h1 += 12
    elif ap1 == 'AM' and h1 == 12: h1 = 0
    if ap2 == 'PM' and h2 != 12: h2 += 12
    elif ap2 == 'AM' and h2 == 12: h2 = 0

    diff = (h2 * 60 + m2) - (h1 * 60 + m1)
    if diff < 0:
        diff += 24 * 60  # wrap

    if diff == 5: return '5m'
    if diff == 15: return '15m'
    return None


def title_to_open_unix(title: str, year_hint: int = 2026) -> int | None:
    m = TITLE_TIME_RE.search(title)
    if not m:
        return None

    month = m.group('month')
    day = int(m.group('day'))
    h1 = int(m.group('h1'))
    m1 = int(m.group('m1'))
    ap1 = m.group('ap1')

    if ap1 == 'PM' and h1 != 12: h1 += 12
    elif ap1 == 'AM' and h1 == 12: h1 = 0

    try:
        dt_et = datetime.strptime(
            f'{year_hint} {month} {day} {h1:02d}:{m1:02d}',
            '%Y %B %d %H:%M'
        )
    except ValueError:
        return None

    dt_utc = dt_et + timedelta(hours=ET_UTC_OFFSET_HOURS)
    return int(dt_utc.replace(tzinfo=timezone.utc).timestamp())


def build_slug(title: str, timeframe: str) -> str | None:
    if timeframe not in ('5m', '15m'):
        return None
    ts = title_to_open_unix(title)
    if ts is None:
        return None
    return f'btc-updown-{timeframe}-{ts}'


# ── gamma lookup ────────────────────────────────────────────────────────

def fetch_market(slug: str) -> dict | None:
    """Return dict with condition_id, closed, up_won, or None on failure."""
    if slug in _slug_cache:
        return _slug_cache[slug]

    try:
        r = requests.get(
            GAMMA_EVENTS,
            params={'slug': slug},
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
    closed = bool(event.get('closed'))
    markets = event.get('markets', [])
    if not markets:
        _slug_cache[slug] = None
        return None

    market = markets[0]
    cid = market.get('conditionId')

    prices_raw = market.get('outcomePrices', '[]')
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        prices = [float(p) for p in prices]
    except Exception:
        prices = []

    # Resolved markets show one outcome at 1.0 and another at 0.0
    resolved = (
        len(prices) >= 2
        and any(abs(p - 1.0) < 0.01 for p in prices)
        and any(abs(p - 0.0) < 0.01 for p in prices)
    )

    up_won = None
    if resolved:
        up_won = prices[0] >= 0.99

    result = {
        'condition_id': cid,
        'closed': closed,
        'resolved': resolved,
        'up_won': up_won,
    }
    _slug_cache[slug] = result
    return result


# ── CSV ─────────────────────────────────────────────────────────────────

def load_csv() -> tuple[list[dict], list[str]]:
    if not Path(CSV_FILE).exists():
        print(f'No file at {CSV_FILE}')
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


# ── main ────────────────────────────────────────────────────────────────

def main():
    rows, fieldnames = load_csv()
    print(f'Loaded {len(rows)} rows from {CSV_FILE}')

    # Add new columns if not present
    new_cols = ['timeframe', 'slug', 'condition_id', 'up_won']
    added = []
    for col in new_cols:
        if col not in fieldnames:
            fieldnames.append(col)
            added.append(col)
    if added:
        print(f'Adding columns: {added}')

    # Backup
    bak = CSV_FILE + '.bak'
    shutil.copy2(CSV_FILE, bak)
    print(f'Backed up to {bak}')

    # Stats
    backfilled_meta = 0
    resolved = 0
    no_slug = 0
    gamma_miss = 0
    already_done = 0

    print(f'\nProcessing rows...')
    for i, row in enumerate(rows, 1):
        if i % 25 == 0:
            print(f'  ({i}/{len(rows)})')

        # Ensure new columns exist on this row
        for col in new_cols:
            row.setdefault(col, '')

        # Skip if already complete
        if row.get('condition_id') and row.get('outcome') in ('WIN', 'LOSS'):
            already_done += 1
            continue

        # Backfill timeframe, slug
        title = row.get('market', '')
        if not row.get('timeframe'):
            tf = infer_timeframe(title)
            if tf:
                row['timeframe'] = tf
        else:
            tf = row['timeframe']

        if not row.get('slug') and tf:
            slug = build_slug(title, tf)
            if slug:
                row['slug'] = slug
        else:
            slug = row.get('slug', '')

        if not slug:
            no_slug += 1
            continue

        # Backfill condition_id and outcome via gamma
        if not row.get('condition_id') or row.get('outcome') == 'PENDING':
            mkt = fetch_market(slug)
            if not mkt:
                gamma_miss += 1
                continue

            if mkt.get('condition_id') and not row.get('condition_id'):
                row['condition_id'] = mkt['condition_id']
                backfilled_meta += 1

            # Update outcome if market is resolved
            if mkt.get('resolved'):
                up_won = mkt['up_won']
                direction = row.get('direction', '').lower()
                won = (direction == 'up' and up_won) or (direction == 'down' and not up_won)
                row['outcome'] = 'WIN' if won else 'LOSS'
                row['up_won'] = 'True' if up_won else 'False'
                resolved += 1

    print(f'\nSummary:')
    print(f'  Already complete:    {already_done}')
    print(f'  condition_id added:  {backfilled_meta}')
    print(f'  Outcomes resolved:   {resolved}')
    print(f'  Could not build slug: {no_slug}')
    print(f'  Gamma miss:          {gamma_miss}')

    save_csv(rows, fieldnames)
    print(f'\nUpdated {CSV_FILE}')


if __name__ == '__main__':
    main()
