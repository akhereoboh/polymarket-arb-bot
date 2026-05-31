"""
bot2_resolution_checker.py

For each bot2 signal in Supabase with outcome=PENDING, query Polymarket's
gamma API by SLUG to check if the market has resolved. If resolved, update
the Supabase row with:
  outcome (WIN/LOSS based on whether bot2's predicted direction matched)
  up_won
  final_up_price / final_down_price
  pnl (computed as if the trade had filled at the logged crowd_price)
  resolved_timestamp

bot2 is in dry-run, so PnL here is hypothetical — what WOULD have happened.
After 24-48h of running, you'll have empirical WR data per asset to decide
which assets are safe to take live.

Usage:
    python3 bot2_resolution_checker.py          # check up to 200 pending
    python3 bot2_resolution_checker.py --limit 50
    python3 bot2_resolution_checker.py --hours 72  # only check signals from last 72h
"""

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import aiohttp
from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')
GAMMA_URL    = 'https://gamma-api.polymarket.com/events'


def _log(msg):
    print(f'[Resolver {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


def _sb_headers(prefer='return=representation'):
    return {
        'apikey':         SUPABASE_KEY,
        'Authorization':  f'Bearer {SUPABASE_KEY}',
        'Content-Type':   'application/json',
        'Prefer':         prefer,
    }


async def fetch_pending_bot2(session, limit, hours_back):
    """Get pending bot2 signals from Supabase."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    url = (f'{SUPABASE_URL}/rest/v1/signals'
           f'?bot=eq.bot2&outcome=eq.PENDING'
           f'&signal_timestamp=gte.{cutoff}'
           f'&order=signal_timestamp.asc&limit={limit}')
    async with session.get(url, headers=_sb_headers()) as r:
        if r.status != 200:
            _log(f'Fetch HTTP {r.status}: {(await r.text())[:200]}')
            return []
        return await r.json()


async def fetch_market_state_by_slug(session, slug):
    """Query Polymarket gamma for a single event by slug."""
    url = f'{GAMMA_URL}?slug={slug}'
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
    except Exception as e:
        _log(f'gamma error for {slug}: {e}')
        return None

    if not data:
        return None
    event = data[0]
    if not event.get('closed'):
        return {'resolved': False}

    markets = event.get('markets') or []
    if not markets:
        return None
    m = markets[0]
    try:
        prices = json.loads(m.get('outcomePrices', '[]'))
    except Exception:
        prices = []
    if len(prices) < 2:
        return None
    up_price   = float(prices[0])
    down_price = float(prices[1])
    up_won = up_price >= 0.99
    down_won = down_price >= 0.99
    if not (up_won or down_won):
        return None
    return {
        'resolved':       True,
        'up_won':         up_won,
        'final_up_price': up_price,
        'final_down_price': down_price,
    }


async def update_signal(session, signal_id, patch):
    url = f'{SUPABASE_URL}/rest/v1/signals?id=eq.{signal_id}'
    async with session.patch(url, json=patch, headers=_sb_headers('return=minimal'),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
        return r.status in (200, 204)


def compute_pnl(crowd_price, intended_shares, predicted_up, up_won):
    """
    Hypothetical PnL if the dry-run trade had actually filled.

    Buy `intended_shares` at `crowd_price`. At resolution:
      - winning side pays $1.00 per share
      - losing side pays $0.00
    PnL = (proceeds) - (cost paid)
    """
    cost = intended_shares * crowd_price
    won = (predicted_up == up_won)
    proceeds = intended_shares if won else 0.0
    return round(proceeds - cost, 4), won


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=200,
                        help='Max signals to check this run')
    parser.add_argument('--hours', type=int, default=168,
                        help='Only check signals from last N hours (default 7d)')
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        _log('Missing Supabase env vars')
        sys.exit(1)

    _log(f'Fetching up to {args.limit} pending bot2 signals from last {args.hours}h...')

    async with aiohttp.ClientSession() as session:
        rows = await fetch_pending_bot2(session, args.limit, args.hours)
        if not rows:
            _log('No pending signals')
            return
        _log(f'Got {len(rows)} pending — checking resolutions')

        # Stats accumulator per asset
        stats = defaultdict(lambda: {'checked': 0, 'unresolved': 0,
                                     'resolved': 0, 'wins': 0, 'losses': 0,
                                     'pnl': 0.0, 'errors': 0})
        updated = 0

        for r in rows:
            asset = (r.get('asset') or '?').upper()
            stats[asset]['checked'] += 1

            slug = r.get('slug', '').strip()
            if not slug:
                stats[asset]['errors'] += 1
                continue

            state = await fetch_market_state_by_slug(session, slug)
            if state is None:
                stats[asset]['errors'] += 1
                continue
            if not state.get('resolved'):
                stats[asset]['unresolved'] += 1
                continue

            # Compute hypothetical outcome
            predicted_up = (r.get('direction') == 'UP')
            up_won       = state['up_won']
            crowd_price  = float(r.get('crowd_price') or 0)
            shares       = int(r.get('intended_shares') or 0)
            pnl, won = compute_pnl(crowd_price, shares, predicted_up, up_won)

            patch = {
                'outcome':            'WIN' if won else 'LOSS',
                'up_won':             up_won,
                'final_up_price':     state['final_up_price'],
                'final_down_price':   state['final_down_price'],
                'pnl':                pnl,
                'resolved_timestamp': datetime.now(timezone.utc).isoformat(),
            }
            ok = await update_signal(session, r['id'], patch)
            if ok:
                updated += 1
                stats[asset]['resolved'] += 1
                if won:
                    stats[asset]['wins'] += 1
                else:
                    stats[asset]['losses'] += 1
                stats[asset]['pnl'] += pnl

            await asyncio.sleep(0.05)

        _log(f'\nUpdated {updated} rows. Per-asset summary:')
        for asset, s in sorted(stats.items()):
            resolved = s['resolved']
            if resolved == 0:
                _log(f'  {asset}: 0 resolved (checked={s["checked"]}, unresolved={s["unresolved"]}, err={s["errors"]})')
                continue
            wr = s['wins'] / resolved * 100
            _log(f'  {asset}: {s["wins"]}W/{s["losses"]}L '
                 f'({wr:.1f}% WR)  hypothetical PnL ${s["pnl"]:+.2f}  '
                 f'(unresolved={s["unresolved"]}, err={s["errors"]})')


if __name__ == '__main__':
    asyncio.run(main())
