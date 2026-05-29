"""
verify_bot2_signals.py

Reads bot2's recent dry-run signals from journalctl, then queries Polymarket
for each market's actual resolution. Computes hit rate.

Usage:
    python3 verify_bot2_signals.py [hours_back]
    default: 6 hours back
"""

import asyncio
import re
import subprocess
import sys
from datetime import datetime, timezone

import aiohttp


# Match lines like:
# [bot2 02:43:12] New: [ETH 15m] Ethereum Up or Down - May 28, 9:30PM-9:45PM ET | 95s | Opening CL: $2,004.4265
# OR Telegram alert lines:
# 🔵 [bot2 DRY] [ETH 15m] signal
SIG_PATTERN = re.compile(
    r'\[bot2 DRY\] \[(\w+) (\d+m)\] signal.*?'
    r'(\w+ Up or Down - [^|\n]+).*?'
    r'Direction: (UP|DOWN)',
    re.DOTALL
)


def parse_recent_journal(hours_back: int = 6) -> list[dict]:
    """Pull bot2 signals from journalctl."""
    try:
        result = subprocess.run(
            ['journalctl', '-u', 'polybot-directional-multi',
             '--since', f'{hours_back} hours ago', '--no-pager'],
            capture_output=True, text=True, timeout=30
        )
        text = result.stdout
    except Exception as e:
        print(f'Failed to read journalctl: {e}')
        return []

    # Pattern for "New: [ASSET tf] Title | seconds | Opening CL: $..."
    # And track when signals fired (a 🔵 alert message)
    # Best approach: match per-line, find the "would have signaled" lines
    signals = []
    lines = text.split('\n')
    for line in lines:
        # Look for direction in our [bot2 DRY] log lines (they go to journal too)
        m = re.search(r'\[(\w+) (\d+m)\] signal \| (.+?) \| Direction: (UP|DOWN)', line)
        if m:
            signals.append({
                'asset': m.group(1).upper(),
                'timeframe': m.group(2),
                'title': m.group(3).strip(),
                'direction': m.group(4),
                'log_line': line,
            })
    return signals


def extract_slug(title: str, asset: str, tf: str) -> str | None:
    """
    Convert a title like 'Ethereum Up or Down - May 28, 9:30PM-9:45PM ET'
    to a slug like 'eth-updown-15m-1779999900'.

    Without the unix timestamp this is approximate; we'd ideally log slug
    in the signal directly. For now, try the gamma search.
    """
    return None  # placeholder — we'll use the title search instead


async def find_market_by_title(session, title: str, asset: str) -> dict | None:
    """Search Polymarket events for a market with matching title."""
    asset_full = {
        'BTC': 'Bitcoin', 'ETH': 'Ethereum', 'SOL': 'Solana',
        'BNB': 'BNB', 'DOGE': 'Dogecoin', 'XRP': 'XRP',
    }.get(asset, asset)
    # The title from log has "9:30PM-9:45PM ET" — we search for that fragment
    try:
        async with session.get(
            'https://gamma-api.polymarket.com/events',
            params={'limit': '100', 'order': 'endDate', 'ascending': 'false'},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15,
        ) as r:
            events = await r.json()
    except Exception:
        return None

    for e in events:
        if title in e.get('title', '') or e.get('title', '') in title:
            return e
    return None


async def verify():
    hours_back = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    signals = parse_recent_journal(hours_back)
    print(f'Found {len(signals)} bot2 signals in journalctl '
          f'(last {hours_back}h)\n')

    if not signals:
        return

    correct = 0
    incorrect = 0
    unresolved = 0
    no_match = 0

    async with aiohttp.ClientSession() as session:
        for s in signals:
            ev = await find_market_by_title(session, s['title'], s['asset'])
            if not ev:
                no_match += 1
                print(f'  [{s["asset"]} {s["timeframe"]}] {s["direction"]:<4} → MARKET NOT FOUND ({s["title"][:50]})')
                continue
            if not ev.get('closed'):
                unresolved += 1
                print(f'  [{s["asset"]} {s["timeframe"]}] {s["direction"]:<4} → not yet resolved')
                continue

            # Resolved — check outcome
            markets = ev.get('markets') or [{}]
            m = markets[0]
            import json
            try:
                prices = json.loads(m.get('outcomePrices', '[]'))
            except Exception:
                prices = []
            if len(prices) < 2:
                no_match += 1
                continue

            up_won = float(prices[0]) >= 0.99
            predicted_up = (s['direction'] == 'UP')
            won = (predicted_up == up_won)

            if won:
                correct += 1
                mark = '✓'
            else:
                incorrect += 1
                mark = '✗'
            print(f'  {mark} [{s["asset"]} {s["timeframe"]}] predicted {s["direction"]:<4} | actual {"UP" if up_won else "DOWN":<4} | {s["title"][:50]}')

    total_decided = correct + incorrect
    win_rate = (correct / total_decided * 100) if total_decided else 0
    print()
    print(f'═══ Summary ═══')
    print(f'  Correct:       {correct}')
    print(f'  Incorrect:     {incorrect}')
    print(f'  Unresolved:    {unresolved}')
    print(f'  No match:      {no_match}')
    print(f'  Win rate:      {win_rate:.1f}% on {total_decided} resolved predictions')


if __name__ == '__main__':
    asyncio.run(verify())
