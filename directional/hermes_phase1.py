import asyncio
import csv
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, '.env'))

BOT1_LOG       = os.path.join(_HERE, 'signals_log.csv')
BOT2_LOG       = os.path.join(_HERE, 'bot2_signals_log.csv')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '')
SUPABASE_URL   = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY   = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')
WINDOW_HOURS   = int(os.getenv('HERMES_WINDOW_HOURS', '24'))


def _log(msg):
    print(f'[Hermes {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


def load_csv_rows(path, window_hours):
    if not Path(path).exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            ts_field = r.get('timestamp') or r.get('signal_timestamp', '')
            try:
                ts = datetime.fromisoformat(ts_field + '+00:00')
            except Exception:
                continue
            if ts < cutoff:
                continue
            r['_ts'] = ts
            rows.append(r)
    return rows


def summarize_bot1(rows):
    total = len(rows)
    if total == 0:
        return {'total_signals': 0}

    wins, losses = [], []
    pending = 0
    by_hour = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
    by_dow  = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})

    for r in rows:
        try:
            pnl = float(r.get('fill_pnl', '') or 0)
        except Exception:
            pnl = 0.0
        h = r['_ts'].hour
        d = r['_ts'].weekday()
        by_hour[h]['count'] += 1
        by_hour[h]['pnl'] += pnl
        by_dow[d]['count'] += 1
        by_dow[d]['pnl'] += pnl
        outcome = r.get('outcome', '')
        if outcome == 'WIN':
            wins.append(pnl)
            by_hour[h]['wins'] += 1
            by_dow[d]['wins'] += 1
        elif outcome == 'LOSS':
            losses.append(pnl)
            by_hour[h]['losses'] += 1
            by_dow[d]['losses'] += 1
        else:
            pending += 1

    resolved = len(wins) + len(losses)
    wr = (len(wins) / resolved * 100) if resolved else 0
    return {
        'total_signals':  total,
        'pending':        pending,
        'resolved':       resolved,
        'wins':           len(wins),
        'losses':         len(losses),
        'win_rate_pct':   round(wr, 1),
        'total_pnl':      round(sum(wins) + sum(losses), 4),
        'avg_win':        round(sum(wins)/len(wins), 4) if wins else 0,
        'avg_loss':       round(sum(losses)/len(losses), 4) if losses else 0,
        'biggest_win':    round(max(wins), 4) if wins else 0,
        'biggest_loss':   round(min(losses), 4) if losses else 0,
        'by_hour':        {h: dict(v) for h, v in sorted(by_hour.items())},
        'by_dow':         {d: dict(v) for d, v in sorted(by_dow.items())},
    }


def summarize_bot2(rows):
    total = len(rows)
    if total == 0:
        return {'total_signals': 0}
    by_asset = defaultdict(lambda: {'count': 0, 'up': 0, 'down': 0,
                                    'dry_run': 0, 'placed': 0, 'errors': 0})
    for r in rows:
        a = r.get('asset', '?').upper()
        d = r.get('direction', '').upper()
        status = r.get('trade_status', '')
        by_asset[a]['count'] += 1
        if d == 'UP':
            by_asset[a]['up'] += 1
        elif d == 'DOWN':
            by_asset[a]['down'] += 1
        if status == 'DRY_RUN':
            by_asset[a]['dry_run'] += 1
        elif status == 'PLACED':
            by_asset[a]['placed'] += 1
        elif status.startswith('ERROR'):
            by_asset[a]['errors'] += 1
    return {
        'total_signals': total,
        'safe_mode':     all(r.get('safe_mode', '').lower() == 'true' for r in rows),
        'by_asset':      {a: dict(v) for a, v in sorted(by_asset.items())},
    }


def format_digest(metrics):
    """Format digest as plain text suitable for Telegram + later analysis."""
    bot1 = metrics['bot1']
    bot2 = metrics['bot2']
    start = datetime.fromisoformat(metrics['window_start'].replace('Z', '+00:00'))
    end = datetime.fromisoformat(metrics['window_end'].replace('Z', '+00:00'))

    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    lines = [
        '📊 Hermes Daily Digest',
        f'Window: {start.strftime("%b %d %H:%M")} → {end.strftime("%b %d %H:%M UTC")}',
        '',
    ]

    if bot1.get('total_signals'):
        lines.extend([
            '═══ bot1 (BTC live) ═══',
            f'Signals fired: {bot1["total_signals"]}',
            f'Resolved: {bot1["resolved"]}  ({bot1["wins"]}W / {bot1["losses"]}L)',
            f'Win rate: {bot1["win_rate_pct"]}%',
            f'Net PnL: ${bot1["total_pnl"]:+.2f}',
            f'Avg W: ${bot1["avg_win"]:+.3f}  Avg L: ${bot1["avg_loss"]:+.3f}',
            f'Biggest W: ${bot1["biggest_win"]:+.2f}  Biggest L: ${bot1["biggest_loss"]:+.2f}',
            f'Pending: {bot1["pending"]}',
            '',
        ])

        # Top winning + losing hours
        hours = bot1.get('by_hour', {})
        if hours:
            sorted_hours = sorted(hours.items(), key=lambda x: x[1]['pnl'], reverse=True)
            top3 = sorted_hours[:3]
            bot3 = sorted_hours[-3:]
            lines.append('Best hours (UTC):')
            for h, v in top3:
                lines.append(f'  {int(h):02d}:00 → {v["count"]}T  ${v["pnl"]:+.2f}  ({v["wins"]}W/{v["losses"]}L)')
            lines.append('Worst hours (UTC):')
            for h, v in bot3:
                lines.append(f'  {int(h):02d}:00 → {v["count"]}T  ${v["pnl"]:+.2f}  ({v["wins"]}W/{v["losses"]}L)')
            lines.append('')

        dow = bot1.get('by_dow', {})
        if dow:
            lines.append('By day-of-week:')
            for d in sorted(dow):
                v = dow[d]
                lines.append(f'  {day_names[int(d)]:<4} → {v["count"]}T  ${v["pnl"]:+.2f}  ({v["wins"]}W/{v["losses"]}L)')
            lines.append('')
    else:
        lines.extend(['bot1: no signals in window', ''])

    if bot2.get('total_signals'):
        lines.extend([
            f'═══ bot2 ({"DRY-RUN" if bot2.get("safe_mode") else "LIVE"}) ═══',
            f'Total signals: {bot2["total_signals"]}',
        ])
        for asset, stats in bot2['by_asset'].items():
            placed_or_dry = stats['placed'] if not bot2.get('safe_mode') else stats['dry_run']
            lines.append(f'  {asset}: {stats["count"]} signals ({stats["up"]}↑/{stats["down"]}↓), {placed_or_dry} placed/dry, {stats["errors"]} errors')
        lines.append('')
    else:
        lines.extend(['bot2: no signals in window', ''])

    lines.append('Paste this digest to Claude for analysis.')
    return '\n'.join(lines)


async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    # Telegram has 4096 char limit; chunk if needed
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    for chunk in chunks:
        try:
            async with session.post(url, json={'chat_id': TELEGRAM_CHAT, 'text': chunk},
                                    timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    body = await r.text()
                    _log(f'Telegram HTTP {r.status}: {body[:200]}')
                    return False
        except Exception as e:
            _log(f'Telegram error: {e}')
            return False
    return True


async def archive_to_supabase(session, metrics, digest_text):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    row = {
        'window_start': metrics['window_start'],
        'window_end':   metrics['window_end'],
        'digest_text':  digest_text,
        'metrics':      metrics,
    }
    url = f'{SUPABASE_URL}/rest/v1/hermes_digests'
    headers = {
        'apikey':         SUPABASE_KEY,
        'Authorization':  f'Bearer {SUPABASE_KEY}',
        'Content-Type':   'application/json',
        'Prefer':         'return=minimal',
    }
    try:
        async with session.post(url, json=row, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (200, 201):
                return True
            body = await r.text()
            _log(f'Supabase HTTP {r.status}: {body[:200]}')
            return False
    except Exception as e:
        _log(f'Supabase error: {e}')
        return False


async def main():
    _log(f'Starting Hermes (window={WINDOW_HOURS}h)')
    bot1_rows = load_csv_rows(BOT1_LOG, WINDOW_HOURS)
    bot2_rows = load_csv_rows(BOT2_LOG, WINDOW_HOURS)
    _log(f'Loaded bot1={len(bot1_rows)} bot2={len(bot2_rows)}')

    metrics = {
        'window_hours': WINDOW_HOURS,
        'window_end':   datetime.now(timezone.utc).isoformat(),
        'window_start': (datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)).isoformat(),
        'bot1':         summarize_bot1(bot1_rows),
        'bot2':         summarize_bot2(bot2_rows),
    }
    _log(f'Metrics: bot1 resolved={metrics["bot1"].get("resolved",0)} '
         f'bot2 total={metrics["bot2"].get("total_signals",0)}')

    digest = format_digest(metrics)

    async with aiohttp.ClientSession() as session:
        sent = await send_telegram(session, digest)
        _log(f'Telegram sent: {sent}')
        archived = await archive_to_supabase(session, metrics, digest)
        _log(f'Supabase archived: {archived}')

    print('\n' + '=' * 60)
    print(digest)
    print('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())











































"""Hermes with claude api key"""
# """
# hermes_phase1.py — daily diagnostic agent for the polymarket bots.

# Runs once per day. Reads signals_log.csv + bot2_signals_log.csv, computes
# metrics over a 24h window, calls Claude API for narrative, posts to Telegram,
# archives to Supabase.
# """

# import asyncio
# import csv
# import json
# import os
# from datetime import datetime, timezone, timedelta
# from collections import defaultdict
# from pathlib import Path

# import aiohttp
# from dotenv import load_dotenv

# _HERE = os.path.dirname(os.path.abspath(__file__))
# load_dotenv(os.path.join(_HERE, '.env'))

# BOT1_LOG       = os.path.join(_HERE, 'signals_log.csv')
# BOT2_LOG       = os.path.join(_HERE, 'bot2_signals_log.csv')
# TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
# TELEGRAM_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '')
# ANTHROPIC_KEY  = os.getenv('ANTHROPIC_API_KEY', '')
# ANTHROPIC_URL  = 'https://api.anthropic.com/v1/messages'
# MODEL          = 'claude-sonnet-4-20250514'
# SUPABASE_URL   = os.getenv('SUPABASE_URL', '').rstrip('/')
# SUPABASE_KEY   = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')
# WINDOW_HOURS   = int(os.getenv('HERMES_WINDOW_HOURS', '24'))


# def _log(msg):
#     print(f'[Hermes {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


# def load_csv_rows(path, window_hours):
#     if not Path(path).exists():
#         return []
#     cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
#     rows = []
#     with open(path) as f:
#         for r in csv.DictReader(f):
#             ts_field = r.get('timestamp') or r.get('signal_timestamp', '')
#             try:
#                 ts = datetime.fromisoformat(ts_field + '+00:00')
#             except Exception:
#                 continue
#             if ts < cutoff:
#                 continue
#             r['_ts'] = ts
#             rows.append(r)
#     return rows


# def summarize_bot1(rows):
#     total = len(rows)
#     if total == 0:
#         return {'total_signals': 0}
#     wins, losses = [], []
#     pending = 0
#     by_hour = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
#     for r in rows:
#         try:
#             pnl = float(r.get('fill_pnl', '') or 0)
#         except Exception:
#             pnl = 0.0
#         h = r['_ts'].hour
#         by_hour[h]['count'] += 1
#         by_hour[h]['pnl'] += pnl
#         outcome = r.get('outcome', '')
#         if outcome == 'WIN':
#             wins.append(pnl)
#             by_hour[h]['wins'] += 1
#         elif outcome == 'LOSS':
#             losses.append(pnl)
#             by_hour[h]['losses'] += 1
#         else:
#             pending += 1
#     resolved = len(wins) + len(losses)
#     wr = (len(wins) / resolved * 100) if resolved else 0
#     return {
#         'total_signals': total,
#         'pending':       pending,
#         'resolved':      resolved,
#         'wins':          len(wins),
#         'losses':        len(losses),
#         'win_rate_pct':  round(wr, 1),
#         'total_pnl':     round(sum(wins) + sum(losses), 4),
#         'avg_win':       round(sum(wins)/len(wins), 4) if wins else 0,
#         'avg_loss':      round(sum(losses)/len(losses), 4) if losses else 0,
#         'biggest_win':   round(max(wins), 4) if wins else 0,
#         'biggest_loss':  round(min(losses), 4) if losses else 0,
#         'by_hour':       {str(h): dict(v) for h, v in sorted(by_hour.items())},
#     }


# def summarize_bot2(rows):
#     total = len(rows)
#     if total == 0:
#         return {'total_signals': 0}
#     by_asset = defaultdict(lambda: {'count': 0, 'up': 0, 'down': 0,
#                                     'dry_run': 0, 'placed': 0, 'errors': 0})
#     for r in rows:
#         a = r.get('asset', '?').upper()
#         d = r.get('direction', '').upper()
#         status = r.get('trade_status', '')
#         by_asset[a]['count'] += 1
#         if d == 'UP':
#             by_asset[a]['up'] += 1
#         elif d == 'DOWN':
#             by_asset[a]['down'] += 1
#         if status == 'DRY_RUN':
#             by_asset[a]['dry_run'] += 1
#         elif status == 'PLACED':
#             by_asset[a]['placed'] += 1
#         elif status.startswith('ERROR'):
#             by_asset[a]['errors'] += 1
#     return {
#         'total_signals': total,
#         'safe_mode':     all(r.get('safe_mode', '').lower() == 'true' for r in rows),
#         'by_asset':      {a: dict(v) for a, v in sorted(by_asset.items())},
#     }


# PROMPT_TEMPLATE = """You are Hermes, a quantitative trading assistant.

# You analyze the past 24h of activity for a directional prediction bot on
# Polymarket. The bot trades BTC up/down markets (bot1) and signals 5 other
# crypto assets (bot2: ETH, SOL, BNB, DOGE, XRP).

# Your job: read the metrics, identify 2-3 patterns worth knowing, suggest
# ONE concrete action. Be honest. Don't sycophantically affirm everything.

# Constraints:
# - Plain text, no markdown headers or bullets.
# - 3-4 short paragraphs maximum.
# - Interpret numbers, don't repeat them.
# - Don't suggest changes that need backtest data we don't have.
# - Note when sample size is too small.
# - Lead with what's most actionable; end with the suggestion.

# Metrics:
# {metrics_json}
# """


# async def get_claude_analysis(session, metrics):
#     if not ANTHROPIC_KEY:
#         return '(ANTHROPIC_API_KEY not configured)'
#     body = {
#         'model':      MODEL,
#         'max_tokens': 800,
#         'messages':   [{'role': 'user', 'content':
#                         PROMPT_TEMPLATE.format(metrics_json=json.dumps(metrics, indent=2))}],
#     }
#     headers = {
#         'x-api-key':         ANTHROPIC_KEY,
#         'anthropic-version': '2023-06-01',
#         'content-type':      'application/json',
#     }
#     try:
#         async with session.post(ANTHROPIC_URL, json=body, headers=headers,
#                                 timeout=aiohttp.ClientTimeout(total=60)) as r:
#             data = await r.json()
#             if r.status != 200:
#                 _log(f'Claude HTTP {r.status}: {str(data)[:200]}')
#                 return '(Claude API error — see hermes log)'
#             text = ''.join(b.get('text', '') for b in data.get('content', [])
#                            if b.get('type') == 'text')
#             return text.strip() or '(empty response)'
#     except Exception as e:
#         return f'(Claude error: {e})'


# def format_telegram_digest(metrics, narrative):
#     bot1 = metrics['bot1']
#     bot2 = metrics['bot2']
#     start = datetime.fromisoformat(metrics['window_start'].replace('Z', '+00:00'))
#     end = datetime.fromisoformat(metrics['window_end'].replace('Z', '+00:00'))
#     lines = [
#         '📊 Hermes Daily Digest',
#         f'Window: {start.strftime("%Y-%m-%d %H:%M")} → {end.strftime("%H:%M UTC")}',
#         '',
#     ]
#     if bot1.get('total_signals'):
#         lines.extend([
#             'bot1 (BTC live):',
#             f'  Signals: {bot1["total_signals"]}',
#             f'  Resolved: {bot1["resolved"]}  ({bot1["wins"]}W / {bot1["losses"]}L)',
#             f'  Win rate: {bot1["win_rate_pct"]}%',
#             f'  Net PnL: ${bot1["total_pnl"]:+.2f}',
#             f'  Avg W: ${bot1["avg_win"]:+.3f}  Avg L: ${bot1["avg_loss"]:+.3f}',
#             f'  Pending: {bot1["pending"]}',
#             '',
#         ])
#     else:
#         lines.extend(['bot1: no signals in window', ''])
#     if bot2.get('total_signals'):
#         lines.extend([
#             f'bot2 ({"DRY" if bot2.get("safe_mode") else "LIVE"}):',
#             f'  Total signals: {bot2["total_signals"]}',
#         ])
#         for asset, stats in bot2['by_asset'].items():
#             lines.append(f'  {asset}: {stats["count"]} ({stats["up"]}↑/{stats["down"]}↓)')
#         lines.append('')
#     else:
#         lines.extend(['bot2: no signals in window', ''])
#     lines.extend(['Analysis:', narrative])
#     return '\n'.join(lines)


# async def send_telegram(session, text):
#     if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
#         return False
#     url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
#     try:
#         async with session.post(url, json={'chat_id': TELEGRAM_CHAT, 'text': text},
#                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
#             return r.status == 200
#     except Exception as e:
#         _log(f'Telegram error: {e}')
#         return False


# async def archive_to_supabase(session, metrics, narrative):
#     if not SUPABASE_URL or not SUPABASE_KEY:
#         return False
#     row = {
#         'window_start': metrics['window_start'],
#         'window_end':   metrics['window_end'],
#         'digest_text':  narrative,
#         'metrics':      metrics,
#     }
#     url = f'{SUPABASE_URL}/rest/v1/hermes_digests'
#     headers = {
#         'apikey':         SUPABASE_KEY,
#         'Authorization':  f'Bearer {SUPABASE_KEY}',
#         'Content-Type':   'application/json',
#         'Prefer':         'return=minimal',
#     }
#     try:
#         async with session.post(url, json=row, headers=headers,
#                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
#             return r.status in (200, 201)
#     except Exception as e:
#         _log(f'Supabase error: {e}')
#         return False


# async def main():
#     _log(f'Starting Hermes (window={WINDOW_HOURS}h)')
#     bot1_rows = load_csv_rows(BOT1_LOG, WINDOW_HOURS)
#     bot2_rows = load_csv_rows(BOT2_LOG, WINDOW_HOURS)
#     _log(f'Loaded bot1={len(bot1_rows)} bot2={len(bot2_rows)}')

#     metrics = {
#         'window_hours': WINDOW_HOURS,
#         'window_end':   datetime.now(timezone.utc).isoformat(),
#         'window_start': (datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)).isoformat(),
#         'bot1': summarize_bot1(bot1_rows),
#         'bot2': summarize_bot2(bot2_rows),
#     }
#     _log(f'Metrics: bot1 resolved={metrics["bot1"].get("resolved",0)} '
#          f'bot2 total={metrics["bot2"].get("total_signals",0)}')

#     async with aiohttp.ClientSession() as session:
#         narrative = await get_claude_analysis(session, metrics)
#         _log(f'Narrative: {narrative[:120]}...')
#         digest = format_telegram_digest(metrics, narrative)
#         sent = await send_telegram(session, digest)
#         _log(f'Telegram sent: {sent}')
#         archived = await archive_to_supabase(session, metrics, narrative)
#         _log(f'Supabase archived: {archived}')

#     print('\n' + '=' * 60)
#     print(digest)
#     print('=' * 60)


# if __name__ == '__main__':
#     asyncio.run(main())