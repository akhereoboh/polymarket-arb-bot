# """
# hermes_phase1.py — Daily diagnostic + suggestion engine.

# Runs at 09:00 UTC daily via systemd timer.

# Steps:
#   1. Read past 7 days of digests from Supabase (memory)
#   2. Read last 24h of bot logs from journalctl (visibility)
#   3. Compute today's metrics from signals_log.csv + bot2_signals_log.csv
#   4. Call Claude API with structured prompt for trend analysis
#   5. Parse any structured suggestion from Claude's response
#   6. Validate suggestion against APPROVED_KNOBS whitelist
#   7. Insert suggestion (if any) into hermes_actions as 'pending'
#   8. Send Telegram digest with analysis + suggestion + approval instructions
#   9. Archive digest to hermes_digests table

# Claude is for narrative + suggestion. If unavailable, falls back to raw stats.
# """

# import asyncio
# import csv
# import json
# import os
# import re
# import subprocess
# from collections import defaultdict
# from datetime import datetime, timezone, timedelta
# from pathlib import Path

# import aiohttp
# from dotenv import load_dotenv


# _HERE = os.path.dirname(os.path.abspath(__file__))
# load_dotenv(os.path.join(_HERE, '.env'))


# # ─── Config ──────────────────────────────────────────────────────────────
# BOT1_LOG       = os.path.join(_HERE, 'signals_log.csv')
# BOT2_LOG       = os.path.join(_HERE, 'bot2_signals_log.csv')
# TELEGRAM_TOKEN = os.getenv('POLY_HERMES_TELEGRAM_BOT_TOKEN', '')
# TELEGRAM_CHAT  = os.getenv('POLY_HERMES_TELEGRAM_BOT_CHATID', '')
# ANTHROPIC_KEY  = os.getenv('ANTHROPIC_API_KEY', '')
# ANTHROPIC_URL  = 'https://api.anthropic.com/v1/messages'
# MODEL          = 'claude-sonnet-4-20250514'
# SUPABASE_URL   = os.getenv('SUPABASE_URL', '').rstrip('/')
# SUPABASE_KEY   = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')
# WINDOW_HOURS   = int(os.getenv('HERMES_WINDOW_HOURS', '24'))
# HISTORY_DAYS   = 7
# SUGGESTION_EXPIRY_HOURS = 48


# # ─── Approved knobs whitelist ────────────────────────────────────────────
# # Format: 'KEY': (min, max, type)
# # min/max=None means no bounds (used for booleans and free-form strings)
# APPROVED_KNOBS = {
#     'MIN_MOVE_PCT':         (0.01,  0.20,   float),
#     'HARD_FILL_CAP':        (0.50,  0.995,  float),
#     'TRADE_AMOUNT':         (1.0,   20.0,   float),
#     'EARLY_FAK_BUFFER':     (0.05,  0.30,   float),
#     'EARLY_ENTRY_WINDOW_5M':  (0,   300,    int),
#     'EARLY_ENTRY_WINDOW_15M': (0,   900,    int),
#     'BOT2_SAFE_MODE':       (None, None,   bool),
#     'BOT2_TRADE_AMOUNT':    (0.5,  10.0,   float),
#     'BOT2_HARD_FILL_CAP':   (0.50, 0.995,  float),
#     'BOT2_ASSETS':          (None, None,   str),
# }


# def _log(msg):
#     print(f'[Hermes {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


# # ─── CSV loading ─────────────────────────────────────────────────────────

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
#     by_dow  = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})

#     for r in rows:
#         try:
#             pnl = float(r.get('fill_pnl', '') or 0)
#         except Exception:
#             pnl = 0.0
#         h = r['_ts'].hour
#         d = r['_ts'].weekday()
#         by_hour[h]['count'] += 1; by_hour[h]['pnl'] += pnl
#         by_dow[d]['count']  += 1; by_dow[d]['pnl']  += pnl
#         outcome = r.get('outcome', '')
#         if outcome == 'WIN':
#             wins.append(pnl); by_hour[h]['wins'] += 1; by_dow[d]['wins'] += 1
#         elif outcome == 'LOSS':
#             losses.append(pnl); by_hour[h]['losses'] += 1; by_dow[d]['losses'] += 1
#         else:
#             pending += 1

#     resolved = len(wins) + len(losses)
#     return {
#         'total_signals': total,
#         'pending':       pending,
#         'resolved':      resolved,
#         'wins':          len(wins),
#         'losses':        len(losses),
#         'win_rate_pct':  round(len(wins)/resolved*100, 1) if resolved else 0,
#         'total_pnl':     round(sum(wins) + sum(losses), 4),
#         'avg_win':       round(sum(wins)/len(wins), 4) if wins else 0,
#         'avg_loss':      round(sum(losses)/len(losses), 4) if losses else 0,
#         'biggest_win':   round(max(wins), 4) if wins else 0,
#         'biggest_loss':  round(min(losses), 4) if losses else 0,
#         'by_hour':       {h: dict(v) for h, v in sorted(by_hour.items())},
#         'by_dow':        {d: dict(v) for d, v in sorted(by_dow.items())},
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
#         if d == 'UP':     by_asset[a]['up']    += 1
#         elif d == 'DOWN': by_asset[a]['down']  += 1
#         if status == 'DRY_RUN':     by_asset[a]['dry_run'] += 1
#         elif status == 'PLACED':    by_asset[a]['placed']  += 1
#         elif status.startswith('ERROR'): by_asset[a]['errors'] += 1
#     return {
#         'total_signals': total,
#         'safe_mode':     all(r.get('safe_mode', '').lower() == 'true' for r in rows),
#         'by_asset':      {a: dict(v) for a, v in sorted(by_asset.items())},
#     }


# # ─── Journalctl log parsing ──────────────────────────────────────────────

# def parse_logs(window_hours):
#     """Pull recent journalctl events from all bot services and count categories."""
#     results = {'bot1': {}, 'bot2': {}, 'watcher': {}}
#     services = {
#         'bot1':    'polybot-directional',
#         'bot2':    'polybot-directional-multi',
#         'watcher': 'polybot-watcher',
#     }
#     for key, svc in services.items():
#         results[key] = _parse_service(svc, window_hours)
#     return results


# def _parse_service(svc, window_hours):
#     try:
#         out = subprocess.run(
#             ['journalctl', '-u', svc, '--since', f'{window_hours} hours ago', '--no-pager', '-o', 'short'],
#             capture_output=True, text=True, timeout=60,
#         )
#         lines = out.stdout.split('\n')
#     except Exception as e:
#         return {'error': str(e)}

#     counters = {
#         'fak_filled':      0,
#         'fak_no_liquidity':0,
#         'fak_error':       0,
#         'gtc_filled':      0,
#         'gtc_skipped':     0,
#         'signals_fired':   0,
#         'crashes':         0,
#         'scanner_errors':  0,
#         'balance_changes': [],
#         'total_lines':     len(lines),
#     }
#     for line in lines:
#         low = line.lower()
#         if 'fak filled' in low or 'fak_filled' in low:
#             counters['fak_filled'] += 1
#         elif 'no sellers at or below' in low or 'fak_no_liquidity' in low:
#             counters['fak_no_liquidity'] += 1
#         elif 'fak error' in low or 'fak_error' in low:
#             counters['fak_error'] += 1
#         elif 'gtc] posting' in low or 'gtc_filled' in low:
#             counters['gtc_filled'] += 1
#         elif 'gtc] best ask' in low and 'exceeds' in low:
#             counters['gtc_skipped'] += 1
#         elif '[signal]' in low and 'no binance' not in low:
#             counters['signals_fired'] += 1
#         elif 'traceback' in low or 'failed with result' in low:
#             counters['crashes'] += 1
#         elif 'scanner] error' in low:
#             counters['scanner_errors'] += 1
#         elif '[trade] balance:' in low:
#             # extract net change amount
#             m = re.search(r'change:\s*\$([+-]?\d+\.?\d*)', line)
#             if m:
#                 try: counters['balance_changes'].append(float(m.group(1)))
#                 except: pass
#     # Reduce balance_changes to a single net + count
#     bc = counters.pop('balance_changes')
#     counters['balance_net'] = round(sum(bc), 4)
#     counters['balance_events'] = len(bc)
#     return counters


# # ─── Supabase: load 7-day history + write digest + suggestion ────────────

# def _supabase_headers(prefer='return=minimal'):
#     return {
#         'apikey':         SUPABASE_KEY,
#         'Authorization':  f'Bearer {SUPABASE_KEY}',
#         'Content-Type':   'application/json',
#         'Prefer':         prefer,
#     }


# async def load_recent_digests(session, days=HISTORY_DAYS):
#     if not SUPABASE_URL or not SUPABASE_KEY:
#         return []
#     url = (
#         f'{SUPABASE_URL}/rest/v1/hermes_digests'
#         f'?order=digest_timestamp.desc&limit={days+1}'
#     )
#     try:
#         async with session.get(url, headers=_supabase_headers('return=representation'),
#                                timeout=aiohttp.ClientTimeout(total=10)) as r:
#             if r.status == 200:
#                 data = await r.json()
#                 return data or []
#             _log(f'Load digests HTTP {r.status}')
#             return []
#     except Exception as e:
#         _log(f'Load digests error: {e}')
#         return []


# async def archive_digest(session, metrics, narrative):
#     if not SUPABASE_URL or not SUPABASE_KEY:
#         return None
#     row = {
#         'window_start':     metrics['window_start'],
#         'window_end':       metrics['window_end'],
#         'digest_text':      narrative,
#         'metrics':          metrics,
#     }
#     url = f'{SUPABASE_URL}/rest/v1/hermes_digests'
#     headers = _supabase_headers('return=representation')
#     try:
#         async with session.post(url, json=row, headers=headers,
#                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
#             if r.status in (200, 201):
#                 data = await r.json()
#                 if data:
#                     return data[0].get('id')
#             return None
#     except Exception as e:
#         _log(f'Archive error: {e}')
#         return None


# async def save_suggestion(session, digest_id, suggestion):
#     if not SUPABASE_URL or not SUPABASE_KEY or not suggestion:
#         return None
#     expires = (datetime.now(timezone.utc) +
#                timedelta(hours=SUGGESTION_EXPIRY_HOURS)).isoformat()
#     row = {
#         'digest_id':         digest_id,
#         'suggestion_text':   suggestion['reasoning'],
#         'suggestion_key':    suggestion['key'],
#         'suggestion_old_val':str(suggestion['old_val']),
#         'suggestion_new_val':str(suggestion['new_val']),
#         'suggestion_expires':expires,
#         'decision':          'pending',
#     }
#     url = f'{SUPABASE_URL}/rest/v1/hermes_actions'
#     try:
#         async with session.post(url, json=row, headers=_supabase_headers('return=representation'),
#                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
#             if r.status in (200, 201):
#                 data = await r.json()
#                 if data:
#                     return data[0].get('id')
#             body = await r.text()
#             _log(f'Save suggestion HTTP {r.status}: {body[:200]}')
#             return None
#     except Exception as e:
#         _log(f'Save suggestion error: {e}')
#         return None


# # ─── Claude prompt + suggestion parsing ──────────────────────────────────

# PROMPT = """You are Hermes, an analyst for a Polymarket prediction bot.

# Your job each day:
# 1. Compare today's metrics vs the past 7 days of digests (provided below)
# 2. Identify 2-3 patterns worth knowing — only ones with strong evidence
# 3. Optionally suggest ONE config change IF the data strongly supports it

# Critical guidance:
# - DO NOT force a suggestion when there isn't strong evidence.
# - DO NOT hallucinate problems that aren't supported by the data.
# - Silence is better than weak speculation.
# - Suggestions can be improvements, not just fixes — "add this filter" is valid even when nothing is broken.
# - Sample size matters. Note when data is too thin to draw conclusions.
# - Trends matter more than single-day numbers. A single bad day is not a pattern.

# When you DO suggest a change, you MUST use this exact JSON format at the end of your response:

# ```json
# {{
#   "suggested_change": {{
#     "key": "MIN_MOVE_PCT",
#     "old_val": "0.025",
#     "new_val": "0.035",
#     "reasoning": "Brief explanation (1-2 sentences) of why this change, what pattern it addresses."
#   }}
# }}
# ```

# Allowed keys (whitelist — ONLY these can be suggested):
# {whitelist_json}

# If no suggestion is warranted, end with: NO_SUGGESTION

# Format your response as 2-3 short paragraphs of analysis followed by either the JSON block or NO_SUGGESTION.

# Current data:
# {today_json}

# Past digests (newest first, up to 7 days):
# {history_json}

# Recent log activity:
# {logs_json}
# """


# async def get_claude_analysis(session, today, history, logs):
#     if not ANTHROPIC_KEY:
#         return None, '(ANTHROPIC_API_KEY not configured)'

#     body = {
#         'model':      MODEL,
#         'max_tokens': 1200,
#         'messages':   [{'role': 'user', 'content':
#                         PROMPT.format(
#                             whitelist_json=json.dumps(list(APPROVED_KNOBS.keys())),
#                             today_json=json.dumps(today, indent=2, default=str),
#                             history_json=json.dumps(_strip_history(history), indent=2, default=str),
#                             logs_json=json.dumps(logs, indent=2, default=str),
#                         )}],
#     }
#     headers = {
#         'x-api-key':         ANTHROPIC_KEY,
#         'anthropic-version': '2023-06-01',
#         'content-type':      'application/json',
#     }
#     for attempt in range(3):
#         try:
#             async with session.post(ANTHROPIC_URL, json=body, headers=headers,
#                                     timeout=aiohttp.ClientTimeout(total=90)) as r:
#                 data = await r.json()
#                 if r.status != 200:
#                     _log(f'Claude HTTP {r.status}: {str(data)[:200]}')
#                     if attempt < 2:
#                         await asyncio.sleep(2 ** attempt)
#                         continue
#                     return None, f'⚠️ Claude API failed after 3 retries (HTTP {r.status})'
#                 text = ''.join(b.get('text', '') for b in data.get('content', [])
#                                if b.get('type') == 'text').strip()
#                 suggestion = _parse_suggestion(text)
#                 return suggestion, text
#         except Exception as e:
#             _log(f'Claude attempt {attempt+1}: {e}')
#             if attempt < 2:
#                 await asyncio.sleep(2 ** attempt)
#                 continue
#             return None, f'⚠️ Claude API failed after 3 retries: {e}'
#     return None, '⚠️ Claude API failed'


# def _strip_history(history):
#     """Trim history rows to essentials so prompt isn't bloated."""
#     out = []
#     for d in history[:HISTORY_DAYS]:
#         m = d.get('metrics', {}) or {}
#         b1 = m.get('bot1', {}) or {}
#         out.append({
#             'date':         d.get('window_end', '')[:10],
#             'signals':      b1.get('total_signals', 0),
#             'resolved':     b1.get('resolved', 0),
#             'wins':         b1.get('wins', 0),
#             'losses':       b1.get('losses', 0),
#             'win_rate':     b1.get('win_rate_pct', 0),
#             'pnl':          b1.get('total_pnl', 0),
#             'avg_win':      b1.get('avg_win', 0),
#             'avg_loss':     b1.get('avg_loss', 0),
#         })
#     return out


# def _parse_suggestion(text):
#     """Find JSON block and validate against whitelist."""
#     if 'NO_SUGGESTION' in text:
#         return None
#     m = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
#     if not m:
#         return None
#     try:
#         block = json.loads(m.group(1))
#         s = block.get('suggested_change', {})
#     except Exception:
#         return None
#     key = s.get('key', '')
#     new_val_raw = s.get('new_val', '')
#     if key not in APPROVED_KNOBS:
#         _log(f'Suggestion rejected — key not in whitelist: {key}')
#         return None
#     min_v, max_v, typ = APPROVED_KNOBS[key]
#     try:
#         if typ is bool:
#             new_val = str(new_val_raw).lower() in ('true', '1', 'yes')
#         elif typ is float:
#             new_val = float(new_val_raw)
#             if min_v is not None and new_val < min_v: raise ValueError
#             if max_v is not None and new_val > max_v: raise ValueError
#         elif typ is int:
#             new_val = int(new_val_raw)
#             if min_v is not None and new_val < min_v: raise ValueError
#             if max_v is not None and new_val > max_v: raise ValueError
#         else:
#             new_val = str(new_val_raw)
#     except Exception as e:
#         _log(f'Suggestion rejected — value out of bounds: {key}={new_val_raw}')
#         return None
#     return {
#         'key':       key,
#         'old_val':   s.get('old_val', ''),
#         'new_val':   new_val,
#         'reasoning': s.get('reasoning', '')[:500],
#     }


# # ─── Telegram digest ─────────────────────────────────────────────────────

# def format_digest(metrics, narrative, suggestion, action_id):
#     bot1 = metrics['bot1']; bot2 = metrics['bot2']; logs = metrics.get('logs', {})
#     start = datetime.fromisoformat(metrics['window_start'].replace('Z', '+00:00'))
#     end   = datetime.fromisoformat(metrics['window_end'].replace('Z', '+00:00'))
#     day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
#     L = []
#     L.extend([
#         '📊 Hermes Daily Digest',
#         f'{start.strftime("%b %d %H:%M")} → {end.strftime("%b %d %H:%M UTC")}',
#         '',
#     ])
#     if bot1.get('total_signals'):
#         L.extend([
#             '═ bot1 (BTC live) ═',
#             f'Signals: {bot1["total_signals"]} | Resolved: {bot1["resolved"]} ({bot1["wins"]}W/{bot1["losses"]}L)',
#             f'WR {bot1["win_rate_pct"]}%  PnL ${bot1["total_pnl"]:+.2f}',
#             f'Avg W ${bot1["avg_win"]:+.3f}  Avg L ${bot1["avg_loss"]:+.3f}',
#             f'Pending: {bot1["pending"]}',
#             '',
#         ])
#     else:
#         L.extend(['bot1: no signals in window', ''])
#     if bot2.get('total_signals'):
#         L.append(f'═ bot2 ({"DRY" if bot2.get("safe_mode") else "LIVE"}) ═')
#         L.append(f'Total: {bot2["total_signals"]}')
#         for a, s in bot2['by_asset'].items():
#             L.append(f'  {a}: {s["count"]} ({s["up"]}↑/{s["down"]}↓) err:{s["errors"]}')
#         L.append('')
#     if logs:
#         b1l = logs.get('bot1', {})
#         if b1l and 'error' not in b1l:
#             L.extend([
#                 '═ logs/24h (bot1) ═',
#                 f'FAK filled {b1l["fak_filled"]} | no-liq {b1l["fak_no_liquidity"]} | err {b1l["fak_error"]}',
#                 f'GTC filled {b1l["gtc_filled"]} | skipped {b1l["gtc_skipped"]}',
#                 f'Crashes: {b1l["crashes"]}  Scanner errs: {b1l["scanner_errors"]}',
#                 f'Balance events: {b1l["balance_events"]} | net ${b1l["balance_net"]:+.2f}',
#                 '',
#             ])
#     L.extend(['═ Analysis ═', narrative, ''])
#     if suggestion and action_id:
#         L.extend([
#             '═ Suggestion ═',
#             f'{suggestion["key"]}: {suggestion["old_val"]} → {suggestion["new_val"]}',
#             f'Reason: {suggestion["reasoning"]}',
#             '',
#             f'Reply "/approve {action_id}" to apply',
#             f'Or "/reject {action_id}" to dismiss',
#             f'Expires in {SUGGESTION_EXPIRY_HOURS}h',
#         ])
#     elif suggestion:
#         L.extend(['═ Suggestion (not saved) ═', f'{suggestion["key"]}: {suggestion["new_val"]}'])
#     return '\n'.join(L)


# async def send_telegram(session, text):
#     if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
#         return False
#     chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
#     url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
#     for c in chunks:
#         try:
#             async with session.post(url, json={'chat_id': TELEGRAM_CHAT, 'text': c},
#                                     timeout=aiohttp.ClientTimeout(total=15)) as r:
#                 if r.status != 200:
#                     body = await r.text()
#                     _log(f'Telegram HTTP {r.status}: {body[:200]}')
#                     return False
#         except Exception as e:
#             _log(f'Telegram error: {e}')
#             return False
#     return True


# # ─── Main ────────────────────────────────────────────────────────────────

# async def main():
#     _log(f'Starting Hermes (window={WINDOW_HOURS}h, history={HISTORY_DAYS}d)')

#     bot1_rows = load_csv_rows(BOT1_LOG, WINDOW_HOURS)
#     bot2_rows = load_csv_rows(BOT2_LOG, WINDOW_HOURS)
#     logs      = parse_logs(WINDOW_HOURS)
#     _log(f'Data: bot1={len(bot1_rows)} bot2={len(bot2_rows)} '
#          f'bot1_logs={logs["bot1"].get("total_lines",0)}')

#     metrics = {
#         'window_hours': WINDOW_HOURS,
#         'window_end':   datetime.now(timezone.utc).isoformat(),
#         'window_start': (datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)).isoformat(),
#         'bot1':         summarize_bot1(bot1_rows),
#         'bot2':         summarize_bot2(bot2_rows),
#         'logs':         logs,
#     }

#     async with aiohttp.ClientSession() as session:
#         history = await load_recent_digests(session, HISTORY_DAYS)
#         _log(f'Loaded {len(history)} historical digests')

#         suggestion, narrative = await get_claude_analysis(session, metrics, history, logs)
#         if suggestion:
#             _log(f'Suggestion: {suggestion["key"]} -> {suggestion["new_val"]}')
#         else:
#             _log('No suggestion (or Claude failed)')

#         # archive digest first so we have an ID for the action linkage
#         digest_id = await archive_digest(session, metrics, narrative)
#         _log(f'Archived digest: id={digest_id}')

#         # save suggestion if any
#         action_id = None
#         if suggestion and digest_id:
#             action_id = await save_suggestion(session, digest_id, suggestion)
#             _log(f'Saved suggestion: action_id={action_id}')

#         digest = format_digest(metrics, narrative, suggestion, action_id)
#         sent = await send_telegram(session, digest)
#         _log(f'Telegram sent: {sent}')

#     print('\n' + '=' * 70)
#     print(digest)
#     print('=' * 70)


# if __name__ == '__main__':
#     asyncio.run(main())