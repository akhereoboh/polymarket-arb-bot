"""
hermes_listener.py — Always-on Telegram command listener for Hermes Phase 1.

Polls Telegram every 5s for new messages. Recognizes:
  /approve <action_id>   apply pending suggestion N
  /reject <action_id>    dismiss pending suggestion N
  /revert                revert the most recent applied change
  /status                show pending suggestions
  /help                  show command reference

For every approved action:
  1. Validate against APPROVED_KNOBS whitelist (defense in depth)
  2. Backup .env to .env.backup.<timestamp>
  3. Apply the change (sed-style replace, or add new line if key absent)
  4. Restart the relevant bot service
  5. Update hermes_actions row to applied=true, decision='approved'
  6. Confirm via Telegram

Runs as systemd service. Logs to journald. Stateless except for last_update_id
which is persisted to disk to survive restarts.
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp
from dotenv import load_dotenv


_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, '.env'))

# ─── Config ──────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '')
SUPABASE_URL   = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY   = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')
ENV_FILE       = os.path.join(_HERE, '.env')
STATE_FILE     = os.path.join(_HERE, '.hermes_listener_state')
POLL_INTERVAL  = 5  # seconds between Telegram getUpdates calls

# ─── Whitelist (MUST match hermes_phase1.py) ─────────────────────────────
APPROVED_KNOBS = {
    'MIN_MOVE_PCT':         (0.01,  0.20,   float),
    'HARD_FILL_CAP':        (0.50,  0.995,  float),
    'TRADE_AMOUNT':         (1.0,   20.0,   float),
    'EARLY_FAK_BUFFER':     (0.05,  0.30,   float),
    'EARLY_ENTRY_WINDOW_5M':  (0,   300,    int),
    'EARLY_ENTRY_WINDOW_15M': (0,   900,    int),
    'BOT2_SAFE_MODE':       (None, None,   bool),
    'BOT2_TRADE_AMOUNT':    (0.5,  10.0,   float),
    'BOT2_HARD_FILL_CAP':   (0.50, 0.995,  float),
    'BOT2_ASSETS':          (None, None,   str),
}

# Which service each knob requires to restart
KNOB_TO_SERVICE = {
    'MIN_MOVE_PCT':            'polybot-directional',
    'HARD_FILL_CAP':           'polybot-directional',
    'TRADE_AMOUNT':            'polybot-directional',
    'EARLY_FAK_BUFFER':        'polybot-directional',
    'EARLY_ENTRY_WINDOW_5M':   'polybot-directional',
    'EARLY_ENTRY_WINDOW_15M':  'polybot-directional',
    'BOT2_SAFE_MODE':          'polybot-directional-multi',
    'BOT2_TRADE_AMOUNT':       'polybot-directional-multi',
    'BOT2_HARD_FILL_CAP':      'polybot-directional-multi',
    'BOT2_ASSETS':             'polybot-directional-multi',
}


def _log(msg):
    print(f'[Listener {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


# ─── State (last_update_id) ──────────────────────────────────────────────

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {'last_update_id': 0}


def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        _log(f'save_state error: {e}')


# ─── Telegram I/O ────────────────────────────────────────────────────────

async def get_updates(session, offset):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates'
    params = {'offset': offset, 'timeout': 25, 'allowed_updates': '["message"]'}
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                _log(f'getUpdates HTTP {r.status}')
                return []
            data = await r.json()
            if not data.get('ok'):
                _log(f'getUpdates error: {data}')
                return []
            return data.get('result', [])
    except asyncio.TimeoutError:
        return []
    except Exception as e:
        _log(f'getUpdates error: {e}')
        return []


async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    try:
        async with session.post(url, json={'chat_id': TELEGRAM_CHAT, 'text': text},
                                timeout=aiohttp.ClientTimeout(total=15)) as r:
            return r.status == 200
    except Exception as e:
        _log(f'send_telegram error: {e}')
        return False


# ─── Supabase ────────────────────────────────────────────────────────────

def _sb_headers():
    return {
        'apikey':         SUPABASE_KEY,
        'Authorization':  f'Bearer {SUPABASE_KEY}',
        'Content-Type':   'application/json',
        'Prefer':         'return=representation',
    }


async def get_action(session, action_id):
    url = f'{SUPABASE_URL}/rest/v1/hermes_actions?id=eq.{action_id}'
    try:
        async with session.get(url, headers=_sb_headers(),
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                return data[0] if data else None
            return None
    except Exception as e:
        _log(f'get_action error: {e}')
        return None


async def get_pending_actions(session):
    url = (f'{SUPABASE_URL}/rest/v1/hermes_actions'
           f'?decision=eq.pending&order=created_at.desc&limit=20')
    try:
        async with session.get(url, headers=_sb_headers(),
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                return await r.json() or []
            return []
    except Exception as e:
        _log(f'get_pending_actions error: {e}')
        return []


async def get_last_approved_action(session):
    url = (f'{SUPABASE_URL}/rest/v1/hermes_actions'
           f'?decision=eq.approved&applied=eq.true&reverted=eq.false'
           f'&order=applied_at.desc&limit=1')
    try:
        async with session.get(url, headers=_sb_headers(),
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json()
                return data[0] if data else None
            return None
    except Exception as e:
        _log(f'get_last_approved_action error: {e}')
        return None


async def update_action(session, action_id, patch):
    url = f'{SUPABASE_URL}/rest/v1/hermes_actions?id=eq.{action_id}'
    try:
        async with session.patch(url, json=patch, headers=_sb_headers(),
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            return r.status in (200, 204)
    except Exception as e:
        _log(f'update_action error: {e}')
        return False


# ─── .env manipulation ───────────────────────────────────────────────────

def backup_env():
    ts = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    backup_path = f'{ENV_FILE}.backup.{ts}'
    shutil.copy2(ENV_FILE, backup_path)
    return backup_path


def apply_env_change(key, value):
    """Find or insert the KEY=VALUE line. Returns (success, old_value_or_None)."""
    if not Path(ENV_FILE).exists():
        return False, None

    with open(ENV_FILE) as f:
        lines = f.readlines()

    new_lines = []
    found = False
    old_value = None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('#') or '=' not in stripped:
            new_lines.append(line)
            continue
        if stripped.startswith(f'{key}='):
            # Capture old value for the audit
            old_value = stripped.split('=', 1)[1].strip().strip('"').strip("'")
            new_lines.append(f'{key}={value}\n')
            found = True
        else:
            new_lines.append(line)

    if not found:
        new_lines.append(f'{key}={value}\n')

    with open(ENV_FILE, 'w') as f:
        f.writelines(new_lines)
    return True, old_value


def restart_service(service):
    try:
        subprocess.run(['systemctl', 'restart', service],
                       check=True, capture_output=True, timeout=30)
        return True, ''
    except subprocess.CalledProcessError as e:
        return False, e.stderr.decode()[:300]
    except Exception as e:
        return False, str(e)[:300]


def validate_value(key, raw):
    """Return (ok, parsed_value_str). Defense in depth: same whitelist check as hermes_phase1."""
    if key not in APPROVED_KNOBS:
        return False, f'Key {key} not in whitelist'
    min_v, max_v, typ = APPROVED_KNOBS[key]
    try:
        if typ is bool:
            v = str(raw).lower() in ('true', '1', 'yes')
            return True, 'true' if v else 'false'
        if typ is float:
            v = float(raw)
            if min_v is not None and v < min_v: return False, f'Below min {min_v}'
            if max_v is not None and v > max_v: return False, f'Above max {max_v}'
            return True, str(v)
        if typ is int:
            v = int(raw)
            if min_v is not None and v < min_v: return False, f'Below min {min_v}'
            if max_v is not None and v > max_v: return False, f'Above max {max_v}'
            return True, str(v)
        return True, str(raw)
    except Exception as e:
        return False, str(e)


# ─── Command handlers ────────────────────────────────────────────────────

async def handle_approve(session, action_id):
    action = await get_action(session, action_id)
    if not action:
        return f'Action #{action_id} not found.'
    if action['decision'] != 'pending':
        return f'Action #{action_id} already {action["decision"]}. Cannot approve.'

    # Check expiry
    try:
        expires = datetime.fromisoformat(action['suggestion_expires'].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) >= expires:
            await update_action(session, action_id,
                                {'decision': 'expired',
                                 'decision_at': datetime.now(timezone.utc).isoformat(),
                                 'decision_source': 'auto_expire'})
            return f'Action #{action_id} expired before approval.'
    except Exception:
        pass

    key = action['suggestion_key']
    new_val_raw = action['suggestion_new_val']
    ok, validated = validate_value(key, new_val_raw)
    if not ok:
        await update_action(session, action_id,
                            {'decision': 'rejected',
                             'decision_at': datetime.now(timezone.utc).isoformat(),
                             'decision_source': 'telegram_reply',
                             'apply_error': f'validation: {validated}'})
        return f'Action #{action_id} REJECTED (validation): {validated}'

    backup = backup_env()
    ok, old_val = apply_env_change(key, validated)
    if not ok:
        await update_action(session, action_id,
                            {'decision': 'approved',
                             'decision_at': datetime.now(timezone.utc).isoformat(),
                             'decision_source': 'telegram_reply',
                             'applied': False,
                             'apply_error': 'env write failed'})
        return f'Action #{action_id}: env write failed.'

    service = KNOB_TO_SERVICE.get(key)
    restart_ok, restart_err = restart_service(service) if service else (True, '')

    patch = {
        'decision':        'approved',
        'decision_at':     datetime.now(timezone.utc).isoformat(),
        'decision_source': 'telegram_reply',
        'applied':         True,
        'applied_at':      datetime.now(timezone.utc).isoformat(),
        'service_restarted': service or '',
    }
    if not restart_ok:
        patch['apply_error'] = f'restart failed: {restart_err}'
    await update_action(session, action_id, patch)

    msg = (f'✅ Action #{action_id} applied.\n'
           f'{key}: {old_val} → {validated}\n'
           f'Service {service}: {"restarted" if restart_ok else "RESTART FAILED — check manually"}\n'
           f'Backup: {os.path.basename(backup)}')
    return msg


async def handle_reject(session, action_id):
    action = await get_action(session, action_id)
    if not action:
        return f'Action #{action_id} not found.'
    if action['decision'] != 'pending':
        return f'Action #{action_id} already {action["decision"]}.'
    await update_action(session, action_id,
                        {'decision':       'rejected',
                         'decision_at':    datetime.now(timezone.utc).isoformat(),
                         'decision_source':'telegram_reply'})
    return f'❌ Action #{action_id} rejected.'


async def handle_revert(session):
    action = await get_last_approved_action(session)
    if not action:
        return 'No applied action to revert.'

    key = action['suggestion_key']
    old_val_raw = action['suggestion_old_val']
    ok, validated = validate_value(key, old_val_raw)
    if not ok:
        # Fall back to writing raw old value if it can't be validated
        # (whitelist may have changed but old value was previously OK)
        validated = old_val_raw

    backup = backup_env()
    ok, current_val = apply_env_change(key, validated)
    if not ok:
        return 'Revert failed: env write error.'

    service = KNOB_TO_SERVICE.get(key)
    restart_ok, restart_err = restart_service(service) if service else (True, '')

    await update_action(session, action['id'],
                        {'reverted':    True,
                         'reverted_at': datetime.now(timezone.utc).isoformat()})

    return (f'🔄 Reverted action #{action["id"]}.\n'
            f'{key}: {current_val} → {validated}\n'
            f'Service {service}: {"restarted" if restart_ok else "RESTART FAILED"}\n'
            f'Backup: {os.path.basename(backup)}')


async def handle_status(session):
    pending = await get_pending_actions(session)
    if not pending:
        return 'No pending suggestions.'
    lines = [f'{len(pending)} pending suggestion(s):']
    for a in pending:
        try:
            exp = datetime.fromisoformat(a['suggestion_expires'].replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            remaining = exp - now
            if remaining.total_seconds() <= 0:
                rem_str = 'EXPIRED'
            else:
                hrs = int(remaining.total_seconds() // 3600)
                rem_str = f'{hrs}h left'
        except Exception:
            rem_str = '?'
        lines.append(
            f'  #{a["id"]}: {a["suggestion_key"]} → {a["suggestion_new_val"]} ({rem_str})'
        )
    return '\n'.join(lines)


HELP_TEXT = """Hermes Listener commands:
  /approve <id>  Apply pending suggestion
  /reject <id>   Dismiss pending suggestion
  /revert        Revert last applied action
  /status        Show pending suggestions
  /help          This help message"""


# ─── Command dispatcher ──────────────────────────────────────────────────

CMD_RE = re.compile(r'^/(\w+)(?:\s+(\d+))?')


async def handle_message(session, msg_text):
    msg_text = msg_text.strip()
    m = CMD_RE.match(msg_text)
    if not m:
        return None  # not a command, ignore
    cmd = m.group(1).lower()
    arg = m.group(2)

    if cmd == 'approve' and arg:
        return await handle_approve(session, int(arg))
    if cmd == 'reject' and arg:
        return await handle_reject(session, int(arg))
    if cmd == 'revert':
        return await handle_revert(session)
    if cmd == 'status':
        return await handle_status(session)
    if cmd == 'help':
        return HELP_TEXT
    return None


# ─── Main loop ───────────────────────────────────────────────────────────

async def main():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        _log('TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — exiting')
        return

    _log('Starting Hermes Listener')
    state = load_state()
    offset = state.get('last_update_id', 0) + 1

    async with aiohttp.ClientSession() as session:
        await send_telegram(session, '🟢 Hermes Listener online.')

        while True:
            try:
                updates = await get_updates(session, offset)
                for u in updates:
                    offset = max(offset, u['update_id'] + 1)
                    msg = u.get('message') or {}
                    chat = msg.get('chat', {})
                    if str(chat.get('id')) != str(TELEGRAM_CHAT):
                        continue
                    text = msg.get('text', '')
                    if not text.startswith('/'):
                        continue
                    _log(f'Cmd: {text[:80]}')
                    reply = await handle_message(session, text)
                    if reply:
                        await send_telegram(session, reply)
                if updates:
                    state['last_update_id'] = offset - 1
                    save_state(state)
            except Exception as e:
                _log(f'Loop error: {e}')
                await asyncio.sleep(2)
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    asyncio.run(main())