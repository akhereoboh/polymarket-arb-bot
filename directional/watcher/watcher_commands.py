"""
Telegram command listener for the watcher service.
Handles /stop_watcher, /start_watcher, /status_watcher, /help_watcher

Restricted to ALLOWED_CHAT_ID from env.
"""

import asyncio
import os
import subprocess
import sys

import aiohttp


_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
from telegram_alerts import send_message  # noqa: E402

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
API_BASE = f'https://api.telegram.org/bot{BOT_TOKEN}'

SERVICE_NAME = 'polybot-watcher'
ALLOWED_CHAT_ID = CHAT_ID

_offset = 0


async def _handle_command(text: str) -> str:
    cmd = text.strip().lower()

    if cmd == '/stop_watcher':
        try:
            subprocess.Popen(
                ['systemctl', 'stop', SERVICE_NAME],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            return f'STOP_WATCHER received. Stopping {SERVICE_NAME}...'
        except Exception as e:
            return f'STOP_WATCHER failed: {e}'

    elif cmd == '/start_watcher':
        try:
            subprocess.Popen(
                ['systemctl', 'start', SERVICE_NAME],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            return f'START_WATCHER received. Starting {SERVICE_NAME}...'
        except Exception as e:
            return f'START_WATCHER failed: {e}'

    elif cmd == '/status_watcher':
        try:
            result = subprocess.run(
                ['systemctl', 'is-active', SERVICE_NAME],
                capture_output=True, text=True, timeout=5,
            )
            status = result.stdout.strip()
            return f'Watcher status: {status}'
        except Exception as e:
            return f'STATUS_WATCHER failed: {e}'

    elif cmd == '/help_watcher':
        return ('Watcher commands:\n'
                '/stop_watcher — stop the watcher\n'
                '/start_watcher — start the watcher\n'
                '/status_watcher — check status\n'
                '/help_watcher — this message')

    return ''


async def poll_watcher_commands() -> None:
    global _offset

    if not BOT_TOKEN or not CHAT_ID:
        print('[WatcherCmd] Listener disabled (no token/chat)', flush=True)
        return

    print('[WatcherCmd] Starting — clearing pending updates...', flush=True)

    # Drain backlog so we don't replay old commands
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f'{API_BASE}/getUpdates',
                params={'offset': -1, 'timeout': 0},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data = await r.json()
            if data.get('ok') and data.get('result'):
                _offset = data['result'][-1]['update_id'] + 1
                print(f'[WatcherCmd] Skipped past update_id {_offset - 1}', flush=True)
        except Exception as e:
            print(f'[WatcherCmd] Could not clear backlog: {e}', flush=True)

    print('[WatcherCmd] Active', flush=True)
    await send_message('Watcher command listener active. Send /help_watcher.')

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f'{API_BASE}/getUpdates',
                    params={
                        'offset': _offset,
                        'timeout': 30,
                        'allowed_updates': '["message"]',
                    },
                    timeout=aiohttp.ClientTimeout(total=40),
                ) as r:
                    data = await r.json()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f'[WatcherCmd] getUpdates error: {e}', flush=True)
                await asyncio.sleep(5)
                continue

            if not data.get('ok'):
                await asyncio.sleep(5)
                continue

            for u in data.get('result', []):
                _offset = u['update_id'] + 1
                msg = u.get('message') or {}
                chat = msg.get('chat', {})
                text = msg.get('text', '')

                if str(chat.get('id')) != str(ALLOWED_CHAT_ID):
                    continue
                if not text.startswith('/') or '_watcher' not in text.lower():
                    # Only handle commands ending in _watcher to avoid colliding
                    # with the directional bot's listener
                    continue

                reply = await _handle_command(text)
                if reply:
                    await send_message(reply)


async def start_watcher_command_listener() -> None:
    asyncio.create_task(poll_watcher_commands())
