"""
Telegram alerts for the directional bot.

Sends plain-text notifications for:
  - Trade entry (immediately on fill)
  - Trade outcome (Polymarket-based resolution, polled until closed)
  - Daily PnL summary (every 24h from bot start)

Configuration via .env:
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

Resolution polling:
  - First check at T + 120s after market close
  - Retries every 60s, up to 5 attempts total (~5 min window)
  - If still unresolved after retries, sends "outcome unknown" alert
"""

import asyncio
import os
import time
from datetime import datetime, timezone

import aiohttp

# ── config ──────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
API_BASE = f'https://api.telegram.org/bot{BOT_TOKEN}'

GAMMA_EVENTS_URL = 'https://gamma-api.polymarket.com/events'

# Resolution polling
FIRST_CHECK_DELAY_SEC = 120
RETRY_INTERVAL_SEC = 60
MAX_RETRIES = 5  # 5 retries x 60s = 5 minutes after first check

# Daily summary cadence — every 24h from bot start
DAILY_SUMMARY_INTERVAL_SEC = 24 * 3600


# ── in-memory state ─────────────────────────────────────────────────────
# Trades completed in the current 24h window — used for daily summary.
# Cleared each time the summary fires.
_session_trades: list[dict] = []


# ── core send ───────────────────────────────────────────────────────────

async def send_message(text: str) -> bool:
    """Send a plain-text message to the configured chat. Returns True on success."""
    if not BOT_TOKEN or not CHAT_ID:
        print('[Telegram] BOT_TOKEN or CHAT_ID not set in .env — skipping send')
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f'{API_BASE}/sendMessage',
                json={'chat_id': CHAT_ID, 'text': text},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data = await r.json()
                if not data.get('ok'):
                    print(f'[Telegram] Send failed: {data}')
                    return False
                return True
    except Exception as e:
        print(f'[Telegram] Send error: {e}')
        return False


# ── alert: entry ────────────────────────────────────────────────────────

async def alert_entry(
    market: dict,
    direction: str,
    shares: int,
    price: float,
    confidence: float,
    cl_pct: float,
    bn_pct: float,
    get_balance_fn,
) -> None:
    """Send entry alert. Fetches a fresh balance from the API."""
    balance = await get_balance_fn()
    cost = round(shares * price, 4)
    side = 'UP' if direction == 'up' else 'DOWN'
    tf = market.get('timeframe', '?')
    end_str = market['end_time'].astimezone(timezone.utc).strftime('%H:%M:%S UTC')

    text = (
        f'TRADE ENTRY [{tf}]\n'
        f'{market["title"]}\n'
        f'\n'
        f'Direction: {side}\n'
        f'Shares: {shares}\n'
        f'Entry price: {price}\n'
        f'Cost: ${cost:.2f}\n'
        f'Confidence: {confidence:.4f}%\n'
        f'CL move: {cl_pct:+.4f}%\n'
        f'BN move: {bn_pct:+.4f}%\n'
        f'Closes at: {end_str}\n'
        f'\n'
        f'Account balance: ${balance:.2f}'
    )
    await send_message(text)


# ── alert: outcome ──────────────────────────────────────────────────────

async def alert_outcome(
    market_title: str,
    timeframe: str,
    direction: str,
    shares: int,
    entry_price: float,
    won: bool,
    pnl: float,
    proceeds: float,
    final_up_price: float,
    final_down_price: float,
    get_balance_fn,
) -> None:
    """Send outcome alert. Fetches a fresh balance from the API."""
    balance = await get_balance_fn()
    side = direction.upper()
    outcome_word = 'WIN' if won else 'LOSS'
    cost = round(shares * entry_price, 4)

    text = (
        f'TRADE {outcome_word} [{timeframe}]\n'
        f'{market_title}\n'
        f'\n'
        f'Direction: {side}\n'
        f'Entry: {entry_price} x {shares} shares\n'
        f'Cost: ${cost:.2f}\n'
        f'Proceeds: ${proceeds:.2f}\n'
        f'PnL: ${pnl:+.2f}\n'
        f'\n'
        f'Resolved UP: {final_up_price}\n'
        f'Resolved DOWN: {final_down_price}\n'
        f'\n'
        f'Account balance: ${balance:.2f}'
    )
    await send_message(text)


async def alert_outcome_unknown(market_title: str, timeframe: str, direction: str) -> None:
    """Sent when polling fails to find a resolved outcome within the retry window."""
    text = (
        f'OUTCOME UNKNOWN [{timeframe}]\n'
        f'{market_title}\n'
        f'\n'
        f'Direction traded: {direction.upper()}\n'
        f'\n'
        f'Market did not resolve within 5 minute retry window.\n'
        f'Check manually on Polymarket.'
    )
    await send_message(text)


# ── outcome resolution (Polymarket-based) ───────────────────────────────

async def _fetch_market_state(session: aiohttp.ClientSession, slug: str) -> dict | None:
    """
    Fetch current market state from gamma /events endpoint by slug.

    Returns dict with closed (bool) and outcome_prices (list[float]), or None on error.
    """
    try:
        async with session.get(
            GAMMA_EVENTS_URL,
            params={'slug': slug},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                print(f'[Telegram] gamma fetch HTTP {r.status} for slug {slug}')
                return None
            data = await r.json()

        if not data or not isinstance(data, list):
            return None

        event = data[0]
        closed = bool(event.get('closed'))
        markets = event.get('markets', [])
        if not markets:
            return None

        market = markets[0]
        prices_raw = market.get('outcomePrices', '[]')
        if isinstance(prices_raw, str):
            import json as _json
            try:
                outcome_prices = [float(p) for p in _json.loads(prices_raw)]
            except Exception:
                outcome_prices = []
        elif isinstance(prices_raw, list):
            outcome_prices = [float(p) for p in prices_raw]
        else:
            outcome_prices = []

        return {'closed': closed, 'outcome_prices': outcome_prices}
    except Exception as e:
        print(f'[Telegram] gamma fetch error for slug {slug}: {e}')
        return None


def _is_resolved(state: dict) -> bool:
    """A market is considered resolved when closed=True AND outcomePrices are 0/1."""
    if not state or not state.get('closed'):
        return False
    prices = state.get('outcome_prices', [])
    if len(prices) < 2:
        return False
    # Resolved markets have one price at 1.0 and one at 0.0 (within tolerance)
    return any(abs(p - 1.0) < 0.01 for p in prices) and any(abs(p - 0.0) < 0.01 for p in prices)


async def _resolve_outcome(
    market: dict,
    direction: str,
    shares: int,
    entry_price: float,
    get_balance_fn,
    update_outcome_fn=None,
) -> None:
    """Wait until T+120s after close, then poll gamma until resolved or retries exhausted."""
    condition_id = market['condition_id']
    title = market['title']
    tf = market.get('timeframe', '?')

    # Wait until market close + first-check delay
    now = datetime.now(timezone.utc)
    wait_sec = (market['end_time'] - now).total_seconds() + FIRST_CHECK_DELAY_SEC
    if wait_sec > 0:
        await asyncio.sleep(wait_sec)

    # Poll for resolution
    slug = market.get('slug', '')
    if not slug:
        await alert_outcome_unknown(title, tf, direction)
        return

    async with aiohttp.ClientSession() as session:
        for attempt in range(MAX_RETRIES + 1):
            state = await _fetch_market_state(session, slug)
            if state and _is_resolved(state):
                prices = state['outcome_prices']
                # By convention in the bot: token_ids[0] = UP, token_ids[1] = DOWN
                # outcomePrices follows the same order as outcomes/tokens.
                up_final = prices[0]
                down_final = prices[1]
                up_won = up_final >= 0.99
                won = (direction == 'up' and up_won) or (direction == 'down' and not up_won)
                proceeds = float(shares) * (1.0 if won else 0.0)
                pnl = round(proceeds - (shares * entry_price), 4)

                # Record for daily summary
                _session_trades.append({
                    'title': title,
                    'timeframe': tf,
                    'direction': direction,
                    'shares': shares,
                    'entry_price': entry_price,
                    'won': won,
                    'pnl': pnl,
                    'resolved_at': datetime.now(timezone.utc),
                })

                await alert_outcome(
                    market_title=title,
                    timeframe=tf,
                    direction=direction,
                    shares=shares,
                    entry_price=entry_price,
                    won=won,
                    pnl=pnl,
                    proceeds=proceeds,
                    final_up_price=up_final,
                    final_down_price=down_final,
                    get_balance_fn=get_balance_fn,
                )
                # Bot-specific writeback (passed via update_outcome_fn callback)
                if update_outcome_fn:
                    try:
                        await update_outcome_fn(
                            condition_id=condition_id,
                            won=won,
                            pnl=pnl,
                            up_won=up_won,
                            final_up_price=up_final,
                            direction=direction,
                        )
                    except Exception as e:
                        print(f'[Telegram] update_outcome_fn failed: {e}')
                return

            # Not resolved — wait and retry
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_INTERVAL_SEC)

    # Exhausted retries — send unknown alert
    await alert_outcome_unknown(title, tf, direction)


def schedule_outcome_check(
    market: dict,
    direction: str,
    shares: int,
    entry_price: float,
    get_balance_fn,
    update_outcome_fn=None,
) -> None:
    """
    Schedule background polling for this market's outcome.
    Should be called immediately after a confirmed trade entry.
    Pass update_outcome_fn=<async callable> to specify which bot's
    writeback runs when the outcome resolves.
    """
    asyncio.create_task(
        _resolve_outcome(market, direction, shares, entry_price,
                         get_balance_fn, update_outcome_fn)
    )



# ── daily PnL summary ───────────────────────────────────────────────────

async def start_daily_summary_loop(get_balance_fn) -> None:
    """
    Background loop: every 24h from bot start, send a PnL summary of the last window.

    Call once from main() with asyncio.create_task().
    """
    bot_start = datetime.now(timezone.utc)
    await send_message(
        f'BOT STARTED\n'
        f'Start time: {bot_start.strftime("%Y-%m-%d %H:%M:%S UTC")}\n'
        f'Daily summary will fire every 24h from now.'
    )

    while True:
        await asyncio.sleep(DAILY_SUMMARY_INTERVAL_SEC)
        try:
            await _send_daily_summary(get_balance_fn)
        except Exception as e:
            print(f'[Telegram] Daily summary error: {e}')


async def _send_daily_summary(get_balance_fn) -> None:
    """Compile and send the 24h summary. Clears _session_trades after."""
    global _session_trades
    trades = list(_session_trades)
    _session_trades = []

    balance = await get_balance_fn()

    if not trades:
        text = (
            f'DAILY PnL SUMMARY (24h)\n'
            f'\n'
            f'No resolved trades in the last 24h.\n'
            f'\n'
            f'Account balance: ${balance:.2f}'
        )
        await send_message(text)
        return

    wins = [t for t in trades if t['won']]
    losses = [t for t in trades if not t['won']]
    total_pnl = sum(t['pnl'] for t in trades)
    win_pnl = sum(t['pnl'] for t in wins)
    loss_pnl = sum(t['pnl'] for t in losses)
    win_rate = len(wins) / len(trades) * 100

    text = (
        f'DAILY PnL SUMMARY (24h)\n'
        f'\n'
        f'Total trades: {len(trades)}\n'
        f'Wins: {len(wins)}\n'
        f'Losses: {len(losses)}\n'
        f'Win rate: {win_rate:.1f}%\n'
        f'\n'
        f'Total PnL: ${total_pnl:+.2f}\n'
        f'Wins PnL: ${win_pnl:+.2f}\n'
        f'Losses PnL: ${loss_pnl:+.2f}\n'
        f'\n'
        f'Account balance: ${balance:.2f}'
    )
    await send_message(text)


# ── Telegram command listener for /stop and /start ─────────────────────

import subprocess

# Telegram getUpdates offset — tracks which messages we've already seen
_telegram_update_offset = 0

# Service name for systemctl
SERVICE_NAME = 'polybot-directional'

# Only accept commands from the configured chat
ALLOWED_CHAT_ID = CHAT_ID  # already loaded from env


async def _handle_command(text: str) -> str:
    """Handle a /command. Returns reply text."""
    cmd = text.strip().lower()

    if cmd == '/stop':
        # Spawn a detached systemctl command so it survives this process being killed
        try:
            subprocess.Popen(
                ['systemctl', 'stop', SERVICE_NAME],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            return f'STOP command received. Stopping {SERVICE_NAME}...'
        except Exception as e:
            return f'STOP failed: {e}'

    elif cmd == '/start':
        try:
            subprocess.Popen(
                ['systemctl', 'start', SERVICE_NAME],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            return f'START command received. Starting {SERVICE_NAME}...'
        except Exception as e:
            return f'START failed: {e}'

    elif cmd == '/status':
        try:
            result = subprocess.run(
                ['systemctl', 'is-active', SERVICE_NAME],
                capture_output=True, text=True, timeout=5,
            )
            status = result.stdout.strip()
            return f'Service status: {status}'
        except Exception as e:
            return f'Status failed: {e}'

    elif cmd == '/help':
        return ('Commands:\n'
                '/stop — stop the bot\n'
                '/start — start the bot\n'
                '/status — check service status\n'
                '/help — this message')

    return ''  # ignore non-command text


async def _poll_telegram_commands() -> None:
    """Background task: long-poll Telegram getUpdates and dispatch commands."""
    global _telegram_update_offset

    if not BOT_TOKEN or not CHAT_ID:
        print('[Telegram] Command listener disabled (no token/chat configured)')
        return

    print('[Telegram] Command listener starting — clearing pending updates...')

    # Drain any pending/stale updates on startup so we don't re-process old /stop commands
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f'{API_BASE}/getUpdates',
                params={'offset': -1, 'timeout': 0},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data = await r.json()
            if data.get('ok') and data.get('result'):
                last_id = data['result'][-1]['update_id']
                _telegram_update_offset = last_id + 1
                print(f'[Telegram] Skipped past update_id {last_id} (cleared backlog)')
        except Exception as e:
            print(f'[Telegram] Could not clear backlog: {e}')

    print('[Telegram] Command listener active')
    await send_message('Command listener active. Send /help for commands.')

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f'{API_BASE}/getUpdates',
                    params={
                        'offset': _telegram_update_offset,
                        'timeout': 30,  # long-poll
                        'allowed_updates': '["message"]',
                    },
                    timeout=aiohttp.ClientTimeout(total=40),
                ) as r:
                    data = await r.json()
            except asyncio.TimeoutError:
                continue  # normal long-poll timeout
            except Exception as e:
                print(f'[Telegram] getUpdates error: {e}')
                await asyncio.sleep(5)
                continue

            if not data.get('ok'):
                await asyncio.sleep(5)
                continue

            updates = data.get('result', [])
            for u in updates:
                _telegram_update_offset = u['update_id'] + 1
                msg = u.get('message') or {}
                chat = msg.get('chat', {})
                text = msg.get('text', '')

                # Restrict to our chat only
                if str(chat.get('id')) != str(ALLOWED_CHAT_ID):
                    continue
                if not text.startswith('/'):
                    continue

                reply = await _handle_command(text)
                if reply:
                    await send_message(reply)


async def start_command_listener() -> None:
    """Public entry point. Call from bot.py main()."""
    asyncio.create_task(_poll_telegram_commands())
