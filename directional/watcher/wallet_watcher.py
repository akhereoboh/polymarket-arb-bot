"""
Polymarket Wallet Watcher.

Polls data-api.polymarket.com/trades for each tracked wallet on a schedule.
For each new trade (deduped via Supabase unique constraint on trade_id):
  1. Records it in Supabase `tracked_trades` table
  2. Sends a Telegram alert with trader, market, side, size, price
  3. If COPY_TRADE_ENABLED and the trade matches the conservative rules,
     fires a copy trade via the existing ClobClient

Copy-trade conservative rules:
  - Only BUY side (we ignore SELL — i.e., position exits)
  - Source trade size in USDC >= COPY_MIN_SIZE_USDC (default $200)
  - Source entry price <= COPY_MAX_PRICE (default 0.85 — same asymmetric cap)
  - Market resolves > COPY_MIN_RESOLUTION_HOURS away (default 1h — avoid 5m/15m)
  - Only wallets in COPY_FROM_WALLETS list (subset of TRACKED_WALLETS)
  - Hard cap per copy trade: COPY_TRADE_AMOUNT (default $5)

Run as: systemd service polybot-watcher.service
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

# Load .env from this script's directory
_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, '.env'))

# Reuse Telegram from directional bot (one level up)
sys.path.insert(0, os.path.dirname(_HERE))
from telegram_alerts import send_message  # noqa: E402

# ─── config ─────────────────────────────────────────────────────────────

DATA_API = 'https://data-api.polymarket.com/trades'
GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events'

# Wallets to watch — proxy addresses (from Polymarket profile URL after @)
# Each entry: (label, proxy_wallet_address)
TRACKED_WALLETS: list[tuple[str, str]] = [
    ('chiiwawinha', '0x1bcb16ab3595079a8a8f0d35a475a3b71bc0b05a'),
    ('badb9af', '0xbadb9af986ee66437bd39e6cd3d3036cbbdc31a7'),
    ("Weatherstappen", "0xb9012e0d9b60d3920286309328b935cdfa609fc4"),
    ("0X3572", "0x886602e27ed4f86b2b6644fdef09074ed4ee7fc8"),
    ("helldfkdsf for esports", "0x62c30b6624cb51121cb7059b3b8853283bb6bfc9")
]

# Subset of TRACKED_WALLETS we copy-trade from (when COPY_TRADE_ENABLED=true)
# Use the same labels as above
COPY_FROM_WALLETS: list[str] = [
    '0xbadb9af',
    "chiiwawinha",
    # "Weatherstappen",
    # "0X3572"
]

# Polling
POLL_INTERVAL_SEC = int(os.getenv('WATCHER_POLL_INTERVAL_SEC', '20'))
TRADES_PER_FETCH = int(os.getenv('WATCHER_TRADES_PER_FETCH', '50'))

# Copy-trade rules (all togglable via .env)
COPY_TRADE_ENABLED = os.getenv('COPY_TRADE_ENABLED', 'false').lower() == 'true'
COPY_MIN_SIZE_USDC = float(os.getenv('COPY_MIN_SIZE_USDC', '200'))
COPY_MAX_PRICE = float(os.getenv('COPY_MAX_PRICE', '0.85'))
COPY_MIN_RESOLUTION_HOURS = float(os.getenv('COPY_MIN_RESOLUTION_HOURS', '1'))
COPY_TRADE_AMOUNT = float(os.getenv('COPY_TRADE_AMOUNT', '5'))

# Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')

# Polymarket client (only loaded if copy-trade enabled)
_clob_client = None

# Track last poll time per wallet to avoid re-fetching too aggressively
_last_poll_per_wallet: dict[str, int] = {}


def _log(msg: str) -> None:
    """Print with timestamp, flush immediately."""
    print(f'[{datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


# ─── Supabase helpers ────────────────────────────────────────────────────

async def _supabase_insert(session: aiohttp.ClientSession, row: dict) -> tuple[bool, bool]:
    """
    Insert a row into tracked_trades. Returns (success, is_new).
    is_new=False means the row was a duplicate (already had this trade_id).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        _log('[Supabase] No URL/key configured — skipping insert')
        return False, False

    url = f'{SUPABASE_URL}/rest/v1/tracked_trades'
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal,resolution=ignore-duplicates',
    }
    try:
        async with session.post(url, json=row, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 201:
                return True, True
            if r.status == 200:
                # Returned but ignored as duplicate
                return True, False
            text = await r.text()
            if 'duplicate' in text.lower() or 'unique' in text.lower():
                return True, False
            _log(f'[Supabase] Insert HTTP {r.status}: {text[:200]}')
            return False, False
    except Exception as e:
        _log(f'[Supabase] Insert error: {e}')
        return False, False


async def _supabase_update_copy_result(
    session: aiohttp.ClientSession,
    trade_id: str,
    copy_traded: bool,
    copy_trade_result: dict,
) -> None:
    """Mark a tracked trade row with copy-trade outcome."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    url = f'{SUPABASE_URL}/rest/v1/tracked_trades?trade_id=eq.{trade_id}'
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }
    try:
        await session.patch(url, json={
            'copy_traded': copy_traded,
            'copy_trade_result': copy_trade_result,
        }, headers=headers, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        _log(f'[Supabase] Update error: {e}')


# ─── Polymarket data fetch ───────────────────────────────────────────────

async def fetch_recent_trades(
    session: aiohttp.ClientSession,
    wallet: str,
    limit: int = 50,
) -> list[dict]:
    """Fetch most recent trades for a wallet, newest first from data-api."""
    try:
        async with session.get(
            DATA_API,
            params={'user': wallet, 'limit': limit},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                _log(f'[data-api] HTTP {r.status} for {wallet[:10]}')
                return []
            data = await r.json()
            if not isinstance(data, list):
                return []
            return data
    except Exception as e:
        _log(f'[data-api] error for {wallet[:10]}: {e}')
        return []


async def fetch_market_info(session: aiohttp.ClientSession, slug: str) -> dict | None:
    """Get event info from gamma to check resolution time."""
    try:
        async with session.get(
            GAMMA_EVENTS,
            params={'slug': slug},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if not data:
                return None
            event = data[0]
            return {
                'end_date': event.get('endDate'),
                'closed': event.get('closed', False),
            }
    except Exception:
        return None


# ─── Telegram alert formatting ───────────────────────────────────────────

def _format_alert(label: str, trade: dict, is_copy_eligible: bool) -> str:
    side = trade['side']
    outcome = trade.get('outcome', '?')
    size = float(trade.get('size', 0))
    price = float(trade.get('price', 0))
    cost = size * price
    title = trade.get('title', 'Unknown market')[:60]
    slug = trade.get('slug', '')

    emoji = '🟢' if side == 'BUY' else '🔴'
    eligibility = '\n💎 Copy-eligible' if is_copy_eligible else ''

    return (
        f'{emoji} {label} traded\n'
        f'{side} {outcome} @ ${price:.3f}\n'
        f'Size: {size:.0f} shares (${cost:.2f})\n'
        f'Market: {title}\n'
        f'https://polymarket.com/event/{slug}'
        f'{eligibility}'
    )


# ─── Copy-trade eligibility ──────────────────────────────────────────────

def _check_copy_eligibility_basic(label: str, trade: dict) -> tuple[bool, str]:
    """Quick eligibility checks that don't need gamma lookup."""
    if not COPY_TRADE_ENABLED:
        return False, 'copy_trade_disabled'
    if label not in COPY_FROM_WALLETS:
        return False, 'wallet_not_in_copy_list'
    if trade.get('side') != 'BUY':
        return False, 'not_a_buy'

    size = float(trade.get('size', 0))
    price = float(trade.get('price', 0))
    cost = size * price
    if cost < COPY_MIN_SIZE_USDC:
        return False, f'size_too_small (${cost:.2f} < ${COPY_MIN_SIZE_USDC})'
    if price > COPY_MAX_PRICE:
        return False, f'price_too_high ({price:.3f} > {COPY_MAX_PRICE})'

    return True, 'eligible'


async def _check_copy_eligibility_resolution(
    session: aiohttp.ClientSession,
    trade: dict,
) -> tuple[bool, str]:
    """Verify market resolves far enough out (avoid 5m/15m markets)."""
    slug = trade.get('slug', '')
    if not slug:
        return False, 'no_slug'
    info = await fetch_market_info(session, slug)
    if not info:
        return False, 'gamma_lookup_failed'
    if info['closed']:
        return False, 'market_already_closed'

    end_date_str = info.get('end_date')
    if not end_date_str:
        return False, 'no_end_date'
    try:
        end_dt = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
    except Exception:
        return False, 'bad_end_date_format'

    hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
    if hours_left < COPY_MIN_RESOLUTION_HOURS:
        return False, f'resolves_too_soon ({hours_left:.2f}h < {COPY_MIN_RESOLUTION_HOURS}h)'

    return True, 'eligible'


# ─── Copy-trade execution ───────────────────────────────────────────────

def _get_clob_client():
    """Lazy-load the CLOB client. Returns None on failure."""
    global _clob_client
    if _clob_client is not None:
        return _clob_client
    try:
        sys.path.insert(0, '/root/my-clob-client')
        from py_clob_client_v2 import ClobClient, SignatureTypeV2, ApiCreds

        creds = ApiCreds(
            api_key=os.getenv('POLYMARKET_API_KEY'),
            api_secret=os.getenv('POLYMARKET_API_SECRET'),
            api_passphrase=os.getenv('POLYMARKET_API_PASSPHRASE'),
        )
        _clob_client = ClobClient(
            host='https://clob.polymarket.com',
            chain_id=137,
            key=os.getenv('POLYMARKET_PRIVATE_KEY'),
            creds=creds,
            signature_type=SignatureTypeV2.POLY_1271,
            funder=os.getenv('POLYMARKET_FUNDER'),
        )
        return _clob_client
    except Exception as e:
        _log(f'[Copy] Failed to init CLOB client: {e}')
        return None


async def _execute_copy_trade(trade: dict) -> dict:
    """
    Place a copy-trade based on the source trade.
    Buys the SAME side (outcome token) the source bought.
    Hard-caps size at COPY_TRADE_AMOUNT USDC.
    """
    client = _get_clob_client()
    if not client:
        return {'status': 'error', 'reason': 'no_client'}

    try:
        from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions
        from py_clob_client_v2.order_builder.constants import BUY
    except Exception as e:
        return {'status': 'error', 'reason': f'import_failed: {e}'}

    asset_id = trade.get('asset')
    if not asset_id:
        return {'status': 'error', 'reason': 'no_asset_id'}

    source_price = float(trade['price'])
    # Pay up to 5 cents above source price, capped at COPY_MAX_PRICE
    our_price = round(min(source_price + 0.05, COPY_MAX_PRICE), 2)
    shares = int(COPY_TRADE_AMOUNT / our_price)
    if shares < 1:
        return {'status': 'skipped', 'reason': 'sub_one_share'}

    try:
        order_args = OrderArgs(
            price=our_price,
            size=shares,
            side=BUY,
            token_id=asset_id,
        )
        signed = client.create_order(
            order_args,
            options=PartialCreateOrderOptions(neg_risk=False),
        )
        result = client.post_order(signed, orderType=OrderType.FAK)
        return {
            'status': 'fired',
            'our_price': our_price,
            'shares': shares,
            'source_price': source_price,
            'result': result,
        }
    except Exception as e:
        return {'status': 'error', 'reason': f'order_failed: {e}'}


# ─── Main loop ───────────────────────────────────────────────────────────

async def watch_loop():
    _log(f'Watcher starting. Tracking {len(TRACKED_WALLETS)} wallet(s).')
    _log(f'Copy-trade enabled: {COPY_TRADE_ENABLED}')
    if COPY_TRADE_ENABLED:
        _log(f'  Copy-from wallets: {COPY_FROM_WALLETS}')
        _log(f'  Min source size: ${COPY_MIN_SIZE_USDC}')
        _log(f'  Max source price: {COPY_MAX_PRICE}')
        _log(f'  Min resolution: {COPY_MIN_RESOLUTION_HOURS}h')
        _log(f'  Copy size: ${COPY_TRADE_AMOUNT}')

    await send_message(
        f'Watcher started. Tracking {len(TRACKED_WALLETS)} wallet(s). '
        f'Copy-trade: {"ON" if COPY_TRADE_ENABLED else "OFF"}.'
    )

    # Initialize per-wallet last-seen timestamp to now to avoid alert flood on startup
    # (we don't want to alert on every historical trade the first time we run)
    startup_ts = int(time.time())

    async with aiohttp.ClientSession() as session:
        # On first poll, seed each wallet's last-seen so we only alert on NEW trades
        for label, addr in TRACKED_WALLETS:
            recent = await fetch_recent_trades(session, addr, limit=5)
            if recent:
                # Newest trade is index 0 in the response
                _last_poll_per_wallet[addr] = int(recent[0]['timestamp'])
                _log(f'Seeded {label}: skipping trades older than '
                     f'{datetime.fromtimestamp(_last_poll_per_wallet[addr], tz=timezone.utc).strftime("%H:%M:%S UTC")}')
            else:
                _last_poll_per_wallet[addr] = startup_ts
                _log(f'Seeded {label}: no recent trades, baseline = now')

        # Polling loop
        poll_count = 0
        while True:
            try:
                await _poll_all_wallets(session)
                poll_count += 1
                # Heartbeat every 30 polls (~10 min at 20s interval)
                if poll_count % 30 == 0:
                    _log(f'Heartbeat: {poll_count} polls completed, watching {len(TRACKED_WALLETS)} wallet(s)')
            except Exception as e:
                _log(f'Poll cycle error: {e}')
                import traceback
                traceback.print_exc()
            await asyncio.sleep(POLL_INTERVAL_SEC)


async def _poll_all_wallets(session: aiohttp.ClientSession):
    for label, addr in TRACKED_WALLETS:
        try:
            await _poll_wallet(session, label, addr)
        except Exception as e:
            _log(f'Error polling {label}: {e}')


async def _poll_wallet(session: aiohttp.ClientSession, label: str, addr: str):
    last_seen = _last_poll_per_wallet.get(addr, 0)
    trades = await fetch_recent_trades(session, addr, limit=TRADES_PER_FETCH)
    if not trades:
        return

    # Filter to genuinely new trades (newer than what we've seen)
    new_trades = [t for t in trades if int(t['timestamp']) > last_seen]
    if not new_trades:
        # Useful debug — confirms we ARE polling but found nothing new
        newest = max(int(t['timestamp']) for t in trades)
        from datetime import datetime as _dt
        newest_dt = _dt.fromtimestamp(newest, tz=timezone.utc).strftime('%H:%M:%S')
        # Only log occasionally to avoid flooding (every 5th call ~ every 100s)
        if hash((addr, last_seen)) % 5 == 0:
            _log(f'Poll {label}: 0 new (latest in api: {newest_dt}, baseline: {last_seen})')
        return
    # Update baseline so we don’t re-alert on the same trade
    _last_poll_per_wallet[addr] = max(int(t['timestamp']) for t in new_trades)

    # 🔔 Fire alerts for each new trade
    for trade in reversed(new_trades):  # oldest first
        await _handle_new_trade(session, label, addr, trade)


async def _handle_new_trade(
    session: aiohttp.ClientSession,
    label: str,
    addr: str,
    trade: dict,
):
    trade_id = trade.get('id') or f"{trade.get('transactionHash', '')}-{trade.get('timestamp', '')}"

    # Build row for Supabase
    row = {
        'trade_id': trade_id,
        'wallet': addr.lower(),
        'wallet_label': label,
        'market_title': trade.get('title', '')[:500],
        'slug': trade.get('slug', ''),
        'condition_id': trade.get('conditionId', ''),
        'outcome': trade.get('outcome', ''),
        'side': trade.get('side', ''),
        'size': float(trade.get('size', 0)),
        'price': float(trade.get('price', 0)),
        'cost': float(trade.get('size', 0)) * float(trade.get('price', 0)),
        'transaction_hash': trade.get('transactionHash', ''),
        'trade_timestamp': datetime.fromtimestamp(
            int(trade['timestamp']), tz=timezone.utc
        ).isoformat(),
        'copy_traded': False,
        'copy_trade_result': None,
    }


    


    success, is_new = await _supabase_insert(session, row)
    if not is_new:
        # Already saw this trade in a previous poll (dedup at DB level)
        return

    _log(f'NEW: {label} {trade.get("side")} {trade.get("outcome")} '
         f'@ {trade.get("price")} size={trade.get("size")} '
         f'market="{trade.get("title", "")[:50]}"')

    # Check copy eligibility (basic, fast)
    eligible_basic, reason_basic = _check_copy_eligibility_basic(label, trade)

    # If basic checks pass and we have copy-trade enabled, check resolution time
    eligible_full = False
    full_reason = reason_basic
    if eligible_basic:
        eligible_full, full_reason = await _check_copy_eligibility_resolution(session, trade)

    # Send Telegram alert (always, regardless of copy outcome)
    msg = _format_alert(label, trade, eligible_full)
    await send_message(msg)

    # If fully eligible, fire copy trade
    if eligible_full:
        _log(f'COPY: firing copy trade for {label} trade {trade_id[:12]}...')
        result = await _execute_copy_trade(trade)
        await _supabase_update_copy_result(
            session, trade_id,
            copy_traded=result.get('status') == 'fired',
            copy_trade_result=result,
        )

        if result.get('status') == 'fired':
            await send_message(
                f'✅ Copy-traded {label}\n'
                f'Our entry: ${result["our_price"]:.3f} × {result["shares"]} shares\n'
                f'Source: ${result["source_price"]:.3f}'
            )
        else:
            await send_message(
                f'❌ Copy-trade failed for {label}: '
                f'{result.get("reason", "unknown")}'
            )


# ─── Entry point ────────────────────────────────────────────────────────

async def main():
    try:
        from watcher_commands import start_watcher_command_listener
        await start_watcher_command_listener()
        await watch_loop()
    except KeyboardInterrupt:
        _log('Watcher stopped by keyboard interrupt')
    except Exception as e:
        _log(f'Watcher crashed: {e}')
        await send_message(f'⚠️ Watcher crashed: {e}')
        raise


if __name__ == '__main__':
    asyncio.run(main())
