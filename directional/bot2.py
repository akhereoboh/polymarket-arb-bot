"""
bot2.py — multi-crypto directional bot.

Runs alongside bot.py. bot.py owns BTC live trading; bot2.py handles all
OTHER crypto up/down markets (ETH/SOL/BNB/DOGE/XRP), DRY-RUN by default
via SAFE_MODE=true.

Imports signal logic and execution helpers from bot.py rather than duplicating.
"""

import asyncio
import os
import sys
import time
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

# Load .env from this script's directory (same as bot.py)
_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, '.env'))

# Make local directory importable so we get bot and crypto_assets
sys.path.insert(0, _HERE)

# Import signal logic and helpers from the live bot
import bot as bot1
from bot import (
    check_signal,
    # get_opening_chainlink_price,
    RPC,
)

# Telegram for our dry-run alerts
from telegram_alerts import send_message

# Our multi-asset utilities
import crypto_assets as ca


# ─── config ──────────────────────────────────────────────────────────────
SAFE_MODE      = os.getenv('BOT2_SAFE_MODE', 'true').lower() == 'true'
ASSETS_TO_SCAN = [
    a.strip().lower()
    for a in os.getenv('BOT2_ASSETS', 'eth,sol,bnb,doge,xrp,hype').split(',')
    if a.strip()
]
ASSETS_TO_SCAN = [a for a in ASSETS_TO_SCAN
                  if a in ca.SUPPORTED_ASSETS and ca.asset_has_chainlink(a)]

TRADE_AMOUNT   = float(os.getenv('BOT2_TRADE_AMOUNT',
                                 os.getenv('TRADE_AMOUNT', '4')))
POLL_INTERVAL  = int(os.getenv('BOT2_POLL_INTERVAL', '5'))


# ─── state ───────────────────────────────────────────────────────────────
_bn_history_by_asset: dict[str, list[tuple[float, float]]] = {a: [] for a in ASSETS_TO_SCAN}
_cl_history_by_asset: dict[str, list[tuple[float, float]]] = {a: [] for a in ASSETS_TO_SCAN}
_opening_prices: dict[str, float] = {}
_traded: set[str] = set()


def _log(msg: str) -> None:
    print(f'[bot2 {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


# ─── per-asset Binance price feeders ─────────────────────────────────────

async def _binance_feeder(asset: str):
    """Mirror of bot.py's price_monitor(), but per-asset."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                price = await ca.get_binance_price(session, asset)
                now = time.time()
                _bn_history_by_asset[asset].append((now, price))
                cutoff = now - 1200
                h = _bn_history_by_asset[asset]
                while h and h[0][0] < cutoff:
                    h.pop(0)
            except Exception as e:
                _log(f'[{asset}] Binance feed error: {e}')
            await asyncio.sleep(5)


# ─── main scanner ────────────────────────────────────────────────────────

async def market_scanner():
    _log(f'Bot2 starting. SAFE_MODE={SAFE_MODE} | Assets={ASSETS_TO_SCAN}')
    _log(f'Trade amount: ${TRADE_AMOUNT} (effective only when SAFE_MODE=false)')
    await send_message(
        f'🤖 bot2 started\n'
        f'Assets: {", ".join(ASSETS_TO_SCAN)}\n'
        f'Mode: {"DRY-RUN (safe)" if SAFE_MODE else "LIVE"}\n'
        f'Trade amount: ${TRADE_AMOUNT}'
    )

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await _scan_once(session)
            except Exception as e:
                _log(f'Scan error: {e}')
                import traceback
                traceback.print_exc()
            await asyncio.sleep(POLL_INTERVAL)


async def _scan_once(session: aiohttp.ClientSession):
    now_ts = time.time()

    # Update Chainlink history for each asset
    for asset in ASSETS_TO_SCAN:
        try:
            cl_price, _ = await ca.get_chainlink_price(session, asset, RPC)
        except Exception as e:
            _log(f'[{asset}] Chainlink fetch failed: {e}')
            continue
        _cl_history_by_asset[asset].append((now_ts, cl_price))
        cutoff = now_ts - 1200
        h = _cl_history_by_asset[asset]
        while h and h[0][0] < cutoff:
            h.pop(0)

    # Get active markets across all assets
    markets = await ca.get_active_crypto_markets(session, set(ASSETS_TO_SCAN))
    if not markets:
        return

    for market in markets:
        await _process_market(session, market, now_ts)


async def _process_market(session, market, now_ts):
    asset       = market['asset']
    cid         = market['condition_id']
    seconds_left = market['seconds_left']
    end_time    = market['end_time']
    tf          = market['timeframe']

    # Need recent CL price for this asset
    cl_hist = _cl_history_by_asset.get(asset, [])
    if not cl_hist:
        return
    cl_price = cl_hist[-1][1]

    # Store opening CL when first seen
    if cid not in _opening_prices:
        opening = await ca.get_opening_chainlink_price_for_asset(
            session, asset, end_time, tf, RPC
        )
        _opening_prices[cid] = opening
        _log(f'New: [{asset.upper()} {tf}] {market["title"]} | {seconds_left:.0f}s | Opening CL: ${opening:,.4f}')

    if cid in _traded:
        return

    opening_price = _opening_prices[cid]

    # Entry window matches bot.py: 60s for 5m, 120s for 15m
    normal_window = 120 if tf == '15m' else 60
    if seconds_left > normal_window:
        return

    # Need a Binance opening price matching the timeframe lookback
    lookback   = 900 if tf == '15m' else 300
    target_ts  = now_ts - lookback
    bn_hist    = _bn_history_by_asset.get(asset, [])
    if not bn_hist:
        return

    # BN price closest to (now - lookback) — same approach as bot.py
    bn_opening = min(bn_hist, key=lambda x: abs(x[0] - target_ts))[1]
    bn_now     = bn_hist[-1][1]

    # Call bot.py's check_signal with the full 9-argument signature.
    # btc_history/cl_history params take this asset's histories (the
    # variable names in bot.py are BTC-specific but the logic is generic).
    result = check_signal(
        cl_price,                       # cl_price
        opening_price,                  # opening_price
        bn_now,                         # binance_now
        bn_opening,                     # binance_opening
        market['up_price'],             # up_price
        market['down_price'],           # down_price
        _bn_history_by_asset[asset],    # btc_history (asset's BN history)
        _cl_history_by_asset[asset],    # cl_history (asset's CL history)
        seconds_left,                   # seconds_left
    )
    if len(result) == 2:
        direction, confidence = result
        momentum_info = None
    else:
        direction, confidence, momentum_info = result

    if direction == 'none':
        return

    # Got a signal — fire (dry-run by default)
    cl_pct = (cl_price - opening_price) / opening_price * 100
    bn_pct = (bn_now - bn_opening) / bn_opening * 100
    crowd_price = market['up_price'] if direction == 'up' else market['down_price']

    msg = (
        f'🔵 [bot2 {"DRY" if SAFE_MODE else "LIVE"}] [{asset.upper()} {tf}] signal\n'
        f'{market["title"]}\n'
        f'Direction: {direction.upper()}\n'
        f'CL: ${cl_price:,.4f} (open ${opening_price:,.4f}) {cl_pct:+.4f}%\n'
        f'BN: ${bn_now:,.4f} (open ${bn_opening:,.4f}) {bn_pct:+.4f}%\n'
        f'Confidence: {confidence:.4f}%\n'
        f'Crowd: {crowd_price}'
    )
    _log(msg.replace('\n', ' | '))
    await send_message(msg)

    _traded.add(cid)

    if not SAFE_MODE:
        await send_message(f'⚠️ bot2 LIVE mode but order execution not wired yet — '
                           f'no order placed for {asset.upper()} {tf}')


async def main():
    try:
        feeders = [asyncio.create_task(_binance_feeder(a)) for a in ASSETS_TO_SCAN]
        scanner = asyncio.create_task(market_scanner())
        await asyncio.gather(scanner, *feeders)
    except KeyboardInterrupt:
        _log('Stopped by keyboard interrupt')
    except Exception as e:
        _log(f'Crashed: {e}')
        try:
            await send_message(f'⚠️ bot2 crashed: {e}')
        except Exception:
            pass
        raise


if __name__ == '__main__':
    asyncio.run(main())