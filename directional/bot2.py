"""
bot2.py — multi-crypto directional bot with live execution.

Runs alongside bot.py. bot.py owns BTC; bot2.py handles ETH/SOL/BNB/DOGE/XRP.

Config (.env):
    BOT2_SAFE_MODE       'true' = dry-run only, no orders placed. Default: true
    BOT2_ASSETS          Comma-separated assets. Default: eth,sol,bnb,doge,xrp
    BOT2_TRADE_AMOUNT    USD per trade. Default: 1
    BOT2_HARD_FILL_CAP   Max fill price. Default: 0.995
    BOT2_POLL_INTERVAL   Seconds between scans. Default: 5
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

# Make local directory importable
sys.path.insert(0, _HERE)
sys.path.insert(0, '/root/my-clob-client')

# Import signal logic + execution helpers from the live bot
import bot as bot1
from bot import (
    check_signal,
    get_order_book,
    fak_filled,
    log_execution_event,
    get_client,
    RPC,
)
from gtc_fallback import place_gtc_fallback

from py_clob_client_v2 import (
    OrderArgs, OrderType, PartialCreateOrderOptions, Side
)

# Telegram for alerts
from telegram_alerts import send_message

# Our multi-asset utilities
import crypto_assets as ca
import csv
from pathlib import Path

from pyth_history import (
    start_pyth_feeder,
    get_pyth_price_at_time,
    get_latest_pyth_price,
)

from signals_db import insert_signal as db_insert_signal, build_signal_row


BOT2_LOG_FILE = os.path.join(_HERE, 'bot2_signals_log.csv')

# ─── config ──────────────────────────────────────────────────────────────
SAFE_MODE        = os.getenv('BOT2_SAFE_MODE', 'true').lower() == 'true'
ASSETS_TO_SCAN   = [
    a.strip().lower()
    for a in os.getenv('BOT2_ASSETS', 'eth,sol,bnb,doge,xrp,hype').split(',')
    if a.strip()
]
ASSETS_TO_SCAN   = [a for a in ASSETS_TO_SCAN
                    if a in ca.SUPPORTED_ASSETS and ca.asset_has_chainlink(a)]

TRADE_AMOUNT     = float(os.getenv('BOT2_TRADE_AMOUNT', '1'))
HARD_FILL_CAP    = float(os.getenv('BOT2_HARD_FILL_CAP', '0.995'))
POLL_INTERVAL    = int(os.getenv('BOT2_POLL_INTERVAL', '5'))
FAK_BUFFER       = 0.05  # how far above raw_price we'll FAK


# ─── state ───────────────────────────────────────────────────────────────
_bn_history_by_asset: dict[str, list[tuple[float, float]]] = {a: [] for a in ASSETS_TO_SCAN}
_cl_history_by_asset: dict[str, list[tuple[float, float]]] = {a: [] for a in ASSETS_TO_SCAN}
_opening_prices: dict[str, float] = {}
_traded: set[str] = set()


def _log(msg: str) -> None:
    print(f'[bot2 {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


async def log_bot2_signal(
    market: dict,
    direction: str,
    confidence: float,
    cl_price: float,
    opening_cl: float,
    bn_price: float,
    opening_bn: float,
    shares: int,
    max_fill_price: float,
    asks_at_cap: int,
    safe_mode: bool,
    trade_result: dict | None = None,
):
    """Append a complete signal row to bot2_signals_log.csv."""
    file_exists = Path(BOT2_LOG_FILE).exists()
    fieldnames = [
        'timestamp', 'asset', 'timeframe', 'market_title', 'slug', 'condition_id',
        'direction', 'crowd_price', 'shares', 'max_fill_price', 'cost_estimate',
        'cl_price', 'opening_cl', 'cl_pct',
        'bn_price', 'opening_bn', 'bn_pct',
        'confidence', 'asks_at_cap',
        'safe_mode', 'trade_status',
        'outcome', 'fill_price', 'fill_size', 'fill_pnl', 'fill_tx',
    ]

    cl_pct = (cl_price - opening_cl) / opening_cl * 100 if opening_cl else 0
    bn_pct = (bn_price - opening_bn) / opening_bn * 100 if opening_bn else 0
    crowd_price = market['up_price'] if direction == 'up' else market['down_price']

    trade_status = 'PENDING'
    if trade_result:
        if trade_result.get('status') == 'dry_run':
            trade_status = 'DRY_RUN'
        elif trade_result.get('status') == 'error':
            trade_status = f"ERROR: {trade_result.get('error', '')[:50]}"
        elif trade_result.get('orderID'):
            trade_status = 'PLACED'
        else:
            trade_status = str(trade_result)[:50]

    with open(BOT2_LOG_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'timestamp':       datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'asset':           market['asset'].upper(),
            'timeframe':       market.get('timeframe', ''),
            'market_title':    market.get('title', ''),
            'slug':            market.get('slug', ''),
            'condition_id':    market.get('condition_id', ''),
            'direction':       direction.upper(),
            'crowd_price':     crowd_price,
            'shares':          shares,
            'max_fill_price':  max_fill_price,
            'cost_estimate':   round(shares * max_fill_price, 4),
            'cl_price':        round(cl_price, 6),
            'opening_cl':      round(opening_cl, 6),
            'cl_pct':          round(cl_pct, 4),
            'bn_price':        round(bn_price, 6),
            'opening_bn':      round(opening_bn, 6),
            'bn_pct':          round(bn_pct, 4),
            'confidence':      round(confidence, 4),
            'asks_at_cap':     asks_at_cap,
            'safe_mode':       safe_mode,
            'trade_status':    trade_status,
            # Outcome columns — populated by analyzer script later
            'outcome':         '',
            'fill_price':      '',
            'fill_size':       '',
            'fill_pnl':        '',
            'fill_tx':         '',
        })

    # Also write to Supabase
    try:
        import aiohttp
        timeframe = market.get('timeframe', '?')
        asset = market.get('asset', '?')
        crowd_price = market['up_price'] if direction == 'up' else market['down_price']
        
        # Determine trade_status text
        if trade_result and trade_result.get('status') == 'dry_run':
            trade_status_text = 'DRY_RUN'
        elif trade_result and trade_result.get('status') == 'error':
            trade_status_text = f'ERROR: {str(trade_result.get("error", ""))[:50]}'
        elif trade_result and trade_result.get('orderID'):
            trade_status_text = 'PLACED'
        else:
            trade_status_text = 'UNKNOWN'
        
        row = build_signal_row(
            bot='bot2',
            asset=asset,
            timeframe=timeframe,
            market=market,
            direction=direction,
            cl_price=cl_price,
            opening_cl=opening_cl,
            bn_price=bn_price,
            opening_bn=opening_bn,
            confidence=confidence,
            intended_shares=shares,
            max_fill_price=max_fill_price,
            crowd_price=crowd_price,
            asks_at_cap=asks_at_cap,
            safe_mode=safe_mode,
            trade_status=trade_status_text,
        )
        async with aiohttp.ClientSession() as session:
            await db_insert_signal(session, row)
    except Exception as e:
        _log(f'Supabase signal write failed: {e}')




# ─── per-asset Binance feeders ───────────────────────────────────────────

async def _binance_feeder(asset: str):
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


# ─── order placement (ported from bot.py's place_trade) ──────────────────

# Polymarket minimum order value — orders below this get rejected by CLOB
POLYMARKET_MIN_ORDER_USDC = 1.00


def _calc_position_size(crowd_price: float) -> int:
    """
    Shares to buy: at least enough to clear Polymarket's $1 min order rule,
    and at least enough to spend our TRADE_AMOUNT budget.
    """
    # +1 ensures we're strictly above min (handles rounding when 1.0/price is integer)
    min_shares_for_polymarket = int(POLYMARKET_MIN_ORDER_USDC / crowd_price) + 1
    shares_for_budget = int(TRADE_AMOUNT / crowd_price)
    return max(min_shares_for_polymarket, shares_for_budget)


async def _place_trade(market: dict, direction: str, shares: int, confidence: float):
    """
    Place a real or dry-run trade. Mirrors bot.py's place_trade pattern:
      1. Check order book for asks at acceptable price
      2. If no asks → GTC fallback
      3. If asks → FAK first, fall through to GTC if it doesn't fill
    """
    asset = market['asset']
    token_id = market['up_token'] if direction == 'up' else market['down_token']
    raw_price = market['up_price'] if direction == 'up' else market['down_price']

    # Check order book
    book = await get_order_book(token_id)
    asks = sorted(book['asks'], key=lambda x: float(x['price']))

    max_price = round(min(raw_price + FAK_BUFFER, HARD_FILL_CAP), 2)
    available_asks = [a for a in asks if float(a['price']) <= max_price]

    ask_count = len(asks)
    side_str = 'UP' if direction == 'up' else 'DOWN'

    _log(f'[Trade] [{asset.upper()} {market["timeframe"]}] {market["title"][:50]}')
    _log(f'  Direction: {side_str} | RawPrice: {raw_price} | MaxFill: {max_price} | Shares: {shares} | Conf: {confidence:.4f}%')
    _log(f'  Cost: ~${shares * max_price:.2f} | DRY: {SAFE_MODE} | Asks at/below cap: {len(available_asks)}/{ask_count}')

    if SAFE_MODE:
        _log(f'  [DRY] Would place {side_str} — skipping execution')
        return {'status': 'dry_run'}

    # ── Branch 1: No asks at acceptable price → GTC fallback ───────────
    if not available_asks:
        _log(f'  → No sellers at/below {max_price} — GTC fallback')
        all_asks_top = float(asks[0]['price']) if asks else None
        log_execution_event(market, direction, 'fak_no_liquidity',
                            raw_price=raw_price,
                            attempted_price=max_price,
                            best_ask=all_asks_top,
                            ask_count=ask_count,
                            asks_below_cap=0,
                            extra=f'asks_count={ask_count}')
        try:
            return await place_gtc_fallback(
                client_factory=get_client,
                market=market,
                direction=direction,
                shares=shares,
                confidence=confidence,
                book=book,
                OrderArgs=OrderArgs,
                OrderType=OrderType,
                PartialCreateOrderOptions=PartialCreateOrderOptions,
                Side=Side,
            )
        except Exception as e:
            _log(f'  [GTC] error: {e}')
            return {'status': 'error', 'error': str(e)}

    # ── Branch 2: Asks available → try FAK first ───────────────────────
    best_ask = float(available_asks[0]['price'])
    price = round(min(best_ask + 0.01, HARD_FILL_CAP), 2)
    total_available = sum(float(a['size']) for a in available_asks)
    _log(f'  → Order book: best ask {best_ask} | avail shares: {total_available:.0f}')
    _log(f'  → FAK at {price}')

    result = None
    try:
        client = get_client()
        result = client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side=Side.BUY,
            ),
            options=PartialCreateOrderOptions(tick_size='0.01', neg_risk=False),
            order_type=OrderType.FAK,
        )
        _log(f'  [Order] FAK result: {result}')
    except Exception as e:
        _log(f'  [Order] FAK error: {e}')
        log_execution_event(market, direction, 'fak_error',
                            raw_price=raw_price,
                            attempted_price=price,
                            best_ask=best_ask,
                            extra=f'exception={str(e)[:200]}')

    # Check fill status if FAK ran without error
    if result is not None:
        filled, shares_filled = fak_filled(result)
        if filled:
            _log(f'  [Order] FAK filled {shares_filled} shares')
            log_execution_event(market, direction, 'fak_filled',
                                raw_price=raw_price,
                                attempted_price=price,
                                best_ask=best_ask,
                                extra=f'shares={shares_filled}')
            return result
        _log(f'  [Order] FAK no-fill — trying GTC fallback')
        log_execution_event(market, direction, 'fak_no_fill',
                            raw_price=raw_price,
                            attempted_price=price,
                            best_ask=best_ask,
                            extra=f'result={str(result)[:200]}')

    # GTC fallback (FAK errored OR didn't fill)
    try:
        return await place_gtc_fallback(
            client_factory=get_client,
            market=market,
            direction=direction,
            shares=shares,
            confidence=confidence,
            book=book,
            OrderArgs=OrderArgs,
            OrderType=OrderType,
            PartialCreateOrderOptions=PartialCreateOrderOptions,
            Side=Side,
            early_mode=False,
            log_callback=log_execution_event,
        )
    except Exception as e:
        _log(f'  [GTC] error: {e}')
        return {'status': 'error', 'error': str(e)}


# ─── main scanner ────────────────────────────────────────────────────────

async def market_scanner():
    _log(f'Bot2 starting. SAFE_MODE={SAFE_MODE} | Assets={ASSETS_TO_SCAN}')
    _log(f'TRADE_AMOUNT=${TRADE_AMOUNT} | HARD_FILL_CAP={HARD_FILL_CAP}')
    await send_message(
        f'🤖 bot2 started\n'
        f'Assets: {", ".join(ASSETS_TO_SCAN)}\n'
        f'Mode: {"DRY-RUN" if SAFE_MODE else "LIVE"}\n'
        f'Trade: ${TRADE_AMOUNT} per fire | Cap: {HARD_FILL_CAP}'
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

    markets = await ca.get_active_crypto_markets(session, set(ASSETS_TO_SCAN))
    if not markets:
        return

    for market in markets:
        await _process_market(session, market, now_ts)


async def _process_market(session, market, now_ts):
    asset = market['asset']
    cid = market['condition_id']
    seconds_left = market['seconds_left']
    end_time = market['end_time']
    tf = market['timeframe']

    cl_hist = _cl_history_by_asset.get(asset, [])
    if not cl_hist:
        return
    cl_price = cl_hist[-1][1]

    if cid not in _opening_prices:
        opening = await ca.get_opening_chainlink_price_for_asset(
            session, asset, end_time, tf, RPC
        )
        _opening_prices[cid] = opening
        _log(f'New: [{asset.upper()} {tf}] {market["title"]} | {seconds_left:.0f}s | Opening CL: ${opening:,.4f}')

    if cid in _traded:
        return

    opening_price = _opening_prices[cid]

    # Entry window matches bot.py
    normal_window = 120 if tf == '15m' else 60
    if seconds_left > normal_window:
        return

    # Need Binance opening price
    lookback = 900 if tf == '15m' else 300
    target_ts = now_ts - lookback
    bn_hist = _bn_history_by_asset.get(asset, [])
    if not bn_hist:
        return

    bn_opening = min(bn_hist, key=lambda x: abs(x[0] - target_ts))[1]
    bn_now = bn_hist[-1][1]

    # Signal check (handle bot.py's inconsistent return)
    # Pyth opening + now for THIS asset (matches CL/BN lookback)
    lookback = 900 if tf == '15m' else 300
    target_ts = now_ts - lookback
    pyth_opening = get_pyth_price_at_time(asset.upper(), target_ts)
    pyth_now     = get_latest_pyth_price(asset.upper())

    result = check_signal(
        cl_price, opening_price,
        bn_now, bn_opening,
        pyth_now, pyth_opening,
        market['up_price'], market['down_price'],
        _bn_history_by_asset[asset],
        _cl_history_by_asset[asset],
        seconds_left,
        asset=asset,
    )
    if len(result) == 2:
        direction, confidence = result
        momentum_info = None
    else:
        direction, confidence, momentum_info = result

    if direction == 'none':
        return

    # Build signal alert
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
    await send_message(msg)

    # Mark as traded BEFORE placing so we don't double-fire on the next poll
    _traded.add(cid)

    # Replacement block for the trade placement section at end of _process_market:

    # Calculate position size and max fill price (for logging)
    shares = _calc_position_size(crowd_price)
    max_fill_price = round(min(crowd_price + FAK_BUFFER, HARD_FILL_CAP), 2)

    # Count available asks at/below cap (for logging)
    asks_at_cap = 0
    try:
        book = await get_order_book(market['up_token'] if direction == 'up' else market['down_token'])
        asks_at_cap = sum(1 for a in book.get('asks', []) if float(a['price']) <= max_fill_price)
    except Exception:
        pass

    # Place the trade (dry-run or real, decided inside _place_trade)
    trade_result = await _place_trade(market, direction, shares, confidence)
    _log(f'Trade result: {trade_result}')

    # Log to CSV — for analysis (works in both DRY and LIVE modes)
    try:
        await log_bot2_signal(
            market=market,
            direction=direction,
            confidence=confidence,
            cl_price=cl_price,
            opening_cl=opening_price,
            bn_price=bn_now,
            opening_bn=bn_opening,
            shares=shares,
            max_fill_price=max_fill_price,
            asks_at_cap=asks_at_cap,
            safe_mode=SAFE_MODE,
            trade_result=trade_result,
        )
    except Exception as e:
        _log(f'CSV log error: {e}')



async def main():
    try:
        feeders = [asyncio.create_task(_binance_feeder(a)) for a in ASSETS_TO_SCAN]
        scanner = asyncio.create_task(market_scanner())

        # Start Pyth Network feeder for all bot2 assets (3rd confirmation source)
        start_pyth_feeder(
            asyncio.get_event_loop(),
            [a.upper() for a in ASSETS_TO_SCAN],
            poll_interval=int(os.getenv('PYTH_POLL_INTERVAL', '5')),
        )
        _log(f'Pyth feeder started for {ASSETS_TO_SCAN}')

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