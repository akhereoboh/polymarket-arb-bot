import asyncio
import aiohttp
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import csv
from pathlib import Path


load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
from telegram_alerts import (
    alert_entry,
    schedule_outcome_check,
    start_daily_summary_loop,
    send_message,
)
from gtc_fallback import (
    place_gtc_fallback, fak_filled, clear_gtc_tracking,
    active_gtc_count, set_on_fill_callback,
)


sys.path.insert(0, '/root/my-clob-client')

from py_clob_client_v2 import (
    ClobClient, SignatureTypeV2, ApiCreds,
    OrderArgs, OrderType, PartialCreateOrderOptions, Side
)
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

# ── config ──────────────────────────────────────────────
DRY_RUN       = os.getenv('DRY_RUN', 'true').lower() == 'true'
MIN_MOVE_PCT  = float(os.getenv('MIN_MOVE_PCT', '0.05'))  # minimum % move to trade
TRADE_AMOUNT  = float(os.getenv('TRADE_AMOUNT', '20'))    # USD per trade
RPC           = 'https://polygon-bor-rpc.publicnode.com'
CL_CONTRACT   = '0xc907E116054Ad103354f2D350FD2514433D57F6f'

# ── state ────────────────────────────────────────────────
_traded         = set()   # condition_ids already traded this session
_signal_context: dict[str, dict] = {}  # condition_id -> {confidence, cl_pct, bn_pct} for fill callbacks
_btc_history    = []      # [(timestamp, binance_price), ...]
_cl_history     = []      # [(timestamp, chainlink_price), ...]
_opening_prices = {}      # condition_id -> chainlink opening price

LOG_FILE = os.path.join(os.path.dirname(__file__), 'signals_log.csv')


def log_signal(market: dict, direction: str, shares: int,
               cl_pct: float, bn_pct: float, confidence: float,
               cl_price: float, opening_price: float, binance_price: float):
    file_exists = Path(LOG_FILE).exists()
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'timestamp', 'market', 'end_time', 'direction',
            'entry_price', 'shares', 'cost', 'cl_pct', 'bn_pct',
            'confidence', 'up_price', 'down_price',
            'cl_price_at_signal', 'opening_cl_price', 'binance_at_signal',
            'dry_run', 'outcome', 'resolution_cl_price'
        ])
        if not file_exists:
            writer.writeheader()

        price = market['up_price'] if direction == 'up' else market['down_price']
        writer.writerow({
            'timestamp':            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'market':               market['title'],
            'end_time':             market['end_time'].strftime('%Y-%m-%d %H:%M:%S'),
            'direction':            direction.upper(),
            'entry_price':          price,
            'shares':               shares,
            'cost':                 round(shares * price, 4),
            'cl_pct':               round(cl_pct, 4),
            'bn_pct':               round(bn_pct, 4),
            'confidence':           round(confidence, 4),
            'up_price':             market['up_price'],
            'down_price':           market['down_price'],
            'cl_price_at_signal':   round(cl_price, 2),
            'opening_cl_price':     round(opening_price, 2),
            'binance_at_signal':    round(binance_price, 2),
            'dry_run':              DRY_RUN,
            'outcome':              'PENDING',
            'resolution_cl_price':  ''
        })
    print(f'[Log] Signal logged to {LOG_FILE}')


def get_client():
    creds = ApiCreds(
        api_key=os.getenv('POLYMARKET_API_KEY'),
        api_secret=os.getenv('POLYMARKET_API_SECRET'),
        api_passphrase=os.getenv('POLYMARKET_API_PASSPHRASE'),
    )
    return ClobClient(
        host='https://clob.polymarket.com',
        chain_id=137,
        key=os.getenv('POLYMARKET_PRIVATE_KEY'),
        creds=creds,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=os.getenv('POLYMARKET_FUNDER'),
    )


async def get_balance() -> float:
    try:
        client = get_client()
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=3)
        )
        return int(bal.get('balance', 0)) / 1_000_000
    except Exception as e:
        print(f'[Balance] Error: {e}')
        return 0.0

async def get_order_book(token_id: str) -> dict:
    """Get order book for a token. Returns asks and bids."""
    try:
        client = get_client()
        book = client.get_order_book(token_id)
        # book has .asks and .bids as list of {price, size} dicts
        asks = book.get('asks', []) if isinstance(book, dict) else (book.asks if hasattr(book, 'asks') else [])
        bids = book.get('bids', []) if isinstance(book, dict) else (book.bids if hasattr(book, 'bids') else [])
        return {'asks': asks, 'bids': bids}
    except Exception as e:
        print(f'[OrderBook] Error: {e}')
        return {'asks': [], 'bids': []}


async def get_chainlink_price(session) -> tuple[float, int]:
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CL_CONTRACT, "data": "0xfeaf968c"}, "latest"],
        "id": 1
    }
    async with session.post(RPC, json=payload,
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
        data = await r.json()
        result = data['result']
        price   = int(result[2+64:2+128],  16) / 1e8
        updated = int(result[2+192:2+256], 16)
        return price, updated


async def get_binance_price(session) -> float:
    async with session.get(
        'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT',
        timeout=aiohttp.ClientTimeout(total=3)
    ) as r:
        data = await r.json()
        return float(data['price'])


async def get_active_btc_markets(session) -> list:
    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    async with session.get(
        'https://gamma-api.polymarket.com/events',
        params={
            'active':       'true',
            'closed':       'false',
            'limit':        '50',
            'order':        'endDate',
            'ascending':    'true',
            'end_date_min': now_str,
        },
        headers={'User-Agent': 'Mozilla/5.0'}
    ) as r:
        events = await r.json()

    now     = datetime.now(timezone.utc)
    markets = []

    for e in events:
        slug = e.get('slug', '')
        if 'btc-updown-5m' not in slug and 'btc-updown-15m' not in slug:
            continue
        if not e.get('active') or e.get('closed'):
            continue

        end_str = e.get('endDate', '')
        if not end_str:
            continue

        end_time     = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
        seconds_left = (end_time - now).total_seconds()

        if seconds_left < 0:
            continue

        tf = '15m' if 'btc-updown-15m' in slug else '5m'

        if seconds_left > 360:
            print(f'[Waiting] [{tf}] {e["title"]} | {seconds_left:.0f}s left')
            continue

        m         = e['markets'][0]
        token_ids = json.loads(m.get('clobTokenIds', '[]'))
        prices    = json.loads(m.get('outcomePrices', '[]'))

        if len(token_ids) < 2 or len(prices) < 2:
            continue

        volume = float(m.get('volume', 0))
        if volume < 1000:
            print(f'[Skip] Low volume ${volume:.0f} — {e["title"]}')
            continue

        markets.append({
            'title':        e['title'],
            'condition_id': m.get('conditionId', ''),
            'slug':         slug,
            'end_time':     end_time,
            'seconds_left': seconds_left,
            'up_token':     token_ids[0],
            'down_token':   token_ids[1],
            'up_price':     float(prices[0]),
            'down_price':   float(prices[1]),
            'volume':       volume,
            'timeframe':    tf,
        })

    return markets


def check_signal(cl_price: float, opening_price: float,
                 binance_now: float, binance_opening: float,
                 up_price: float, down_price: float,
                 btc_history: list, cl_history: list,
                 seconds_left: float) -> tuple[str, float]:

    cl_pct = (cl_price - opening_price) / opening_price * 100
    bn_pct = (binance_now - binance_opening) / binance_opening * 100

    cl_up     = cl_pct > 0
    bn_up     = bn_pct > 0
    cl_strong = abs(cl_pct) >= MIN_MOVE_PCT
    bn_strong = abs(bn_pct) >= MIN_MOVE_PCT

    # condition 1 — chainlink move must be strong enough
    if not cl_strong:
        return 'none', 0.0

    # condition 2 — binance move must also be strong enough independently
    if not bn_strong:
        return 'none', 0.0

    # condition 3 — both must agree on direction
    if cl_up != bn_up:
        return 'none', 0.0

    # condition 4 — conflict detector: magnitudes must not wildly differ
    spread = abs(abs(cl_pct) - abs(bn_pct))
    avg    = (abs(cl_pct) + abs(bn_pct)) / 2
    if avg > 0:
        conflict_ratio = spread / avg
        if conflict_ratio > 1.5:
            print(f'  → Signal conflict (CL:{cl_pct:+.4f}% BN:{bn_pct:+.4f}% ratio:{conflict_ratio:.2f}) — skipping')
            return 'none', 0.0

    direction = 'up' if cl_up else 'down'

    # condition 5 — crowd must not strongly disagree
    crowd_price = up_price if direction == 'up' else down_price
    if crowd_price < 0.35:
        print(f'  → Crowd strongly disagrees ({crowd_price}) — skipping')
        return 'none', 0.0

    # condition 6 — short term momentum confirmation (1,2,3,4 min) on Binance
    now_ts     = time.time()
    timeframes = [60, 120, 180, 240]
    bn_confirmations  = 0
    bn_checked        = 0

    for lookback in timeframes:
        target    = now_ts - lookback
        ref_price = None
        for ts, px in reversed(btc_history):
            if ts <= target:
                ref_price = px
                break
        if ref_price:
            pct = (binance_now - ref_price) / ref_price * 100
            bn_checked += 1
            if direction == 'up' and pct > 0:
                bn_confirmations += 1
            elif direction == 'down' and pct < 0:
                bn_confirmations += 1

    # condition 7 — short term momentum confirmation on Chainlink
    cl_confirmations = 0
    cl_checked       = 0

    for lookback in timeframes:
        target    = now_ts - lookback
        ref_price = None
        for ts, px in reversed(cl_history):
            if ts <= target:
                ref_price = px
                break
        if ref_price:
            pct = (cl_price - ref_price) / ref_price * 100
            cl_checked += 1
            if direction == 'up' and pct > 0:
                cl_confirmations += 1
            elif direction == 'down' and pct < 0:
                cl_confirmations += 1

    total_confirmations = bn_confirmations + cl_confirmations
    total_checks        = bn_checked + cl_checked

    if total_checks >= 4:
        momentum_score = total_confirmations / total_checks
        print(f'  → Momentum: {total_confirmations}/{total_checks} (BN:{bn_confirmations}/{bn_checked} CL:{cl_confirmations}/{cl_checked})')
        if momentum_score < 0.5:
            print(f'  → Momentum against signal — skipping')
            return 'none', 0.0


    # bonus confidence if crowd agrees
    crowd_bonus = 0.05 if crowd_price > 0.55 else 0.0
    confidence  = (abs(cl_pct) + abs(bn_pct)) / 2 + crowd_bonus
    return direction, confidence


def calc_position_size(direction: str, up_price: float,
                       down_price: float, confidence: float) -> tuple[int, float]:
    price = up_price if direction == 'up' else down_price

    if confidence >= 0.15:
        amount = TRADE_AMOUNT
    elif confidence >= 0.10:
        amount = TRADE_AMOUNT * 0.6
    else:
        amount = TRADE_AMOUNT * 0.3

    shares = int(amount / price)
    shares = max(5, shares)
    cost   = round(shares * price, 4)
    return shares, cost


async def place_trade(market: dict, direction: str,
                      shares: int, confidence: float):
    token_id = market['up_token'] if direction == 'up' else market['down_token']
    raw_price = market['up_price'] if direction == 'up' else market['down_price']

    # check order book for available liquidity at our price
    book = await get_order_book(token_id)
    asks = sorted(book['asks'], key=lambda x: float(x['price']))  # ascending ✅

    # find best ask at or below our max price (raw + 0.05)
    max_price = round(min(raw_price + 0.05, 0.99), 2)
    available_asks = [a for a in asks if float(a['price']) <= max_price]

    if not available_asks:
        if not available_asks:
            print(f'  → No sellers at or below {max_price} — routing to GTC fallback')
            gtc_result = await place_gtc_fallback(
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
            return gtc_result

    # use actual best ask price + tiny buffer
    best_ask = float(available_asks[0]['price'])
    total_available = sum(float(a['size']) for a in available_asks)
    price = round(min(best_ask + 0.01, 0.99), 2)
    print(f'  → Order book: best ask {best_ask} | available shares: {total_available:.0f}')
    side_str = 'UP' if direction == 'up' else 'DOWN'

    print(
        f'[Trade] {market["title"]}\n'
        f'  Direction: {side_str} | Price: {price} | '
        f'Shares: {shares} | Confidence: {confidence:.4f}%\n'
        f'  Cost: ~${shares * price:.2f} | DRY_RUN: {DRY_RUN}'
    )

    if DRY_RUN:
        print(f'  [DRY RUN] Would place {side_str} order')
        return {'status': 'dry_run', 'logged': True}

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
        print(f'  [Order] FAK result: {result}')

        filled, shares_filled = fak_filled(result)
        if filled:
            print(f'  [Order] FAK filled {shares_filled} shares')
            return result

        # FAK didn't fill — try GTC fallback
        print(f'  [Order] FAK no-fill — trying GTC fallback')
        gtc_result = await place_gtc_fallback(
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
        return gtc_result

    except Exception as e:
        print(f'  [Order] Error: {e}')
        return {'status': 'error', 'error': str(e)}


async def get_opening_chainlink_price(session, market_end_time, timeframe='5m') -> float:
    lookback         = 900 if timeframe == '15m' else 300
    market_start_ts  = market_end_time.timestamp() - lookback

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CL_CONTRACT, "data": "0xfeaf968c"}, "latest"],
        "id": 1
    }
    async with session.post(RPC, json=payload,
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
        data          = await r.json()
        result        = data['result']
        current_round = int(result[2:2+64], 16)

    now_ts      = time.time()
    seconds_ago = now_ts - market_start_ts
    rounds_ago  = int(seconds_ago / 33)
    target_round = current_round - rounds_ago

    round_hex = hex(target_round)[2:].zfill(64)
    payload2  = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CL_CONTRACT, "data": "0x9a6fc8f5" + round_hex}],
        "id": 1
    }
    async with session.post(RPC, json=payload2,
                            timeout=aiohttp.ClientTimeout(total=5)) as r:
        data = await r.json()
        if 'result' in data and data['result'] and data['result'] != '0x':
            res   = data['result']
            price = int(res[2+64:2+128], 16) / 1e8
            if price > 0:
                return price

    return int(result[2+64:2+128], 16) / 1e8


async def price_monitor():
    """Continuously update Binance price history."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                price  = await get_binance_price(session)
                now    = time.time()
                _btc_history.append((now, price))
                cutoff = now - 1200
                while _btc_history and _btc_history[0][0] < cutoff:
                    _btc_history.pop(0)
            except Exception as e:
                print(f'[BinanceFeed] Error: {e}')
            await asyncio.sleep(5)


async def market_scanner():
    """Main scanning loop."""
    async with aiohttp.ClientSession() as session:
        print(f'[Bot] Starting directional bot | DRY_RUN={DRY_RUN}')
        print(f'[Bot] Min move: {MIN_MOVE_PCT}% | Trade amount: ${TRADE_AMOUNT}')

        while True:
            try:
                now = datetime.now(timezone.utc)

                # get chainlink price and store in history
                cl_price, cl_updated = await get_chainlink_price(session)
                now_ts = time.time()
                _cl_history.append((now_ts, cl_price))
                cutoff = now_ts - 1200
                while _cl_history and _cl_history[0][0] < cutoff:
                    _cl_history.pop(0)

                # get active markets
                markets = await get_active_btc_markets(session)

                for market in markets:
                    cid          = market['condition_id']
                    seconds_left = market['seconds_left']
                    end_time     = market['end_time']
                    tf           = market.get('timeframe', '5m')

                    # store opening price when market first seen
                    if cid not in _opening_prices:
                        opening = await get_opening_chainlink_price(session, end_time, tf)
                        _opening_prices[cid] = opening
                        print(f'[Market] New: {market["title"]} | {seconds_left:.0f}s left | Opening CL: ${opening:,.2f}')

                    # skip if already traded
                    if cid in _traded:
                        continue

                    opening_price = _opening_prices[cid]

                    # entry window: 120s for 15m, 60s for 5m
                    entry_window = 120 if tf == '15m' else 60

                    if seconds_left > entry_window:
                        cl_pct = (cl_price - opening_price) / opening_price * 100
                        print(
                            f'[Monitor] [{tf}] {market["title"][:40]} | '
                            f'{seconds_left:.0f}s | '
                            f'CL: ${cl_price:,.2f} ({cl_pct:+.4f}%) | '
                            f'UP:{market["up_price"]} DOWN:{market["down_price"]} Vol:${market["volume"]:.0f}'
                        )
                        continue

                    # get binance opening price (matches market window)
                    lookback   = 900 if tf == '15m' else 300
                    target_ts  = now_ts - lookback
                    bn_opening = None
                    for ts, px in reversed(_btc_history):
                        if ts <= target_ts:
                            bn_opening = px
                            break

                    if not bn_opening or not _btc_history:
                        print(f'[Signal] No Binance history yet')
                        continue

                    binance_now = _btc_history[-1][1]

                    # check signal
                    direction, confidence = check_signal(
                        cl_price, opening_price,
                        binance_now, bn_opening,
                        market['up_price'], market['down_price'],
                        _btc_history, _cl_history,
                        seconds_left
                    )

                    cl_pct = (cl_price - opening_price) / opening_price * 100
                    bn_pct = (binance_now - bn_opening) / bn_opening * 100

                    print(
                        f'[Signal] [{tf}] {market["title"][:40]} | {seconds_left:.0f}s left\n'
                        f'  CL: ${cl_price:,.2f} ({cl_pct:+.4f}%) | '
                        f'BN: ${binance_now:,.2f} ({bn_pct:+.4f}%)\n'
                        f'  Direction: {direction.upper()} | Confidence: {confidence:.4f}%'
                    )

                    if direction == 'none':
                        print(f'  → Signals disagree or too weak — skipping')
                        continue

                    # check price not too one-sided — poor risk/reward at extremes
                    trade_price = market['up_price'] if direction == 'up' else market['down_price']
                    if trade_price < 0.15 or trade_price > 0.85:
                        print(f'  → Market too one-sided ({trade_price}) — poor risk/reward, skipping')
                        continue

                    # calculate position
                    shares, cost = calc_position_size(
                        direction,
                        market['up_price'],
                        market['down_price'],
                        confidence
                    )

                    #place trade
                    _signal_context[cid] = {
                        'confidence': confidence,
                        'cl_pct': cl_pct,
                        'bn_pct': bn_pct,
                    }
                    _traded.add(cid)
                    log_signal(market, direction, shares, cl_pct, bn_pct, confidence,
                               cl_price, _opening_prices[cid], _btc_history[-1][1])
                    bal_before = await get_balance()
                    trade_result = await place_trade(market, direction, shares, confidence)
                    await asyncio.sleep(2)
                    bal_after = await get_balance()
                    change    = bal_after - bal_before
                    print(f'[Trade] Balance: ${bal_before:.4f} → ${bal_after:.4f} | Change: ${change:+.4f}')

                    if change < 0:
                        print(f'[Trade] ✅ Order confirmed on Polymarket — balance decreased')
                        # Fire Telegram entry alert and schedule outcome poll
                        entry_price = market['up_price'] if direction == 'up' else market['down_price']
                        await alert_entry(
                            market, direction, shares, entry_price,
                            confidence, cl_pct, bn_pct, get_balance,
                        )
                        schedule_outcome_check(
                            market, direction, shares, entry_price, get_balance,
                        )
                    else:
                        print(f'[Trade] ⚠️ Balance unchanged — order may not have filled')

            except Exception as err:
                print(f'[Scanner] Error: {err}')
            await asyncio.sleep(5)


async def _on_gtc_fill(market: dict, fill_info: dict) -> None:
    """Called by gtc_fallback when a GTC fills. Fires Telegram alert + outcome tracking."""
    cid = market['condition_id']
    _traded.add(cid)

    direction = fill_info['direction']
    shares = fill_info['shares']
    fill_price = fill_info['price']

    # Pull the signal context stashed at trade time
    meta = _signal_context.pop(cid, {})
    confidence = meta.get('confidence', 0)
    cl_pct = meta.get('cl_pct', 0)
    bn_pct = meta.get('bn_pct', 0)

    await alert_entry(
        market, direction, shares, fill_price,
        confidence, cl_pct, bn_pct, get_balance,
    )
    schedule_outcome_check(
        market, direction, shares, fill_price, get_balance,
    )


async def main():
    set_on_fill_callback(_on_gtc_fill)
    await asyncio.gather(
        price_monitor(),
        market_scanner(),
        start_daily_summary_loop(get_balance),
    )


if __name__ == '__main__':
    asyncio.run(main())