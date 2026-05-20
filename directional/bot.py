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
ENTRY_WINDOW  = 60    # seconds before resolution to consider entry
RPC           = 'https://polygon-bor-rpc.publicnode.com'
CL_CONTRACT   = '0xc907E116054Ad103354f2D350FD2514433D57F6f'

# ── state ────────────────────────────────────────────────
_traded       = set()          # condition_ids already traded this session
_btc_history  = []             # [(timestamp, binance_price), ...]
_opening_prices = {}           # condition_id -> chainlink opening price

import csv
from pathlib import Path

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
        
        raw_price = market['up_price'] if direction == 'up' else market['down_price']
        price = round(min(raw_price + 0.02, 0.99), 2)
        writer.writerow({
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'market': market['title'],
            'end_time': market['end_time'].strftime('%Y-%m-%d %H:%M:%S'),
            'direction': direction.upper(),
            'entry_price': price,
            'shares': shares,
            'cost': round(shares * price, 4),
            'cl_pct': round(cl_pct, 4),
            'bn_pct': round(bn_pct, 4),
            'confidence': round(confidence, 4),
            'up_price': market['up_price'],
            'down_price': market['down_price'],
            'cl_price_at_signal': round(cl_price, 2),
            'opening_cl_price': round(opening_price, 2),
            'binance_at_signal': round(binance_price, 2),
            'dry_run': DRY_RUN,
            'outcome': 'PENDING',
            'resolution_cl_price': ''
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
            'active': 'true',
            'closed': 'false',
            'limit': '50',
            'order': 'endDate',
            'ascending': 'true',
            'end_date_min': now_str,  # only markets ending in the future
        },
        headers={'User-Agent': 'Mozilla/5.0'}
    ) as r:
        events = await r.json()

    now = datetime.now(timezone.utc)
    markets = []

    for e in events:
        if 'btc-updown-5m' not in e.get('slug', ''):
            continue
        if not e.get('active') or e.get('closed'):
            continue

        end_str = e.get('endDate', '')
        if not end_str:
            continue

        end_time = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
        seconds_left = (end_time - now).total_seconds()

        if seconds_left < 0:
            continue
        if seconds_left > 360:
            print(f'[Waiting] {e["title"]} | {seconds_left:.0f}s left')
            continue

        m = e['markets'][0]
        token_ids = json.loads(m.get('clobTokenIds', '[]'))
        prices    = json.loads(m.get('outcomePrices', '[]'))

        if len(token_ids) < 2 or len(prices) < 2:
            continue

        markets.append({
            'title':        e['title'],
            'condition_id': m.get('conditionId', ''),
            'slug':         e['slug'],
            'end_time':     end_time,
            'seconds_left': seconds_left,
            'up_token':     token_ids[0],
            'down_token':   token_ids[1],
            'up_price':     float(prices[0]),
            'down_price':   float(prices[1]),
        })

    return markets


def check_signal(cl_price: float, opening_price: float,
                 binance_now: float, binance_60s_ago: float,
                 up_price: float, down_price: float) -> tuple[str, float]:
    cl_pct    = (cl_price - opening_price) / opening_price * 100
    bn_pct    = (binance_now - binance_60s_ago) / binance_60s_ago * 100

    cl_up     = cl_pct  > 0
    bn_up     = bn_pct  > 0
    cl_strong = abs(cl_pct) >= MIN_MOVE_PCT

    # condition 1 — chainlink and binance must agree
    if cl_up != bn_up:
        return 'none', 0.0

    # condition 2 — chainlink move must be strong enough
    if not cl_strong:
        return 'none', 0.0

    direction = 'up' if cl_up else 'down'

    # condition 3 — polymarket crowd must not strongly disagree
    # if we say UP, crowd price for UP must be >= 0.40 (crowd not strongly against us)
    # if we say DOWN, crowd price for DOWN must be >= 0.40
    crowd_price = up_price if direction == 'up' else down_price
    if crowd_price < 0.35:
        print(f'  → Crowd strongly disagrees ({crowd_price}) — skipping')
        return 'none', 0.0

    # bonus confidence if crowd agrees (price > 0.55)
    crowd_bonus = 0.05 if crowd_price > 0.55 else 0.0
    confidence = (abs(cl_pct) + abs(bn_pct)) / 2 + crowd_bonus
    return direction, confidence


def calc_position_size(direction: str, up_price: float,
                       down_price: float, confidence: float) -> tuple[int, float]:
    """
    Returns (shares, cost).
    More confidence + cheaper price = more shares.
    """
    price = up_price if direction == 'up' else down_price

    # base amount scales with confidence
    if confidence >= 0.15:
        amount = TRADE_AMOUNT
    elif confidence >= 0.10:
        amount = TRADE_AMOUNT * 0.6
    else:
        amount = TRADE_AMOUNT * 0.3

    shares = int(amount / price)
    shares = max(5, shares)   # minimum 5 shares
    cost   = round(shares * price, 4)
    return shares, cost


async def place_trade(market: dict, direction: str,
                      shares: int, confidence: float):
    token_id = market['up_token'] if direction == 'up' else market['down_token']
    # add 0.01 buffer to hit actual ask price
    raw_price = market['up_price'] if direction == 'up' else market['down_price']
    price = round(min(raw_price + 0.01, 0.99), 2)
    side_str = 'UP' if direction == 'up' else 'DOWN'

    print(
        f'[Trade] {market["title"]}\n'
        f'  Direction: {side_str} | Price: {price} | '
        f'Shares: {shares} | Confidence: {confidence:.4f}%\n'
        f'  Cost: ~${shares * price:.2f} | DRY_RUN: {DRY_RUN}'
    )

    if DRY_RUN:
        print(f'  [DRY RUN] Would place {side_str} order')
        return {'status': 'dry_run', 'logged':True}

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
        print(f'  [Order] Result: {result}')
        return result
    except Exception as e:
        print(f'  [Order] Error: {e}')
        return {'status': 'error', 'error': str(e)}
    
    
async def get_opening_chainlink_price(session, market_end_time) -> float:
    """Get Chainlink price at market start (5 minutes before end)."""
    market_start_ts = market_end_time.timestamp() - 300  # 5 min before end
    
    # get current round
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
        current_round = int(result[2:2+64], 16)
        current_updated = int(result[2+192:2+256], 16)
    
    # estimate round at market start
    now_ts = time.time()
    seconds_ago = now_ts - market_start_ts
    rounds_ago = int(seconds_ago / 33)  # ~33 seconds per round
    target_round = current_round - rounds_ago
    
    # get that round's price
    round_hex = hex(target_round)[2:].zfill(64)
    payload2 = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CL_CONTRACT, "data": "0x9a6fc8f5" + round_hex}],
        "id": 1
    }
    async with session.post(RPC, json=payload2,
                           timeout=aiohttp.ClientTimeout(total=5)) as r:
        data = await r.json()
        if 'result' in data and data['result'] and data['result'] != '0x':
            res = data['result']
            price = int(res[2+64:2+128], 16) / 1e8
            if price > 0:
                return price
    
    # fallback to current price
    return int(result[2+64:2+128], 16) / 1e8

async def price_monitor():
    """Continuously update Binance price history."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                price = await get_binance_price(session)
                now   = time.time()
                _btc_history.append((now, price))
                # keep only last 2 minutes
                cutoff = now - 120
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

                # get chainlink price
                cl_price, cl_updated = await get_chainlink_price(session)
                cl_dt = datetime.fromtimestamp(cl_updated, tz=timezone.utc)

                # get active markets
                markets = await get_active_btc_markets(session)

                for market in markets:
                    cid          = market['condition_id']
                    seconds_left = market['seconds_left']
                    end_time     = market['end_time']

                    # store opening price when market first seen
                    if cid not in _opening_prices:
                        opening = await get_opening_chainlink_price(session, end_time)
                        _opening_prices[cid] = opening
                        print(f'[Market] New: {market["title"]} | {seconds_left:.0f}s left | Opening CL: ${opening:,.2f}')
                        

                    # skip if already traded
                    if cid in _traded:
                        continue

                    opening_price = _opening_prices[cid]

                    # only check in entry window
                    if seconds_left > ENTRY_WINDOW:
                        cl_pct = (cl_price - opening_price) / opening_price * 100
                        print(
                            f'[Monitor] {market["title"][:45]} | '
                            f'{seconds_left:.0f}s | '
                            f'CL: ${cl_price:,.2f} ({cl_pct:+.4f}%) | '
                            f'UP:{market["up_price"]} DOWN:{market["down_price"]}'
                        )
                        continue

                    # get binance 60s ago
                    now_ts     = time.time()
                    target_ts  = now_ts - 60
                    bn_60s_ago = None
                    for ts, px in reversed(_btc_history):
                        if ts <= target_ts:
                            bn_60s_ago = px
                            break

                    if not bn_60s_ago or not _btc_history:
                        print(f'[Signal] No Binance history yet')
                        continue

                    binance_now = _btc_history[-1][1]

                    # check signal
                    direction, confidence = check_signal(
                        cl_price, opening_price, binance_now, bn_60s_ago,
                        market['up_price'], market['down_price']
                    )

                    cl_pct = (cl_price - opening_price) / opening_price * 100
                    bn_pct = (binance_now - bn_60s_ago) / bn_60s_ago * 100

                    print(
                        f'[Signal] {market["title"][:45]} | {seconds_left:.0f}s left\n'
                        f'  CL: ${cl_price:,.2f} ({cl_pct:+.4f}%) | '
                        f'BN: ${binance_now:,.2f} ({bn_pct:+.4f}%)\n'
                        f'  Direction: {direction.upper()} | Confidence: {confidence:.4f}%'
                    )

                    if direction == 'none':
                        print(f'  → Signals disagree or too weak — skipping')
                        continue

                    # calculate position
                    shares, cost = calc_position_size(
                        direction,
                        market['up_price'],
                        market['down_price'],
                        confidence
                    )

                    # place trade
                    _traded.add(cid)
                    log_signal(market, direction, shares, cl_pct, bn_pct, confidence,
                               cl_price, _opening_prices[cid], _btc_history[-1][1])
                    bal_before = await get_balance()
                    await place_trade(market, direction, shares, confidence)
                    await asyncio.sleep(2)
                    bal_after = await get_balance()
                    change = bal_after - bal_before
                    print(f'[Trade] Balance: ${bal_before:.4f} → ${bal_after:.4f} | Change: ${change:+.4f}')
                    if change < 0:
                        print(f'[Trade] ✅ Order confirmed on Polymarket — balance decreased')
                    else:
                        print(f'[Trade] ⚠️ Balance unchanged — order may not have filled')
          

            except Exception as err:
                print(f'[Scanner] Error: {err}')
            await asyncio.sleep(5)


async def main():
    await asyncio.gather(
        price_monitor(),
        market_scanner(),
    )


if __name__ == '__main__':
    asyncio.run(main())