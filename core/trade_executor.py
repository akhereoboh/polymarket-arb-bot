import sys
import os
import json
import asyncio
import aiohttp
import time
from dotenv import load_dotenv
load_dotenv('/root/polymarket-arb-bot/.env')

from py_clob_client_v2 import ClobClient, SignatureTypeV2, ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions, Side
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType


def get_clob_client():
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


async def get_btc_market():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            'https://gamma-api.polymarket.com/events',
            params={'active': 'true', 'closed': 'false', 'limit': 50,
                    'order': 'createdAt', 'ascending': 'false'},
            headers={'User-Agent': 'Mozilla/5.0'}
        ) as resp:
            events = await resp.json()
    for e in events:
        if 'updown' not in e.get('slug', ''):
            continue
        if not e.get('active') or e.get('closed'):
            continue
        m = e['markets'][0]
        token_ids = json.loads(m.get('clobTokenIds', '[]'))
        prices = json.loads(m.get('outcomePrices', '[]'))
        best_ask = m.get('bestAsk', 0)
        if not token_ids:
            continue
        if prices and len(prices) >= 2 and float(prices[0]) > 0:
            return token_ids, float(prices[0]), float(prices[1]), e["title"]
        elif best_ask and 0.01 <= float(best_ask) <= 0.99:
            up = float(best_ask)
            down = round(1.0 - up, 4)
            return token_ids, up, down, e["title"]
    return None, None, None, None


def check_balance():
    client = get_clob_client()
    bal = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=3)
    )
    balance = int(bal.get('balance', 0)) / 1_000_000
    print(json.dumps({"balance": balance, "raw": bal}))
    return balance


def execute_arb(up_token: str, down_token: str, up_price: float,
                down_price: float, size: int = 5):
    client = get_clob_client()
    results = {}

    print(f'Placing UP leg: {size} shares @ {up_price}')
    try:
        up = client.create_and_post_order(
            order_args=OrderArgs(token_id=up_token, price=up_price,
                                 size=size, side=Side.BUY),
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FOK,
        )
        results['up'] = str(up)
        print(f'UP result: {up}')
    except Exception as e:
        results['up_error'] = str(e)
        print(f'UP error: {e}')

    print(f'Placing DOWN leg: {size} shares @ {down_price}')
    try:
        down = client.create_and_post_order(
            order_args=OrderArgs(token_id=down_token, price=down_price,
                                 size=size, side=Side.BUY),
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FOK,
        )
        results['down'] = str(down)
        print(f'DOWN result: {down}')
    except Exception as e:
        results['down_error'] = str(e)
        print(f'DOWN error: {e}')

    print(json.dumps({"status": "done", "results": results}))
    return results


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "balance":
        check_balance()

    elif mode == "test":
        token_ids, up_price, down_price, title = asyncio.run(get_btc_market())
        if not token_ids:
            print('No market found')
            sys.exit(1)
        print(f'Market: {title}')
        print(f'UP={up_price} DOWN={down_price} Total={round(up_price+down_price,4)}')
        execute_arb(token_ids[0], token_ids[1], up_price, down_price)

    elif mode == "order":
        args = json.loads(sys.argv[2])
        execute_arb(
            args["up_token"],
            args["down_token"],
            args["up_price"],
            args["down_price"],
            args.get("size", 5)
        )