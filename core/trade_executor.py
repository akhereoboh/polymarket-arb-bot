import sys
import os
import json
import asyncio
import aiohttp
from dotenv import load_dotenv
load_dotenv('/root/polymarket-arb-bot/.env')

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs


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
        if 'btc-updown-15m' in e.get('slug', '') and e.get('active') and not e.get('closed'):
            m = e['markets'][0]
            token_ids = json.loads(m.get('clobTokenIds', '[]'))
            prices = json.loads(m.get('outcomePrices', '[]'))
            print(f'Market: {e["title"]} | Prices: {prices}')
            return token_ids, prices
    return None, None


def execute_arb(up_token, down_token, up_price, down_price, size=5):
    client = ClobClient(
        'https://clob.polymarket.com',
        key=os.getenv('POLYMARKET_PRIVATE_KEY'),
        chain_id=137,
        signature_type=1,
        funder=os.getenv('POLYMARKET_FUNDER')
    )
    client.set_api_creds(client.create_or_derive_api_creds())

    print('\nPlacing UP leg...')
    up = client.create_and_post_order(
        OrderArgs(token_id=up_token, price=up_price, size=size, side="BUY")
    )
    print(f'UP: {up}')

    print('\nPlacing DOWN leg...')
    down = client.create_and_post_order(
        OrderArgs(token_id=down_token, price=down_price, size=size, side="BUY")
    )
    print(f'DOWN: {down}')

    return {"up": str(up), "down": str(down)}


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # called with args from main bot
        args = json.loads(sys.argv[1])
        execute_arb(
            args["up_token"],
            args["down_token"],
            args["up_price"],
            args["down_price"],
            args.get("size", 5)
        )
    else:
        # test mode — find market and trade
        token_ids, prices = asyncio.run(get_btc_market())
        if not token_ids:
            print('No market found')
            sys.exit(1)
        execute_arb(
            token_ids[0],
            token_ids[1],
            float(prices[0]),
            float(prices[1]),
            size=5
        )