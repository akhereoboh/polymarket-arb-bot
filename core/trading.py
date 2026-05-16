import os
import sys
import json
import asyncio
import aiohttp
from dotenv import load_dotenv
load_dotenv('/root/polymarket-arb-bot/.env')

sys.path.insert(0, '/root/my-clob-client')

from py_clob_client_v2 import ClobClient, SignatureTypeV2, ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions, Side
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType, PostOrdersV2Args


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


async def get_balance() -> float:
    try:
        client = get_clob_client()
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=3)
        )
        return int(bal.get('balance', 0)) / 1_000_000
    except Exception as e:
        print(f'[Trading] Balance error: {e}')
        return 0.0


async def execute_arb_trade(market: dict, shares: int = 5) -> dict:
    token_ids = market.get('token_ids', [])
    if len(token_ids) < 2:
        return {'error': 'Missing token IDs'}

    up_token = token_ids[0]
    down_token = token_ids[1]
    up_price = market.get('up_ask', 0.5)
    down_price = market.get('down_ask', 0.5)
    total_cost = round((up_price + down_price) * shares, 4)

    dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'

    if dry_run:
        print(
            f'[Trading] DRY RUN — Arb trade:\n'
            f'  BUY {shares} UP @ ${up_price}\n'
            f'  BUY {shares} DOWN @ ${down_price}\n'
            f'  Total: ${total_cost}'
        )
        return {'status': 'dry_run', 'total_cost': total_cost}

    balance = await get_balance()
    if balance < total_cost:
        print(f'[Trading] Insufficient balance: ${balance:.4f} < ${total_cost:.4f}')
        return {'error': 'insufficient_balance', 'balance': balance, 'needed': total_cost}

    print(f'[Trading] Executing arb — Balance: ${balance:.4f} | Cost: ${total_cost:.4f}')

    client = get_clob_client()

    try:
        # build both orders
        up_order = client.create_order(
            order_args=OrderArgs(token_id=up_token, price=up_price, size=shares, side=Side.BUY),
            options=PartialCreateOrderOptions(tick_size='0.01', neg_risk=False),
        )
        down_order = client.create_order(
            order_args=OrderArgs(token_id=down_token, price=down_price, size=shares, side=Side.BUY),
            options=PartialCreateOrderOptions(tick_size='0.01', neg_risk=False),
        )

        # submit both atomically in one batch call
        batch = [
            PostOrdersV2Args(order=up_order, orderType=OrderType.FOK),
            PostOrdersV2Args(order=down_order, orderType=OrderType.FOK),
        ]
        result = client.post_orders(batch)
        print(f'[Trading] Batch result: {result}')

        # check if both filled
        success = True
        if isinstance(result, list):
            for r in result:
                if isinstance(r, dict):
                    if not r.get('success') and 'fully filled' not in str(r):
                        success = False
        
        return {
            'status': 'executed' if success else 'failed',
            'result': str(result),
            'total_cost': total_cost,
        }

    except Exception as e:
        err = str(e)
        if 'fully filled' in err:
            print(f'[Trading] FOK rejected — no liquidity')
            return {'status': 'failed', 'error': 'no_liquidity', 'total_cost': total_cost}
        print(f'[Trading] Trade error: {e}')
        return {'status': 'failed', 'error': err, 'total_cost': total_cost}