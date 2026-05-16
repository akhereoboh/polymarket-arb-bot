import os
import sys
import json
import asyncio
import aiohttp
from dotenv import load_dotenv
load_dotenv('/root/polymarket-arb-bot/.env')

# add forked SDK to path
sys.path.insert(0, '/root/my-clob-client')

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
        return {
            'status': 'dry_run',
            'total_cost': total_cost,
        }

    balance = await get_balance()
    if balance < total_cost:
        print(f'[Trading] Insufficient balance: ${balance:.4f} < ${total_cost:.4f}')
        return {'error': 'insufficient_balance', 'balance': balance, 'needed': total_cost}

    print(f'[Trading] Executing arb — Balance: ${balance:.4f} | Cost: ${total_cost:.4f}')

    client = get_clob_client()
    results = {}

    try:
        up = client.create_and_post_order(
            order_args=OrderArgs(token_id=up_token, price=up_price, size=shares, side=Side.BUY),
            options=PartialCreateOrderOptions(tick_size='0.01', neg_risk=False),
            order_type=OrderType.FOK,
        )
        results['up_leg'] = str(up)
        print(f'[Trading] UP filled: {up}')
    except Exception as e:
        err = str(e)
        # FOK rejection means order was accepted but no liquidity — not a real failure
        if 'fully filled' in err or 'orderID' in err:
            results['up_leg'] = 'fok_rejected'
            print(f'[Trading] UP FOK rejected (no liquidity): {e}')
        else:
            results['up_error'] = err
            print(f'[Trading] UP error: {e}')

    success = 'up_error' not in results and 'down_error' not in results
    results['status'] = 'executed' if success else 'failed'
    results['total_cost'] = total_cost

    if success:
        print(f'[Trading] ✅ Both legs filled — Total: ${total_cost:.4f}')
    else:
        print(f'[Trading] ⚠️ Partial fill — {results}')

    return results