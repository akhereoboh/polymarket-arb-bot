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


def check_depth(client, token_id: str, price: float, shares: int) -> bool:
    """
    OPTION 3 — Check order book depth.
    For a BUY at price X, check asks side has >= shares available at price <= X.
    """
    try:
        book = client.get_order_book(token_id)
        asks = book.get('asks', [])
        available = sum(
            float(a['size']) for a in asks
            if float(a['price']) <= price
        )
        has_depth = available >= shares
        print(f'[Depth] token={token_id[:10]}... price={price} need={shares} available={available:.2f} ok={has_depth}')
        return has_depth
    except Exception as e:
        print(f'[Depth] Error checking depth: {e}')
        return False


async def cancel_leg(client, order_id: str, token_id: str, shares: int, side: str):
    """
    OPTION 2 — Cancel/close a filled leg by selling it back.
    """
    try:
        print(f'[Trading] Closing exposed {side} leg — selling {shares} shares back')
        close_side = Side.SELL if side == 'UP' else Side.SELL
        result = client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=0.5,  # market price
                size=shares,
                side=close_side
            ),
            options=PartialCreateOrderOptions(tick_size='0.01', neg_risk=False),
            order_type=OrderType.FOK,
        )
        print(f'[Trading] Close result: {result}')
        return result
    except Exception as e:
        print(f'[Trading] Failed to close leg: {e}')
        return None


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

    client = get_clob_client()

    # OPTION 3 — check depth on both sides before placing
    up_ok = check_depth(client, up_token, up_price, shares)
    down_ok = check_depth(client, down_token, down_price, shares)

    if not up_ok or not down_ok:
        print(f'[Trading] Insufficient depth — UP:{up_ok} DOWN:{down_ok} — skipping')
        return {'status': 'skipped', 'reason': 'no_depth', 'total_cost': total_cost}

    print(f'[Trading] Depth OK — executing arb. Balance: ${balance:.4f} | Cost: ${total_cost:.4f}')

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

        # OPTION 1 — submit both atomically
        batch = [
            PostOrdersV2Args(order=up_order, orderType=OrderType.FOK),
            PostOrdersV2Args(order=down_order, orderType=OrderType.FOK),
        ]
        result = client.post_orders(batch)
        print(f'[Trading] Batch result: {result}')

        # parse results
        results_list = result if isinstance(result, list) else [result]
        up_result = results_list[0] if len(results_list) > 0 else {}
        down_result = results_list[1] if len(results_list) > 1 else {}

        up_filled = isinstance(up_result, dict) and up_result.get('success')
        down_filled = isinstance(down_result, dict) and down_result.get('success')

        if up_filled and down_filled:
            print(f'[Trading] ✅ Both legs filled — Total: ${total_cost:.4f}')
            return {'status': 'executed', 'result': str(result), 'total_cost': total_cost}

        # OPTION 2 — one leg filled, close it
        if up_filled and not down_filled:
            print(f'[Trading] ⚠️ UP filled but DOWN failed — closing UP leg')
            await cancel_leg(client, None, up_token, shares, 'UP')
            return {'status': 'failed', 'error': 'partial_closed_up', 'total_cost': total_cost}

        if down_filled and not up_filled:
            print(f'[Trading] ⚠️ DOWN filled but UP failed — closing DOWN leg')
            await cancel_leg(client, None, down_token, shares, 'DOWN')
            return {'status': 'failed', 'error': 'partial_closed_down', 'total_cost': total_cost}

        # neither filled
        print(f'[Trading] Both legs rejected — no exposure')
        return {'status': 'failed', 'error': 'no_liquidity', 'total_cost': total_cost}

    except Exception as e:
        err = str(e)
        print(f'[Trading] Trade error: {e}')
        return {'status': 'failed', 'error': err, 'total_cost': total_cost}