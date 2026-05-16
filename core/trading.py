import os
import time
import hmac
import hashlib
import base64
import json
import aiohttp
from eth_account import Account
from eth_account.messages import encode_defunct
from py_clob_client_v2.clob_types import PostOrdersV2Args, OrderType as OT

CLOB_BASE = "https://clob.polymarket.com"

def get_trading_config():
    return {
        "private_key": os.getenv("POLYMARKET_PRIVATE_KEY"),
        "api_key": os.getenv("POLYMARKET_API_KEY"),
        "api_secret": os.getenv("POLYMARKET_API_SECRET"),
        "api_passphrase": os.getenv("POLYMARKET_API_PASSPHRASE"),
        "funder": os.getenv("POLYMARKET_FUNDER"),
        "signature_type": int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1")),
        "dry_run": os.getenv("DRY_RUN", "true").lower() == "true",
    }


def build_hmac_signature(api_secret: str, timestamp: str, method: str, path: str, body: str = "") -> str:
    """Build HMAC signature for Polymarket API authentication."""
    message = timestamp + method + path + body
    # add padding if needed
    secret = api_secret
    padding = 4 - len(secret) % 4
    if padding != 4:
        secret += "=" * padding
    secret_bytes = base64.b64decode(secret)
    signature = hmac.new(secret_bytes, message.encode(), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()


def get_auth_headers(method: str, path: str, body: str = "") -> dict:
    """Generate authenticated headers for CLOB API."""
    config = get_trading_config()
    timestamp = str(int(time.time() * 1000))
    signature = build_hmac_signature(
        config["api_secret"], timestamp, method, path, body
    )
    return {
        "POLY-API-KEY": config["api_key"],
        "POLY-SIGNATURE": signature,
        "POLY-TIMESTAMP": timestamp,
        "POLY-PASSPHRASE": config["api_passphrase"],
        "Content-Type": "application/json",
    }


async def get_balance() -> float:
    """Get USDC balance from Polymarket."""
    try:
        async with aiohttp.ClientSession() as session:
            headers = get_auth_headers("GET", "/balance")
            async with session.get(
                f"{CLOB_BASE}/balance",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"[Trading] Balance error {resp.status}: {text}")
                    return 0.0
                data = await resp.json()
                return float(data.get("balance", 0))
    except Exception as e:
        print(f"[Trading] Balance error: {e}")
        return 0.0


async def place_market_order(token_id: str, side: str, amount_usdc: float) -> dict:
    """
    Place a market order on Polymarket CLOB.
    side: "BUY" or "SELL"
    amount_usdc: amount in USDC to spend
    """
    config = get_trading_config()

    if config["dry_run"]:
        print(f"[Trading] DRY RUN — would place {side} order: token={token_id[:20]}... amount=${amount_usdc}")
        return {"status": "dry_run", "token_id": token_id, "side": side, "amount": amount_usdc}

    try:
        order_payload = {
            "token_id": token_id,
            "price": 0,  # market order
            "side": side,
            "size_type": "USDC",
            "size": str(amount_usdc),
            "type": "MARKET",
            "time_in_force": "FOK",  # fill or kill — all or nothing
        }

        body = json.dumps(order_payload)
        headers = get_auth_headers("POST", "/order", body)

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{CLOB_BASE}/order",
                headers=headers,
                data=body,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if resp.status == 200:
                    print(f"[Trading] Order placed: {side} ${amount_usdc} token={token_id[:20]}...")
                else:
                    print(f"[Trading] Order error {resp.status}: {data}")
                return data

    except Exception as e:
        print(f"[Trading] Order exception: {e}")
        return {"error": str(e)}


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

        # submit both atomically
        
        batch = [
            PostOrdersV2Args(order=up_order, orderType=OT.FOK),
            PostOrdersV2Args(order=down_order, orderType=OT.FOK),
        ]
        result = client.post_orders(batch)
        print(f'[Trading] Batch result: {result}')

        success = True
        for r in (result if isinstance(result, list) else [result]):
            if isinstance(r, dict) and not r.get('success') and 'fully filled' not in str(r):
                success = False

        return {
            'status': 'executed' if success else 'failed',
            'result': str(result),
            'total_cost': total_cost,
        }

    except Exception as e:
        err = str(e)
        if 'fully filled' in err:
            print(f'[Trading] FOK rejected — no liquidity: {e}')
            return {'status': 'failed', 'error': 'no_liquidity', 'total_cost': total_cost}
        print(f'[Trading] Trade error: {e}')
        return {'status': 'failed', 'error': err, 'total_cost': total_cost}