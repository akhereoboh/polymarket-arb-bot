import os
import time
import hmac
import hashlib
import base64
import json
import aiohttp
from eth_account import Account
from eth_account.messages import encode_defunct

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
    secret_bytes = base64.b64decode(api_secret)
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
    """
    Execute a pure arb trade — buy both UP and DOWN sides.
    Uses FOK (fill or kill) — if either leg fails, we don't execute.
    Returns result dict with both leg outcomes.
    """
    config = get_trading_config()
    token_ids = market.get("token_ids", [])

    if len(token_ids) < 2:
        return {"error": "Missing token IDs"}

    up_token = token_ids[0]
    down_token = token_ids[1]
    up_price = market.get("up_ask", 0.5)
    down_price = market.get("down_ask", 0.5)
    up_cost = round(up_price * shares, 4)
    down_cost = round(down_price * shares, 4)

    if config["dry_run"]:
        print(
            f"[Trading] DRY RUN — Arb trade:\n"
            f"  BUY {shares} UP shares @ ${up_price} = ${up_cost}\n"
            f"  BUY {shares} DOWN shares @ ${down_price} = ${down_cost}\n"
            f"  Total: ${up_cost + down_cost:.4f}"
        )
        return {
            "status": "dry_run",
            "up_leg": {"status": "simulated", "cost": up_cost},
            "down_leg": {"status": "simulated", "cost": down_cost},
            "total_cost": up_cost + down_cost,
        }

    # check balance first
    balance = await get_balance()
    total_cost = up_cost + down_cost

    if balance < total_cost:
        print(f"[Trading] Insufficient balance: ${balance:.4f} < ${total_cost:.4f}")
        return {"error": "insufficient_balance", "balance": balance, "needed": total_cost}

    print(f"[Trading] Executing arb — Balance: ${balance:.4f} | Cost: ${total_cost:.4f}")

    # place both legs
    up_result = await place_market_order(up_token, "BUY", up_cost)
    down_result = await place_market_order(down_token, "BUY", down_cost)

    success = (
        up_result.get("status") not in ["error", None] and
        down_result.get("status") not in ["error", None] and
        "error" not in up_result and
        "error" not in down_result
    )

    result = {
        "status": "executed" if success else "failed",
        "up_leg": up_result,
        "down_leg": down_result,
        "total_cost": total_cost,
    }

    if success:
        print(f"[Trading] ✅ Both legs filled — Total: ${total_cost:.4f}")
    else:
        print(f"[Trading] ❌ One or both legs failed — Up: {up_result} | Down: {down_result}")

    return result