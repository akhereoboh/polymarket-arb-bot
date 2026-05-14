#!/root/polymarket-arb-bot/trading_venv/bin/python3
import sys
import os
import json
from dotenv import load_dotenv
load_dotenv('/root/polymarket-arb-bot/.env')

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs

def execute_arb(up_token: str, down_token: str, up_price: float, down_price: float, size: int = 5):
    client = ClobClient(
        'https://clob.polymarket.com',
        key=os.getenv('POLYMARKET_PRIVATE_KEY'),
        chain_id=137,
        signature_type=1,
        funder=os.getenv('POLYMARKET_FUNDER')
    )
    client.set_api_creds(client.create_or_derive_api_creds())

    try:
        up_result = client.create_and_post_order(
            OrderArgs(token_id=up_token, price=up_price, size=size, side="BUY")
        )
        down_result = client.create_and_post_order(
            OrderArgs(token_id=down_token, price=down_price, size=size, side="BUY")
        )
        print(json.dumps({
            "status": "success",
            "up": up_result,
            "down": down_result
        }))
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))

if __name__ == "__main__":
    args = json.loads(sys.argv[1])
    execute_arb(
        args["up_token"],
        args["down_token"],
        args["up_price"],
        args["down_price"],
        args.get("size", 5)
    )