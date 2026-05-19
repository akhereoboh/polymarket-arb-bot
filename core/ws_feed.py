import asyncio
import json
import os
import websockets
from core.polymarket import get_markets_with_orderbook
from utils.db import log_arb_trade
from core.scanner import format_arb_alert
from core.trading import execute_arb_trade
import websocket as ws_client
import threading

ARB_THRESHOLD = float(os.getenv("ARB_THRESHOLD", "0.991"))
SHARES = int(os.getenv("ORDER_SIZE", "5"))

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# in-memory price book — token_id -> best price
_price_book: dict[str, dict] = {}

# market metadata — condition_id -> market info
_market_map: dict[str, dict] = {}

# token_id -> condition_id mapping
_token_to_market: dict[str, str] = {}

# already traded this session
_traded: set = set()


async def build_market_map():
    """
    Fetch all active markets and build lookup maps.
    Called once on startup and periodically to catch new markets.
    """
    global _market_map, _token_to_market

    markets = await get_markets_with_orderbook()

    # clear old maps before rebuilding to prevent accumulation
    _market_map.clear()
    _token_to_market.clear()

    for m in markets:
        condition_id = m["condition_id"]
        token_ids = m.get("token_ids", [])
        _market_map[condition_id] = m
        m['condition_id'] = condition_id  # ensure condition_id is in market dict
        for tid in token_ids:
            _token_to_market[tid] = condition_id
            # seed price book with embedded prices
            outcomes = ["up", "down"]
            prices = [m.get("up_ask", 0.5), m.get("down_ask", 0.5)]
            for outcome, price in zip(outcomes, prices):
                _price_book[tid] = price

    print(f"[WS] Market map built — {len(_market_map)} markets, {len(_token_to_market)} tokens")
    return list(_token_to_market.keys())


def get_current_prices(condition_id: str) -> tuple:
    market = _market_map.get(condition_id)
    if not market:
        return None, None
    token_ids = market.get("token_ids", [])
    if len(token_ids) < 2:
        return None, None

    def extract_price(entry):
        if isinstance(entry, dict):
            return entry.get("price")
        return entry  # already a float

    up_price = extract_price(_price_book.get(token_ids[0]))
    down_price = extract_price(_price_book.get(token_ids[1]))
    return up_price, down_price

def get_asks_from_book(token_id: str) -> list:
    entry = _price_book.get(token_id, {})
    if isinstance(entry, dict):
        return entry.get("asks", [])
    return []


async def check_arb(condition_id: str, send_alert_fn=None):
    """
    Check if current prices create an arb opportunity.
    Called every time a price update arrives.
    """
    if condition_id in _traded:
        return

    up_price, down_price = get_current_prices(condition_id)
    if up_price is None or down_price is None:
        return

    total = round(up_price + down_price, 4)
    gap = round(1.0 - total, 4)

    if total > ARB_THRESHOLD:
        return

    # arb found
    _traded.add(condition_id)
    market = _market_map[condition_id]
    market['condition_id'] = condition_id
    total_invested = round(total * SHARES, 4)
    expected_payout = float(SHARES)
    expected_profit = round(expected_payout - total_invested, 4)
    profit_pct = round(expected_profit / total_invested * 100, 4)

    opportunity = {
        "asset": market["asset"],
        "market_question": market["question"],
        "market_id": condition_id,
        "slug": market["slug"],
        "timeframe": market["timeframe"],
        "up_price": up_price,
        "down_price": down_price,
        "total_cost": total,
        "arb_profit": gap,
        "shares": SHARES,
        "total_invested": total_invested,
        "expected_payout": expected_payout,
        "expected_profit": expected_profit,
        "profit_pct": profit_pct,
        "market_end_time": market.get("end_date"),
    }

    print(
        f"[WS-ARB] 🎯 OPPORTUNITY → {market['asset']} | "
        f"UP:{up_price} + DOWN:{down_price} = {total} | "
        f"Profit: ${expected_profit}"
    )

    trade_result = await execute_arb_trade(market, SHARES)
    if trade_result.get("status") != "executed":
        print(f'[WS] Trade not executed — status: {trade_result.get("status")} — skipping log')
        return
    await log_arb_trade(opportunity)

    if send_alert_fn:
        try:
            await send_alert_fn(format_arb_alert(opportunity))
        except Exception as e:
            print(f"[WS] Telegram error: {e}")





def process_book_update(data: dict, send_alert_fn=None) -> list:
    """
    Process a book snapshot or price_change event from WebSocket.
    Updates the price book and checks for arb.
    """
    tasks = []

    # handle both single object and list
    events = data if isinstance(data, list) else [data]

    for event in events:
        event_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")  # this is the token_id

        if not asset_id or asset_id not in _token_to_market:
            continue

        condition_id = _token_to_market[asset_id]

        if event_type == "book":
            # full book snapshot
            asks = event.get("asks", [])
            if asks:
                _price_book[asset_id] = {
                    "price": float(asks[0]["price"]),
                    "asks": asks  # keep full list
                }

        elif event_type == "price_change":
            changes = event.get("changes", [])
            for change in changes:
                side = change.get("side", "")
                price = change.get("price")
                if side == "ASK" and price:
                    # preserve existing asks list, just update price
                    existing = _price_book.get(asset_id, {})
                    existing_asks = existing.get("asks", []) if isinstance(existing, dict) else []
                    _price_book[asset_id] = {
                        "price": float(price),
                        "asks": existing_asks
                    }
                    break

        tasks.append(condition_id)

    return list(set(tasks))


async def ws_listener(send_alert_fn=None):
    """
    Main WebSocket listener loop.
    Connects, subscribes to all active markets, processes updates.
    Reconnects automatically on disconnect.
    """
    while True:
        try:
            print("[WS] Building market map...")
            token_ids = await build_market_map()

            if not token_ids:
                print("[WS] No token IDs found, retrying in 30s...")
                await asyncio.sleep(30)
                continue

            print(f"[WS] Connecting to {WS_URL}...")

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
            ) as ws:
                print("[WS] Connected")

                # subscribe to all token order books
                sub_message = {
                    "type": "market",
                    "assets_ids": token_ids,
                }
                await ws.send(json.dumps(sub_message))
                print(f"[WS] Subscribed to {len(token_ids)} tokens")

                # listen for updates
                async for message in ws:
                    try:
                        data = json.loads(message)
                        condition_ids = process_book_update(data, send_alert_fn)

                        # check arb for any updated markets
                        for cid in condition_ids:
                            await check_arb(cid, send_alert_fn)

                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"[WS] Message error: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[WS] Connection closed: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[WS] Error: {e} — reconnecting in 10s...")
            await asyncio.sleep(10)


async def refresh_market_map_loop():
    """
    Refresh market map every 10 minutes to catch new markets.
    New 5m and 15m markets open constantly.
    """
    while True:
        await asyncio.sleep(600)
        print("[WS] Refreshing market map...")
        await build_market_map()

# shared dict to track pending order IDs and their fill status
_pending_orders: dict[str, dict] = {}  # order_id -> {side, token_id, filled}

def register_pending_orders(up_order_id: str, down_order_id: str, condition_id: str):
    """Register GTC orders to watch for fills via user WebSocket."""
    _pending_orders[up_order_id] = {'side': 'up', 'condition_id': condition_id, 'filled': False}
    _pending_orders[down_order_id] = {'side': 'down', 'condition_id': condition_id, 'filled': False}

def get_fill_status(up_order_id: str, down_order_id: str) -> tuple:
    """Check if both orders are filled."""
    up = _pending_orders.get(up_order_id, {}).get('filled', False)
    down = _pending_orders.get(down_order_id, {}).get('filled', False)
    return up, down

def clear_pending_orders(up_order_id: str, down_order_id: str):
    """Remove orders from tracking."""
    _pending_orders.pop(up_order_id, None)
    _pending_orders.pop(down_order_id, None)

async def user_ws_listener():
    """
    User WebSocket listener for real-time fill notifications.
    Runs alongside the market WebSocket.
    """
    

    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    sub_msg = {
        "auth": {
            "apiKey": os.getenv("POLYMARKET_API_KEY"),
            "secret": os.getenv("POLYMARKET_API_SECRET"),
            "passphrase": os.getenv("POLYMARKET_API_PASSPHRASE"),
        },
        "type": "user",
        "markets": [],
        "assets_ids": [],
        "initial_dump": True,
        "subscriptions": ["balance", "trade"]
    }

    def on_open(ws):
        print("[UserWS] Connected")
        ws.send(json.dumps(sub_msg))

    def on_message(ws, message):
        try:
            data = json.loads(message)
            events = data if isinstance(data, list) else [data]
            for event in events:
                order_id = event.get('id') or event.get('order_id') or event.get('orderID')
                event_type = event.get('type', '')
                if order_id and order_id in _pending_orders:
                    if event_type in ('trade', 'order_filled', 'TRADE', 'FILLED'):
                        _pending_orders[order_id]['filled'] = True
                        side = _pending_orders[order_id]['side']
                        print(f"[UserWS] ✅ {side.upper()} order filled: {order_id[:20]}...")
        except Exception as e:
            print(f"[UserWS] Message error: {e}")

    def on_error(ws, error):
        print(f"[UserWS] Error: {error}")

    def on_close(ws, code, reason):
        print(f"[UserWS] Closed: {code} {reason} — will reconnect")

    while True:
        try:
            print("[UserWS] Connecting...")
            ws = ws_client.WebSocketApp(
                uri,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            # run in executor to avoid blocking asyncio
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, ws.run_forever)
        except Exception as e:
            print(f"[UserWS] Exception: {e}")
        await asyncio.sleep(5)