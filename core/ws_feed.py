import asyncio
import json
import os
import websockets
from core.polymarket import get_markets_with_orderbook
from utils.db import log_arb_trade
from core.scanner import format_arb_alert

ARB_THRESHOLD = float(os.getenv("ARB_THRESHOLD", "0.991"))
SHARES = int(os.getenv("ORDER_SIZE", "5"))

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# in-memory price book — token_id -> best price
_price_book: dict[str, float] = {}

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
    markets = await get_markets_with_orderbook()
    for m in markets:
        condition_id = m["condition_id"]
        token_ids = m.get("token_ids", [])
        _market_map[condition_id] = m
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
    """
    Get current up/down prices for a market from the price book.
    Returns (up_price, down_price) or (None, None).
    """
    market = _market_map.get(condition_id)
    if not market:
        return None, None

    token_ids = market.get("token_ids", [])
    if len(token_ids) < 2:
        return None, None

    up_price = _price_book.get(token_ids[0])
    down_price = _price_book.get(token_ids[1])
    return up_price, down_price


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
                best_ask = float(asks[0]["price"])
                _price_book[asset_id] = best_ask

        elif event_type == "price_change":
            # price update
            changes = event.get("changes", [])
            for change in changes:
                side = change.get("side", "")
                price = change.get("price")
                if side == "ASK" and price:
                    _price_book[asset_id] = float(price)
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