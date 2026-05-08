"""
WebSocket feed for real-time orderbook updates from Polymarket CLOB.
Replaces the 30-second polling loop for Strategy 3 (realtime intramarket).

Polymarket WebSocket docs:
- URL: wss://ws-subscriptions-clob.polymarket.com/ws/market
- Subscribe by sending asset_ids (token IDs for UP and DOWN sides)
- Receives price_change and book events in real time
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Callable, Optional

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RECONNECT_DELAY = 5   # seconds before reconnecting after disconnect
MAX_RECONNECTS = 999  # effectively infinite


# In-memory orderbook state per token
# token_id -> {"best_bid": float, "best_ask": float, "last_update": float}
_orderbooks: dict[str, dict] = {}

# token_id -> market info (so we know which market/asset this token belongs to)
_token_to_market: dict[str, dict] = {}


def register_markets(markets: list[dict]):
    """
    Register active markets so we know which token IDs to subscribe to
    and which market each token belongs to.
    Markets must have clobTokenIds in their data.
    """
    _token_to_market.clear()
    for market in markets:
        token_ids_raw = market.get("clob_token_ids", "[]")
        try:
            token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        except Exception:
            continue

        if len(token_ids) < 2:
            continue

        up_token = token_ids[0]
        down_token = token_ids[1]

        _token_to_market[up_token] = {**market, "side": "UP", "paired_token": down_token}
        _token_to_market[down_token] = {**market, "side": "DOWN", "paired_token": up_token}

    print(f"[WebSocket] Registered {len(_token_to_market)} tokens for {len(markets)} markets")


def get_current_prices(market: dict) -> Optional[tuple[float, float]]:
    """
    Get current best ask prices for UP and DOWN sides of a market.
    Returns (up_ask, down_ask) or None if not available.
    """
    token_ids_raw = market.get("clob_token_ids", "[]")
    try:
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
    except Exception:
        return None

    if len(token_ids) < 2:
        return None

    up_book = _orderbooks.get(token_ids[0])
    down_book = _orderbooks.get(token_ids[1])

    if not up_book or not down_book:
        return None

    up_ask = up_book.get("best_ask")
    down_ask = down_book.get("best_ask")

    if not up_ask or not down_ask:
        return None

    return (up_ask, down_ask)


def _process_book_event(data: dict):
    """Process a full book snapshot event."""
    asset_id = data.get("asset_id")
    if not asset_id:
        return

    bids = data.get("bids", [])
    asks = data.get("asks", [])

    best_bid = max((float(b["price"]) for b in bids), default=None) if bids else None
    best_ask = min((float(a["price"]) for a in asks), default=None) if asks else None

    _orderbooks[asset_id] = {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "last_update": time.time(),
    }


def _process_price_change(data: dict):
    """Process a price change event."""
    # price_change uses 'market' not 'asset_id', and 'price_changes' not 'changes'
    market_id = data.get("market")
    price_changes = data.get("price_changes", [])

    for change in price_changes:
        asset_id = change.get("asset_id")
        if not asset_id:
            continue

        if asset_id not in _orderbooks:
            _orderbooks[asset_id] = {
                "best_bid": None,
                "best_ask": None,
                "last_update": time.time()
            }

        side = change.get("side", "").upper()
        price = float(change.get("price", 0))
        size = float(change.get("size", 0))
        current = _orderbooks[asset_id]

        if side == "BUY":
            if size == 0:
                if current.get("best_bid") == price:
                    current["best_bid"] = None
            else:
                if current.get("best_bid") is None or price > current["best_bid"]:
                    current["best_bid"] = price
        elif side == "SELL":
            if size == 0:
                if current.get("best_ask") == price:
                    current["best_ask"] = None
            else:
                if current.get("best_ask") is None or price < current["best_ask"]:
                    current["best_ask"] = price

        current["last_update"] = time.time()
        return asset_id  # return so caller can notify


async def run_websocket(
    markets: list[dict],
    on_price_update: Callable,
    stop_event: asyncio.Event = None,
):
    """
    Main WebSocket loop.
    
    Connects to Polymarket, subscribes to all active 5m market tokens,
    processes incoming events, and calls on_price_update when prices change.
    
    on_price_update(market, up_ask, down_ask) is called with updated prices.
    Reconnects automatically on disconnect.
    """
    import aiohttp

    register_markets(markets)
    token_ids = list(_token_to_market.keys())

    if not token_ids:
        print("[WebSocket] No tokens to subscribe to")
        return

    reconnect_count = 0

    while reconnect_count < MAX_RECONNECTS:
        if stop_event and stop_event.is_set():
            break

        try:
            print(f"[WebSocket] Connecting... (attempt {reconnect_count + 1})")

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_URL,
                    heartbeat=30,
                    receive_timeout=60,
                ) as ws:
                    print(f"[WebSocket] Connected. Subscribing to {len(token_ids)} tokens...")

                    # subscribe to all token orderbooks
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": "market",
                        "auth": {},
                        "assets_ids": token_ids,
                    }
                    await ws.send_str(json.dumps(subscribe_msg))
                    reconnect_count = 0  # reset on successful connect

                    async for msg in ws:
                        if stop_event and stop_event.is_set():
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                # Polymarket sends single object or array
                                raw = msg.data
                                if raw.startswith("["):
                                    events = json.loads(raw)
                                else:
                                    events = [json.loads(raw)]

                                for event in events:
                                    event_type = event.get("event_type", "")

                                    if event_type == "book":
                                        _process_book_event(event)
                                        asset_id = event.get("asset_id")
                                        if asset_id:
                                            await _notify_update(asset_id, on_price_update)

                                    elif event_type == "price_change":
                                        price_changes = event.get("price_changes", [])
                                        for change in price_changes:
                                            asset_id = change.get("asset_id")
                                            _process_price_change(event)
                                            if asset_id:
                                                await _notify_update(asset_id, on_price_update)

                            except json.JSONDecodeError:
                                pass
                            except Exception as e:
                                print(f"[WebSocket] Event processing error: {e}")

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"[WebSocket] Error: {ws.exception()}")
                            break

                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            print("[WebSocket] Connection closed by server")
                            break

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[WebSocket] Connection error: {e}")

        reconnect_count += 1
        if reconnect_count < MAX_RECONNECTS:
            print(f"[WebSocket] Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

    print("[WebSocket] Loop ended")


async def _notify_update(asset_id: str, on_price_update: Callable):
    """When a token updates, check if both sides of its market are ready and notify."""
    if not asset_id or asset_id not in _token_to_market:
        return

    market_info = _token_to_market[asset_id]
    paired_token = market_info.get("paired_token")

    my_book = _orderbooks.get(asset_id)
    paired_book = _orderbooks.get(paired_token) if paired_token else None

    if not my_book or not paired_book:
        return

    side = market_info.get("side")
    if side == "UP":
        up_ask = my_book.get("best_ask")
        down_ask = paired_book.get("best_ask")
    else:
        up_ask = paired_book.get("best_ask")
        down_ask = my_book.get("best_ask")

    if up_ask and down_ask:
        try:
            await on_price_update(market_info, up_ask, down_ask)
        except Exception as e:
            print(f"[WebSocket] on_price_update error: {e}")