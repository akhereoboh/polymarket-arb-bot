import aiohttp
import asyncio
import json
from typing import Optional

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

# Focus on most liquid markets only
ASSETS = {
    "btc": "BTC",
    "eth": "ETH",
}

TIMEFRAMES = ["15m", "5m"]


async def fetch_active_updown_markets(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch active updown markets using embedded prices from gamma API.
    Much more reliable than orderbook for price discovery.
    """
    try:
        async with session.get(
            f"{GAMMA_BASE}/events",
            params={
                "active": "true",
                "closed": "false",
                "limit": 100,
                "order": "createdAt",
                "ascending": "false"
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                print(f"[Polymarket] API error: {resp.status}")
                return []

            events = await resp.json()
            if not isinstance(events, list):
                return []

            results = []

            for e in events:
                slug = e.get("slug", "")
                if not e.get("active") or e.get("closed"):
                    continue
                if "updown" not in slug:
                    continue

                # check timeframe
                timeframe = None
                for tf in TIMEFRAMES:
                    if f"-{tf}-" in slug:
                        timeframe = tf
                        break
                if not timeframe:
                    continue

                # check asset
                asset_key = None
                for key in ASSETS:
                    if slug.startswith(f"{key}-updown"):
                        asset_key = key
                        break
                if not asset_key:
                    continue

                markets = e.get("markets", [])
                if not markets:
                    continue

                market = markets[0]
                condition_id = market.get("conditionId")
                if not condition_id:
                    continue

                # extract embedded prices directly from gamma API
                try:
                    outcome_prices = json.loads(market.get("outcomePrices", "[]"))
                    outcomes = json.loads(market.get("outcomes", "[]"))
                    token_ids = json.loads(market.get("clobTokenIds", "[]"))
                except Exception:
                    continue

                if not outcome_prices or not outcomes:
                    continue

                up_price = None
                down_price = None
                for label, price in zip(outcomes, outcome_prices):
                    if label.lower() == "up":
                        up_price = float(price)
                    elif label.lower() == "down":
                        down_price = float(price)

                if up_price is None or down_price is None:
                    continue

                total = round(up_price + down_price, 4)
                gap = round(1.0 - total, 4)

                results.append({
                    "condition_id": condition_id,
                    "question": e.get("title", ""),
                    "slug": slug,
                    "asset": ASSETS[asset_key],
                    "timeframe": timeframe,
                    "end_date": e.get("endDate") or market.get("endDate"),
                    "event_id": e.get("id"),
                    "up_ask": up_price,
                    "down_ask": down_price,
                    "total": total,
                    "gap": gap,
                    "token_ids": token_ids,
                    "liquidity": market.get("liquidityNum", 0),
                })

            print(f"[Polymarket] Active markets found: {len(results)}")
            return results

    except Exception as ex:
        print(f"[Polymarket] fetch error: {ex}")
        return []


async def get_markets_with_orderbook() -> list[dict]:
    """
    Main entry point.
    Returns active updown markets with embedded prices.
    No separate orderbook call needed for price discovery.
    """
    async with aiohttp.ClientSession() as session:
        return await fetch_active_updown_markets(session)


async def fetch_clob_orderbook(token_id: str) -> Optional[dict]:
    """
    Fetch live orderbook for a specific token.
    Used when we need real-time ask depth for execution.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{CLOB_BASE}/book",
                params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                asks = data.get("asks", [])
                bids = data.get("bids", [])
                best_ask = float(asks[0]["price"]) if asks else None
                best_bid = float(bids[0]["price"]) if bids else None
                return {
                    "best_ask": best_ask,
                    "best_bid": best_bid,
                    "asks": asks[:5],
                    "bids": bids[:5],
                }
    except Exception as ex:
        print(f"[Polymarket] orderbook error {token_id}: {ex}")
        return None