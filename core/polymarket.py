import aiohttp
import asyncio
from typing import Optional

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Assets we monitor and their Binance symbols for price feed
ASSETS = {
    "btc": "BTC",
    "eth": "ETH",
    "sol": "SOL",
    "xrp": "XRP",
    "doge": "DOGE",
    "bnb": "BNB",
}

ALLOWED_TIMEFRAMES = ["5m", "15m", "4h"]

async def fetch_active_updown_markets(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch active up/down markets using the confirmed gamma events API.
    Prices are embedded in the response — no second API call needed.
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
                events = []

            results = []

            for e in events:
                slug = e.get("slug", "")
                active = e.get("active", False)
                closed = e.get("closed", True)

                if not active or closed:
                    continue

                # must be an updown market
                if "updown" not in slug:
                    continue

                # check asset
                asset_key = None
                for key in ASSETS:
                    if slug.startswith(f"{key}-updown"):
                        asset_key = key
                        break
                if not asset_key:
                    continue

                # check timeframe
                timeframe = None
                for tf in ALLOWED_TIMEFRAMES:
                    if f"-{tf}-" in slug:
                        timeframe = tf
                        break
                if not timeframe:
                    continue

                # get market data — prices are embedded
                markets = e.get("markets", [])
                if not markets:
                    continue

                market = markets[0]
                outcome_prices = market.get("outcomePrices", "[]")
                outcomes = market.get("outcomes", "[]")

                # parse embedded prices
                try:
                    import json
                    prices = json.loads(outcome_prices)
                    outcome_labels = json.loads(outcomes)
                except Exception:
                    continue

                if len(prices) < 2 or len(outcome_labels) < 2:
                    continue

                # map Up/Down to yes/no equivalent
                # map Up/Down to yes/no equivalent
                up_price = None
                down_price = None
                for label, price in zip(outcome_labels, prices):
                    if label.lower() == "up":
                        up_price = float(price)
                    elif label.lower() == "down":
                        down_price = float(price)

                if up_price is None or down_price is None:
                    continue

                # use bestAsk for arb calculation — that's the real buy price
                best_bid = market.get("bestBid")
                best_ask = market.get("bestAsk")

                # for arb: we need ask prices for both sides
                # bestAsk is for the UP token — DOWN ask is approximately 1 - bestBid
                up_ask = float(best_ask) if best_ask else up_price
                down_ask = round(1.0 - float(best_bid), 4) if best_bid else down_price

                results.append({
                    "id": market.get("conditionId"),
                    "condition_id": market.get("conditionId"),
                    "question": e.get("title", ""),
                    "slug": slug,
                    "asset": ASSETS[asset_key],
                    "timeframe": timeframe,
                    "end_date": e.get("endDate"),
                    "yes_price": up_price,      # mid price for momentum strategy
                    "no_price": down_price,     # mid price for momentum strategy
                    "up_ask": up_ask,           # actual buy price for arb
                    "down_ask": down_ask,       # actual buy price for arb
                    "spread": round(up_price + down_price - 1.0, 4),
                    "liquidity": market.get("liquidityNum", 0),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "clob_token_ids": market.get("clobTokenIds", "[]"),
                })

            print(f"[Polymarket] Active updown markets: {len(results)}")
            for m in results:
                print(f"  {m['question']} | Up:{m['yes_price']} Down:{m['no_price']}")

            return results

    except Exception as e:
        print(f"[Polymarket] Error: {e}")
        return []


async def get_markets_with_prices() -> list[dict]:
    """Main entry point — returns all active updown markets with prices."""
    async with aiohttp.ClientSession() as session:
        return await fetch_active_updown_markets(session)


# kept for auto-close compatibility
async def fetch_prices(session: aiohttp.ClientSession, condition_id: str) -> Optional[dict]:
    """Fetch latest prices for an existing open trade."""
    try:
        from utils.db import get_open_trades
        # re-fetch from events to get updated prices
        markets = await fetch_active_updown_markets(session)
        for m in markets:
            if m["condition_id"] == condition_id:
                return {
                    "yes_price": m["yes_price"],
                    "no_price": m["no_price"],
                }
        return None
    except Exception as e:
        print(f"[Polymarket] fetch_prices error: {e}")
        return None