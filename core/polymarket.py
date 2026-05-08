import aiohttp
import asyncio
import json
from typing import Optional
from datetime import datetime, timezone

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

ASSETS = {
    "btc": "BTC",
    "eth": "ETH",
    "sol": "SOL",
    "xrp": "XRP",
    "doge": "DOGE",
    "bnb": "BNB",
}

TIMEFRAMES = ["15m", "5m"]

async def fetch_active_updown_markets(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch all active 15m updown markets."""
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

                # only 15m timeframe
                if not any(f"-{tf}-" in slug for tf in TIMEFRAMES):
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

                # parse end time
                end_date = e.get("endDate") or market.get("endDate")

                results.append({
                    "condition_id": condition_id,
                    "question": e.get("title", ""),
                    "slug": slug,
                    "asset": ASSETS[asset_key],
                    "timeframe": "15m",
                    "end_date": end_date,
                    "event_id": e.get("id"),
                })

            return results

    except Exception as ex:
        print(f"[Polymarket] fetch markets error: {ex}")
        return []


async def fetch_clob_orderbook_prices(
    session: aiohttp.ClientSession,
    condition_id: str
) -> Optional[dict]:
    """
    Fetch live ask prices from CLOB orderbook.
    We use ASK prices because that's what you pay when buying.
    Returns up_ask and down_ask.
    """
    try:
        # fetch the market data which includes tokens
        async with session.get(
            f"{CLOB_BASE}/markets/{condition_id}",
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        tokens = data.get("tokens", [])
        if len(tokens) < 2:
            return None

        up_ask = None
        down_ask = None

        for token in tokens:
            outcome = token.get("outcome", "").lower()
            token_id = token.get("token_id")
            if not token_id:
                continue

            # fetch orderbook for this token
            async with session.get(
                f"{CLOB_BASE}/book",
                params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as book_resp:
                if book_resp.status != 200:
                    continue
                book = await book_resp.json()

            # best ask = lowest sell price = what you pay to buy
            asks = book.get("asks", [])
            if not asks:
                # fallback to market price
                price = token.get("price")
                if price:
                    if outcome == "up":
                        up_ask = float(price)
                    elif outcome == "down":
                        down_ask = float(price)
                continue

            # asks are sorted ascending — first is best (lowest)
            best_ask = float(asks[0]["price"])

            if outcome == "up":
                up_ask = best_ask
            elif outcome == "down":
                down_ask = best_ask

        if up_ask is None or down_ask is None:
            return None

        total = round(up_ask + down_ask, 4)

        return {
            "up_ask": up_ask,
            "down_ask": down_ask,
            "total": total,
            "gap": round(1.0 - total, 4),
        }

    except Exception as ex:
        print(f"[Polymarket] orderbook error {condition_id}: {ex}")
        return None


async def get_markets_with_orderbook() -> list[dict]:
    """
    Main entry point.
    Returns active 15m markets with live orderbook ask prices.
    """
    async with aiohttp.ClientSession() as session:
        markets = await fetch_active_updown_markets(session)
        if not markets:
            return []

        # fetch orderbook prices for all markets in parallel
        price_tasks = [
            fetch_clob_orderbook_prices(session, m["condition_id"])
            for m in markets
        ]
        prices_list = await asyncio.gather(*price_tasks, return_exceptions=True)

        enriched = []
        for market, prices in zip(markets, prices_list):
            if isinstance(prices, dict):
                market.update(prices)
                enriched.append(market)

        return enriched