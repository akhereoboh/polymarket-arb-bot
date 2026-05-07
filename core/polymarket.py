"""This file talks to Polymarket's API. 
Its only job is to find active BTC and ETH direction markets and return their live YES/NO prices."""


import aiohttp
import asyncio
from typing import Optional

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

ASSETS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
}

DIRECTION_PHRASES = [
    "higher", "up", "above", "increase", "rise",
    "lower", "down", "below", "decrease", "fall"
]


async def fetch_active_markets(session: aiohttp.ClientSession, asset: str) -> list[dict]:
    keywords = ASSETS.get(asset.upper(), [])
    results = []

    for keyword in keywords:
        try:
            async with session.get(
                f"{GAMMA_BASE}/markets",
                params={"active": "true", "closed": "false", "limit": 50, "keyword": keyword},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
                markets = data if isinstance(data, list) else data.get("markets", [])

                for m in markets:
                    question = m.get("question", "").lower()
                    if not any(p in question for p in DIRECTION_PHRASES):
                        continue
                    if not any(kw in question for kw in keywords):
                        continue
                    results.append({
                        "id": m.get("id"),
                        "condition_id": m.get("conditionId"),
                        "question": m.get("question"),
                        "asset": asset.upper(),
                        "end_date": m.get("endDate"),
                        "tokens": m.get("tokens", []),
                    })

        except Exception as e:
            print(f"[Polymarket] Error fetching {asset}: {e}")

    seen, unique = set(), []
    for m in results:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique.append(m)
    return unique


async def fetch_prices(session: aiohttp.ClientSession, condition_id: str) -> Optional[dict]:
    try:
        async with session.get(
            f"{CLOB_BASE}/markets/{condition_id}",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            yes_price, no_price = None, None

            for token in data.get("tokens", []):
                outcome = token.get("outcome", "").upper()
                price = token.get("price")
                if price is None:
                    continue
                if outcome == "YES":
                    yes_price = float(price)
                elif outcome == "NO":
                    no_price = float(price)

            if yes_price is None or no_price is None:
                return None

            return {
                "yes_price": yes_price,
                "no_price": no_price,
                "spread": round(yes_price + no_price - 1.0, 4),
            }
    except Exception as e:
        print(f"[Polymarket] Price fetch error {condition_id}: {e}")
        return None


async def get_markets_with_prices(asset: str) -> list[dict]:
    async with aiohttp.ClientSession() as session:
        markets = await fetch_active_markets(session, asset)
        if not markets:
            return []

        prices_list = await asyncio.gather(*[
            fetch_prices(session, m["condition_id"])
            for m in markets if m.get("condition_id")
        ], return_exceptions=True)

        enriched = []
        for market, prices in zip(markets, prices_list):
            if isinstance(prices, dict):
                market.update(prices)
                enriched.append(market)
        return enriched