import aiohttp
import asyncio

import os
from typing import Optional

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

_anthropic_client = None



async def classify_markets_with_claude(markets: list[dict], asset: str) -> list[dict]:
    if not markets:
        return []

    asset_label = "Bitcoin (BTC)" if asset == "BTC" else "Ethereum (ETH)"
    questions_text = "\n".join([
        f"{i+1}. {m['question']}"
        for i, m in enumerate(markets)
    ])

    prompt = f"""You are filtering Polymarket prediction market questions.

I need ONLY markets asking whether {asset_label} price will be HIGHER or LOWER than a specific price level at a specific future time.

Valid examples:
- "Will BTC be above $80,000 on May 15?"
- "Will Bitcoin close above $75,000 this week?"
- "Will ETH be higher than $2,500 on Friday?"
- "Will Bitcoin be higher than its current price on Sunday?"

Invalid examples:
- "Will Bitcoin hit $1 million before GTA VI?" (milestone not direction)
- "Will MegaETH do an airdrop?" (not a price question)
- "Will Netherlands win the World Cup?" (wrong asset)
- "Will Bitcoin ETF get approved?" (event not price)

Markets to evaluate:
{questions_text}

Reply with ONLY the numbers of valid price direction markets, comma separated.
If none are valid reply with: NONE
Example: 1, 3, 7"""

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("[Claude] No API key found, skipping classification")
        return markets

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 100,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    print(f"[Claude] API error {resp.status}")
                    return markets
                data = await resp.json()
                response = data["content"][0]["text"].strip()
                print(f"[Claude] Classification: {response}")

                if response == "NONE" or not response:
                    return []

                valid_indices = []
                for part in response.split(","):
                    part = part.strip()
                    if part.isdigit():
                        idx = int(part) - 1
                        if 0 <= idx < len(markets):
                            valid_indices.append(idx)

                valid_markets = [markets[i] for i in valid_indices]
                print(f"[Claude] {len(valid_markets)} valid markets")
                for m in valid_markets:
                    print(f"  ✓ {m['question']}")
                return valid_markets

    except Exception as e:
        print(f"[Claude] Classification error: {e}")
        return markets


async def fetch_active_markets(session: aiohttp.ClientSession, asset: str) -> list[dict]:
    search_terms = ["bitcoin"] if asset == "BTC" else ["ethereum"]
    results = []

    for keyword in search_terms:
        try:
            async with session.get(
                f"{GAMMA_BASE}/markets",
                params={"active": "true", "closed": "false", "limit": 100, "keyword": keyword},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    print(f"[Polymarket] HTTP {resp.status} for {keyword}")
                    continue
                data = await resp.json()
                markets = data if isinstance(data, list) else data.get("markets", [])
                print(f"[Polymarket] '{keyword}' returned {len(markets)} raw markets")

                for m in markets:
                    if not m.get("conditionId"):
                        continue
                    results.append({
                        "id": m.get("id"),
                        "condition_id": m.get("conditionId"),
                        "question": m.get("question"),
                        "asset": asset.upper(),
                        "end_date": m.get("endDate"),
                    })

        except Exception as e:
            print(f"[Polymarket] Error fetching {keyword}: {e}")

    # deduplicate
    seen, unique = set(), []
    for m in results:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique.append(m)

    print(f"[Polymarket] {asset} raw unique markets: {len(unique)}")
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
        # step 1 — fetch all raw markets
        all_markets = await fetch_active_markets(session, asset)
        if not all_markets:
            return []

        # step 2 — Claude filters to only genuine direction markets
        direction_markets = await classify_markets_with_claude(all_markets, asset)
        if not direction_markets:
            return []

        # step 3 — fetch live YES/NO prices for valid markets
        prices_list = await asyncio.gather(*[
            fetch_prices(session, m["condition_id"])
            for m in direction_markets
        ], return_exceptions=True)

        enriched = []
        for market, prices in zip(direction_markets, prices_list):
            if isinstance(prices, dict):
                market.update(prices)
                enriched.append(market)

        print(f"[Polymarket] {asset} final markets with prices: {len(enriched)}")
        return enriched