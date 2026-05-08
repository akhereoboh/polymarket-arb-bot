"""
Price feed using CryptoCompare — close enough to Chainlink for our purposes.
Chainlink itself doesn't have a free public REST API.
"""
import aiohttp
import asyncio
from typing import Optional

CRYPTOCOMPARE_BASE = "https://min-api.cryptocompare.com/data"

SYMBOLS = {
    "BTC": "BTC",
    "ETH": "ETH",
    "SOL": "SOL",
    "XRP": "XRP",
    "DOGE": "DOGE",
    "BNB": "BNB",
}


async def get_price(asset: str) -> Optional[float]:
    symbol = SYMBOLS.get(asset.upper())
    if not symbol:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{CRYPTOCOMPARE_BASE}/price",
                params={"fsym": symbol, "tsyms": "USD"},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return float(data.get("USD", 0)) or None
    except Exception as e:
        print(f"[Chainlink] Price error {asset}: {e}")
        return None


async def get_prices_bulk(assets: list[str]) -> dict[str, float]:
    """Fetch multiple asset prices in one call."""
    symbols = ",".join([SYMBOLS[a.upper()] for a in assets if a.upper() in SYMBOLS])
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{CRYPTOCOMPARE_BASE}/pricemulti",
                params={"fsyms": symbols, "tsyms": "USD"},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
                return {
                    asset.upper(): float(data[SYMBOLS[asset.upper()]]["USD"])
                    for asset in assets
                    if asset.upper() in SYMBOLS
                    and SYMBOLS[asset.upper()] in data
                }
    except Exception as e:
        print(f"[Chainlink] Bulk price error: {e}")
        return {}