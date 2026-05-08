"""This file fetches live BTC and ETH prices from Binance and calculates momentum 
— meaning how strongly the price has been moving up or down over the last few hours."""

import aiohttp
import asyncio
from typing import Optional

BINANCE_BASE = "https://api.binance.com/api/v3"

SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT",
    "BNB": "BNBUSDT",
}


async def fetch_price(session: aiohttp.ClientSession, asset: str) -> Optional[float]:
    symbol = SYMBOLS.get(asset.upper())
    if not symbol:
        return None
    try:
        async with session.get(
            f"{BINANCE_BASE}/ticker/price",
            params={"symbol": symbol},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return float(data["price"])
    except Exception as e:
        print(f"[Binance] Price error {asset}: {e}")
        return None


async def fetch_klines(session: aiohttp.ClientSession, asset: str) -> list[float]:
    """
    Fetch last 6 hourly closing prices.
    This tells us the recent trend — has price been climbing or falling?
    """
    symbol = SYMBOLS.get(asset.upper())
    if not symbol:
        return []
    try:
        async with session.get(
            f"{BINANCE_BASE}/klines",
            params={"symbol": symbol, "interval": "1h", "limit": 6},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            # each kline is a list, index 4 is the closing price
            return [float(k[4]) for k in data]
    except Exception as e:
        print(f"[Binance] Klines error {asset}: {e}")
        return []


def calculate_momentum(closes: list[float]) -> dict:
    """
    Given last 6 hourly closes, work out:
    - direction: UP, DOWN, or FLAT
    - strength: 0.0 (weak) to 1.0 (very strong)
    - how much price moved in last 1 hour and last 6 hours
    """
    if len(closes) < 2:
        return {
            "direction": "FLAT",
            "strength": 0.0,
            "short_pct": 0.0,
            "medium_pct": 0.0,
        }

    short_pct = (closes[-1] - closes[-2]) / closes[-2] * 100   # last 1 hour
    medium_pct = (closes[-1] - closes[0]) / closes[0] * 100    # last 6 hours

    # strength is based on 6h move — a 5% move = full strength 1.0
    strength = min(abs(medium_pct) / 5.0, 1.0)

    if medium_pct > 0.5:
        direction = "UP"
    elif medium_pct < -0.5:
        direction = "DOWN"
    else:
        direction = "FLAT"

    return {
        "direction": direction,
        "strength": round(strength, 3),
        "short_pct": round(short_pct, 3),
        "medium_pct": round(medium_pct, 3),
    }


async def get_asset_data(asset: str) -> Optional[dict]:
    """
    Main function — returns price + momentum for BTC or ETH.
    Everything else in the bot calls this.
    """
    async with aiohttp.ClientSession() as session:
        price, closes = await asyncio.gather(
            fetch_price(session, asset),
            fetch_klines(session, asset),
        )

    if price is None:
        return None

    momentum = calculate_momentum(closes)

    return {
        "asset": asset.upper(),
        "price": price,
        "momentum": momentum,
    }