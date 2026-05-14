import asyncio
import os
import time
from core.polymarket import get_markets_with_orderbook
from core.capital_manager import can_open_trade, get_current_capital, get_max_trades
from utils.db import log_arb_trade
from core.trading import execute_arb_trade

ARB_THRESHOLD = float(os.getenv("ARB_THRESHOLD", "0.991"))
SHARES = int(os.getenv("ORDER_SIZE", "5"))

_traded_markets: set = set()
_cache_timestamp: float = 0
_market_cache: list = []
CACHE_TTL = 30


async def get_cached_markets() -> list:
    global _market_cache, _cache_timestamp
    now = time.time()
    if now - _cache_timestamp < CACHE_TTL and _market_cache:
        return _market_cache
    markets = await get_markets_with_orderbook()
    if markets:
        _market_cache = markets
        _cache_timestamp = now
        print(f"[Scanner] Cache refreshed — {len(markets)} markets")
    return _market_cache


async def scan_once(send_alert_fn=None) -> list:
    opportunities = []

    markets = await get_cached_markets()
    if not markets:
        return []

    best = min(markets, key=lambda m: m.get("total", 99))
    print(
        f"[Scanner] {len(markets)} markets | "
        f"Best: {best['asset']} {best['timeframe']} "
        f"total={best.get('total')} gap={best.get('gap')}"
    )

    for market in markets:
        condition_id = market["condition_id"]
        up_ask = market.get("up_ask")
        down_ask = market.get("down_ask")
        total = market.get("total")

        if up_ask is None or down_ask is None or total is None:
            continue

        if condition_id in _traded_markets:
            continue

        if total > ARB_THRESHOLD:
            continue

        # check capital before opening trade
        can_trade, info = await can_open_trade()
        if not can_trade:
            print(
                f"[Scanner] At capacity — {info['open_trades']}/{info['max_trades']} trades open. "
                f"Capital: ${info['capital']:.4f}"
            )
            break

        _traded_markets.add(condition_id)

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
            "up_price": up_ask,
            "down_price": down_ask,
            "total_cost": total,
            "arb_profit": market.get("gap"),
            "shares": SHARES,
            "total_invested": total_invested,
            "expected_payout": expected_payout,
            "expected_profit": expected_profit,
            "profit_pct": profit_pct,
            "market_end_time": market.get("end_date"),
        }

        opportunities.append(opportunity)

        capital = info["capital"]
        max_t = info["max_trades"]

        print(
            f"[ARB] OPPORTUNITY → {market['asset']} | "
            f"UP:{up_ask} + DOWN:{down_ask} = {total} | "
            f"Profit: ${expected_profit} | "
            f"Capital: ${capital:.4f} | "
            f"Trades: {info['open_trades']+1}/{max_t}"
        )

        trade_result = await execute_arb_trade(market, SHARES)

        if trade_result.get("status") == "failed":
            print(f"[Scanner] Trade execution failed, skipping log")
            _traded_markets.discard(condition_id)
            continue

        await log_arb_trade(opportunity)

        if send_alert_fn:
            msg = (
                format_arb_alert(opportunity) +
                f"\n\n💰 Capital: ${capital:.4f} | "
                f"Trade {info['open_trades']+1}/{max_t}"
            )
            try:
                await send_alert_fn(msg)
            except Exception as e:
                print(f"[Scanner] Telegram error: {e}")

    return opportunities


def format_arb_alert(opp: dict) -> str:
    return (
        f"🎯 *ARB OPPORTUNITY*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Asset:* {opp['asset']}\n"
        f"*Market:* {opp['market_question']}\n\n"
        f"*UP ask:* ${opp['up_price']:.3f}\n"
        f"*DOWN ask:* ${opp['down_price']:.3f}\n"
        f"*Total cost:* ${opp['total_cost']:.4f}\n"
        f"*Gap:* ${opp['arb_profit']:.4f}\n\n"
        f"*Shares:* {opp['shares']} each side\n"
        f"*Total invested:* ${opp['total_invested']:.2f}\n"
        f"*Expected payout:* ${opp['expected_payout']:.2f}\n"
        f"*Expected profit:* ${opp['expected_profit']:.4f} "
        f"({opp['profit_pct']:.2f}%)"
    )