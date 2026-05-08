import asyncio
import os
from datetime import datetime, timezone
from core.polymarket import get_markets_with_orderbook
from utils.db import log_arb_trade

ARB_THRESHOLD = float(os.getenv("ARB_THRESHOLD", "0.991"))
SHARES = int(os.getenv("ORDER_SIZE", "5"))

# track which markets we already entered this cycle
_traded_markets: set = set()


def reset_traded_markets():
    """Call this when a new market cycle starts."""
    global _traded_markets
    _traded_markets = set()


async def scan_once(send_alert_fn=None) -> list:
    """
    One scan cycle — check all active 15m markets for arb opportunity.
    Runs every 1 second.
    """
    opportunities = []

    markets = await get_markets_with_orderbook()
    if not markets:
        return []

    for market in markets:
        condition_id = market["condition_id"]
        up_ask = market.get("up_ask")
        down_ask = market.get("down_ask")
        total = market.get("total")

        if up_ask is None or down_ask is None or total is None:
            continue

        # skip if already traded this market this session
        if condition_id in _traded_markets:
            continue

        # pure arb condition
        if total > ARB_THRESHOLD:
            continue

        # opportunity found
        _traded_markets.add(condition_id)

        total_invested = round(total * SHARES, 4)
        expected_payout = round(1.0 * SHARES, 4)
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

        print(
            f"[ARB] OPPORTUNITY → {market['asset']} | "
            f"UP:{up_ask} + DOWN:{down_ask} = {total} | "
            f"Gap:{market.get('gap')} | "
            f"Profit/trade: ${expected_profit}"
        )

        # log to supabase
        await log_arb_trade(opportunity)

        # send telegram alert
        if send_alert_fn:
            msg = format_arb_alert(opportunity)
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
        f"({opp['profit_pct']:.2f}%)\n\n"
        f"📋 Paper trade logged to Supabase"
    )