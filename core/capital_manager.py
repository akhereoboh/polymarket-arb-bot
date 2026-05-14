import os
from utils.db import get_arb_stats, get_open_arb_trades

STARTING_CAPITAL = float(os.getenv("STARTING_CAPITAL", "20.0"))
CAPITAL_RESERVE = 0.10   # keep 10% as reserve
TRADE_COST = 4.95        # cost per trade (5 shares × $0.99)


async def get_current_capital() -> float:
    """
    Current capital = starting capital + total profits so far.
    """
    stats = await get_arb_stats()
    total_profit = stats.get("total_profit", 0.0)
    return round(STARTING_CAPITAL + total_profit, 4)


async def get_max_trades() -> int:
    """
    Max simultaneous trades based on current capital.
    Formula: floor(capital × 0.90 / 5)
    Minimum 1, no upper cap.
    """
    capital = await get_current_capital()
    available = capital * (1 - CAPITAL_RESERVE)
    max_trades = int(available // TRADE_COST)
    return max(1, max_trades)


async def can_open_trade() -> tuple[bool, dict]:
    """
    Check if we can open a new trade right now.
    Returns (can_trade, info_dict).
    """
    capital = await get_current_capital()
    max_trades = await get_max_trades()
    open_trades = await get_open_arb_trades()
    open_count = len(open_trades)

    available = capital * (1 - CAPITAL_RESERVE)
    deployed = open_count * TRADE_COST
    remaining = round(available - deployed, 4)

    info = {
        "capital": capital,
        "available": round(available, 4),
        "deployed": round(deployed, 4),
        "remaining": remaining,
        "open_trades": open_count,
        "max_trades": max_trades,
        "can_trade": open_count < max_trades,
    }

    return open_count < max_trades, info


def format_capital_summary(info: dict) -> str:
    return (
        f"💰 *Capital Status*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Total capital:* ${info['capital']:.4f}\n"
        f"*Available (90%):* ${info['available']:.4f}\n"
        f"*Deployed:* ${info['deployed']:.4f}\n"
        f"*Remaining:* ${info['remaining']:.4f}\n\n"
        f"*Open trades:* {info['open_trades']}\n"
        f"*Max trades:* {info['max_trades']}\n"
        f"*Can open new:* {'✅ Yes' if info['can_trade'] else '❌ No — at capacity'}"
    )