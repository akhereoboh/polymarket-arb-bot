"""
Pure arbitrage strategy for 5-minute BTC/ETH/SOL/XRP markets.

Logic: Buy BOTH UP and DOWN when total cost < threshold.
One side always pays $1.00 at settlement.
If you paid $0.99 total, profit = $0.01 per share guaranteed.
No prediction needed — pure math.
"""

import os
from dataclasses import dataclass
from typing import Optional

ARB_THRESHOLD = float(os.getenv("ARB_THRESHOLD", "0.99"))
ORDER_SIZE = int(os.getenv("ORDER_SIZE", "5"))  # minimum is 5 shares on Polymarket


@dataclass
class ArbSignal:
    asset: str
    market_question: str
    market_id: str
    condition_id: str
    up_price: float
    down_price: float
    total_cost: float
    profit_per_share: float
    profit_pct: float
    total_investment: float
    expected_profit: float
    timeframe: str
    confidence: str


def evaluate_arb(market: dict) -> Optional[ArbSignal]:
    """
    Check if a 5m market has an arbitrage opportunity.
    Returns ArbSignal if total cost < threshold, else None.
    """
    # use actual ask prices — what you really pay
    up_price = market.get("up_ask") or market.get("yes_price")
    down_price = market.get("down_ask") or market.get("no_price")
    timeframe = market.get("timeframe", "")

    if up_price is None or down_price is None:
        return None

    # only run pure arb on 5m markets
    if timeframe not in ("5m", "15m"):
        return None

    total_cost = round(up_price + down_price, 4)

    if total_cost >= ARB_THRESHOLD:
        return None

    profit_per_share = round(1.0 - total_cost, 4)
    profit_pct = round(profit_per_share / total_cost * 100, 2)
    total_investment = round(total_cost * ORDER_SIZE, 4)
    expected_profit = round(profit_per_share * ORDER_SIZE, 4)

    if profit_pct > 5:
        confidence = "HIGH"
    elif profit_pct > 2:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return ArbSignal(
        asset=market.get("asset", ""),
        market_question=market.get("question", ""),
        market_id=market.get("condition_id", ""),
        condition_id=market.get("condition_id", ""),
        up_price=up_price,
        down_price=down_price,
        total_cost=total_cost,
        profit_per_share=profit_per_share,
        profit_pct=profit_pct,
        total_investment=total_investment,
        expected_profit=expected_profit,
        timeframe=timeframe,
        confidence=confidence,
    )


def format_arb_message(signal: ArbSignal) -> str:
    """Format arb signal for Telegram."""
    conf_emoji = {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "👀"}.get(signal.confidence, "")
    return (
        f"💰 *PURE ARB DETECTED* {conf_emoji}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Asset:* {signal.asset} | *Timeframe:* {signal.timeframe}\n"
        f"*Market:* {signal.market_question}\n\n"
        f"*UP price:* ${signal.up_price:.4f}\n"
        f"*DOWN price:* ${signal.down_price:.4f}\n"
        f"*Total cost:* ${signal.total_cost:.4f}\n\n"
        f"*Profit per share:* ${signal.profit_per_share:.4f}\n"
        f"*Profit %:* {signal.profit_pct:.2f}%\n\n"
        f"*Order size:* {ORDER_SIZE} shares each side\n"
        f"*Total investment:* ${signal.total_investment:.4f}\n"
        f"*Expected profit:* ${signal.expected_profit:.4f}\n\n"
        f"_Buy BOTH sides — profit guaranteed regardless of direction_"
    )