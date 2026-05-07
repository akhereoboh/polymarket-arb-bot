"""This is the brain of the bot. It takes the Binance momentum and the Polymarket YES/NO prices and asks one question: is the crowd pricing this wrong relative to what the price is actually doing?"""

"""If BTC pumped hard (strength 1.0) we say YES should fairly be priced at 0.70. If it pumped moderately (strength 0.5) we say 0.60. If Polymarket has YES at 0.45 when we think it's worth 0.70 — that 0.25 gap is your edge and the signal fires.
The confidence level is just how big the gap is combined with how strong the momentum is. Big gap + strong momentum = HIGH confidence."""



import os
from dataclasses import dataclass
from typing import Optional

DIVERGENCE_THRESHOLD = float(os.getenv("DIVERGENCE_THRESHOLD", "0.08"))


@dataclass
class Signal:
    asset: str
    market_question: str
    market_id: str
    signal_type: str           # "BUY_YES" or "BUY_NO"
    entry_price: float         # price to enter at
    fair_value: float          # what we think it should be priced at
    divergence: float          # gap between fair value and market price
    momentum_direction: str
    momentum_strength: float
    asset_price: float
    confidence: str            # HIGH, MEDIUM, LOW
    reason: str


def momentum_to_fair_value(momentum: dict) -> Optional[float]:
    """
    Convert momentum into what we think the YES price should be.

    Logic:
    - No momentum (FLAT) → we have no edge, return None
    - Strong UP trend → YES should be priced higher than 0.50
    - Strong DOWN trend → YES should be priced lower than 0.50

    We never go above 0.70 or below 0.30 because we're not a
    price prediction model — we're just detecting crowd mistakes.
    """
    direction = momentum.get("direction", "FLAT")
    strength = momentum.get("strength", 0.0)

    if direction == "FLAT":
        return None

    if direction == "UP":
        return round(0.50 + (0.20 * strength), 4)
    elif direction == "DOWN":
        return round(0.50 - (0.20 * strength), 4)

    return None


def evaluate_market(market: dict, asset_data: dict) -> Optional[Signal]:
    """
    Core function. Given one Polymarket market and Binance data,
    decide if there's a signal worth alerting on.
    """
    momentum = asset_data.get("momentum", {})
    yes_price = market.get("yes_price")
    no_price = market.get("no_price")

    if yes_price is None or no_price is None:
        return None

    fair_value = momentum_to_fair_value(momentum)
    if fair_value is None:
        return None

    # positive divergence = YES is underpriced (buy YES)
    # negative divergence = YES is overpriced (buy NO)
    divergence = round(fair_value - yes_price, 4)

    if abs(divergence) < DIVERGENCE_THRESHOLD:
        return None

    direction = momentum["direction"]
    strength = momentum["strength"]
    medium_pct = momentum.get("medium_pct", 0.0)

    if divergence > 0:
        signal_type = "BUY_YES"
        entry_price = yes_price
        reason = (
            f"{asset_data['asset']} is up {medium_pct:+.2f}% over 6 hours "
            f"but Polymarket only prices YES at {yes_price:.2f}. "
            f"Crowd is slow — YES looks underpriced."
        )
    else:
        signal_type = "BUY_NO"
        entry_price = no_price
        reason = (
            f"{asset_data['asset']} is {direction.lower()} {medium_pct:+.2f}% over 6 hours "
            f"but Polymarket has pushed YES to {yes_price:.2f}. "
            f"Crowd overreacted — fade it, buy NO."
        )

    if strength > 0.6 and abs(divergence) > 0.12:
        confidence = "HIGH"
    elif strength > 0.3 and abs(divergence) > 0.08:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return Signal(
        asset=asset_data["asset"],
        market_question=market.get("question", ""),
        market_id=market.get("id", ""),
        signal_type=signal_type,
        entry_price=entry_price,
        fair_value=fair_value,
        divergence=divergence,
        momentum_direction=direction,
        momentum_strength=strength,
        asset_price=asset_data["price"],
        confidence=confidence,
        reason=reason,
    )


def format_signal_message(signal: Signal) -> str:
    """Format signal into a readable Telegram message."""
    emoji = "🟢" if signal.signal_type == "BUY_YES" else "🔴"
    conf_emoji = {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "👀"}.get(signal.confidence, "")
    target = round(min(signal.entry_price + 0.15, 0.95), 3)

    return (
        f"{emoji} *{signal.signal_type.replace('_', ' ')}* {conf_emoji}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Asset:* {signal.asset} @ ${signal.asset_price:,.2f}\n"
        f"*Market:* {signal.market_question}\n\n"
        f"*Entry:* ${signal.entry_price:.3f}\n"
        f"*Our fair value:* ${signal.fair_value:.3f}\n"
        f"*Edge:* {abs(signal.divergence)*100:.1f}%\n\n"
        f"*Momentum:* {signal.momentum_direction} "
        f"(strength {signal.momentum_strength:.0%})\n"
        f"*Confidence:* {signal.confidence}\n\n"
        f"_{signal.reason}_\n\n"
        f"📋 Paper trade: entry ${signal.entry_price:.3f} → target ${target:.3f}"
    )