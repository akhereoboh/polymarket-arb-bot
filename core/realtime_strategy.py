"""
Strategy 3: Real-time intramarket trading.

Logic:
- Each 5m market has a reference price (BTC price at window start)
- We track current price vs reference price in real time
- We calculate what UP probability SHOULD be based on:
  * How far price has moved from reference
  * How much time is left in the window
- If Polymarket's actual UP price diverges from our fair value by enough → enter

Example:
- Market started at BTC $81,000
- With 1 minute left, BTC is at $81,200 (+0.25%)
- UP should be priced at ~0.82 (very likely to stay up)
- Polymarket has UP at 0.60 → that's a 22% edge → BUY UP

The math:
- The further above reference with less time left = higher UP probability
- We use a simple sigmoid-style calculation
- Edge = |our_fair_value - market_price|
"""

import os
import math
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional

REALTIME_THRESHOLD = float(os.getenv("REALTIME_THRESHOLD", "0.12"))  # 12% edge needed
MIN_TIME_REMAINING = 30    # don't enter if less than 30 seconds left
MAX_TIME_REMAINING = 270   # don't enter more than 4.5 minutes before close (too uncertain)

# Store reference prices: market_id -> {"ref_price": float, "start_time": datetime}
_reference_prices: dict[str, dict] = {}


def register_market_start(market_id: str, ref_price: float, start_time: datetime):
    """Call this when we first see a market to record its reference price."""
    if market_id not in _reference_prices:
        _reference_prices[market_id] = {
            "ref_price": ref_price,
            "start_time": start_time,
        }


def calculate_fair_value(
    ref_price: float,
    current_price: float,
    seconds_remaining: float,
    total_seconds: float = 300,  # 5 minute window
) -> float:
    """
    Calculate what the UP probability should be given:
    - How much price has moved from reference
    - How much time is left

    Returns a probability between 0.0 and 1.0.

    Logic:
    - If price is well above reference with little time left → UP probability is high
    - If price is at reference → 50/50
    - The less time remaining, the more confident we are about the outcome
    - A small move early = weak signal. Same move with 30 seconds left = strong signal.
    """
    if total_seconds <= 0:
        return 0.5

    pct_change = (current_price - ref_price) / ref_price * 100
    time_elapsed_ratio = 1.0 - (seconds_remaining / total_seconds)

    # confidence increases as time runs out
    confidence = min(time_elapsed_ratio * 2, 1.0)

    # base probability shift from price change
    # 0.5% move = significant for 5 minute window
    sensitivity = 20.0
    raw_shift = math.tanh(pct_change * sensitivity / 100) * 0.45

    # scale by confidence — early in market, shift is smaller
    scaled_shift = raw_shift * confidence

    fair_value = round(0.5 + scaled_shift, 4)
    return max(0.05, min(0.95, fair_value))


@dataclass
class RealtimeSignal:
    asset: str
    market_question: str
    market_id: str
    signal_type: str        # "BUY_UP" or "BUY_DOWN"
    entry_price: float
    fair_value: float
    edge: float
    ref_price: float
    current_price: float
    pct_from_ref: float
    seconds_remaining: float
    confidence: str
    reason: str


def evaluate_realtime(
    market: dict,
    current_price: float,
) -> Optional[RealtimeSignal]:
    """
    Evaluate a 5m market in real time.
    Returns a signal if there's enough edge, else None.
    """
    market_id = market.get("condition_id", "")
    timeframe = market.get("timeframe", "")

    # only run on 5m markets
    if timeframe not in ("5m", "15m"):
        return None

    up_price = market.get("yes_price")
    down_price = market.get("no_price")
    end_date = market.get("end_date")

    if not up_price or not down_price or not end_date:
        return None

    # calculate time remaining
    try:
        if isinstance(end_date, str):
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        else:
            end_dt = end_date
        now = datetime.now(timezone.utc)
        seconds_remaining = (end_dt - now).total_seconds()
    except Exception:
        return None

    # skip if too early or too late
    if seconds_remaining < MIN_TIME_REMAINING:
        return None
    if seconds_remaining > MAX_TIME_REMAINING:
        return None

    # get or set reference price
    ref_data = _reference_prices.get(market_id)
    if not ref_data:
        # first time seeing this market — current price IS our reference
        # we'll get a better signal on next scan
        _reference_prices[market_id] = {
            "ref_price": current_price,
            "start_time": datetime.now(timezone.utc),
            "total_seconds": 300,
        }
        return None

    ref_price = ref_data["ref_price"]
    total_seconds = ref_data.get("total_seconds", 300)
    pct_from_ref = round((current_price - ref_price) / ref_price * 100, 4)

    # calculate fair value
    fair_value = calculate_fair_value(ref_price, current_price, seconds_remaining, total_seconds)

    # check edge
    up_edge = fair_value - up_price      # positive = UP is underpriced
    down_edge = (1 - fair_value) - down_price  # positive = DOWN is underpriced

    best_edge = max(up_edge, down_edge)
    if best_edge < REALTIME_THRESHOLD:
        return None

    if up_edge >= down_edge:
        signal_type = "BUY_UP"
        entry_price = up_price
        edge = up_edge
        reason = (
            f"{market['asset']} is {pct_from_ref:+.3f}% from reference ${ref_price:,.2f} "
            f"with {seconds_remaining:.0f}s left. "
            f"UP should be {fair_value:.2f} but market has it at {up_price:.2f}."
        )
    else:
        signal_type = "BUY_DOWN"
        entry_price = down_price
        edge = down_edge
        reason = (
            f"{market['asset']} is {pct_from_ref:+.3f}% from reference ${ref_price:,.2f} "
            f"with {seconds_remaining:.0f}s left. "
            f"DOWN should be {1-fair_value:.2f} but market has it at {down_price:.2f}."
        )

    if edge > 0.20:
        confidence = "HIGH"
    elif edge > 0.15:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return RealtimeSignal(
        asset=market["asset"],
        market_question=market.get("question", ""),
        market_id=market_id,
        signal_type=signal_type,
        entry_price=entry_price,
        fair_value=fair_value,
        edge=round(edge, 4),
        ref_price=ref_price,
        current_price=current_price,
        pct_from_ref=pct_from_ref,
        seconds_remaining=seconds_remaining,
        confidence=confidence,
        reason=reason,
    )


def format_realtime_message(signal: RealtimeSignal) -> str:
    conf_emoji = {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "👀"}.get(signal.confidence, "")
    direction = "📈" if signal.signal_type == "BUY_UP" else "📉"
    time_left = f"{signal.seconds_remaining:.0f}s"

    return (
        f"{direction} *REALTIME SIGNAL* {conf_emoji}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Asset:* {signal.asset} | *Time left:* {time_left}\n"
        f"*Market:* {signal.market_question}\n\n"
        f"*Reference price:* ${signal.ref_price:,.2f}\n"
        f"*Current price:* ${signal.current_price:,.2f} "
        f"({signal.pct_from_ref:+.3f}%)\n\n"
        f"*Our fair value:* {signal.fair_value:.3f}\n"
        f"*Market price:* {signal.entry_price:.3f}\n"
        f"*Edge:* {signal.edge*100:.1f}%\n\n"
        f"*Signal:* {signal.signal_type.replace('_', ' ')}\n"
        f"*Confidence:* {signal.confidence}\n\n"
        f"_{signal.reason}_"
    )