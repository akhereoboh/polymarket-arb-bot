import asyncio
import time
from core.polymarket import get_markets_with_prices
from core.binance_feed import get_asset_data
from core.signal_engine import evaluate_market, format_signal_message
from utils.db import log_signal

ASSETS = ["BTC", "ETH"]

# cooldown per market — don't re-alert same market within 1 hour
_fired: dict[str, float] = {}
COOLDOWN = 3600


async def scan_once(send_alert_fn=None) -> list:
    """Run one full scan across BTC and ETH markets."""
    all_signals = []

    for asset in ASSETS:
        print(f"[Scanner] Scanning {asset}...")

        asset_data, markets = await asyncio.gather(
            get_asset_data(asset),
            get_markets_with_prices(asset),
        )

        if not asset_data:
            print(f"[Scanner] Could not fetch {asset} data")
            continue

        if not markets:
            print(f"[Scanner] No active {asset} direction markets found")
            continue

        m = asset_data["momentum"]
        print(
            f"[Scanner] {asset} @ ${asset_data['price']:,.2f} | "
            f"{m['direction']} {m['medium_pct']:+.2f}% | "
            f"Markets: {len(markets)}"
        )

        for market in markets:
            signal = evaluate_market(market, asset_data)
            if signal is None:
                continue

            # cooldown check
            last = _fired.get(signal.market_id, 0)
            if time.time() - last < COOLDOWN:
                print(f"[Scanner] {signal.market_id} on cooldown, skipping")
                continue

            _fired[signal.market_id] = time.time()
            all_signals.append(signal)

            print(
                f"[Scanner] SIGNAL → {signal.signal_type} | "
                f"Edge: {abs(signal.divergence)*100:.1f}% | "
                f"Confidence: {signal.confidence}"
            )

            if send_alert_fn:
                try:
                    await send_alert_fn(format_signal_message(signal))
                except Exception as e:
                    print(f"[Scanner] Telegram error: {e}")

            await log_signal(signal)

    if not all_signals:
        print("[Scanner] No signals this cycle")

    return all_signals