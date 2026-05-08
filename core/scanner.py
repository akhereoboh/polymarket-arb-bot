import asyncio
import time
from core.polymarket import get_markets_with_prices
from core.binance_feed import get_asset_data
from core.signal_engine import evaluate_market, format_signal_message
from utils.db import log_signal

_fired: dict[str, float] = {}
COOLDOWN = 900  # 15 minutes cooldown per market


async def scan_once(send_alert_fn=None) -> list:
    all_signals = []

    markets = await get_markets_with_prices()
    if not markets:
        print("[Scanner] No active markets found")
        return []

    # get unique assets
    assets_needed = list(set(m["asset"] for m in markets))

    # fetch all price data in parallel
    asset_results = await asyncio.gather(*[
        get_asset_data(asset) for asset in assets_needed
    ])
    asset_map = {
        asset: data
        for asset, data in zip(assets_needed, asset_results)
        if data is not None
    }

    print(f"[Scanner] {len(markets)} markets | Assets: {list(asset_map.keys())}")

    for market in markets:
        asset = market["asset"]
        asset_data = asset_map.get(asset)
        if not asset_data:
            continue

        signal = evaluate_market(market, asset_data)
        if signal is None:
            continue

        last = _fired.get(signal.market_id, 0)
        if time.time() - last < COOLDOWN:
            continue

        _fired[signal.market_id] = time.time()
        all_signals.append(signal)

        print(
            f"[Scanner] SIGNAL → {signal.signal_type} {signal.asset} | "
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