import asyncio
import time
from core.polymarket import get_markets_with_prices
from core.binance_feed import get_asset_data
from core.signal_engine import evaluate_market, format_signal_message
from core.pure_arb import evaluate_arb, format_arb_message
from utils.db import log_signal, log_arb_signal

# separate cooldowns for each strategy
_momentum_fired: dict[str, float] = {}
_arb_fired: dict[str, float] = {}
MOMENTUM_COOLDOWN = 900   # 15 minutes
ARB_COOLDOWN = 300        # 5 minutes — matches market timeframe


async def scan_once(send_alert_fn=None) -> list:
    all_signals = []

    # fetch ALL markets — 5m, 15m, 4h
    markets = await get_markets_with_prices()
    if not markets:
        print("[Scanner] No active markets found")
        return []

    # split by timeframe
    markets_5m = [m for m in markets if m.get("timeframe") == "5m"]
    markets_15m = [m for m in markets if m.get("timeframe") == "15m"]
    markets_4h = [m for m in markets if m.get("timeframe") == "4h"]

    print(f"[Scanner] Markets — 5m:{len(markets_5m)} 15m:{len(markets_15m)} 4h:{len(markets_4h)}")

    # ── STRATEGY 1: PURE ARB on 5m markets ──────────────────────────
    for market in markets_5m:
        signal = evaluate_arb(market)
        if signal is None:
            continue

        last = _arb_fired.get(signal.market_id, 0)
        if time.time() - last < ARB_COOLDOWN:
            continue

        _arb_fired[signal.market_id] = time.time()
        all_signals.append(("ARB", signal))

        print(
            f"[Scanner] ARB SIGNAL → {signal.asset} {signal.timeframe} | "
            f"Cost: {signal.total_cost:.4f} | "
            f"Profit: {signal.profit_pct:.2f}%"
        )

        if send_alert_fn:
            try:
                await send_alert_fn(format_arb_message(signal))
            except Exception as e:
                print(f"[Scanner] Telegram error: {e}")

        await log_arb_signal(signal)

    # ── STRATEGY 2: MOMENTUM on 15m + 4h markets ────────────────────
    momentum_markets = markets_15m + markets_4h
    if momentum_markets:
        assets_needed = list(set(m["asset"] for m in momentum_markets))
        asset_results = await asyncio.gather(*[
            get_asset_data(asset) for asset in assets_needed
        ])
        asset_map = {
            asset: data
            for asset, data in zip(assets_needed, asset_results)
            if data is not None
        }

        for market in momentum_markets:
            asset = market["asset"]
            asset_data = asset_map.get(asset)
            if not asset_data:
                continue

            signal = evaluate_market(market, asset_data)
            if signal is None:
                continue

            last = _momentum_fired.get(signal.market_id, 0)
            if time.time() - last < MOMENTUM_COOLDOWN:
                continue

            _momentum_fired[signal.market_id] = time.time()
            all_signals.append(("MOMENTUM", signal))

            print(
                f"[Scanner] MOMENTUM SIGNAL → {signal.signal_type} {signal.asset} | "
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