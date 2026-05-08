import asyncio
import time
from core.polymarket import get_markets_with_prices
from core.binance_feed import get_asset_data
from core.chainlink import get_prices_bulk
from core.signal_engine import evaluate_market, format_signal_message
from core.pure_arb import evaluate_arb, format_arb_message
from core.realtime_strategy import evaluate_realtime, format_realtime_message
from utils.db import log_signal, log_arb_signal, log_realtime_signal

_momentum_fired: dict[str, float] = {}
_arb_fired: dict[str, float] = {}
_realtime_fired: dict[str, float] = {}

MOMENTUM_COOLDOWN = 900
ARB_COOLDOWN = 300
REALTIME_COOLDOWN = 60   # 1 minute cooldown for realtime signals


async def scan_once(send_alert_fn=None) -> list:
    """Full scan — runs every 15 minutes for momentum and arb."""
    all_signals = []
    markets = await get_markets_with_prices()
    if not markets:
        print("[Scanner] No active markets found")
        return []

    markets_5m = [m for m in markets if m.get("timeframe") == "5m"]
    markets_15m = [m for m in markets if m.get("timeframe") == "15m"]
    markets_4h = [m for m in markets if m.get("timeframe") == "4h"]

    print(f"[Scanner] Markets — 5m:{len(markets_5m)} 15m:{len(markets_15m)} 4h:{len(markets_4h)}")

    # ── STRATEGY 1: PURE ARB on 5m ──────────────────────────────────
    for market in markets_5m:
        signal = evaluate_arb(market)
        if signal is None:
            continue
        last = _arb_fired.get(signal.market_id, 0)
        if time.time() - last < ARB_COOLDOWN:
            continue
        _arb_fired[signal.market_id] = time.time()
        all_signals.append(("ARB", signal))
        print(f"[Scanner] ARB → {signal.asset} | Cost:{signal.total_cost:.4f} | Profit:{signal.profit_pct:.2f}%")
        if send_alert_fn:
            try:
                await send_alert_fn(format_arb_message(signal))
            except Exception as e:
                print(f"[Scanner] Telegram error: {e}")
        await log_arb_signal(signal)

    # ── STRATEGY 2: MOMENTUM on 15m + 4h ────────────────────────────
    momentum_markets = markets_15m + markets_4h
    if momentum_markets:
        assets_needed = list(set(m["asset"] for m in momentum_markets))
        asset_results = await asyncio.gather(*[get_asset_data(a) for a in assets_needed])
        asset_map = {a: d for a, d in zip(assets_needed, asset_results) if d}

        for market in momentum_markets:
            asset_data = asset_map.get(market["asset"])
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
            print(f"[Scanner] MOMENTUM → {signal.signal_type} {signal.asset} | Edge:{abs(signal.divergence)*100:.1f}%")
            if send_alert_fn:
                try:
                    await send_alert_fn(format_signal_message(signal))
                except Exception as e:
                    print(f"[Scanner] Telegram error: {e}")
            await log_signal(signal)

    if not all_signals:
        print("[Scanner] No signals this cycle")

    return all_signals


async def realtime_scan(send_alert_fn=None) -> list:
    """
    Realtime scan — runs every 30 seconds.
    Only checks 5m markets with time remaining between 30s and 4.5min.
    """
    all_signals = []

    markets = await get_markets_with_prices()
    markets_5m = [m for m in markets if m.get("timeframe") == "5m"]

    if not markets_5m:
        return []

    # fetch current prices for all assets at once
    assets = list(set(m["asset"] for m in markets_5m))
    prices = await get_prices_bulk(assets)

    if not prices:
        return []

    for market in markets_5m:
        asset = market["asset"]
        current_price = prices.get(asset)
        if not current_price:
            continue

        signal = evaluate_realtime(market, current_price)
        if signal is None:
            continue

        last = _realtime_fired.get(signal.market_id, 0)
        if time.time() - last < REALTIME_COOLDOWN:
            continue

        _realtime_fired[signal.market_id] = time.time()
        all_signals.append(("REALTIME", signal))

        print(
            f"[Realtime] SIGNAL → {signal.signal_type} {signal.asset} | "
            f"Edge:{signal.edge*100:.1f}% | "
            f"{signal.seconds_remaining:.0f}s left"
        )

        if send_alert_fn:
            try:
                await send_alert_fn(format_realtime_message(signal))
            except Exception as e:
                print(f"[Realtime] Telegram error: {e}")

        await log_realtime_signal(signal)

    return all_signals