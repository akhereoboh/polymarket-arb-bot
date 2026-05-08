import asyncio
import os
import sys
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from tgram.bot import send_message, get_application, register_handlers
from core.scanner import scan_once
from core.polymarket import get_markets_with_prices
from core.chainlink import get_prices_bulk
from core.realtime_strategy import evaluate_realtime, format_realtime_message, _reference_prices
from core.websocket_feed import run_websocket
from utils.db import check_and_close_open_trades, log_realtime_signal

SCAN_INTERVAL = 15 * 60
AUTOCLOSE_INTERVAL = 3 * 60

# shared state
_active_markets: list[dict] = []
_ws_stop_event = asyncio.Event()

# cooldown for realtime signals
_realtime_fired: dict[str, float] = {}
REALTIME_COOLDOWN = 60


async def on_price_update(market: dict, up_ask: float, down_ask: float):
    """
    Called by WebSocket feed every time UP or DOWN price changes.
    Evaluates realtime signal immediately.
    """
    import time
    from core.chainlink import get_price

    asset = market.get("asset", "")
    current_price = await get_price(asset)
    if not current_price:
        return

    # update market with live websocket prices
    live_market = {**market, "yes_price": up_ask, "no_price": down_ask}

    signal = evaluate_realtime(live_market, current_price)
    if signal is None:
        return

    last = _realtime_fired.get(signal.market_id, 0)
    if time.time() - last < REALTIME_COOLDOWN:
        return

    _realtime_fired[signal.market_id] = time.time()

    print(
        f"[Realtime] SIGNAL → {signal.signal_type} {signal.asset} | "
        f"Edge:{signal.edge*100:.1f}% | {signal.seconds_remaining:.0f}s left"
    )

    try:
        await send_message(format_realtime_message(signal))
    except Exception as e:
        print(f"[Realtime] Telegram error: {e}")

    await log_realtime_signal(signal)


async def scanner_loop():
    """Scan every 15 minutes for momentum and arb signals."""
    global _active_markets
    print("[Main] Scanner loop started")
    while True:
        try:
            signals = await scan_once(send_alert_fn=send_message)
            # refresh active markets for websocket
            _active_markets = await get_markets_with_prices()
        except Exception as e:
            print(f"[Main] Scanner error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


async def websocket_loop():
    """
    WebSocket loop — runs continuously.
    Refreshes market subscriptions every 15 minutes to catch new markets.
    """
    global _active_markets
    print("[Main] WebSocket loop started")

    # wait for first scanner run to populate markets
    await asyncio.sleep(15)

    while True:
        try:
            # get fresh 5m markets only
            markets = [m for m in _active_markets if m.get("timeframe") == "5m"]
            if not markets:
                markets = await get_markets_with_prices()
                markets = [m for m in markets if m.get("timeframe") == "5m"]

            if markets:
                print(f"[WebSocket] Starting with {len(markets)} 5m markets")
                # run websocket — will reconnect internally on drops
                # we restart every 15 minutes to refresh market list
                stop = asyncio.Event()
                ws_task = asyncio.create_task(
                    run_websocket(markets, on_price_update, stop)
                )
                await asyncio.sleep(15 * 60)  # run for 15 minutes
                stop.set()
                ws_task.cancel()
                try:
                    await ws_task
                except asyncio.CancelledError:
                    pass
            else:
                print("[WebSocket] No 5m markets found, retrying in 60s")
                await asyncio.sleep(60)

        except Exception as e:
            print(f"[WebSocket] Loop error: {e}")
            await asyncio.sleep(10)


async def autoclose_loop():
    print("[Main] Auto-close loop started")
    await asyncio.sleep(10)
    while True:
        try:
            await check_and_close_open_trades(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Auto-close error: {e}")
        await asyncio.sleep(AUTOCLOSE_INTERVAL)


async def main():
    print("[Main] Starting Polymarket Arb Bot...")

    # initial market load
    global _active_markets
    _active_markets = await get_markets_with_prices()

    try:
        await send_message(
            "🤖 *Polymarket Arb Bot started*\n"
            "━━━━━━━━━━━━━━━━\n"
            "Running 3 strategies:\n"
            "⚡ *Pure Arb* — 5m markets\n"
            "📊 *Momentum* — 15m markets\n"
            "🎯 *Realtime* — WebSocket live feed\n\n"
            "Commands: /status /stats /open /scan"
        )
    except Exception as e:
        print(f"[Main] Startup message failed: {e}")

    app = get_application()
    register_handlers(app)
    await app.initialize()
    await app.start()

    await asyncio.gather(
        scanner_loop(),
        websocket_loop(),
        autoclose_loop(),
        app.updater.start_polling(),
    )


if __name__ == "__main__":
    asyncio.run(main())