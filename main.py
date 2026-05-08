import asyncio
import os
import sys
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from tgram.bot import send_message, get_application, register_handlers
from core.scanner import scan_once, realtime_scan
from utils.db import check_and_close_open_trades

SCAN_INTERVAL = 15 * 60      # 15 minutes — momentum + arb
REALTIME_INTERVAL = 30       # 30 seconds — realtime intramarket
AUTOCLOSE_INTERVAL = 3 * 60  # 3 minutes — auto close checker


async def scanner_loop():
    print("[Main] Scanner loop started")
    while True:
        try:
            await scan_once(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Scanner error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


async def realtime_loop():
    print("[Main] Realtime loop started")
    await asyncio.sleep(60)  # wait 1 minute for markets to register reference prices
    while True:
        try:
            await realtime_scan(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Realtime error: {e}")
        await asyncio.sleep(REALTIME_INTERVAL)


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
    try:
        await send_message(
            "🤖 *Polymarket Arb Bot started*\n"
            "━━━━━━━━━━━━━━━━\n"
            "Running 3 strategies:\n"
            "⚡ *Pure Arb* — 5m markets (every 15min scan)\n"
            "📊 *Momentum* — 15m markets (every 15min scan)\n"
            "🎯 *Realtime* — 5m intramarket (every 30s)\n\n"
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
        realtime_loop(),
        autoclose_loop(),
        app.updater.start_polling(),
    )


if __name__ == "__main__":
    asyncio.run(main())