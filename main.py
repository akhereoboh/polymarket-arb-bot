import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

from telegram.bot import send_message, get_application, register_handlers
from core.scanner import scan_once
from utils.db import check_and_close_open_trades

SCAN_INTERVAL = 15 * 60       # 15 minutes
AUTOCLOSE_INTERVAL = 3 * 60   # 3 minutes


async def scanner_loop():
    """Scan for new signals every 15 minutes."""
    print("[Main] Scanner loop started")
    while True:
        try:
            await scan_once(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Scanner error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


async def autoclose_loop():
    """Check open trades every 3 minutes and close if target/stop hit."""
    print("[Main] Auto-close loop started")
    while True:
        try:
            await check_and_close_open_trades(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Auto-close error: {e}")
        await asyncio.sleep(AUTOCLOSE_INTERVAL)


async def main():
    print("[Main] Starting Polymarket Arb Bot...")

    # send startup message
    try:
        await send_message(
            "🤖 *Polymarket Arb Bot started*\n"
            "━━━━━━━━━━━━━━━━\n"
            "Monitoring BTC and ETH direction markets.\n"
            "Scanning every 15 minutes.\n"
            "Auto-close checking every 3 minutes.\n\n"
            "Commands: /status /stats /open /scan"
        )
    except Exception as e:
        print(f"[Main] Startup message failed: {e}")

    # build telegram app
    app = get_application()
    register_handlers(app)
    await app.initialize()
    await app.start()

    # run all loops concurrently
    await asyncio.gather(
        scanner_loop(),
        autoclose_loop(),
        app.updater.start_polling(),
    )


if __name__ == "__main__":
    asyncio.run(main())