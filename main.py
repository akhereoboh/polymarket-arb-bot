import asyncio
import os
import sys
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from tgram.bot import send_message, get_application, register_handlers
from core.scanner import scan_once
from utils.db import settle_expired_trades

SCAN_INTERVAL = 1       # 1 second — pure arb needs speed
SETTLE_INTERVAL = 60    # check for expired markets every 60 seconds


async def scanner_loop():
    print("[Main] Pure arb scanner started — 1 second intervals")
    while True:
        try:
            await scan_once(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Scanner error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


async def settlement_loop():
    print("[Main] Settlement checker started — 60 second intervals")
    await asyncio.sleep(30)  # stagger startup
    while True:
        try:
            await settle_expired_trades(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Settlement error: {e}")
        await asyncio.sleep(SETTLE_INTERVAL)


async def main():
    print("[Main] Starting Polymarket Pure Arb Bot...")
    try:
        await send_message(
            "🎯 *Pure Arb Bot Started*\n"
            "━━━━━━━━━━━━━━━━\n"
            "Scanning every 1 second.\n"
            "Assets: BTC, ETH, SOL, XRP, DOGE, BNB\n"
            "Threshold: UP + DOWN ≤ 0.991\n\n"
            "Will alert when arb gap found 🔍"
        )
    except Exception as e:
        print(f"[Main] Startup message failed: {e}")

    app = get_application()
    register_handlers(app)
    await app.initialize()
    await app.start()

    await asyncio.gather(
        scanner_loop(),
        settlement_loop(),
        app.updater.start_polling(),
    )


if __name__ == "__main__":
    asyncio.run(main())