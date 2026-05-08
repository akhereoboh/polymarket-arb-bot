import asyncio
import os
import sys
sys.stdout.reconfigure(line_buffering=True)

from dotenv import load_dotenv
load_dotenv()

from tgram.bot import send_message, get_application, register_handlers
from core.scanner import scan_once
from utils.db import settle_expired_trades
from core.ws_feed import ws_listener, refresh_market_map_loop

SCAN_INTERVAL = 30      # polling fallback every 30 seconds
SETTLE_INTERVAL = 60    # check settlements every 60 seconds


async def scanner_loop():
    """Fallback polling scanner — runs every 30s as backup to WebSocket."""
    print("[Main] Fallback scanner started — 30 second intervals")
    while True:
        try:
            await scan_once(send_alert_fn=send_message)
        except Exception as e:
            print(f"[Main] Scanner error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


async def settlement_loop():
    print("[Main] Settlement checker started")
    await asyncio.sleep(30)
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
            "WebSocket: real-time price feed\n"
            "Fallback: polling every 30s\n"
            "Assets: BTC, ETH\n"
            "Threshold: UP + DOWN ≤ 0.991\n\n"
            "Watching for arb gaps 👀"
        )
    except Exception as e:
        print(f"[Main] Startup message failed: {e}")

    app = get_application()
    register_handlers(app)
    await app.initialize()
    await app.start()

    await asyncio.gather(
        ws_listener(send_alert_fn=send_message),
        refresh_market_map_loop(),
        scanner_loop(),
        settlement_loop(),
        app.updater.start_polling(),
    )


if __name__ == "__main__":
    asyncio.run(main())