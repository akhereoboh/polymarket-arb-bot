import os
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode
from utils.db import get_paper_stats, get_open_trades

CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
_application = None


def get_application():
    global _application
    if _application:
        return _application
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    _application = ApplicationBuilder().token(token).build()
    return _application


async def send_message(text: str):
    """
    Global send function — every other file calls this to send Telegram messages.
    Uses markdown formatting.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    bot = Bot(token=token)
    await bot.send_message(
        chat_id=CHAT_ID,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Polymarket Arb Bot*\n"
        "━━━━━━━━━━━━━━━━\n"
        "Monitoring BTC and ETH direction markets.\n\n"
        "*Commands:*\n"
        "/status — bot status and next scan time\n"
        "/stats — paper trading performance\n"
        "/open — view open paper trades\n"
        "/scan — trigger manual scan now\n"
        "/help — show this message",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from core.binance_feed import get_asset_data

    btc = await get_asset_data("BTC")
    eth = await get_asset_data("ETH")

    lines = ["📡 *Bot Status*\n━━━━━━━━━━━━━━━━\n"]

    for data in [btc, eth]:
        if not data:
            continue
        m = data["momentum"]
        dir_emoji = "📈" if m["direction"] == "UP" else "📉" if m["direction"] == "DOWN" else "➡️"
        lines.append(
            f"{dir_emoji} *{data['asset']}:* ${data['price']:,.2f}\n"
            f"   Trend: {m['direction']} | "
            f"6h: {m['medium_pct']:+.2f}% | "
            f"Strength: {m['strength']:.0%}\n"
        )

    lines.append("\n✅ Scanner is running")
    await update.message.reply_text("".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await get_paper_stats()

    if not stats or stats.get("total_closed", 0) == 0:
        await update.message.reply_text(
            "📊 *Paper Trading Stats*\n━━━━━━━━━━━━━━━━\n"
            "No closed trades yet.\n"
            f"Open trades: {stats.get('open_trades', 0)}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    pnl = stats["total_pnl"]
    pnl_emoji = "🟢" if pnl > 0 else "🔴"

    await update.message.reply_text(
        f"📊 *Paper Trading Stats*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Total closed:* {stats['total_closed']}\n"
        f"*Open trades:* {stats['open_trades']}\n\n"
        f"*Wins:* {stats['wins']} | *Losses:* {stats['losses']}\n"
        f"*Win rate:* {stats['win_rate']}%\n\n"
        f"{pnl_emoji} *Total PnL:* {pnl:+.4f}\n"
        f"*Avg PnL per trade:* {stats['avg_pnl']:+.4f}\n\n"
        f"*Targets hit:* {stats.get('targets_hit', 0)}\n"
        f"*Stops hit:* {stats.get('stops_hit', 0)}\n\n"
        f"*BTC trades:* {stats.get('btc_trades', 0)} | "
        f"*ETH trades:* {stats.get('eth_trades', 0)}",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    trades = await get_open_trades()

    if not trades:
        await update.message.reply_text("No open paper trades right now.")
        return

    lines = ["📋 *Open Paper Trades*\n━━━━━━━━━━━━━━━━\n"]
    for t in trades:
        lines.append(
            f"*{t['asset']}* — {t['signal_type'].replace('_', ' ')}\n"
            f"Entry: ${t['paper_entry']:.3f} | "
            f"Target: ${t['paper_target']:.3f} | "
            f"Stop: ${t['paper_stop']:.3f}\n"
            f"_{t['market_question'][:60]}..._\n\n"
        )

    await update.message.reply_text("".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually trigger a scan right now."""
    from core.scanner import scan_once
    await update.message.reply_text("🔍 Running manual scan...")
    signals = await scan_once(send_alert_fn=send_message)
    if not signals:
        await update.message.reply_text("Scan complete. No signals found this cycle.")
    else:
        await update.message.reply_text(f"Scan complete. {len(signals)} signal(s) sent.")


def register_handlers(app):
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("scan", cmd_scan))