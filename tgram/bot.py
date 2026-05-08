import os
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from utils.db import get_arb_stats, get_open_arb_trades

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
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    bot = Bot(token=token)
    await bot.send_message(
        chat_id=CHAT_ID,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎯 *Polymarket Pure Arb Bot*\n"
        "━━━━━━━━━━━━━━━━\n"
        "Scanning BTC, ETH, SOL, XRP, DOGE, BNB\n"
        "15 minute up/down markets — pure arbitrage\n\n"
        "*Commands:*\n"
        "/status — current market scan\n"
        "/stats — paper trading performance\n"
        "/open — open arb positions\n"
        "/scan — trigger manual scan\n"
        "/help — this message",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from core.polymarket import get_markets_with_orderbook
    markets = await get_markets_with_orderbook()

    if not markets:
        await update.message.reply_text("No active markets found right now.")
        return

    lines = ["📡 *Live Market Prices*\n━━━━━━━━━━━━━━━━\n"]
    for m in markets:
        total = m.get("total", 0)
        gap = m.get("gap", 0)
        arb = "🎯 ARB!" if total <= 0.991 else ""
        lines.append(
            f"*{m['asset']}* {arb}\n"
            f"UP:{m.get('up_ask', '?')} + DOWN:{m.get('down_ask', '?')} = {total}\n"
            f"Gap: {gap}\n\n"
        )

    await update.message.reply_text("".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await get_arb_stats()

    if not stats or stats.get("total_settled", 0) == 0:
        await update.message.reply_text(
            f"📊 *Arb Stats*\n━━━━━━━━━━━━━━━━\n"
            f"No settled trades yet.\n"
            f"Open positions: {stats.get('open_trades', 0)}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    await update.message.reply_text(
        f"📊 *Arb Paper Trading Stats*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Settled trades:* {stats['total_settled']}\n"
        f"*Open positions:* {stats['open_trades']}\n\n"
        f"*Total invested:* ${stats['total_invested']:.4f}\n"
        f"*Total profit:* ${stats['total_profit']:.4f}\n"
        f"*Avg profit/trade:* ${stats['avg_profit']:.4f}\n"
        f"*ROI:* {stats['roi_pct']:.4f}%",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    trades = await get_open_arb_trades()
    if not trades:
        await update.message.reply_text("No open arb positions right now.")
        return

    lines = ["📋 *Open Arb Positions*\n━━━━━━━━━━━━━━━━\n"]
    for t in trades:
        lines.append(
            f"*{t['asset']}* — cost ${t['total_cost']:.4f}\n"
            f"Expected profit: ${t['expected_profit']:.4f}\n"
            f"Expires: {t['market_end_time']}\n\n"
        )
    await update.message.reply_text("".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from core.scanner import scan_once
    await update.message.reply_text("🔍 Running manual scan...")
    opps = await scan_once(send_alert_fn=send_message)
    if not opps:
        await update.message.reply_text("Scan complete. No arb opportunities found.")
    else:
        await update.message.reply_text(f"Found {len(opps)} opportunity/ies!")


def register_handlers(app):
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("scan", cmd_scan))