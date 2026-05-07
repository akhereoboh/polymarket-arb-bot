"""This file saves every signal to Supabase and tracks your paper 
trading performance. Every time the bot spots an opportunity it logs it — 
so at the end of the week you can look back and see how many signals were correct."""

import os
from datetime import datetime, timezone
from typing import Optional
from supabase import create_client, Client

_client: Optional[Client] = None

TAKE_PROFIT = 0.15   # close if price moves +0.15 in our favor
STOP_LOSS = 0.08     # close if price moves -0.08 against us


def get_client() -> Optional[Client]:
    global _client
    if _client:
        return _client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("[Supabase] Missing credentials in .env")
        return None
    _client = create_client(url, key)
    return _client


async def log_signal(signal) -> bool:
    """Save a new signal to the signals table."""
    client = get_client()
    if not client:
        return False
    try:
        target = round(min(signal.entry_price + TAKE_PROFIT, 0.95), 4)
        stop = round(max(signal.entry_price - STOP_LOSS, 0.02), 4)

        client.table("signals").insert({
            "asset": signal.asset,
            "market_question": signal.market_question,
            "market_id": signal.market_id,
            "signal_type": signal.signal_type,
            "entry_price": signal.entry_price,
            "implied_fair_value": signal.fair_value,
            "divergence": signal.divergence,
            "momentum_direction": signal.momentum_direction,
            "momentum_strength": signal.momentum_strength,
            "asset_price": signal.asset_price,
            "confidence": signal.confidence,
            "reason": signal.reason,
            "status": "OPEN",
            "paper_entry": signal.entry_price,
            "paper_target": target,
            "paper_stop": stop,
        }).execute()
        print(f"[Supabase] Signal logged: {signal.signal_type} {signal.asset}")
        return True
    except Exception as e:
        print(f"[Supabase] Error logging signal: {e}")
        return False


async def _close_trade_internal(client: Client, trade: dict, exit_price: float, reason: str) -> dict:
    """Internal — closes a trade and returns result info for Telegram notification."""
    entry = trade["paper_entry"]
    pnl = round(exit_price - entry, 4)
    pnl_pct = round(pnl / entry * 100, 2)

    client.table("signals").update({
        "status": "CLOSED",
        "paper_result": exit_price,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "close_reason": reason,
        "closed_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", trade["id"]).execute()

    return {
        "trade": trade,
        "exit_price": exit_price,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "reason": reason,
    }


async def check_and_close_open_trades(send_alert_fn=None) -> list:
    """
    The auto-close engine.
    
    Runs on a loop. For every open trade:
    - Fetches the current market price from Polymarket
    - Checks if target or stop has been hit
    - Closes automatically and sends Telegram notification
    
    Returns list of trades that were closed this cycle.
    """
    from core.polymarket import fetch_prices
    import aiohttp

    client = get_client()
    if not client:
        return []

    try:
        result = client.table("signals").select("*").eq("status", "OPEN").execute()
        open_trades = result.data
    except Exception as e:
        print(f"[AutoClose] Error fetching open trades: {e}")
        return []

    if not open_trades:
        return []

    closed_this_cycle = []

    async with aiohttp.ClientSession() as session:
        for trade in open_trades:
            condition_id = trade.get("market_id")
            if not condition_id:
                continue

            prices = await fetch_prices(session, condition_id)
            if not prices:
                continue

            signal_type = trade["signal_type"]
            entry = trade["paper_entry"]
            target = trade["paper_target"]
            stop = trade["paper_stop"]

            # current price depends on what we bought
            if signal_type == "BUY_YES":
                current_price = prices["yes_price"]
            else:
                current_price = prices["no_price"]

            close_reason = None

            if current_price >= target:
                close_reason = "TARGET_HIT"
            elif current_price <= stop:
                close_reason = "STOP_HIT"

            if close_reason:
                result = await _close_trade_internal(client, trade, current_price, close_reason)
                closed_this_cycle.append(result)

                if send_alert_fn:
                    pnl = result["pnl"]
                    pnl_pct = result["pnl_pct"]
                    emoji = "✅" if pnl > 0 else "❌"
                    reason_text = "Target hit 🎯" if close_reason == "TARGET_HIT" else "Stop loss hit 🛑"

                    msg = (
                        f"{emoji} *Paper trade closed*\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"*{trade['asset']}* — {trade['signal_type'].replace('_', ' ')}\n"
                        f"*Reason:* {reason_text}\n\n"
                        f"*Entry:* ${entry:.3f}\n"
                        f"*Exit:* ${current_price:.3f}\n"
                        f"*PnL:* {pnl:+.4f} ({pnl_pct:+.2f}%)\n\n"
                        f"_{trade['market_question']}_"
                    )
                    try:
                        await send_alert_fn(msg)
                    except Exception as e:
                        print(f"[AutoClose] Telegram notify failed: {e}")

                print(f"[AutoClose] Closed {trade['id']} | {close_reason} | PnL: {pnl:+.4f}")

    return closed_this_cycle


async def get_paper_stats() -> dict:
    """Pull performance stats from all closed paper trades."""
    client = get_client()
    if not client:
        return {}
    try:
        result = client.table("signals").select(
            "pnl, pnl_pct, status, signal_type, confidence, asset, close_reason"
        ).execute()
        rows = result.data

        closed = [r for r in rows if r["status"] == "CLOSED" and r["pnl"] is not None]
        open_trades = [r for r in rows if r["status"] == "OPEN"]

        if not closed:
            return {
                "total_closed": 0,
                "open_trades": len(open_trades),
                "message": "No closed trades yet"
            }

        wins = [r for r in closed if r["pnl"] > 0]
        total_pnl = sum(r["pnl"] for r in closed)
        win_rate = len(wins) / len(closed) * 100
        targets_hit = [r for r in closed if r.get("close_reason") == "TARGET_HIT"]
        stops_hit = [r for r in closed if r.get("close_reason") == "STOP_HIT"]

        return {
            "total_closed": len(closed),
            "open_trades": len(open_trades),
            "wins": len(wins),
            "losses": len(closed) - len(wins),
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_pnl, 4),
            "avg_pnl": round(total_pnl / len(closed), 4),
            "targets_hit": len(targets_hit),
            "stops_hit": len(stops_hit),
            "btc_trades": len([r for r in closed if r["asset"] == "BTC"]),
            "eth_trades": len([r for r in closed if r["asset"] == "ETH"]),
        }
    except Exception as e:
        print(f"[Supabase] Error fetching stats: {e}")
        return {}


async def get_open_trades() -> list:
    """Fetch all currently open paper trades."""
    client = get_client()
    if not client:
        return []
    try:
        result = client.table("signals").select(
            "id, asset, market_question, signal_type, paper_entry, paper_target, paper_stop, created_at"
        ).eq("status", "OPEN").execute()
        return result.data
    except Exception as e:
        print(f"[Supabase] Error fetching open trades: {e}")
        return []