import os
import aiohttp
from datetime import datetime, timezone
from typing import Optional

SUPABASE_URL = None
SUPABASE_KEY = None

TAKE_PROFIT = 0.15
STOP_LOSS = 0.08


def get_credentials():
    global SUPABASE_URL, SUPABASE_KEY
    if not SUPABASE_URL:
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    return SUPABASE_URL, SUPABASE_KEY


def headers():
    _, key = get_credentials()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }


async def log_signal(signal) -> bool:
    url, _ = get_credentials()
    if not url:
        return False
    try:
        target = round(min(signal.entry_price + TAKE_PROFIT, 0.95), 4)
        stop = round(max(signal.entry_price - STOP_LOSS, 0.02), 4)
        payload = {
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
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{url}/rest/v1/signals",
                json=payload,
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status in (200, 201):
                    print(f"[Supabase] Signal logged: {signal.signal_type} {signal.asset}")
                    return True
                else:
                    text = await resp.text()
                    print(f"[Supabase] Log error {resp.status}: {text}")
                    return False
    except Exception as e:
        print(f"[Supabase] log_signal error: {e}")
        return False


async def check_and_close_open_trades(send_alert_fn=None) -> list:
    url, _ = get_credentials()
    if not url:
        return []

    try:
        async with aiohttp.ClientSession() as session:
            # fetch open trades
            async with session.get(
                f"{url}/rest/v1/signals",
                params={"status": "eq.OPEN", "select": "*"},
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                open_trades = await resp.json()

            if not open_trades:
                return []

            from core.polymarket import get_markets_with_prices
            markets = await get_markets_with_prices()
            price_map = {m["condition_id"]: m for m in markets}

            closed_this_cycle = []

            for trade in open_trades:
                condition_id = trade.get("market_id")
                market = price_map.get(condition_id)
                if not market:
                    continue

                signal_type = trade["signal_type"]
                entry = trade["paper_entry"]
                target = trade["paper_target"]
                stop = trade["paper_stop"]

                current_price = market["yes_price"] if signal_type == "BUY_YES" else market["no_price"]

                close_reason = None
                if current_price >= target:
                    close_reason = "TARGET_HIT"
                elif current_price <= stop:
                    close_reason = "STOP_HIT"

                if not close_reason:
                    continue

                pnl = round(current_price - entry, 4)
                pnl_pct = round(pnl / entry * 100, 2)

                # close in supabase
                async with session.patch(
                    f"{url}/rest/v1/signals",
                    params={"id": f"eq.{trade['id']}"},
                    json={
                        "status": "CLOSED",
                        "paper_result": current_price,
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                        "close_reason": close_reason,
                        "closed_at": datetime.now(timezone.utc).isoformat(),
                    },
                    headers=headers(),
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status not in (200, 204):
                        continue

                closed_this_cycle.append(trade)

                if send_alert_fn:
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
                        print(f"[AutoClose] Telegram error: {e}")

                print(f"[AutoClose] Closed {trade['id']} | {close_reason} | PnL: {pnl:+.4f}")

            return closed_this_cycle

    except Exception as e:
        print(f"[AutoClose] Error: {e}")
        return []


async def get_paper_stats() -> dict:
    url, _ = get_credentials()
    if not url:
        return {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{url}/rest/v1/signals",
                params={"select": "pnl,pnl_pct,status,signal_type,confidence,asset,close_reason"},
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return {}
                rows = await resp.json()

        closed = [r for r in rows if r["status"] == "CLOSED" and r["pnl"] is not None]
        open_trades = [r for r in rows if r["status"] == "OPEN"]

        if not closed:
            return {"total_closed": 0, "open_trades": len(open_trades)}

        wins = [r for r in closed if r["pnl"] > 0]
        total_pnl = sum(r["pnl"] for r in closed)

        return {
            "total_closed": len(closed),
            "open_trades": len(open_trades),
            "wins": len(wins),
            "losses": len(closed) - len(wins),
            "win_rate": round(len(wins) / len(closed) * 100, 1),
            "total_pnl": round(total_pnl, 4),
            "avg_pnl": round(total_pnl / len(closed), 4),
            "targets_hit": len([r for r in closed if r.get("close_reason") == "TARGET_HIT"]),
            "stops_hit": len([r for r in closed if r.get("close_reason") == "STOP_HIT"]),
            "btc_trades": len([r for r in closed if r["asset"] == "BTC"]),
            "eth_trades": len([r for r in closed if r["asset"] == "ETH"]),
        }
    except Exception as e:
        print(f"[Supabase] stats error: {e}")
        return {}


async def get_open_trades() -> list:
    url, _ = get_credentials()
    if not url:
        return []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{url}/rest/v1/signals",
                params={
                    "status": "eq.OPEN",
                    "select": "id,asset,market_question,signal_type,paper_entry,paper_target,paper_stop,created_at"
                },
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                return await resp.json()
    except Exception as e:
        print(f"[Supabase] get_open_trades error: {e}")
        return []