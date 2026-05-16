import os
import aiohttp
from datetime import datetime, timezone

SUPABASE_URL = None
SUPABASE_KEY = None


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


async def log_arb_trade(opp: dict) -> bool:
    """Log a new arb opportunity to Supabase."""
    url, _ = get_credentials()
    if not url:
        return False
    try:
        payload = {
            "asset": opp["asset"],
            "market_question": opp["market_question"],
            "market_id": opp["market_id"],
            "slug": opp["slug"],
            "timeframe": opp["timeframe"],
            "up_price": opp["up_price"],
            "down_price": opp["down_price"],
            "total_cost": opp["total_cost"],
            "arb_profit": opp["arb_profit"],
            "shares": opp["shares"],
            "total_invested": opp["total_invested"],
            "expected_payout": opp["expected_payout"],
            "expected_profit": opp["expected_profit"],
            "status": "OPEN",
            "trade_type": "LIVE",
            "market_end_time": opp["market_end_time"],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{url}/rest/v1/arb_trades",
                json=payload,
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status in (200, 201):
                    print(f"[Supabase] Arb trade logged: {opp['asset']}")
                    return True
                text = await resp.text()
                print(f"[Supabase] Log error {resp.status}: {text}")
                return False
    except Exception as e:
        print(f"[Supabase] log_arb_trade error: {e}")
        return False


async def settle_expired_trades(send_alert_fn=None) -> list:
    """
    Check all OPEN arb trades whose market_end_time has passed.
    Since we bought both sides, payout is always SHARES * 1.0.
    Profit = payout - total_invested. Always positive if arb was valid.
    """
    url, _ = get_credentials()
    if not url:
        return []

    now = datetime.now(timezone.utc).isoformat()

    try:
        async with aiohttp.ClientSession() as session:
            # fetch open trades that have expired
            async with session.get(
                f"{url}/rest/v1/arb_trades",
                params={
                    "status": "eq.OPEN",
                    "market_end_time": f"lt.{now}",
                    "select": "*"
                },
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                trades = await resp.json()

        if not trades:
            return []

        settled = []

        for trade in trades:
            shares = trade.get("shares", 5)
            total_invested = trade.get("total_invested", 0)
            expected_payout = shares * 1.0
            actual_profit = round(expected_payout - total_invested, 4)
            profit_pct = round(actual_profit / total_invested * 100, 4)

            async with aiohttp.ClientSession() as session:
                async with session.patch(
                    f"{url}/rest/v1/arb_trades",
                    params={"id": f"eq.{trade['id']}"},
                    json={
                        "status": "SETTLED",
                        "actual_payout": expected_payout,
                        "actual_profit": actual_profit,
                        "closed_at": now,
                    },
                    headers=headers(),
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status not in (200, 204):
                        continue

            settled.append(trade)
            print(
                f"[Settle] {trade['asset']} settled | "
                f"Invested: ${total_invested} | "
                f"Profit: ${actual_profit} ({profit_pct}%)"
            )

            if send_alert_fn:
                msg = (
                    f"✅ *Arb trade settled*\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"*Asset:* {trade['asset']}\n"
                    f"*Market:* {trade['market_question']}\n\n"
                    f"*Invested:* ${total_invested:.4f}\n"
                    f"*Payout:* ${expected_payout:.4f}\n"
                    f"*Profit:* ${actual_profit:.4f} ({profit_pct:.2f}%)\n\n"
                    f"_Both sides bought — direction didn't matter_ 🎯"
                )
                try:
                    await send_alert_fn(msg)
                except Exception as e:
                    print(f"[Settle] Telegram error: {e}")

        return settled

    except Exception as e:
        print(f"[Settle] Error: {e}")
        return []


async def get_arb_stats() -> dict:
    """Overall paper trading performance."""
    url, _ = get_credentials()
    if not url:
        return {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{url}/rest/v1/arb_trades",
                params={"select": "actual_profit,total_invested,status,asset"},
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return {}
                rows = await resp.json()

        settled = [r for r in rows if r["status"] == "SETTLED"]
        open_trades = [r for r in rows if r["status"] == "OPEN"]

        if not settled:
            return {
                "total_settled": 0,
                "open_trades": len(open_trades),
                "message": "No settled trades yet"
            }

        total_profit = sum(r["actual_profit"] for r in settled if r["actual_profit"])
        total_invested = sum(r["total_invested"] for r in settled if r["total_invested"])

        return {
            "total_settled": len(settled),
            "open_trades": len(open_trades),
            "total_profit": round(total_profit, 4),
            "total_invested": round(total_invested, 4),
            "avg_profit": round(total_profit / len(settled), 4),
            "roi_pct": round(total_profit / total_invested * 100, 4) if total_invested else 0,
        }
    except Exception as e:
        print(f"[Supabase] stats error: {e}")
        return {}


async def get_open_arb_trades() -> list:
    url, _ = get_credentials()
    if not url:
        return []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{url}/rest/v1/arb_trades",
                params={
                    "status": "eq.OPEN",
                    "select": "id,asset,market_question,total_cost,expected_profit,market_end_time,created_at"
                },
                headers=headers(),
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return []
                return await resp.json()
    except Exception as e:
        print(f"[Supabase] open trades error: {e}")
        return []