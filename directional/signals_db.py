"""
signals_db.py

Async Supabase write helpers shared by bot.py and bot2.py.

Mirrors the pattern used by watcher/wallet_watcher.py — uses aiohttp directly
against Supabase REST API rather than the v2 SDK (which had httpx
incompatibility issues with our env).

Three public functions:
    insert_signal(session, row)
    update_signal_fill(session, condition_id, bot, direction, **fill_data)
    update_signal_outcome(session, condition_id, bot, direction, **outcome_data)

All are best-effort: they log errors but never raise, so DB problems
don't break trading.
"""

import os
from datetime import datetime, timezone

import aiohttp


SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY') or os.getenv('SUPABASE_KEY', '')

_TABLE = 'signals'


def _log(msg: str) -> None:
    print(f'[SignalsDB {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


def _headers(prefer: str = 'return=minimal') -> dict:
    return {
        'apikey':         SUPABASE_KEY,
        'Authorization':  f'Bearer {SUPABASE_KEY}',
        'Content-Type':   'application/json',
        'Prefer':         prefer,
    }


async def insert_signal(session: aiohttp.ClientSession, row: dict) -> bool:
    """
    Insert a new signal row. Returns True on success, False on failure.

    The row dict should match the `signals` table columns. Anything
    missing is left NULL by Postgres.

    On duplicate (unique constraint on condition_id+bot+direction),
    silently returns True — we treat duplicates as success since the
    intended state (a row exists) is true.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        _log('No Supabase URL/key — skipping insert')
        return False

    url = f'{SUPABASE_URL}/rest/v1/{_TABLE}'
    headers = _headers('return=minimal,resolution=ignore-duplicates')
    try:
        async with session.post(url, json=row, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (200, 201):
                return True
            text = await r.text()
            if 'duplicate' in text.lower() or 'unique' in text.lower():
                return True   # idempotent: row already exists
            _log(f'Insert HTTP {r.status}: {text[:200]}')
            return False
    except Exception as e:
        _log(f'Insert error: {e}')
        return False


async def update_signal_fill(
    session: aiohttp.ClientSession,
    condition_id: str,
    bot: str,
    direction: str,
    fill_method: str,           # 'fak' or 'gtc'
    fill_price: float,
    fill_size: int,
    fill_tx: str = '',
    trade_status: str | None = None,
) -> bool:
    """
    Update a signal row to mark it as filled. Keyed on
    (condition_id, bot, direction).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False

    patch = {
        'filled':         True,
        'fill_method':    fill_method,
        'fill_price':     round(float(fill_price), 6),
        'fill_size':      int(fill_size),
        'fill_tx':        fill_tx,
        'fill_timestamp': datetime.now(timezone.utc).isoformat(),
    }
    if trade_status:
        patch['trade_status'] = trade_status

    # PostgREST filter syntax — match by composite key
    params = (
        f'condition_id=eq.{condition_id}'
        f'&bot=eq.{bot}'
        f'&direction=eq.{direction.upper()}'
    )
    url = f'{SUPABASE_URL}/rest/v1/{_TABLE}?{params}'
    try:
        async with session.patch(url, json=patch, headers=_headers(),
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (200, 204):
                return True
            text = await r.text()
            _log(f'Fill update HTTP {r.status}: {text[:200]}')
            return False
    except Exception as e:
        _log(f'Fill update error: {e}')
        return False


async def update_signal_outcome(
    session: aiohttp.ClientSession,
    condition_id: str,
    bot: str,
    direction: str,
    outcome: str,                  # 'WIN' / 'LOSS' / 'EXPIRED'
    up_won: bool,
    pnl: float,
    final_up_price: float | None = None,
    final_down_price: float | None = None,
) -> bool:
    """
    Update a signal row with resolution data. Keyed on
    (condition_id, bot, direction).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False

    patch = {
        'outcome':            outcome.upper(),
        'up_won':             bool(up_won),
        'pnl':                round(float(pnl), 6),
        'resolved_timestamp': datetime.now(timezone.utc).isoformat(),
    }
    if final_up_price is not None:
        patch['final_up_price'] = round(float(final_up_price), 6)
    if final_down_price is not None:
        patch['final_down_price'] = round(float(final_down_price), 6)

    params = (
        f'condition_id=eq.{condition_id}'
        f'&bot=eq.{bot}'
        f'&direction=eq.{direction.upper()}'
    )
    url = f'{SUPABASE_URL}/rest/v1/{_TABLE}?{params}'
    try:
        async with session.patch(url, json=patch, headers=_headers(),
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status in (200, 204):
                return True
            text = await r.text()
            _log(f'Outcome update HTTP {r.status}: {text[:200]}')
            return False
    except Exception as e:
        _log(f'Outcome update error: {e}')
        return False


# ─── Convenience: build a row dict from a signal in a consistent shape ─────

def build_signal_row(
    *,
    bot: str,
    asset: str,
    timeframe: str,
    market: dict,
    direction: str,
    cl_price: float,
    opening_cl: float,
    bn_price: float,
    opening_bn: float,
    confidence: float,
    intended_shares: int,
    max_fill_price: float,
    crowd_price: float,
    asks_at_cap: int = 0,
    best_ask_at_signal: float | None = None,
    momentum_score: str = '',
    early_mode: bool = False,
    safe_mode: bool = False,
    trade_status: str = 'PENDING',
) -> dict:
    """
    Build a dict matching the `signals` table schema from the variables
    bot.py / bot2.py have at signal-fire time.
    """
    cl_pct = (cl_price - opening_cl) / opening_cl * 100 if opening_cl else 0
    bn_pct = (bn_price - opening_bn) / opening_bn * 100 if opening_bn else 0
    return {
        'signal_timestamp':   datetime.now(timezone.utc).isoformat(),
        'bot':                bot,
        'asset':              asset.upper(),
        'timeframe':          timeframe,
        'market_title':       (market.get('title') or '')[:500],
        'slug':               market.get('slug', ''),
        'condition_id':       market.get('condition_id', ''),
        'direction':          direction.upper(),
        'cl_price':           round(float(cl_price), 6),
        'opening_cl':         round(float(opening_cl), 6),
        'cl_pct':             round(float(cl_pct), 4),
        'bn_price':           round(float(bn_price), 6),
        'opening_bn':         round(float(opening_bn), 6),
        'bn_pct':             round(float(bn_pct), 4),
        'confidence':         round(float(confidence), 4),
        'momentum_score':     momentum_score,
        'early_mode':         early_mode,
        'crowd_price':        round(float(crowd_price), 4),
        'intended_shares':    int(intended_shares),
        'max_fill_price':     round(float(max_fill_price), 4),
        'cost_estimate':      round(intended_shares * max_fill_price, 4),
        'asks_at_cap':        int(asks_at_cap),
        'best_ask_at_signal': round(float(best_ask_at_signal), 4) if best_ask_at_signal else None,
        'safe_mode':          bool(safe_mode),
        'trade_status':       trade_status,
        'filled':             False,
        'outcome':            'PENDING',
    }