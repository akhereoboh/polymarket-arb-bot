"""
GTC fallback for the directional bot.

Drop this into directional/gtc_fallback.py. Import and call from place_trade
when the FAK order doesn't fill.

Strategy:
  1. FAK fires at best-ask + 0.01 (current behavior)
  2. If FAK returns 0 shares filled OR result indicates no fill:
     - Post a GTC at a less aggressive price (best_ask, no buffer)
     - Schedule auto-cancel at T - 5 seconds before market close
  3. Auto-cancel runs in background — does not block scanner loop
"""

import asyncio
import time
from datetime import datetime, timezone


# Hard deadline before market close — cancel any unfilled GTC by this time.
# Below this many seconds before close, the order is too risky to keep open.
T_MINUS_CANCEL_SEC = 5
FILL_POLL_INTERVAL_SEC = 2

# Track active GTC orders so we don't double-post on the same market.
# Maps condition_id -> {order_id, cancel_at_ts, market_title}
_active_gtcs: dict[str, dict] = {}


def fak_filled(result: dict) -> tuple[bool, int]:
    """
    Inspect a FAK result and return (was_filled, shares_filled).

    Polymarket CLOB FAK responses vary in shape. Common indicators:
      - 'status' = 'matched' or 'success' or 'live' suggest fill
      - 'making_amount' or 'taking_amount' nonzero suggests partial/full fill
      - 'orderHashes' present suggests on-chain match
      - 'errorMsg' or 'error' field suggests failure
    """
    if not isinstance(result, dict):
        return False, 0

    if result.get('status') in ('error', 'no_liquidity', 'dry_run'):
        return False, 0

    if result.get('errorMsg') or result.get('error'):
        return False, 0

    # Try to extract filled shares
    for k in ('takingAmount', 'taking_amount', 'matchedAmount', 'matched_amount', 'size_matched'):
        v = result.get(k)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return True, int(fv)
            except (ValueError, TypeError):
                pass

    # Order hashes typically mean matched on-chain
    if result.get('orderHashes') or result.get('order_hashes'):
        return True, 0  # filled but size unknown — treat as filled

    # If we got an orderID but no fill indication, FAK didn't match
    if result.get('orderID') or result.get('order_id'):
        # Could be partial — be conservative and treat as no-fill
        return False, 0

    return False, 0


async def place_gtc_fallback(
    client_factory,         # callable returning a fresh ClobClient
    market: dict,
    direction: str,
    shares: int,
    confidence: float,
    book: dict,             # the order book we already fetched in place_trade
    OrderArgs, OrderType, PartialCreateOrderOptions, Side,
) -> dict:
    """
    Post a GTC order at a less aggressive price than the FAK attempt.

    Returns dict with order_id, cancel_at_ts, status.
    Spawns a background task that auto-cancels at T - 5s before market close.
    """
    cid = market['condition_id']
    if cid in _active_gtcs:
        print(f'  [GTC] Already have active GTC on {market["title"][:40]} — skipping')
        return {'status': 'duplicate'}

    token_id = market['up_token'] if direction == 'up' else market['down_token']
    asks = sorted(book['asks'], key=lambda x: float(x['price']))
    if not asks:
        return {'status': 'no_asks'}

    # GTC sits at the current best ask (no buffer — more passive than FAK)
    best_ask = float(asks[0]['price'])
    gtc_price = round(min(best_ask, 0.99), 2)

    seconds_left = (market['end_time'] - datetime.now(timezone.utc)).total_seconds()
    cancel_at_ts = time.time() + seconds_left - T_MINUS_CANCEL_SEC

    if cancel_at_ts <= time.time() + 2:
        print(f'  [GTC] Not enough time left ({seconds_left:.0f}s) — skipping')
        return {'status': 'too_late'}

    side_str = 'UP' if direction == 'up' else 'DOWN'
    print(
        f'  [GTC] Placing fallback: {side_str} {shares} @ {gtc_price} '
        f'(auto-cancel in {seconds_left - T_MINUS_CANCEL_SEC:.0f}s)'
    )

    try:
        client = client_factory()
        result = client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=gtc_price,
                size=shares,
                side=Side.BUY,
            ),
            options=PartialCreateOrderOptions(tick_size='0.01', neg_risk=False),
            order_type=OrderType.GTC,
        )
        order_id = (
            result.get('orderID') or result.get('order_id')
            if isinstance(result, dict) else None
        )
        print(f'  [GTC] Posted: order_id={order_id} result={result}')

        if order_id:
            _active_gtcs[cid] = {
                'order_id': order_id,
                'cancel_at_ts': cancel_at_ts,
                'market_title': market['title'],
                'token_id': token_id,
                'shares': shares,
                'price': gtc_price,
                'direction': direction,
                'market_ref': market,
                'status': 'open',
            }
            # Fire and forget — background cancel watcher + fill poller
            asyncio.create_task(_cancel_watcher(client_factory, cid))
            asyncio.create_task(_fill_poller(client_factory, cid))

        return {'status': 'posted', 'order_id': order_id, 'price': gtc_price}
    except Exception as e:
        print(f'  [GTC] Error posting: {e}')
        return {'status': 'error', 'error': str(e)}


async def _cancel_watcher(client_factory, condition_id: str) -> None:
    """Background task: sleep until cancel_at_ts, then cancel the GTC."""
    info = _active_gtcs.get(condition_id)
    if not info:
        return

    wait = info['cancel_at_ts'] - time.time()
    if wait > 0:
        await asyncio.sleep(wait)

    # Re-check it's still tracked (might have been removed by a fill notification)
    info = _active_gtcs.get(condition_id)
    if not info:
        return

    order_id = info['order_id']
    title = info['market_title']
    print(f'  [GTC] Auto-cancelling order {order_id} on {title[:40]}')

    try:
        client = client_factory()
        # py_clob_client_v2 cancel API — try the common method names
        if hasattr(client, 'cancel'):
            res = client.cancel(order_id=order_id)
        elif hasattr(client, 'cancel_order'):
            res = client.cancel_order(order_id)
        else:
            print(f'  [GTC] No cancel method found on client')
            res = None
        print(f'  [GTC] Cancel result: {res}')
    except Exception as e:
        print(f'  [GTC] Cancel error: {e}')
    finally:
        _active_gtcs.pop(condition_id, None)


def clear_gtc_tracking(condition_id: str) -> None:
    """Call this if you detect a fill via balance change — stops the cancel watcher."""
    _active_gtcs.pop(condition_id, None)


def active_gtc_count() -> int:
    return len(_active_gtcs)

# Callback set by bot.py at startup so we can fire Telegram alerts on fill.
# Signature: async def on_fill(market: dict, fill_info: dict) -> None
_on_fill_callback = None


def set_on_fill_callback(callback) -> None:
    """Register a coroutine to be called when a GTC fills.

    callback(market, fill_info) where fill_info = {
        'order_id', 'shares', 'price', 'cost', 'filled_at_ts', 'direction'
    }
    """
    global _on_fill_callback
    _on_fill_callback = callback


def _fetch_order_status(client, order_id: str) -> dict | None:
    """Try common py_clob_client_v2 method names for fetching a single order."""
    for method_name in ('get_order', 'getOrder', 'order'):
        if hasattr(client, method_name):
            try:
                fn = getattr(client, method_name)
                result = fn(order_id)
                if isinstance(result, dict):
                    return {
                        'status': (result.get('status') or result.get('state') or '').lower(),
                        'size_matched': float(
                            result.get('size_matched')
                            or result.get('sizeMatched')
                            or result.get('matched_amount')
                            or result.get('matchedAmount')
                            or 0
                        ),
                        'raw': result,
                    }
            except Exception as e:
                print(f'  [GTC-poll] {method_name}({order_id}) failed: {e}')
                continue
    return None


async def _fill_poller(client_factory, condition_id: str) -> None:
    """Poll CLOB every 2s until the GTC fills (full fill only) or is cancelled."""
    while True:
        await asyncio.sleep(FILL_POLL_INTERVAL_SEC)

        info = _active_gtcs.get(condition_id)
        if not info or info.get('status') != 'open':
            # Cancelled, removed, or already handled
            return

        try:
            client = client_factory()
            status = _fetch_order_status(client, info['order_id'])
        except Exception as e:
            print(f'  [GTC-poll] Error fetching status: {e}')
            continue

        if status is None:
            continue

        target_shares = info['shares']
        size_matched = status.get('size_matched', 0)
        order_status = status.get('status', '')

        # Detect exchange-side cancellation/expiry
        if order_status in ('cancelled', 'canceled', 'expired'):
            print(f'  [GTC-poll] Order {info["order_id"]} terminated by exchange ({order_status})')
            info['status'] = 'cancelled'
            return

        # Partial fill — log but keep waiting (per spec: full fill only)
        if 0 < size_matched < target_shares:
            print(f'  [GTC-poll] Partial fill: {size_matched}/{target_shares} (waiting for full)')
            continue

        # Full fill detected
        if size_matched >= target_shares or order_status in ('matched', 'filled'):
            info['status'] = 'filled'
            fill_info = {
                'order_id': info['order_id'],
                'shares': target_shares,
                'price': info['price'],
                'cost': round(target_shares * info['price'], 4),
                'filled_at_ts': time.time(),
                'direction': info['direction'],
            }
            print(f'  [GTC-poll] FULL FILL: {target_shares} @ {info["price"]} on {info["market_title"][:40]}')

            if _on_fill_callback:
                try:
                    await _on_fill_callback(info['market_ref'], fill_info)
                except Exception as e:
                    print(f'  [GTC-poll] on_fill callback error: {e}')

            return