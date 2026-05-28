"""
positions_watcher.py

Positions-based polling to complement the /trades endpoint.

The /trades endpoint on data-api can lag for hours or miss trades entirely
(limit fills, relayer trades). The /positions endpoint reflects the CURRENT
holdings, so by snapshotting positions each cycle and diffing, we detect:

  - NEW position (market appears that wasn't there)            -> entry alert
  - size INCREASED on an existing position                    -> add alert (entry-like)
  - size DECREASED                                             -> partial-exit alert
  - position disappears                                        -> full-exit alert
    (UNLESS it resolved/was redeemed — we skip those)

Dedup with the /trades path: a short-lived "recently alerted" cache keyed by
(wallet, asset) prevents the same event firing twice when both paths see it.

Copy-trade: only fires on TRULY NEW positions (not size-increases), per config.
"""

import os
import time
from datetime import datetime, timezone

import aiohttp


POSITIONS_API = 'https://data-api.polymarket.com/positions'

# Per-wallet snapshot of positions: {addr: {asset_id: position_dict}}
_position_snapshots: dict[str, dict[str, dict]] = {}

# Dedup cache shared with the trades path: {(addr, asset): expiry_ts}
# When the trades path alerts on something, it adds the key here; positions
# path checks it before alerting (and vice versa).
_recently_alerted: dict[tuple[str, str], float] = {}
_DEDUP_WINDOW_SEC = 180  # 3 minutes


def mark_alerted(addr: str, asset: str) -> None:
    """Called by either path after it alerts, to suppress the other path."""
    _recently_alerted[(addr.lower(), str(asset))] = time.time() + _DEDUP_WINDOW_SEC


def _was_recently_alerted(addr: str, asset: str) -> bool:
    key = (addr.lower(), str(asset))
    exp = _recently_alerted.get(key)
    if exp is None:
        return False
    if time.time() > exp:
        del _recently_alerted[key]
        return False
    return True


def _prune_dedup_cache() -> None:
    now = time.time()
    for k in [k for k, v in _recently_alerted.items() if now > v]:
        del _recently_alerted[k]


async def fetch_positions(session: aiohttp.ClientSession, wallet: str,
                          limit: int = 100) -> list[dict]:
    """Fetch current positions for a wallet. Returns list of position dicts."""
    try:
        async with session.get(
            POSITIONS_API,
            params={'user': wallet, 'limit': limit},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return []
            data = await r.json()
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _is_resolved_or_redeemed(pos: dict) -> bool:
    """
    A position that resolved or was redeemed should NOT count as a 'sell'.
    Heuristics: redeemable flag set, or current price is 0/1 (market settled),
    or currentValue is 0 with redeemable true.
    """
    if pos.get('redeemable'):
        return True
    cur_price = pos.get('curPrice')
    if cur_price is not None:
        try:
            cp = float(cur_price)
            if cp <= 0.001 or cp >= 0.999:
                return True
        except (TypeError, ValueError):
            pass
    return False


def seed_positions(addr: str, positions: list[dict]) -> None:
    """Store initial snapshot without firing alerts (called once on startup)."""
    snap = {}
    for p in positions:
        asset = p.get('asset')
        if asset:
            snap[str(asset)] = p
    _position_snapshots[addr.lower()] = snap


def diff_positions(addr: str, positions: list[dict]) -> list[dict]:
    """
    Compare current positions to last snapshot, return a list of change events.
    Each event: {
        'kind': 'new' | 'increase' | 'decrease' | 'exit',
        'position': <current or last position dict>,
        'old_size': float, 'new_size': float,
    }
    Updates the stored snapshot.
    """
    addr_l = addr.lower()
    prev = _position_snapshots.get(addr_l, {})
    curr = {}
    for p in positions:
        asset = p.get('asset')
        if asset:
            curr[str(asset)] = p

    events = []

    # Check current positions against previous
    for asset, pos in curr.items():
        new_size = float(pos.get('size', 0) or 0)
        if asset not in prev:
            # Brand new position — but only if size is meaningful and not
            # an artifact of a just-resolved market
            if new_size > 0 and not _is_resolved_or_redeemed(pos):
                events.append({
                    'kind': 'new', 'position': pos,
                    'old_size': 0.0, 'new_size': new_size,
                })
        else:
            old_size = float(prev[asset].get('size', 0) or 0)
            # Size increased meaningfully (>1% to avoid float noise)
            if new_size > old_size * 1.01 and new_size - old_size > 0.01:
                events.append({
                    'kind': 'increase', 'position': pos,
                    'old_size': old_size, 'new_size': new_size,
                })
            elif new_size < old_size * 0.99 and old_size - new_size > 0.01:
                # Decreased — but skip if it's a resolution/redemption
                if not _is_resolved_or_redeemed(pos):
                    events.append({
                        'kind': 'decrease', 'position': pos,
                        'old_size': old_size, 'new_size': new_size,
                    })

    # Check for positions that fully disappeared (full exit)
    for asset, pos in prev.items():
        if asset not in curr:
            old_size = float(pos.get('size', 0) or 0)
            # Position gone. If the prev snapshot showed it as resolvable, it
            # likely just settled — skip. Otherwise it's a real full exit.
            if old_size > 0 and not _is_resolved_or_redeemed(pos):
                events.append({
                    'kind': 'exit', 'position': pos,
                    'old_size': old_size, 'new_size': 0.0,
                })

    # Update snapshot
    _position_snapshots[addr_l] = curr
    _prune_dedup_cache()
    return events


def format_position_event(label: str, event: dict) -> str:
    """Human-readable Telegram alert for a position change."""
    pos = event['position']
    kind = event['kind']
    title = (pos.get('title') or 'Unknown market')[:60]
    outcome = pos.get('outcome', '?')
    avg_price = pos.get('avgPrice', 0)
    slug = pos.get('slug', '')
    new_size = event['new_size']
    old_size = event['old_size']

    if kind == 'new':
        emoji, verb = '🟢', 'opened'
        detail = f'{new_size:.0f} shares @ avg ${float(avg_price):.3f}'
    elif kind == 'increase':
        emoji, verb = '🟢', 'added to'
        detail = f'{old_size:.0f} → {new_size:.0f} shares (avg ${float(avg_price):.3f})'
    elif kind == 'decrease':
        emoji, verb = '🟡', 'trimmed'
        detail = f'{old_size:.0f} → {new_size:.0f} shares'
    else:  # exit
        emoji, verb = '🔴', 'exited'
        detail = f'closed {old_size:.0f} shares'

    cur_val = pos.get('currentValue')
    pnl = pos.get('cashPnl')
    extra = ''
    if pnl is not None:
        try:
            extra = f'\nPnL so far: ${float(pnl):+.2f}'
        except (TypeError, ValueError):
            pass

    return (
        f'{emoji} {label} {verb} position\n'
        f'{outcome} — {detail}\n'
        f'Market: {title}'
        f'{extra}\n'
        f'https://polymarket.com/event/{slug}'
    )
