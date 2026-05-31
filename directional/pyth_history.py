"""
pyth_history.py — Rolling per-asset price history backed by Pyth.

Mirrors the architecture of bot.py's _btc_history and _cl_history:
each asset has a list of (timestamp, price) tuples covering the most
recent ~20 minutes.

Public API:
    start_pyth_feeders(loop, assets, poll_interval=5)
    get_opening_pyth_price(asset: str, target_ts: float) -> float | None
    get_latest_pyth_price(asset: str) -> float | None

Updates happen asynchronously in background tasks. Failures are silent
(prints a warning, retries on next interval).
"""

import asyncio
import time
from datetime import datetime, timezone

import aiohttp

from pyth_feed import get_pyth_prices_batch, feed_id_for_asset


# Module-level state: per-asset history of (timestamp, price)
_history: dict[str, list[tuple[float, float]]] = {}
_HISTORY_SECONDS = 1200  # 20 minutes retention
_active_assets: list[str] = []


def _log(msg):
    print(f'[PythHist {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


def _trim(asset: str) -> None:
    cutoff = time.time() - _HISTORY_SECONDS
    h = _history.get(asset, [])
    while h and h[0][0] < cutoff:
        h.pop(0)


async def _feeder_loop(asset: str, poll_interval: int):
    """Background task: poll Pyth for one asset, append to history."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                batch = await get_pyth_prices_batch(session, [asset])
                price = batch.get(asset.upper())
                if price is not None:
                    _history.setdefault(asset.upper(), []).append((time.time(), price))
                    _trim(asset.upper())
                # else: silent failure, retry next iteration
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log(f'{asset} feeder error: {e}')
            await asyncio.sleep(poll_interval)


async def _multi_feeder_loop(assets: list[str], poll_interval: int):
    """
    Single-session feeder loop that polls multiple assets in one HTTP call.
    More efficient than per-asset feeders.
    """
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                batch = await get_pyth_prices_batch(session, assets)
                now = time.time()
                for a, p in batch.items():
                    if p is not None:
                        _history.setdefault(a, []).append((now, p))
                        _trim(a)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log(f'multi-feeder error: {e}')
            await asyncio.sleep(poll_interval)


def start_pyth_feeder(loop: asyncio.AbstractEventLoop,
                       assets: list[str],
                       poll_interval: int = 5) -> asyncio.Task:
    """
    Start the Pyth feeder for the given list of assets.

    Returns the asyncio.Task so caller can cancel on shutdown.
    Uses a single batched HTTP call per cycle to be efficient.
    """
    global _active_assets
    _active_assets = [a.upper() for a in assets if feed_id_for_asset(a)]
    skipped = [a.upper() for a in assets if not feed_id_for_asset(a)]
    if skipped:
        _log(f'Skipping assets with no Pyth feed: {skipped}')
    if not _active_assets:
        _log('No supported assets to feed — feeder not started')
        return None
    _log(f'Starting Pyth feeder for {_active_assets} (poll={poll_interval}s)')
    return loop.create_task(_multi_feeder_loop(_active_assets, poll_interval))


def get_latest_pyth_price(asset: str) -> float | None:
    """Most recent Pyth price for the asset, or None if no history yet."""
    h = _history.get(asset.upper(), [])
    if not h:
        return None
    return h[-1][1]


def get_pyth_price_at_time(asset: str, target_ts: float) -> float | None:
    """
    Find the price closest to (but at or before) target_ts.
    Used to derive an "opening" Pyth price for a market window.
    """
    h = _history.get(asset.upper(), [])
    if not h:
        return None
    # Walk backward to find the most recent ts <= target_ts
    for ts, p in reversed(h):
        if ts <= target_ts:
            return p
    # If target_ts is older than our entire history, return earliest
    return h[0][1]


def get_pyth_move_pct(asset: str, opening_ts: float) -> float | None:
    """
    Return percent move from Pyth opening price (at opening_ts) to latest price.
    Returns None if history not deep enough.
    """
    opening = get_pyth_price_at_time(asset, opening_ts)
    latest = get_latest_pyth_price(asset)
    if opening is None or latest is None or opening == 0:
        return None
    return (latest - opening) / opening * 100


def history_size(asset: str) -> int:
    """How many price entries do we have for this asset? Useful for warm-up checks."""
    return len(_history.get(asset.upper(), []))


def history_age_seconds(asset: str) -> float | None:
    """How old is the oldest entry? Returns None if no history."""
    h = _history.get(asset.upper(), [])
    if not h:
        return None
    return time.time() - h[0][0]


# Quick CLI test: build a 30-sec history then read it
if __name__ == '__main__':
    async def _test():
        loop = asyncio.get_event_loop()
        task = start_pyth_feeder(loop, ['BTC', 'ETH', 'SOL'], poll_interval=3)
        print('Building history for 15 seconds...')
        await asyncio.sleep(15)
        for a in ['BTC', 'ETH', 'SOL']:
            n = history_size(a)
            latest = get_latest_pyth_price(a)
            age = history_age_seconds(a)
            print(f'  {a}: {n} entries, latest=${latest}, age={age:.1f}s')
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_test())