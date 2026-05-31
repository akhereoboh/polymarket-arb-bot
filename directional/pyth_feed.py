"""
pyth_feed.py — Pyth Network price feed (3rd confirmation source).

Pyth's public service is also named "Hermes" (https://hermes.pyth.network).
To avoid confusion with our internal Hermes diagnostic agent, we refer to
Pyth's HTTP service as PythHermes throughout this module.

Public, no auth needed. ~400ms update frequency.

Usage:
    from pyth_feed import get_pyth_price

    price = await get_pyth_price(session, 'BTC')
    # returns float or None
"""

import asyncio
from datetime import datetime, timezone

import aiohttp


# Pyth price feed IDs — official mainnet hex IDs for major crypto/USD pairs
# Source: https://www.pyth.network/price-feeds
PYTH_FEED_IDS = {
    'BTC':  '0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43',
    'ETH':  '0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace',
    'SOL':  '0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d',
    'BNB':  '0x2f95862b045670cd22bee3114c39763a4a08beeb663b145d283c31d7d1101c4f',
    'DOGE': '0xdcef50dd0a4cd2dcc17e45df1676dcb336a11a61c69df7a0299b0150c672d25c',
    'XRP':  '0xec5d399846a9209f3fe5881d70aae9268c94339ff9817e8d18ff19fa05eea1c8',
}

PYTH_HERMES_URL = 'https://hermes.pyth.network/v2/updates/price/latest'


def _log(msg):
    print(f'[Pyth {datetime.now(timezone.utc).strftime("%H:%M:%S")}] {msg}', flush=True)


def feed_id_for_asset(asset: str) -> str | None:
    """Return Pyth feed ID for an asset symbol (case-insensitive)."""
    return PYTH_FEED_IDS.get(asset.upper())


async def get_pyth_price(session: aiohttp.ClientSession, asset: str,
                          timeout_sec: float = 5.0) -> float | None:
    """
    Fetch latest Pyth price for a given asset symbol.

    Returns float (USD price) or None on failure.

    Pyth quotes prices as integers with an exponent — e.g.
        price=7341234567890, expo=-8  →  $73,412.34567890

    We convert that to a float USD price.
    """
    feed_id = feed_id_for_asset(asset)
    if not feed_id:
        _log(f'No feed ID for asset {asset}')
        return None

    params = {'ids[]': feed_id, 'parsed': 'true', 'encoding': 'hex'}
    try:
        async with session.get(PYTH_HERMES_URL, params=params,
                               timeout=aiohttp.ClientTimeout(total=timeout_sec)) as r:
            if r.status != 200:
                _log(f'{asset} HTTP {r.status}')
                return None
            data = await r.json()
    except asyncio.TimeoutError:
        _log(f'{asset} timeout')
        return None
    except Exception as e:
        _log(f'{asset} error: {e}')
        return None

    # Parse the response shape:
    #   data['parsed'][0]['price']['price']  → integer
    #   data['parsed'][0]['price']['expo']   → integer exponent (typically -8)
    parsed = data.get('parsed', [])
    if not parsed:
        _log(f'{asset} no parsed data in response')
        return None
    price_obj = parsed[0].get('price', {})
    raw_price = price_obj.get('price')
    expo      = price_obj.get('expo')
    if raw_price is None or expo is None:
        _log(f'{asset} malformed price object: {price_obj}')
        return None
    try:
        price = float(raw_price) * (10 ** int(expo))
        return price
    except Exception as e:
        _log(f'{asset} parse error: {e}')
        return None


async def get_pyth_prices_batch(session: aiohttp.ClientSession,
                                  assets: list[str],
                                  timeout_sec: float = 8.0) -> dict[str, float | None]:
    """
    Fetch multiple assets in one HTTP call.
    Returns dict {asset: price_or_None}.
    """
    feed_ids = []
    asset_by_id = {}
    for a in assets:
        fid = feed_id_for_asset(a)
        if fid:
            feed_ids.append(fid)
            asset_by_id[fid.lower().lstrip('0x')] = a.upper()

    if not feed_ids:
        return {a.upper(): None for a in assets}

    params = [('ids[]', fid) for fid in feed_ids]
    params.append(('parsed', 'true'))
    params.append(('encoding', 'hex'))

    result = {a.upper(): None for a in assets}
    try:
        async with session.get(PYTH_HERMES_URL, params=params,
                               timeout=aiohttp.ClientTimeout(total=timeout_sec)) as r:
            if r.status != 200:
                _log(f'batch HTTP {r.status}')
                return result
            data = await r.json()
    except Exception as e:
        _log(f'batch error: {e}')
        return result

    for entry in data.get('parsed', []):
        eid = entry.get('id', '').lower().lstrip('0x')
        asset = asset_by_id.get(eid)
        if not asset:
            continue
        p = entry.get('price', {})
        raw = p.get('price'); expo = p.get('expo')
        if raw is None or expo is None:
            continue
        try:
            result[asset] = float(raw) * (10 ** int(expo))
        except Exception:
            pass
    return result


# Quick CLI test: `python3 pyth_feed.py`
if __name__ == '__main__':
    async def _test():
        async with aiohttp.ClientSession() as session:
            for asset in ['BTC', 'ETH', 'SOL', 'BNB', 'DOGE', 'XRP']:
                p = await get_pyth_price(session, asset)
                print(f'  {asset}: ${p:,.4f}' if p else f'  {asset}: FAILED')
            print('\nBatch test:')
            batch = await get_pyth_prices_batch(session, ['BTC', 'ETH', 'SOL'])
            for k, v in batch.items():
                print(f'  {k}: ${v:,.4f}' if v else f'  {k}: FAILED')

    asyncio.run(_test())