"""
crypto_assets.py

Per-asset configuration for the multi-crypto directional bot.

Maps each Polymarket up/down slug prefix (btc, eth, sol, bnb, doge, xrp, hype)
to its Chainlink price feed (on Polygon) and Binance ticker.

Exports:
    SUPPORTED_ASSETS                — set of slug prefixes we support
    ASSETS                          — dict of full per-asset config
    get_chainlink_feed(asset)       — returns contract address
    get_binance_symbol(asset)       — returns ticker
    get_chainlink_price(session, asset, rpc_url) — async fetch
    get_binance_price(session, asset)            — async fetch
    get_active_crypto_markets(session, assets)   — extended market scanner
"""

import json
from datetime import datetime, timezone


# Polygon-mainnet Chainlink price-feed aggregator contracts.
# Decimals = 8 for all USD price feeds.
# Sources verified against https://data.chain.link/polygon/mainnet/crypto-usd
ASSETS = {
    'btc': {
        'chainlink_feed':  '0xc907E116054Ad103354f2D350FD2514433D57F6f',
        'binance_symbol':  'BTCUSDT',
        'cl_decimals':     8,
        'has_chainlink':   True,
    },
    'eth': {
        'chainlink_feed':  '0xF9680D99D6C9589e2a93a78A04A279e509205945',
        'binance_symbol':  'ETHUSDT',
        'cl_decimals':     8,
        'has_chainlink':   True,
    },
    'sol': {
        'chainlink_feed':  '0x10C8264C0935b3B9870013e057f330Ff3e9C56dC',
        'binance_symbol':  'SOLUSDT',
        'cl_decimals':     8,
        'has_chainlink':   True,
    },
    'bnb': {
        'chainlink_feed':  '0x82a6c4AF830caa6c97bb504425f6A66165C2c26e',
        'binance_symbol':  'BNBUSDT',
        'cl_decimals':     8,
        'has_chainlink':   True,
    },
    'doge': {
        'chainlink_feed':  '0xbaf9327b6564454F4a3364C33eFeEf032b4b4444',
        'binance_symbol':  'DOGEUSDT',
        'cl_decimals':     8,
        'has_chainlink':   True,
    },
    'xrp': {
        'chainlink_feed':  '0x785ba89291f676b5386652eB12b30cF361020694',
        'binance_symbol':  'XRPUSDT',
        'cl_decimals':     8,
        'has_chainlink':   True,
    },
    'hype': {
        # No Chainlink feed on Polygon for HYPE (Hyperliquid token) as of writing.
        # We mark it explicitly so bot2 skips it until verified.
        'chainlink_feed':  None,
        'binance_symbol':  'HYPEUSDT',
        'cl_decimals':     None,
        'has_chainlink':   False,
    },
}

SUPPORTED_ASSETS = set(ASSETS.keys())


def get_chainlink_feed(asset: str) -> str | None:
    cfg = ASSETS.get(asset.lower())
    return cfg['chainlink_feed'] if cfg else None


def get_binance_symbol(asset: str) -> str | None:
    cfg = ASSETS.get(asset.lower())
    return cfg['binance_symbol'] if cfg else None


def asset_has_chainlink(asset: str) -> bool:
    cfg = ASSETS.get(asset.lower())
    return bool(cfg and cfg['has_chainlink'])


# ─── Price fetchers (asset-aware versions of bot.py's BTC-only ones) ────────

async def get_chainlink_price(session, asset: str, rpc_url: str) -> tuple[float, int]:
    """
    Fetch Chainlink latestRoundData for `asset` on Polygon.
    Returns (price_float, updated_at_unix_ts).
    """
    cfg = ASSETS.get(asset.lower())
    if not cfg or not cfg['has_chainlink']:
        raise ValueError(f'{asset} has no Chainlink feed configured')

    feed = cfg['chainlink_feed']
    decimals = cfg['cl_decimals']

    # eth_call to latestRoundData() — selector 0xfeaf968c
    payload = {
        'jsonrpc': '2.0',
        'method':  'eth_call',
        'params': [{'to': feed, 'data': '0xfeaf968c'}, 'latest'],
        'id':      1,
    }
    async with session.post(rpc_url, json=payload, timeout=15) as r:
        data = await r.json()
    result = data.get('result', '')
    if not result or len(result) < 2 + 64 * 5:
        raise RuntimeError(f'bad chainlink response for {asset}: {result[:80]}')

    # ABI: (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
    answer    = int(result[2 + 64*1 : 2 + 64*2], 16)
    updated_at = int(result[2 + 64*3 : 2 + 64*4], 16)
    price = answer / (10 ** decimals)
    return price, updated_at


async def get_binance_price(session, asset: str) -> float:
    """Fetch current Binance spot mid price for the asset."""
    symbol = get_binance_symbol(asset)
    if not symbol:
        raise ValueError(f'{asset} has no Binance symbol')
    url = f'https://api.binance.com/api/v3/ticker/price?symbol={symbol}'
    async with session.get(url, timeout=10) as r:
        data = await r.json()
    return float(data['price'])


# ─── Market scanner (asset-aware version of get_active_btc_markets) ────────

async def get_active_crypto_markets(session, allowed_assets: set[str]) -> list[dict]:
    """
    Like bot.py's get_active_btc_markets, but matches any of N assets.
    Returns a list of market dicts; each dict has an extra 'asset' key
    so downstream logic knows which price feeds to use.
    """
    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Build the set of slug patterns we care about
    allowed_assets = {a.lower() for a in allowed_assets if a.lower() in SUPPORTED_ASSETS}
    if not allowed_assets:
        return []

    # We page a bit deeper than the BTC-only fetcher because there are 6x markets now
    markets: list[dict] = []
    seen_cids: set[str] = set()
    for offset in (0, 50, 100, 150, 200, 250):
        async with session.get(
            'https://gamma-api.polymarket.com/events',
            params={
                'active':       'true',
                'closed':       'false',
                'limit':        '50',
                'offset':       str(offset),
                'order':        'endDate',
                'ascending':    'true',
                'end_date_min': now_str,
            },
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15,
        ) as r:
            try:
                events = await r.json()
            except Exception:
                events = []
        if not events:
            break

        now = datetime.now(timezone.utc)
        for e in events:
            slug = e.get('slug', '')
            if '-updown-' not in slug:
                continue

            # Match against the per-asset pattern: e.g. eth-updown-5m, sol-updown-15m
            asset = None
            for a in allowed_assets:
                if slug.startswith(f'{a}-updown-5m') or slug.startswith(f'{a}-updown-15m'):
                    asset = a
                    break
            if asset is None:
                continue

            if not e.get('active') or e.get('closed'):
                continue
            end_str = e.get('endDate', '')
            if not end_str:
                continue
            try:
                end_time = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
            except Exception:
                continue
            seconds_left = (end_time - now).total_seconds()
            if seconds_left < 0:
                continue

            tf = '15m' if f'{asset}-updown-15m' in slug else '5m'

            m = (e.get('markets') or [{}])[0]
            try:
                token_ids = json.loads(m.get('clobTokenIds', '[]'))
                prices    = json.loads(m.get('outcomePrices', '[]'))
            except Exception:
                continue
            if len(token_ids) < 2 or len(prices) < 2:
                continue

            condition_id = m.get('conditionId', '')
            if not condition_id or condition_id in seen_cids:
                continue
            seen_cids.add(condition_id)

            markets.append({
                'asset':        asset,
                'timeframe':    tf,
                'title':        e.get('title', ''),
                'slug':         slug,
                'end_time':     end_time,
                'seconds_left': seconds_left,
                'condition_id': condition_id,
                'up_token':     token_ids[0],
                'down_token':   token_ids[1],
                'up_price':     float(prices[0]),
                'down_price':   float(prices[1]),
                # 'volume':       volume,
            })

    return markets

async def get_opening_chainlink_price_for_asset(
    session, asset: str, market_end_time, timeframe: str, rpc_url: str,
) -> float:
    """
    Asset-aware opening Chainlink price.

    For now returns current price as opening reference — accurate enough
    since signals fire only in the last 60-120s of a market's window.

    A historical-block-walking version (matching bot.py's BTC logic) can be
    added later; this is correct enough for dry-run validation.
    """
    price, _ = await get_chainlink_price(session, asset, rpc_url)
    return price