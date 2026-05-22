"""
Fetch Chainlink BTC/USD historical price data from Polygon.

Walks backwards from the current round, sampling at approximately 1-minute
cadence to match the 1-minute Binance candle data in /tmp/btc_candles.json.

Output: /tmp/cl_history.json — list of [timestamp, price] pairs sorted ascending.

Idempotent: if the output file exists, resumes from the oldest timestamp already
fetched. Safe to interrupt and restart.
"""

import json
import os
import sys
import time
from pathlib import Path

import requests

RPC = 'https://polygon-bor-rpc.publicnode.com'
CL_CONTRACT = '0xc907E116054Ad103354f2D350FD2514433D57F6f'

# We want roughly 1-minute spacing. Chainlink updates ~every 33s, so we sample
# every other round. This gives ~66s spacing — close enough to 60s for backtest.
SAMPLE_EVERY_N_ROUNDS = 2

# 7 days of history
TARGET_DAYS = 7
TARGET_SECONDS = TARGET_DAYS * 24 * 3600

OUTPUT_FILE = '/tmp/cl_history.json'

# Polite delay between RPC calls to avoid getting rate-limited
SLEEP_BETWEEN_CALLS = 0.05


def rpc_call(method: str, params: list, retries: int = 3) -> dict:
    """Make a JSON-RPC call with retries."""
    payload = {'jsonrpc': '2.0', 'method': method, 'params': params, 'id': 1}
    for attempt in range(retries):
        try:
            r = requests.post(RPC, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()
            if 'error' in data:
                raise RuntimeError(f'RPC error: {data["error"]}')
            return data
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f'  RPC retry {attempt+1}/{retries} after {wait}s: {e}', file=sys.stderr)
            time.sleep(wait)


def get_latest_round() -> tuple[int, float, int]:
    """Returns (round_id, price, updated_at)."""
    data = rpc_call('eth_call', [{'to': CL_CONTRACT, 'data': '0xfeaf968c'}, 'latest'])
    result = data['result']
    round_id = int(result[2:2+64], 16)
    price = int(result[2+64:2+128], 16) / 1e8
    updated_at = int(result[2+192:2+256], 16)
    return round_id, price, updated_at


def get_round_data(round_id: int) -> tuple[float, int] | None:
    """
    Fetch a specific historical round. Returns (price, updated_at) or None if
    the round doesn't exist / returned empty.
    """
    round_hex = hex(round_id)[2:].zfill(64)
    data = rpc_call('eth_call', [
        {'to': CL_CONTRACT, 'data': '0x9a6fc8f5' + round_hex},
        'latest'
    ])
    result = data.get('result')
    if not result or result == '0x' or len(result) < 2 + 256:
        return None
    price = int(result[2+64:2+128], 16) / 1e8
    updated_at = int(result[2+192:2+256], 16)
    if price <= 0 or updated_at <= 0:
        return None
    return price, updated_at


def load_existing() -> list:
    """Load existing history if present."""
    if not Path(OUTPUT_FILE).exists():
        return []
    try:
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
        # Validate shape
        if isinstance(data, list) and all(
            isinstance(x, list) and len(x) == 2 for x in data
        ):
            return data
    except Exception as e:
        print(f'Could not load existing file: {e}', file=sys.stderr)
    return []


def save_history(history: list) -> None:
    """Save history sorted ascending by timestamp."""
    history.sort(key=lambda x: x[0])
    # Dedupe by timestamp (in case of resume overlap)
    seen = set()
    deduped = []
    for ts, px in history:
        if ts not in seen:
            seen.add(ts)
            deduped.append([ts, px])
    tmp = OUTPUT_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(deduped, f)
    os.replace(tmp, OUTPUT_FILE)


def main():
    print(f'[Chainlink] Fetching {TARGET_DAYS} days of BTC/USD history')
    print(f'[Chainlink] Sampling every {SAMPLE_EVERY_N_ROUNDS} rounds (~{SAMPLE_EVERY_N_ROUNDS * 33}s apart)')

    existing = load_existing()
    print(f'[Chainlink] Existing entries: {len(existing)}')

    latest_round, latest_price, latest_ts = get_latest_round()
    print(f'[Chainlink] Latest round: {latest_round}')
    print(f'[Chainlink] Latest price: ${latest_price:,.2f} at {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(latest_ts))}')

    now = int(time.time())
    cutoff_ts = now - TARGET_SECONDS

    # If we have existing data, only fill in what's missing.
    # Strategy: start from latest_round, walk back, skip rounds already covered
    # by existing data (within 30s tolerance).
    existing_ts = sorted(ts for ts, _ in existing)

    def is_covered(ts: int) -> bool:
        """Is this timestamp already represented in existing data?"""
        if not existing_ts:
            return False
        # Binary search would be faster but for ~10k items linear is fine
        for ets in existing_ts:
            if abs(ets - ts) < 30:
                return True
            if ets > ts + 30:
                return False
        return False

    history = list(existing)
    round_id = latest_round
    fetched_count = 0
    skipped_count = 0
    save_every = 100

    print(f'[Chainlink] Walking back from round {latest_round} to cutoff {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(cutoff_ts))}')

    try:
        while True:
            result = get_round_data(round_id)
            if result is None:
                # Round doesn't exist — could be a gap, try the next one back
                round_id -= SAMPLE_EVERY_N_ROUNDS
                if round_id <= 0:
                    print('[Chainlink] Reached round 0 — stopping')
                    break
                continue

            price, updated_at = result

            if updated_at < cutoff_ts:
                print(f'[Chainlink] Reached cutoff at round {round_id} (ts {updated_at})')
                break

            if not is_covered(updated_at):
                history.append([updated_at, price])
                fetched_count += 1
                if fetched_count % 50 == 0:
                    age_hours = (now - updated_at) / 3600
                    print(f'  [{fetched_count}] round={round_id} ts={updated_at} '
                          f'price=${price:,.2f} age={age_hours:.1f}h')
            else:
                skipped_count += 1

            if fetched_count > 0 and fetched_count % save_every == 0:
                save_history(history)

            round_id -= SAMPLE_EVERY_N_ROUNDS
            if round_id <= 0:
                break
            time.sleep(SLEEP_BETWEEN_CALLS)

    except KeyboardInterrupt:
        print('\n[Chainlink] Interrupted — saving progress')
    except Exception as e:
        print(f'\n[Chainlink] Error: {e} — saving progress')

    save_history(history)
    print(f'[Chainlink] Done. Fetched {fetched_count} new, skipped {skipped_count} covered, total {len(history)}')
    print(f'[Chainlink] Saved to {OUTPUT_FILE}')

    if history:
        oldest = min(ts for ts, _ in history)
        newest = max(ts for ts, _ in history)
        span_hours = (newest - oldest) / 3600
        print(f'[Chainlink] Coverage: {span_hours:.1f}h ({span_hours/24:.2f} days)')


if __name__ == '__main__':
    main()
