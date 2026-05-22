"""
Backtest the directional bot signal logic against historical Binance + Chainlink data.

Inputs:
  /tmp/btc_candles.json   — Binance 1m klines (10,080 candles)
  /tmp/cl_history.json    — Chainlink rounds (built by fetch_chainlink_history.py)

For every 5-minute and 15-minute window in the data, this script:
  1. Computes the signal at T-60s (5m markets) or T-120s (15m markets)
  2. Records the simulated trade direction and confidence
  3. Resolves outcome by comparing Chainlink price at window close vs window open
  4. Outputs per-trade CSV plus aggregated win-rate tables

Crowd price filter: approximated based on move magnitude (per user spec).
Order book check: not modeled (assumed fill at signal price).

Run:
  python backtest.py
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ─── config (mirrors bot.py) ──────────────────────────────────────────────
MIN_MOVE_PCT = 0.05
MOMENTUM_TIMEFRAMES_SEC = [60, 120, 180, 240]
MOMENTUM_THRESHOLD = 0.5            # need >50% of checks to confirm direction
CONFLICT_RATIO_MAX = 1.5
CROWD_PRICE_MIN = 0.35
MARKET_PRICE_LOW = 0.15             # bot skips trades below this
MARKET_PRICE_HIGH = 0.85            # or above this

ENTRY_WINDOW_5M = 60                # check signal 60s before close
ENTRY_WINDOW_15M = 120              # 120s before close for 15m markets

# Crowd price approximation: given a move magnitude (% of price), estimate
# what the Polymarket UP/DOWN price probably looked like. Calibrated loosely
# from intuition — a 0.05% move would barely tilt the crowd, a 0.5% move
# would already have the crowd heavily favoring the direction.
def approximate_crowd_price(move_pct: float, direction: str) -> float:
    """Return approximate price for the chosen direction's token."""
    # Sigmoid-ish — at 0% move, crowd ~0.50. At ±0.3% move, crowd ~0.80.
    import math
    # Direction-aligned move magnitude
    aligned = abs(move_pct) if (
        (direction == 'up' and move_pct > 0) or (direction == 'down' and move_pct < 0)
    ) else -abs(move_pct)
    # Squash to [0.10, 0.90]
    p = 0.5 + 0.4 * math.tanh(aligned / 0.15)
    return max(0.05, min(0.95, p))


# ─── data loading ─────────────────────────────────────────────────────────

def load_btc_candles(path: str = '/tmp/btc_candles.json') -> list:
    """
    Load Binance 1-minute candles.
    Expected format: list of klines, each [open_time_ms, open, high, low, close, volume, close_time_ms, ...]
    Returns list of dicts: {ts: int_seconds, open: float, close: float, high: float, low: float}
    sorted ascending by timestamp.
    """
    with open(path) as f:
        raw = json.load(f)

    candles = []
    for k in raw:
        # Be flexible about shape — handle dict or list form
        if isinstance(k, dict):
            ts = int(k.get('openTime', k.get('open_time', k.get('time', 0))))
            if ts > 1e12:  # milliseconds
                ts = ts // 1000
            candles.append({
                'ts': ts,
                'open': float(k['open']),
                'close': float(k['close']),
                'high': float(k['high']),
                'low': float(k['low']),
            })
        else:
            ts_ms = int(k[0])
            ts = ts_ms // 1000 if ts_ms > 1e12 else ts_ms
            candles.append({
                'ts': ts,
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
            })

    candles.sort(key=lambda c: c['ts'])
    return candles


def load_cl_history(path: str = '/tmp/cl_history.json') -> list:
    """Load Chainlink history as list of [ts, price], sorted ascending."""
    with open(path) as f:
        data = json.load(f)
    data.sort(key=lambda x: x[0])
    return [(int(ts), float(px)) for ts, px in data]


# ─── price lookups ────────────────────────────────────────────────────────

def bn_price_at(candles: list, ts: int) -> float | None:
    """
    Binance price as of timestamp ts. Uses the close of the most recent candle
    whose open_time <= ts. Returns None if before first candle.
    """
    # Binary search
    lo, hi = 0, len(candles) - 1
    if ts < candles[0]['ts']:
        return None
    if ts >= candles[-1]['ts']:
        return candles[-1]['close']
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if candles[mid]['ts'] <= ts:
            lo = mid
        else:
            hi = mid - 1
    return candles[lo]['close']


def cl_price_at(cl_history: list, ts: int) -> float | None:
    """Chainlink price as of ts — most recent round with updated_at <= ts."""
    if not cl_history or ts < cl_history[0][0]:
        return None
    lo, hi = 0, len(cl_history) - 1
    if ts >= cl_history[-1][0]:
        return cl_history[-1][1]
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if cl_history[mid][0] <= ts:
            lo = mid
        else:
            hi = mid - 1
    return cl_history[lo][1]


# ─── signal logic (mirrors bot.py check_signal) ───────────────────────────

def check_signal_backtest(
    cl_now: float, cl_open: float,
    bn_now: float, bn_open: float,
    bn_history_window: list,        # [(ts, price), ...] for last ~5 min, ascending
    cl_history_window: list,        # same shape for chainlink
    now_ts: int,
    use_crowd_filter: bool = True,
) -> tuple[str, float, dict]:
    """
    Returns (direction, confidence, diagnostics).
    Direction is 'up', 'down', or 'none'.
    Diagnostics dict explains which condition failed for analysis.
    """
    diag = {'failed_at': None}

    cl_pct = (cl_now - cl_open) / cl_open * 100
    bn_pct = (bn_now - bn_open) / bn_open * 100

    cl_up = cl_pct > 0
    bn_up = bn_pct > 0
    cl_strong = abs(cl_pct) >= MIN_MOVE_PCT
    bn_strong = abs(bn_pct) >= MIN_MOVE_PCT

    diag.update({'cl_pct': cl_pct, 'bn_pct': bn_pct})

    if not cl_strong:
        diag['failed_at'] = 'cl_weak'
        return 'none', 0.0, diag
    if not bn_strong:
        diag['failed_at'] = 'bn_weak'
        return 'none', 0.0, diag
    if cl_up != bn_up:
        diag['failed_at'] = 'disagree'
        return 'none', 0.0, diag

    spread = abs(abs(cl_pct) - abs(bn_pct))
    avg = (abs(cl_pct) + abs(bn_pct)) / 2
    if avg > 0:
        conflict_ratio = spread / avg
        diag['conflict_ratio'] = conflict_ratio
        if conflict_ratio > CONFLICT_RATIO_MAX:
            diag['failed_at'] = 'conflict'
            return 'none', 0.0, diag

    direction = 'up' if cl_up else 'down'

    # Crowd filter — approximated
    if use_crowd_filter:
        # Use the average of the two moves as the "evidence" for the direction
        evidence_pct = (cl_pct + bn_pct) / 2
        crowd_price = approximate_crowd_price(evidence_pct, direction)
        diag['crowd_price'] = crowd_price
        if crowd_price < CROWD_PRICE_MIN:
            diag['failed_at'] = 'crowd'
            return 'none', 0.0, diag
        # Also skip if market is too one-sided (>0.85 or <0.15)
        if crowd_price < MARKET_PRICE_LOW or crowd_price > MARKET_PRICE_HIGH:
            diag['failed_at'] = 'one_sided'
            return 'none', 0.0, diag

    # Momentum checks — BN
    def momentum_check(history_window: list, current_price: float) -> tuple[int, int]:
        confirmations = 0
        checked = 0
        for lookback in MOMENTUM_TIMEFRAMES_SEC:
            target = now_ts - lookback
            ref_price = None
            for ts, px in reversed(history_window):
                if ts <= target:
                    ref_price = px
                    break
            if ref_price:
                pct = (current_price - ref_price) / ref_price * 100
                checked += 1
                if direction == 'up' and pct > 0:
                    confirmations += 1
                elif direction == 'down' and pct < 0:
                    confirmations += 1
        return confirmations, checked

    bn_conf, bn_checked = momentum_check(bn_history_window, bn_now)
    cl_conf, cl_checked = momentum_check(cl_history_window, cl_now)

    total_conf = bn_conf + cl_conf
    total_checked = bn_checked + cl_checked
    diag['momentum'] = f'{total_conf}/{total_checked} (BN:{bn_conf}/{bn_checked} CL:{cl_conf}/{cl_checked})'

    if total_checked >= 4:
        if total_conf / total_checked < MOMENTUM_THRESHOLD:
            diag['failed_at'] = 'momentum'
            return 'none', 0.0, diag

    crowd_bonus = 0.0
    if use_crowd_filter and diag.get('crowd_price', 0) > 0.55:
        crowd_bonus = 0.05
    confidence = (abs(cl_pct) + abs(bn_pct)) / 2 + crowd_bonus
    return direction, confidence, diag


# ─── window simulation ────────────────────────────────────────────────────

def build_history_window(history: list, end_ts: int, lookback_sec: int = 300) -> list:
    """Get all (ts, price) entries in (end_ts - lookback, end_ts]."""
    start = end_ts - lookback_sec
    # Find first index >= start
    lo, hi = 0, len(history)
    while lo < hi:
        mid = (lo + hi) // 2
        if history[mid][0] < start:
            lo = mid + 1
        else:
            hi = mid
    # Collect until we pass end_ts
    out = []
    i = lo
    while i < len(history) and history[i][0] <= end_ts:
        out.append(history[i])
        i += 1
    return out


def simulate_windows(
    candles: list,
    cl_history: list,
    window_sec: int,
    label: str,
    use_crowd_filter: bool = True,
) -> list:
    """
    Walk through every window of length window_sec in the data and simulate a trade.

    Polymarket BTC up/down 5m markets settle on the minute (e.g., :00, :05, :10).
    15m markets settle on the quarter-hour. We approximate by stepping through
    every possible window aligned to candle boundaries (1m steps).

    Returns list of trade dicts.
    """
    trades = []
    entry_offset = ENTRY_WINDOW_5M if window_sec == 300 else ENTRY_WINDOW_15M

    # Build BN history as (ts, close) list from candles, for momentum lookups
    bn_history_full = [(c['ts'], c['close']) for c in candles]

    # We need enough history for the longest momentum check (240s back from entry)
    # and we need the window-open price (window_sec back from close)
    min_lookback = window_sec + max(MOMENTUM_TIMEFRAMES_SEC) + 60

    # Step through possible market closes, aligned to candle starts
    for c in candles:
        market_close_ts = c['ts']
        entry_ts = market_close_ts - entry_offset
        window_open_ts = market_close_ts - window_sec

        # Need history reaching back before window open
        if entry_ts - max(MOMENTUM_TIMEFRAMES_SEC) < candles[0]['ts']:
            continue
        if window_open_ts < candles[0]['ts']:
            continue
        if not cl_history or window_open_ts < cl_history[0][0]:
            continue
        if entry_ts > candles[-1]['ts']:
            continue

        # Prices at entry
        bn_now = bn_price_at(candles, entry_ts)
        bn_open = bn_price_at(candles, window_open_ts)
        cl_now = cl_price_at(cl_history, entry_ts)
        cl_open = cl_price_at(cl_history, window_open_ts)

        if None in (bn_now, bn_open, cl_now, cl_open):
            continue

        # History windows for momentum (need at least last 240s ascending)
        bn_window = build_history_window(bn_history_full, entry_ts, lookback_sec=400)
        cl_window = build_history_window(cl_history, entry_ts, lookback_sec=400)

        direction, confidence, diag = check_signal_backtest(
            cl_now, cl_open,
            bn_now, bn_open,
            bn_window, cl_window,
            entry_ts,
            use_crowd_filter=use_crowd_filter,
        )

        if direction == 'none':
            continue

        # Resolve outcome — Chainlink at window close vs window open
        cl_resolve = cl_price_at(cl_history, market_close_ts)
        if cl_resolve is None:
            continue
        resolved_up = cl_resolve > cl_open
        won = (direction == 'up' and resolved_up) or (direction == 'down' and not resolved_up)

        entry_dt = datetime.fromtimestamp(entry_ts, tz=timezone.utc)
        # ET = UTC-5 (EST) or UTC-4 (EDT). Use -4 for May 2026 (DST is active).
        et_dt = entry_dt - timedelta(hours=4)

        trades.append({
            'market_type': label,
            'entry_ts': entry_ts,
            'entry_utc': entry_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'entry_et': et_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'hour_et': et_dt.hour,
            'hour_utc': entry_dt.hour,
            'direction': direction,
            'confidence': round(confidence, 4),
            'cl_pct': round(diag.get('cl_pct', 0), 4),
            'bn_pct': round(diag.get('bn_pct', 0), 4),
            'crowd_price': round(diag.get('crowd_price', 0), 3) if use_crowd_filter else None,
            'momentum': diag.get('momentum', ''),
            'cl_open': round(cl_open, 2),
            'cl_close': round(cl_resolve, 2),
            'bn_open': round(bn_open, 2),
            'bn_entry': round(bn_now, 2),
            'won': won,
        })

    return trades


# ─── reporting ────────────────────────────────────────────────────────────

def session_for_hour_et(h: int) -> str:
    """Classify hour-of-day (ET) into trading session."""
    if 1 <= h < 6:
        return 'asian'
    if 6 <= h < 8:
        return 'eu_overlap'
    if 8 <= h < 11:
        return 'us_open'
    if 11 <= h < 16:
        return 'us_main'
    if 16 <= h < 20:
        return 'us_late'
    return 'off_hours'


def report_summary(trades: list, label: str) -> None:
    if not trades:
        print(f'\n=== {label}: NO TRADES ===')
        return

    wins = sum(1 for t in trades if t['won'])
    total = len(trades)
    print(f'\n=== {label} ===')
    print(f'Total trades: {total}')
    print(f'Wins: {wins} ({wins/total*100:.1f}%)')
    print(f'Losses: {total - wins} ({(total-wins)/total*100:.1f}%)')


def report_by_hour_et(trades: list, label: str) -> None:
    if not trades:
        return
    by_hour = defaultdict(lambda: {'wins': 0, 'total': 0})
    for t in trades:
        h = t['hour_et']
        by_hour[h]['total'] += 1
        if t['won']:
            by_hour[h]['wins'] += 1
    print(f'\n--- {label}: Win rate by hour (ET) ---')
    print(f'{"Hour ET":<8} {"Trades":<8} {"Wins":<6} {"Win %":<8}')
    for h in sorted(by_hour):
        d = by_hour[h]
        pct = d['wins'] / d['total'] * 100
        print(f'{h:<8} {d["total"]:<8} {d["wins"]:<6} {pct:>5.1f}%')


def report_by_session(trades: list, label: str) -> None:
    if not trades:
        return
    by_sess = defaultdict(lambda: {'wins': 0, 'total': 0})
    for t in trades:
        s = session_for_hour_et(t['hour_et'])
        by_sess[s]['total'] += 1
        if t['won']:
            by_sess[s]['wins'] += 1
    print(f'\n--- {label}: Win rate by session ---')
    print(f'{"Session":<14} {"Trades":<8} {"Wins":<6} {"Win %":<8}')
    order = ['asian', 'eu_overlap', 'us_open', 'us_main', 'us_late', 'off_hours']
    for s in order:
        if s not in by_sess:
            continue
        d = by_sess[s]
        pct = d['wins'] / d['total'] * 100
        print(f'{s:<14} {d["total"]:<8} {d["wins"]:<6} {pct:>5.1f}%')


def write_trades_csv(trades: list, path: str) -> None:
    if not trades:
        return
    fieldnames = list(trades[0].keys())
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(trades)
    print(f'  Wrote {len(trades)} rows to {path}')


# ─── main ─────────────────────────────────────────────────────────────────

def main():
    print('Loading data...')
    candles = load_btc_candles()
    print(f'  Binance candles: {len(candles)}')
    if candles:
        span_h = (candles[-1]['ts'] - candles[0]['ts']) / 3600
        print(f'  Span: {span_h:.1f}h ({span_h/24:.2f} days)')

    cl_history = load_cl_history()
    print(f'  Chainlink rounds: {len(cl_history)}')
    if cl_history:
        span_h = (cl_history[-1][0] - cl_history[0][0]) / 3600
        print(f'  Span: {span_h:.1f}h ({span_h/24:.2f} days)')

    # Find overlap window
    if candles and cl_history:
        overlap_start = max(candles[0]['ts'], cl_history[0][0])
        overlap_end = min(candles[-1]['ts'], cl_history[-1][0])
        overlap_h = (overlap_end - overlap_start) / 3600
        print(f'  Overlap: {overlap_h:.1f}h')

    print('\nRunning 5m backtest...')
    trades_5m = simulate_windows(candles, cl_history, 300, '5m', use_crowd_filter=True)

    print('Running 15m backtest...')
    trades_15m = simulate_windows(candles, cl_history, 900, '15m', use_crowd_filter=True)

    all_trades = trades_5m + trades_15m
    all_trades.sort(key=lambda t: t['entry_ts'])

    report_summary(trades_5m, '5m markets')
    report_summary(trades_15m, '15m markets')
    report_summary(all_trades, 'ALL markets combined')

    report_by_session(trades_5m, '5m')
    report_by_session(trades_15m, '15m')
    report_by_session(all_trades, 'Combined')

    report_by_hour_et(all_trades, 'Combined')

    print('\nWriting CSVs...')
    write_trades_csv(trades_5m, '/tmp/backtest_5m.csv')
    write_trades_csv(trades_15m, '/tmp/backtest_15m.csv')
    write_trades_csv(all_trades, '/tmp/backtest_all.csv')

    # Also run a no-filter version to see signal-only performance
    print('\n\n=========================================')
    print('SIGNAL-ONLY (no crowd filter) comparison')
    print('=========================================')
    trades_5m_raw = simulate_windows(candles, cl_history, 300, '5m', use_crowd_filter=False)
    trades_15m_raw = simulate_windows(candles, cl_history, 900, '15m', use_crowd_filter=False)
    all_raw = trades_5m_raw + trades_15m_raw

    report_summary(all_raw, 'Signal-only ALL')
    report_by_session(all_raw, 'Signal-only Combined')


if __name__ == '__main__':
    main()
