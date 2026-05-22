
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
 
 
# ─── config (mirrors bot.py check_signal) ──────────────────────────────
MIN_MOVE_PCT = 0.05
MOMENTUM_TIMEFRAMES_SEC = [60, 120, 180, 240]
MOMENTUM_THRESHOLD = 0.5
CONFLICT_RATIO_MAX = 1.5
 
ENTRY_OFFSETS_15M = [600, 450, 300, 180, 120, 60]  # seconds before close
ENTRY_OFFSETS_5M = [180, 120, 90, 60, 30]
 
 
# ─── data loading ─────────────────────────────────────────────────────
 
def load_candles(path: str = '/tmp/btc_candles.json') -> list:
    if not Path(path).exists():
        print(f'Missing {path} — cannot proceed.')
        sys.exit(1)
    with open(path) as f:
        raw = json.load(f)
    out = []
    for k in raw:
        if isinstance(k, dict):
            ts = int(k.get('openTime', k.get('open_time', k.get('time', 0))))
            if ts > 1e12:
                ts //= 1000
            out.append({'ts': ts, 'close': float(k['close'])})
        else:
            ts = int(k[0])
            ts = ts // 1000 if ts > 1e12 else ts
            out.append({'ts': ts, 'close': float(k[4])})
    out.sort(key=lambda c: c['ts'])
    return out
 
 
def load_cl(path: str = '/tmp/cl_history.json') -> list:
    if not Path(path).exists():
        print(f'Missing {path} — cannot proceed.')
        sys.exit(1)
    with open(path) as f:
        raw = json.load(f)
    out = [(int(ts), float(px)) for ts, px in raw]
    out.sort(key=lambda x: x[0])
    return out
 
 
# ─── price lookups (binary search) ─────────────────────────────────────
 
def bn_at(candles: list, ts: int) -> float | None:
    if not candles or ts < candles[0]['ts']:
        return None
    if ts >= candles[-1]['ts']:
        return candles[-1]['close']
    lo, hi = 0, len(candles) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if candles[mid]['ts'] <= ts:
            lo = mid
        else:
            hi = mid - 1
    return candles[lo]['close']
 
 
def cl_at(history: list, ts: int) -> float | None:
    if not history or ts < history[0][0]:
        return None
    if ts >= history[-1][0]:
        return history[-1][1]
    lo, hi = 0, len(history) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if history[mid][0] <= ts:
            lo = mid
        else:
            hi = mid - 1
    return history[lo][1]
 
 
def history_window(history: list, end_ts: int, lookback_sec: int) -> list:
    """Return list of (ts, price) entries from end_ts - lookback_sec to end_ts inclusive."""
    start = end_ts - lookback_sec
    lo, hi = 0, len(history)
    while lo < hi:
        mid = (lo + hi) // 2
        if history[mid][0] < start:
            lo = mid + 1
        else:
            hi = mid
    out = []
    i = lo
    while i < len(history) and history[i][0] <= end_ts:
        out.append(history[i])
        i += 1
    return out
 
 
# ─── signal (mirrors bot.py check_signal, no crowd filter) ─────────────
 
def check_signal(
    cl_now: float, cl_open: float,
    bn_now: float, bn_open: float,
    bn_hist: list, cl_hist: list,
    now_ts: int,
) -> tuple[str, float, dict]:
    """
    Returns (direction, confidence, diagnostics).
    direction in {'up', 'down', 'none'}.
    No crowd-price filter (we can't reconstruct historical Polymarket prices).
    """
    diag = {'failed_at': None}
    cl_pct = (cl_now - cl_open) / cl_open * 100
    bn_pct = (bn_now - bn_open) / bn_open * 100
 
    diag.update({'cl_pct': cl_pct, 'bn_pct': bn_pct})
 
    if abs(cl_pct) < MIN_MOVE_PCT:
        diag['failed_at'] = 'cl_weak'
        return 'none', 0.0, diag
    if abs(bn_pct) < MIN_MOVE_PCT:
        diag['failed_at'] = 'bn_weak'
        return 'none', 0.0, diag
 
    cl_up = cl_pct > 0
    bn_up = bn_pct > 0
    if cl_up != bn_up:
        diag['failed_at'] = 'disagree'
        return 'none', 0.0, diag
 
    spread = abs(abs(cl_pct) - abs(bn_pct))
    avg = (abs(cl_pct) + abs(bn_pct)) / 2
    if avg > 0:
        conflict = spread / avg
        diag['conflict_ratio'] = conflict
        if conflict > CONFLICT_RATIO_MAX:
            diag['failed_at'] = 'conflict'
            return 'none', 0.0, diag
 
    direction = 'up' if cl_up else 'down'
 
    # momentum checks
    def momentum(hist: list, current: float) -> tuple[int, int]:
        conf = 0
        checked = 0
        for lookback in MOMENTUM_TIMEFRAMES_SEC:
            target = now_ts - lookback
            ref = None
            for ts, px in reversed(hist):
                if ts <= target:
                    ref = px
                    break
            if ref is not None:
                pct = (current - ref) / ref * 100
                checked += 1
                if direction == 'up' and pct > 0:
                    conf += 1
                elif direction == 'down' and pct < 0:
                    conf += 1
        return conf, checked
 
    bn_conf, bn_checked = momentum(bn_hist, bn_now)
    cl_conf, cl_checked = momentum(cl_hist, cl_now)
 
    total_conf = bn_conf + cl_conf
    total_checked = bn_checked + cl_checked
    diag['momentum'] = f'{total_conf}/{total_checked}'
 
    if total_checked >= 4:
        if total_conf / total_checked < MOMENTUM_THRESHOLD:
            diag['failed_at'] = 'momentum'
            return 'none', 0.0, diag
 
    confidence = (abs(cl_pct) + abs(bn_pct)) / 2
    return direction, confidence, diag
 
 
# ─── window simulation across multiple offsets ─────────────────────────
 
def session_for_hour_et(h: int) -> str:
    if 1 <= h < 6:   return 'asian'
    if 6 <= h < 8:   return 'eu_overlap'
    if 8 <= h < 11:  return 'us_open'
    if 11 <= h < 16: return 'us_main'
    if 16 <= h < 20: return 'us_late'
    return 'off_hours'
 
 
def simulate_offsets(
    candles: list,
    cl_hist: list,
    window_sec: int,
    offsets: list[int],
    label: str,
) -> list[dict]:
    """
    For every possible market window in the data, evaluate the signal at
    each entry offset and record what would have happened.
    """
    trades = []
    bn_hist_full = [(c['ts'], c['close']) for c in candles]
 
    min_needed_lookback = window_sec + max(MOMENTUM_TIMEFRAMES_SEC) + 60
 
    for c in candles:
        market_close_ts = c['ts']
        window_open_ts = market_close_ts - window_sec
 
        if window_open_ts < candles[0]['ts'] + min_needed_lookback:
            continue
        if not cl_hist or window_open_ts < cl_hist[0][0]:
            continue
 
        cl_open = cl_at(cl_hist, window_open_ts)
        bn_open = bn_at(candles, window_open_ts)
        cl_resolve = cl_at(cl_hist, market_close_ts)
        if None in (cl_open, bn_open, cl_resolve):
            continue
 
        resolved_up = cl_resolve > cl_open
 
        for offset in offsets:
            entry_ts = market_close_ts - offset
            if entry_ts <= window_open_ts:
                continue  # offset larger than window; doesn't make sense
            if entry_ts < candles[0]['ts']:
                continue
 
            cl_now = cl_at(cl_hist, entry_ts)
            bn_now = bn_at(candles, entry_ts)
            if cl_now is None or bn_now is None:
                continue
 
            bn_window = history_window(bn_hist_full, entry_ts, 400)
            cl_window = history_window(cl_hist, entry_ts, 400)
 
            direction, confidence, diag = check_signal(
                cl_now, cl_open, bn_now, bn_open,
                bn_window, cl_window, entry_ts,
            )
 
            if direction == 'none':
                continue
 
            won = (direction == 'up' and resolved_up) or (direction == 'down' and not resolved_up)
            entry_dt = datetime.fromtimestamp(entry_ts, tz=timezone.utc)
            et_dt = entry_dt - timedelta(hours=4)
 
            trades.append({
                'market_type': label,
                'offset_sec': offset,
                'entry_ts': entry_ts,
                'entry_utc': entry_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'hour_et': et_dt.hour,
                'session': session_for_hour_et(et_dt.hour),
                'direction': direction,
                'confidence': round(confidence, 4),
                'cl_pct_at_entry': round(diag.get('cl_pct', 0), 4),
                'bn_pct_at_entry': round(diag.get('bn_pct', 0), 4),
                'momentum': diag.get('momentum', ''),
                'cl_open': round(cl_open, 2),
                'cl_resolve': round(cl_resolve, 2),
                'won': won,
            })
 
    return trades
 
 
# ─── reporting ─────────────────────────────────────────────────────────
 
def report_by_offset(trades: list, label: str) -> None:
    print(f'\n=== {label}: Win rate by entry offset (seconds before close) ===')
    by_offset = defaultdict(lambda: {'wins': 0, 'total': 0, 'avg_conf': []})
    for t in trades:
        o = t['offset_sec']
        by_offset[o]['total'] += 1
        by_offset[o]['avg_conf'].append(t['confidence'])
        if t['won']:
            by_offset[o]['wins'] += 1
    print(f'{"Offset":>8s} {"trades":>7s} {"wins":>6s} {"win%":>7s} {"avg conf":>9s}')
    for o in sorted(by_offset.keys(), reverse=True):
        d = by_offset[o]
        wr = d['wins'] / d['total'] * 100
        ac = sum(d['avg_conf']) / len(d['avg_conf'])
        print(f'  T-{o:<5d}  {d["total"]:6d}  {d["wins"]:5d}  {wr:6.1f}  {ac:8.4f}')
 
 
def report_by_offset_and_session(trades: list, label: str) -> None:
    print(f'\n--- {label}: Win rate by offset × session (ET) ---')
    by_key = defaultdict(lambda: {'wins': 0, 'total': 0})
    for t in trades:
        key = (t['offset_sec'], t['session'])
        by_key[key]['total'] += 1
        if t['won']:
            by_key[key]['wins'] += 1
 
    sessions = ['asian', 'eu_overlap', 'us_open', 'us_main', 'us_late', 'off_hours']
    offsets = sorted({t['offset_sec'] for t in trades}, reverse=True)
 
    header = f'{"Offset":>8s}'
    for s in sessions:
        header += f' {s[:9]:>10s}'
    print(header)
 
    for o in offsets:
        row = f'  T-{o:<5d}'
        for s in sessions:
            d = by_key.get((o, s))
            if not d:
                row += f' {"-":>10s}'
            else:
                wr = d['wins'] / d['total'] * 100
                row += f' {wr:5.1f}%({d["total"]:3d})'
        print(row)
 
 
def write_csv(trades: list, path: str) -> None:
    if not trades:
        return
    fields = list(trades[0].keys())
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(trades)
    print(f'\nWrote {len(trades)} rows to {path}')
 
 
# ─── main ──────────────────────────────────────────────────────────────
 
def main():
    print('Loading data...')
    candles = load_candles()
    cl_hist = load_cl()
    print(f'  Candles: {len(candles)} ({(candles[-1]["ts"] - candles[0]["ts"]) / 3600:.1f}h)')
    print(f'  Chainlink: {len(cl_hist)} rounds')
 
    overlap_h = (min(candles[-1]['ts'], cl_hist[-1][0]) - max(candles[0]['ts'], cl_hist[0][0])) / 3600
    print(f'  Overlap: {overlap_h:.1f}h')
 
    print('\nSimulating 15m windows...')
    trades_15m = simulate_offsets(candles, cl_hist, 900, ENTRY_OFFSETS_15M, '15m')
    print(f'  Generated {len(trades_15m)} signal-firings across offsets')
 
    print('Simulating 5m windows...')
    trades_5m = simulate_offsets(candles, cl_hist, 300, ENTRY_OFFSETS_5M, '5m')
    print(f'  Generated {len(trades_5m)} signal-firings across offsets')
 
    if trades_15m:
        report_by_offset(trades_15m, '15m markets')
        report_by_offset_and_session(trades_15m, '15m markets')
 
    if trades_5m:
        report_by_offset(trades_5m, '5m markets')
        report_by_offset_and_session(trades_5m, '5m markets')
 
    all_trades = trades_15m + trades_5m
    write_csv(all_trades, '/tmp/entry_timing_analysis.csv')
 
    # Sanity reminder
    print('\n' + '=' * 64)
    print(' Reminder: this measures direction-prediction accuracy only.')
    print(' It does NOT account for:')
    print('   - The actual Polymarket entry price (lower at earlier offsets)')
    print('   - The crowd-price filter (we removed it for this test)')
    print('   - The one-sided cap (we removed it for this test)')
    print(' If earlier offsets show win rate >= 60%, early entry is worth')
    print(' a live test because the payoff math is forgiving at lower prices.')
    print('=' * 64)
 
 
if __name__ == '__main__':
    main()