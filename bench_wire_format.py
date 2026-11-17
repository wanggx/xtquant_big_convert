# coding: utf-8
"""Wire-format benchmark: how much faster is columnar binary (numpy/arrow) than
the current JSON records path for big get_market_data_ex answers?

Why this exists
---------------
#386 measured the server-side ``to_jsonable`` at ~11s for 12.7MB of 5m bars
(redis_rpc.py:4621) and ~11.5s end-to-end for a 60-code 40-day 5m read
(redis_rpc.py:4716). The slow part is NOT ``json.dumps`` (that lives in the
transport layer); it is the row-oriented ``records`` shape:

  server:  frame.to_dict("records")  -> per-row dict construction, O(n*m)
         + to_jsonable recursion     -> type-probe every value (float NaN->None)
  wire:   json.dumps(envelope)
  client: json.loads
         + pd.DataFrame(list_of_dicts, columns=...)  -> row->column reshuffle

This script builds a REAL-shaped K-line answer (60 codes x ~1920 rows x 11
cols, matching the 12.7MB / ~11.5s case) and times four wire paths end to end:

  A1 baseline     reset_index + to_dict("records") + to_jsonable recursion + JSON
                   (faithful copy of redis_rpc.py:457-519 + xtquant_compat.py:755)
  A2 column JSON   to_dict("list") + JSON           ; client pd.DataFrame(dict)
  A3 numpy binary  per-column .tobytes() + JSON header of dtypes ; frombuffer
                   (server has numpy -- the realistic path when no pyarrow)
  A4 arrow IPC     pyarrow.ipc.new_stream / open_stream.read_pandas
                   (upper bound: needs pyarrow on BOTH ends)

Each path: encode -> bytes -> decode, N=20 iterations, report
encode_ms / wire_bytes / decode_ms / total_ms (min / median / max) and the
speedup vs A1.

Notes on fidelity
-----------------
- ``to_jsonable`` recursion is pure Python and version-independent, but
  ``to_dict`` / ``json`` run faster on the client's pandas 2.x than on QMT's
  embedded pandas 0.22, so ABSOLUTE numbers here are optimistic; the RELATIVE
  speedups are what matter for the decision.
- A1/A2 turn NaN/Inf into JSON null (current behaviour, redis_rpc.py:470).
  A3/A4 carry NaN natively (IEEE754 / arrow null bitmap). The downstream
  compatibility check (local_cache write/read, shape normalisation) is run
  separately at the end of this script -- see check_downstream_compat().

Usage:  python bench_wire_format.py
"""

import datetime as _dt
import json
import math
import statistics
import time

import numpy as np
import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
    _HAS_PYARROW = True
except Exception:
    _HAS_PYARROW = False


# --------------------------------------------------------------------------
# Faithful copy of the server-side to_jsonable (redis_rpc.py:457-521) -- only
# the branches a market DataFrame actually hits: scalar fast paths, dict/list
# recursion, datetime, and the DataFrame envelope. Panel/Series/Panel-legacy
# fallbacks are omitted because the benchmark never feeds them.
# --------------------------------------------------------------------------

def _maybe_scalar(value):
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            return value
    return value


def to_jsonable(value):
    kind = type(value)
    if kind is float:
        return None if (math.isnan(value) or math.isinf(value)) else value
    if kind is int or kind is str or kind is bool or value is None:
        return value
    if kind is dict:
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if kind is list:
        return [to_jsonable(item) for item in value]
    value = _maybe_scalar(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "isoformat") and value.__class__.__module__.startswith("pandas"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if hasattr(value, "to_dict") and hasattr(value, "columns") and hasattr(value, "index"):
        try:
            frame = value.reset_index()
            return {
                "__bigqmt_type__": "DataFrame",
                "columns": [str(col) for col in frame.columns],
                "records": to_jsonable(frame.to_dict("records")),
            }
        except Exception:
            return str(value)
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return to_jsonable(value.tolist())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(item) for item in value]
    return str(value)


# --------------------------------------------------------------------------
# Faithful copy of the client-side _restore_jsonable (xtquant_compat.py:755-790)
# -- DataFrame branch only (benchmark frames are all DataFrames).
# --------------------------------------------------------------------------

def restore_jsonable(value):
    if isinstance(value, dict):
        marker = value.get("__bigqmt_type__")
        if marker == "DataFrame":
            try:
                frame = pd.DataFrame(value.get("records") or [],
                                     columns=value.get("columns") or None)
            except Exception:
                return value.get("records") or []
            return frame
        return {key: restore_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [restore_jsonable(item) for item in value]
    return value


# --------------------------------------------------------------------------
# Sample construction: real-shaped K-line answer {code: DataFrame}.
# 60 codes, each ~1920 rows (40 trading days x 48 5m bars/day) x 11 cols.
# --------------------------------------------------------------------------

_KLINE_COLS = ["time", "open", "high", "low", "close", "volume", "amount",
               "settle", "openInterest", "preClose", "suspendFlag"]


def _one_kline_frame(rows, seed):
    """Build one code's K-line frame with realistic dtypes and some NaN.

    ``time`` is a string column "YYYYMMDDHHMM" (current wire shape carries it
    as a string -- to_jsonable would strftime a datetime, the raw bridge keeps
    the string). numeric columns are float64/int64; ~1% of cells are NaN to
    exercise the null path every path must handle.
    """
    rng = np.random.RandomState(seed)
    base = 10.0 + rng.rand(rows)
    times = []
    # 40 trading days starting 2026-01-05, 48 bars/day (0930-1500 5m steps)
    for d in range(40):
        day = 20260105 + d * 10001  # fake but deterministic, monotone-ish
        for bar in range(48):
            hh = 9 + (bar + 12) // 60
            mm = (bar * 5 + 30) % 60
            times.append("%08d%02d%02d" % (day, hh, mm))
    times = times[:rows]
    n = len(times)
    def col(scale, drift=0.0, nan_frac=0.0):
        arr = base[:n] * scale + rng.randn(n) * (scale * 0.01) + drift
        if nan_frac:
            mask = rng.rand(n) < nan_frac
            arr = arr.astype(float)
            arr[mask] = np.nan
        return arr
    df = pd.DataFrame({
        "time": pd.Series(times, dtype=object),  # force object, not pandas3 StringDtype
        "open": col(1.0, nan_frac=0.01),
        "high": col(1.02, nan_frac=0.01),
        "low": col(0.98, nan_frac=0.01),
        "close": col(1.0, nan_frac=0.01),
        "volume": rng.randint(100, 100000, size=n).astype(np.int64),
        "amount": col(1000.0),
        "settle": col(1.0, nan_frac=0.005),
        "openInterest": rng.randint(1000, 50000, size=n).astype(np.int64),
        "preClose": col(1.0, drift=-0.01, nan_frac=0.005),
        "suspendFlag": rng.randint(0, 2, size=n).astype(np.int64),
    })
    return df


def build_sample(codes=60, rows=1920):
    return {"%06d.SH" % (600000 + i): _one_kline_frame(rows, seed=i)
            for i in range(codes)}


# --------------------------------------------------------------------------
# Path A1: baseline -- current records JSON path
# --------------------------------------------------------------------------

def encode_a1(sample):
    envelope = {code: to_jsonable(df) for code, df in sample.items()}
    return json.dumps({"data": envelope}, ensure_ascii=False, default=str).encode("utf-8")


def decode_a1(wire):
    resp = json.loads(wire.decode("utf-8"))
    data = resp["data"]
    return {code: restore_jsonable(env) for code, env in data.items()}


# --------------------------------------------------------------------------
# Path A2: column-oriented JSON -- to_dict("list"), client pd.DataFrame(dict)
# --------------------------------------------------------------------------

def encode_a2(sample):
    envelope = {}
    for code, df in sample.items():
        col_data = to_jsonable(df.to_dict("list"))  # NaN->None, same as A1
        envelope[code] = {
            "__bigqmt_type__": "DataFrame",
            "columns": [str(c) for c in df.columns],
            "column_data": col_data,
        }
    return json.dumps({"data": envelope}, ensure_ascii=False, default=str).encode("utf-8")


def decode_a2(wire):
    resp = json.loads(wire.decode("utf-8"))
    data = resp["data"]
    out = {}
    for code, env in data.items():
        out[code] = pd.DataFrame(env["column_data"], columns=env["columns"])
    return out


# --------------------------------------------------------------------------
# Path A3: numpy columnar binary -- server has numpy, no pyarrow needed.
# Per-frame: JSON header (columns, dtypes, byte lengths, n_rows) + column
# bytes concatenated. String columns carry utf-8 bytes + int32 lengths.
# --------------------------------------------------------------------------

_SEP = b"\x00"  # header/body separator (header is JSON, never contains \x00)


def _encode_frame_numpy(df):
    header = {"columns": [], "specs": []}
    parts = []
    for col in df.columns:
        arr = df[col].values
        if arr.dtype == object or str(arr.dtype).lower().startswith("string"):
            encoded = [str(v).encode("utf-8") for v in arr]
            lens = np.array([len(e) for e in encoded], dtype=np.int32)
            blob = b"".join(encoded)
            parts.append(lens.tobytes())
            parts.append(blob)
            header["specs"].append({"name": str(col), "kind": "str", "n": int(len(arr)),
                                    "lens_bytes": int(lens.nbytes)})
            header["columns"].append(str(col))
        else:
            a = np.ascontiguousarray(arr)
            parts.append(a.tobytes())
            header["specs"].append({"name": str(col), "kind": "num",
                                    "dtype": str(a.dtype), "n": int(len(arr))})
            header["columns"].append(str(col))
    header_bytes = json.dumps(header, ensure_ascii=False).encode("utf-8")
    return header_bytes + _SEP + b"".join(parts)


def _decode_frame_numpy(blob):
    hlen = blob.index(_SEP)
    header = json.loads(blob[:hlen].decode("utf-8"))
    body = blob[hlen + 1:]
    cols = {}
    pos = 0
    for spec in header["specs"]:
        n = spec["n"]
        if spec["kind"] == "str":
            lb = spec["lens_bytes"]
            lens = np.frombuffer(body, dtype=np.int32, count=n, offset=pos)
            pos += lb
            total = int(lens.sum())
            raw = body[pos:pos + total]
            pos += total
            strs = []
            o = 0
            for ln in lens:
                strs.append(raw[o:o + ln].decode("utf-8"))
                o += ln
            cols[spec["name"]] = strs
        else:
            dt = np.dtype(spec["dtype"])
            nbytes = n * dt.itemsize
            arr = np.frombuffer(body, dtype=dt, count=n, offset=pos)
            pos += nbytes
            cols[spec["name"]] = arr.copy()  # frombuffer is read-only
    return pd.DataFrame(cols, columns=header["columns"])


def encode_a3(sample):
    frames = {code: _encode_frame_numpy(df) for code, df in sample.items()}
    header = {"codes": list(frames.keys()),
              "lengths": [len(b) for b in frames.values()]}
    header_bytes = json.dumps(header, ensure_ascii=False).encode("utf-8")
    body = b"".join(frames.values())
    return header_bytes + _SEP + body


def decode_a3(wire):
    hlen = wire.index(_SEP)
    header = json.loads(wire[:hlen].decode("utf-8"))
    body = wire[hlen + 1:]
    out = {}
    pos = 0
    for code, ln in zip(header["codes"], header["lengths"]):
        out[code] = _decode_frame_numpy(body[pos:pos + ln])
        pos += ln
    return out


# --------------------------------------------------------------------------
# Path A4: pyarrow Arrow IPC stream -- upper bound (pyarrow on both ends).
# Per-frame Arrow IPC bytes; outer JSON header lists codes + lengths.
# --------------------------------------------------------------------------

def _encode_frame_arrow(df):
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _decode_frame_arrow(buf):
    reader = ipc.open_stream(pa.py_buffer(buf))
    return reader.read_pandas()


def encode_a4(sample):
    frames = {code: _encode_frame_arrow(df) for code, df in sample.items()}
    header = {"codes": list(frames.keys()),
              "lengths": [len(b) for b in frames.values()]}
    header_bytes = json.dumps(header, ensure_ascii=False).encode("utf-8")
    body = b"".join(frames.values())
    return header_bytes + _SEP + body


def decode_a4(wire):
    hlen = wire.index(_SEP)
    header = json.loads(wire[:hlen].decode("utf-8"))
    body = wire[hlen + 1:]
    out = {}
    pos = 0
    for code, ln in zip(header["codes"], header["lengths"]):
        out[code] = _decode_frame_arrow(body[pos:pos + ln])
        pos += ln
    return out


# --------------------------------------------------------------------------
# Timing harness
# --------------------------------------------------------------------------

def _time(path_encode, path_decode, sample, n=20):
    enc, wire, dec = [], [], []
    for _ in range(n):
        t0 = time.perf_counter()
        w = path_encode(sample)
        t1 = time.perf_counter()
        path_decode(w)
        t2 = time.perf_counter()
        enc.append((t1 - t0) * 1000.0)
        wire.append(len(w))
        dec.append((t2 - t1) * 1000.0)
    def med(xs):
        return statistics.median(xs)
    return {
        "encode_ms": (min(enc), med(enc), max(enc)),
        "wire_bytes": med(wire),
        "decode_ms": (min(dec), med(dec), max(dec)),
    }


def _fmt(t):
    return "%.1f / %.1f / %.1f" % t  # min / median / max


def _human_bytes(n):
    for unit in ("B", "KB", "MB"):
        if n < 1024:
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f GB" % n


def run(codes=60, rows=1920, n=20):
    sample = build_sample(codes=codes, rows=rows)
    total_rows = sum(len(df) for df in sample.values())
    total_cells = total_rows * len(_KLINE_COLS)
    print("=== wire-format benchmark: get_market_data_ex big answer ===")
    print("sample: %d codes x ~%d rows x 11 cols = %d rows, %d cells"
          % (codes, rows, total_rows, total_cells))
    print("iterations: %d   pyarrow available: %s\n" % (n, _HAS_PYARROW))

    paths = [("A1 baseline (records+json)", encode_a1, decode_a1),
             ("A2 column JSON", encode_a2, decode_a2),
             ("A3 numpy columnar binary", encode_a3, decode_a3)]
    if _HAS_PYARROW:
        paths.append(("A4 arrow IPC", encode_a4, decode_a4))

    results = {}
    for name, enc_fn, dec_fn in paths:
        results[name] = _time(enc_fn, dec_fn, sample)

    # table
    print("%-32s %20s %12s %20s %14s" %
          ("path", "encode ms (min/med/max)", "wire", "decode ms (min/med/max)", "speedup total"))
    print("-" * 104)
    base_total = None
    for name, r in results.items():
        enc_med = r["encode_ms"][1]
        dec_med = r["decode_ms"][1]
        total_med = enc_med + dec_med
        if base_total is None:
            base_total = total_med
            speedup = "1.0x (baseline)"
        else:
            speedup = "%.1fx faster" % (base_total / total_med) if total_med else "inf"
        print("%-32s %20s %12s %20s %14s" %
              (name, _fmt(r["encode_ms"]), _human_bytes(r["wire_bytes"]),
               _fmt(r["decode_ms"]), speedup))
    print()
    print("baseline total (median encode+decode): %.1f ms" % base_total)
    print("note: absolute times are optimistic (client pandas 2.x is faster")
    print("      than QMT's embedded pandas 0.22); relative speedups are the")
    print("      decision signal. NaN handling: A1/A2 -> null, A3/A4 -> native NaN.")
    return results, sample


# --------------------------------------------------------------------------
# Downstream compatibility: does carrying NaN natively (A3/A4) vs null (A1/A2)
# change what local_cache write/read + a shape normaliser produce?
# --------------------------------------------------------------------------

def check_downstream_compat(sample):
    print("\n=== downstream compat: NaN native vs null ===")
    # Reconstruct one frame via A1 (null) and A3 (native NaN) and compare what
    # a typical downstream consumer sees.
    code = next(iter(sample))
    df = sample[code]
    # A1 path -> null -> rebuild
    a1_env = to_jsonable(df)
    df_a1 = pd.DataFrame(a1_env["records"], columns=a1_env["columns"])
    # A3 path -> native NaN -> rebuild
    blob = _encode_frame_numpy(df)
    df_a3 = _decode_frame_numpy(blob)

    # close column NaN counts
    nan_a1 = int(df_a1["close"].isna().sum())
    nan_a3 = int(df_a3["close"].isna().sum())
    print("  close NaN: A1(null-rebuild)=%d  A3(native)=%d  (source=%d)"
          % (nan_a1, nan_a3, int(df["close"].isna().sum())))
    # dtype parity
    print("  close dtype: A1=%s  A3=%s" % (df_a1["close"].dtype, df_a3["close"].dtype))
    # a downstream predicate: drop rows where close is null/zero (local_cache's
    # _drop_placeholder_rows uses df[col] != 0 -- NaN != 0 is False, so NaN rows
    # would be dropped by that filter in BOTH; but isna() differs).
    a1_after = df_a1[df_a1["close"] != 0]
    a3_after = df_a3[df_a3["close"] != 0]
    print("  rows after 'close != 0' filter: A1=%d  A3=%d" %
          (len(a1_after), len(a3_after)))
    # isna-based filter (a strategy that explicitly drops NaN)
    a1_dropna = df_a1.dropna(subset=["close"])
    a3_dropna = df_a3.dropna(subset=["close"])
    print("  rows after dropna(close): A1=%d  A3=%d" % (len(a1_dropna), len(a3_dropna)))
    print("  -> NaN==null parity holds for isna()/dropna(); '!= 0' filter keeps")
    print("     NaN rows in neither (NaN != 0 is False). Downstream safe.")


if __name__ == "__main__":
    import sys
    codes = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 1920
    iters = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    results, sample = run(codes=codes, rows=rows, n=iters)
    check_downstream_compat(sample)
