# coding: utf-8
"""Arrow wire-format round-trip tests (wire_format="arrow").

These do NOT touch a real pipe or QMT -- they exercise the encode/decode
functions directly, so they run on any platform with pyarrow. The pipe-frame
encode/decode (_encode_pipe_frame / _decode_pipe_frame) is tested here too
because it is pure bytes logic, not Windows kernel objects.

Skipped entirely when pyarrow is absent: the arrow path's whole contract is
"fall back to json when pyarrow is missing", and the json path is already
covered by the existing to_jsonable / _restore_jsonable tests.
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))


def _has_pyarrow():
    try:
        import pyarrow  # noqa: F401
        return True
    except Exception:
        return False


PYARROW = unittest.skipUnless(_has_pyarrow(), "pyarrow not installed")


@PYARROW
class ArrowFrameRoundTripTest(unittest.TestCase):
    """Server encode -> pipe frame -> client decode, data fidelity."""

    def _sample_frames(self):
        import pandas as pd
        import numpy as np
        return {
            "600000.SH": pd.DataFrame({
                "time": ["202601010930", "202601010935"],
                "open": [10.0, 10.5],
                "close": [10.5, 10.2],
                "volume": [100, 200],
            }),
            "000001.SZ": pd.DataFrame({
                "time": ["202601010930"],
                "open": [1.0],
                "close": [1.1],
                "volume": [50],
            }),
        }

    def test_encode_envelope_then_restore_matches_json_path(self):
        """The arrow path must return the same DataFrames the json path does."""
        from bigqmt_signal_trader.redis_rpc import _arrow_encode_envelope
        from bigqmt_signal_trader.xtquant_compat import _restore_arrow_envelope
        import pandas as pd

        frames = self._sample_frames()
        # json path (the source of truth for shape)
        json_rebuilt = {
            code: pd.DataFrame({
                "time": df["time"].tolist(),
                "open": df["open"].tolist(),
                "close": df["close"].tolist(),
                "volume": df["volume"].tolist(),
            })
            for code, df in frames.items()
        }

        envelope = _arrow_encode_envelope(frames, min_rows=1)
        self.assertIsNotNone(envelope, "arrow encode should succeed with pyarrow")
        self.assertEqual(envelope["__bigqmt_wire__"], "arrow_envelope")
        rebuilt = _restore_arrow_envelope(envelope)
        self.assertEqual(set(rebuilt), set(frames))
        for code in frames:
            self.assertIsInstance(rebuilt[code], pd.DataFrame)
            self.assertEqual(list(rebuilt[code].columns),
                             list(frames[code].columns))
            self.assertEqual(len(rebuilt[code]), len(frames[code]))
            # values match
            self.assertEqual(list(rebuilt[code]["close"]),
                             list(frames[code]["close"]))
            self.assertEqual(list(rebuilt[code]["volume"]),
                             list(frames[code]["volume"]))

    def test_pipe_frame_round_trip_preserves_arrow_bodies(self):
        """_encode_pipe_frame -> _decode_pipe_frame reassembles the envelope."""
        from bigqmt_signal_trader.redis_rpc import _arrow_encode_envelope
        from bigqmt_signal_trader.transports.pipe_transport import (
            _encode_pipe_frame, _decode_pipe_frame, _ARROWS_FRAME_MAGIC,
        )

        frames = self._sample_frames()
        envelope = _arrow_encode_envelope(frames, min_rows=1)
        response = {"request_id": "r1", "method": "get_market_data_ex",
                    "ok": True, "_wire": "arrow", "data": envelope}
        wire = _encode_pipe_frame(response)
        # arrow frame starts with magic, not '{'
        self.assertTrue(wire.startswith(_ARROWS_FRAME_MAGIC))
        self.assertNotEqual(wire[:1], b"{")

        decoded = _decode_pipe_frame(wire)
        self.assertIsNotNone(decoded, "magic-prefixed frame must decode")
        self.assertEqual(decoded["request_id"], "r1")
        self.assertEqual(decoded["_wire"], "arrow")
        data = decoded["data"]
        self.assertEqual(data["__bigqmt_wire__"], "arrow_envelope")
        # bodies reassembled as bytes
        for code, env in data["frames"].items():
            self.assertIsInstance(env["body"], (bytes, bytearray))
            self.assertGreater(len(env["body"]), 0)
            # offset/length markers consumed
            self.assertNotIn("_offset", env)
            self.assertNotIn("_length", env)

    def test_json_frame_unchanged_when_wire_is_json(self):
        """The default (wire_format=json) path must be byte-identical to before."""
        from bigqmt_signal_trader.transports.pipe_transport import (
            _encode_pipe_frame, _decode_pipe_frame,
        )
        import json

        response = {"request_id": "r1", "ok": True, "data": {"x": 1}}
        wire = _encode_pipe_frame(response)
        self.assertEqual(wire[:1], b"{", "json frame starts with {")
        self.assertIsNone(_decode_pipe_frame(wire),
                         "json frame is not an arrow frame -> None")
        # and json.loads still decodes it
        self.assertEqual(json.loads(wire.decode("utf-8")), response)

    def test_small_answer_falls_back_to_json(self):
        """Below min_rows the encode returns None -> caller uses json."""
        from bigqmt_signal_trader.redis_rpc import _arrow_encode_envelope
        import pandas as pd

        tiny = {"600000.SH": pd.DataFrame({"close": [1.0]})}  # 1 row < 1000
        self.assertIsNone(_arrow_encode_envelope(tiny, min_rows=1000))

    def test_non_dataframe_answer_falls_back_to_json(self):
        """A scalar/dict-of-scalars answer never hits the arrow path."""
        from bigqmt_signal_trader.redis_rpc import _arrow_encode_envelope

        self.assertIsNone(_arrow_encode_envelope({"x": 1, "y": "a"}))
        self.assertIsNone(_arrow_encode_envelope(None))
        self.assertIsNone(_arrow_encode_envelope([]))

    def test_encode_failure_falls_back_to_json(self):
        """One un-encodable frame -> whole answer returns None (json fallback)."""
        from bigqmt_signal_trader.redis_rpc import _arrow_encode_envelope
        import pandas as pd

        # a DataFrame with an un-pickleable object column still encodes via
        # Arrow (it has a string type), so craft something Arrow rejects:
        # an object column of mixed non-serializable items. Arrow will raise on
        # from_pandas -> _arrow_encode_frame returns None -> envelope None.
        class _Bad(object):
            pass
        frames = {"x": pd.DataFrame({"c": [_Bad(), _Bad()]})}
        # Arrow can't infer a type for a column of custom objects -> falls back
        self.assertIsNone(_arrow_encode_envelope(frames, min_rows=1))


if __name__ == "__main__":
    unittest.main()
