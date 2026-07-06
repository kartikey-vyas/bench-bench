import asyncio
import time
import unittest

from bench_harness.config import WorkloadConfig
from bench_harness.python_client import run_one_request


def sse(payload: str) -> str:
    return f"data: {payload}\n\n"


def stream_pieces(chunk_count: int, content: str) -> list[str]:
    pieces = [sse('{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}')]
    for _ in range(chunk_count):
        pieces.append(sse('{"choices":[{"index":0,"delta":{"content":"%s"},"finish_reason":null}]}' % content))
    pieces.append(sse('{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}'))
    pieces.append(sse("[DONE]"))
    return pieces


class FakeResponse:
    status_code = 200

    def __init__(self, pieces):
        self._pieces = pieces

    async def aiter_text(self):
        for piece in self._pieces:
            yield piece

    async def aread(self):
        return b""


class FakeStream:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc):
        return False


class FakeClient:
    def __init__(self, pieces):
        self._pieces = pieces

    def stream(self, method, url, json=None):
        return FakeStream(FakeResponse(self._pieces))


def make_config(chunks=3):
    return WorkloadConfig(
        base_url="http://example", duration_seconds=1.0, warmup_seconds=0.0,
        concurrency=1, chunks_per_response=chunks, chunk_bytes=4,
        ttfc_ms=0, events_per_second=0, output_dir="results",
    )


class RunOneRequestTests(unittest.TestCase):
    def test_counts_content_chunks_and_completes(self):
        pieces = stream_pieces(3, "xxxx")
        window_end = time.perf_counter() + 60
        m = asyncio.run(run_one_request(FakeClient(pieces), make_config(3), 0, 0, window_end))
        self.assertTrue(m.ok)
        self.assertEqual(m.chunks, 3)          # role/finish events not counted
        self.assertEqual(m.bytes, 12)
        self.assertEqual(m.window_chunks, m.chunks)
        self.assertGreater(m.first_chunk_ms, 0.0)
        self.assertGreaterEqual(m.stream_ms, 0.0)
        self.assertGreaterEqual(m.max_gap_ms, 0.0)

    def test_missing_done_marks_not_ok(self):
        pieces = stream_pieces(3, "xxxx")[:-1]
        window_end = time.perf_counter() + 60
        m = asyncio.run(run_one_request(FakeClient(pieces), make_config(3), 0, 0, window_end))
        self.assertFalse(m.ok)
        self.assertEqual(m.chunks, 3)

    def test_window_end_in_past_yields_zero_window_chunks(self):
        pieces = stream_pieces(3, "xxxx")
        m = asyncio.run(run_one_request(FakeClient(pieces), make_config(3), 0, 0, 0.0))
        self.assertTrue(m.ok)
        self.assertEqual(m.chunks, 3)
        self.assertEqual(m.window_chunks, 0)


if __name__ == "__main__":
    unittest.main()
