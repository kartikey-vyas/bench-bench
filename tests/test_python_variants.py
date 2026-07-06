import asyncio
import unittest
from types import SimpleNamespace

from bench_harness.config import WorkloadConfig
from bench_harness.python_deferred_client import BoundaryCounter
from bench_harness.python_deferred_client import run_one_request as deferred_request
from bench_harness.python_openai_client import run_one_request as openai_request


def make_config(chunks=3):
    return WorkloadConfig(
        base_url="http://example", duration_seconds=1.0, warmup_seconds=0.0,
        concurrency=1, chunks_per_response=chunks, chunk_bytes=4,
        ttfc_ms=0, events_per_second=0, output_dir="results",
    )


def sse(payload: str) -> bytes:
    return f"data: {payload}\n\n".encode()


def stream_bytes(chunk_count: int, content: str) -> list[bytes]:
    pieces = [sse('{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}')]
    for _ in range(chunk_count):
        pieces.append(sse('{"choices":[{"index":0,"delta":{"content":"%s"},"finish_reason":null}]}' % content))
    pieces.append(sse('{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}'))
    pieces.append(sse("[DONE]"))
    return pieces


class BoundaryCounterTests(unittest.TestCase):
    def test_counts_within_and_across_reads(self):
        counter = BoundaryCounter()
        self.assertEqual(counter.feed(b"data: a\n"), 0)   # ends mid-boundary
        self.assertEqual(counter.feed(b"\ndata: b\n\n"), 2)
        self.assertEqual(counter.feed(b"data: c\n\ndata: d\n\n"), 2)
        self.assertEqual(counter.feed(b""), 0)


class FakeByteResponse:
    status_code = 200

    def __init__(self, pieces):
        self._pieces = pieces

    async def aiter_bytes(self):
        for piece in self._pieces:
            yield piece

    async def aread(self):
        return b""


class FakeByteStream:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc):
        return False


class FakeHttpxClient:
    def __init__(self, pieces):
        self._pieces = pieces

    def stream(self, method, url, json=None):
        return FakeByteStream(FakeByteResponse(self._pieces))


class DeferredClientTests(unittest.TestCase):
    def test_counts_content_after_close_and_completes(self):
        pieces = stream_bytes(3, "xxxx")
        m = asyncio.run(deferred_request(FakeHttpxClient(pieces), make_config(3), 0, 0))
        self.assertTrue(m.ok)
        self.assertEqual(m.chunks, 3)
        self.assertEqual(m.bytes, 12)
        self.assertGreater(m.first_chunk_ms, 0.0)

    def test_missing_done_marks_not_ok_but_still_counts(self):
        pieces = stream_bytes(3, "xxxx")[:-1]
        m = asyncio.run(deferred_request(FakeHttpxClient(pieces), make_config(3), 0, 0))
        self.assertFalse(m.ok)
        self.assertEqual(m.chunks, 3)

    def test_split_boundary_across_reads(self):
        pieces = stream_bytes(2, "xxxx")
        blob = b"".join(pieces)
        # Re-split so an event boundary straddles two reads.
        split = blob.find(b"\n\n") + 1
        m = asyncio.run(
            deferred_request(FakeHttpxClient([blob[:split], blob[split:]]), make_config(2), 0, 0)
        )
        self.assertTrue(m.ok)
        self.assertEqual(m.chunks, 2)


def sdk_chunk(content):
    delta = SimpleNamespace(content=content)
    return SimpleNamespace(choices=[SimpleNamespace(delta=delta)])


class FakeSdkStream:
    def __init__(self, chunks):
        self._chunks = chunks

    def __aiter__(self):
        self._iter = iter(self._chunks)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeSdkClient:
    def __init__(self, chunks):
        stream = FakeSdkStream(chunks)

        async def create(**kwargs):
            self.create_kwargs = kwargs
            return stream

        self.chat = SimpleNamespace(completions=SimpleNamespace(create=create))


class OpenAiClientTests(unittest.TestCase):
    def test_counts_content_chunks_and_passes_pacing_extra_body(self):
        chunks = [sdk_chunk(""), sdk_chunk("xxxx"), sdk_chunk("xxxx"), sdk_chunk(None)]
        client = FakeSdkClient(chunks)
        m = asyncio.run(openai_request(client, make_config(2), 0, 5))
        self.assertTrue(m.ok)
        self.assertEqual(m.chunks, 2)          # empty/None deltas not counted
        self.assertEqual(m.bytes, 8)
        extra = client.create_kwargs["extra_body"]
        self.assertEqual(extra["chunks"], 2)
        self.assertEqual(extra["request_id"], "python-openai-0-5")

    def test_exception_marks_not_ok(self):
        class ExplodingClient:
            def __init__(self):
                async def create(**kwargs):
                    raise RuntimeError("boom")
                self.chat = SimpleNamespace(completions=SimpleNamespace(create=create))

        m = asyncio.run(openai_request(ExplodingClient(), make_config(2), 0, 0))
        self.assertFalse(m.ok)
        self.assertEqual(m.chunks, 0)


if __name__ == "__main__":
    unittest.main()
