import unittest

from bench_harness.sse import SseDecoder


class SseDecoderTests(unittest.TestCase):
    def test_decoder_returns_complete_data_events_and_buffers_partial_lines(self):
        decoder = SseDecoder()

        self.assertEqual(decoder.feed('data: {"a":'), [])
        self.assertEqual(decoder.feed('1}\n\ndata: [DONE]\n\n'), ['{"a":1}', "[DONE]"])

    def test_decoder_ignores_comments_and_blank_events(self):
        decoder = SseDecoder()

        self.assertEqual(decoder.feed(": keepalive\n\ndata: hello\n\n\n"), ["hello"])


if __name__ == "__main__":
    unittest.main()
