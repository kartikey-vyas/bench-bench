from __future__ import annotations


class SseDecoder:
    """Incremental decoder for the small SSE subset used by streaming APIs."""

    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, text: str) -> list[str]:
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        events: list[str] = []

        while "\n\n" in self._buffer:
            raw_event, self._buffer = self._buffer.split("\n\n", 1)
            data_lines: list[str] = []

            for line in raw_event.split("\n"):
                if not line or line.startswith(":"):
                    continue
                if line.startswith("data:"):
                    value = line[5:]
                    if value.startswith(" "):
                        value = value[1:]
                    data_lines.append(value)

            if data_lines:
                events.append("\n".join(data_lines))

        return events
