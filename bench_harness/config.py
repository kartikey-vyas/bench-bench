from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkloadConfig:
    base_url: str
    duration_seconds: float
    warmup_seconds: float
    concurrency: int
    chunks_per_response: int
    chunk_bytes: int
    ttfc_ms: int
    events_per_second: int
    output_dir: str

    @classmethod
    def from_path(cls, path: str | Path) -> "WorkloadConfig":
        data = json.loads(Path(path).read_text())
        config = cls(**data)
        config.validate()
        return config

    @property
    def endpoint(self) -> str:
        return f"{self.base_url.rstrip('/')}/v1/chat/completions"

    def validate(self) -> None:
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be > 0")
        if self.warmup_seconds < 0:
            raise ValueError("warmup_seconds must be >= 0")
        if self.concurrency <= 0:
            raise ValueError("concurrency must be > 0")
        if self.chunks_per_response <= 0:
            raise ValueError("chunks_per_response must be > 0")
        if self.chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be > 0")
        if self.ttfc_ms < 0:
            raise ValueError("ttfc_ms must be >= 0")
        if self.events_per_second < 0:
            raise ValueError("events_per_second must be >= 0")

    def request_payload(self, worker_index: int, sequence: int, language: str) -> dict[str, Any]:
        return {
            "model": "synthetic",
            "messages": [{"role": "user", "content": "benchmark"}],
            "stream": True,
            "chunks": self.chunks_per_response,
            "chunk_bytes": self.chunk_bytes,
            "ttfc_ms": self.ttfc_ms,
            "events_per_second": self.events_per_second,
            "request_id": f"{language}-{worker_index}-{sequence}",
        }

    def result_config(self) -> dict[str, Any]:
        return asdict(self)
