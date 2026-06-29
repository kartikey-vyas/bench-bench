from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkloadConfig:
    base_url: str
    total_requests: int
    concurrency: int
    chunks_per_response: int
    chunk_bytes: int
    delay_us: int
    warmup_requests: int
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
        if self.total_requests < 0:
            raise ValueError("total_requests must be >= 0")
        if self.concurrency <= 0:
            raise ValueError("concurrency must be > 0")
        if self.chunks_per_response <= 0:
            raise ValueError("chunks_per_response must be > 0")
        if self.chunk_bytes < 0:
            raise ValueError("chunk_bytes must be >= 0")
        if self.delay_us < 0:
            raise ValueError("delay_us must be >= 0")
        if self.warmup_requests < 0:
            raise ValueError("warmup_requests must be >= 0")

    def request_payload(self, request_index: int, language: str) -> dict[str, Any]:
        return {
            "model": "synthetic",
            "messages": [{"role": "user", "content": "benchmark"}],
            "stream": True,
            "chunks": self.chunks_per_response,
            "chunk_bytes": self.chunk_bytes,
            "delay_us": self.delay_us,
            "request_id": f"{language}-{request_index}",
        }

    def result_config(self) -> dict[str, int | str]:
        return asdict(self)
