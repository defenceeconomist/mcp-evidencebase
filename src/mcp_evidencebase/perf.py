"""In-process performance counters and timing summaries for hot paths."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class PerfStat:
    """Aggregated in-process counter/timing data for one operation name."""

    count: int = 0
    total_seconds: float = 0.0
    last_seconds: float = 0.0

    def snapshot(self) -> dict[str, float | int]:
        average_seconds = self.total_seconds / self.count if self.count else 0.0
        return {
            "count": self.count,
            "total_seconds": round(self.total_seconds, 6),
            "last_seconds": round(self.last_seconds, 6),
            "average_seconds": round(average_seconds, 6),
        }


_LOCK = threading.Lock()
_STATS: dict[str, PerfStat] = {}


def _get_stat(name: str) -> PerfStat:
    normalized_name = str(name).strip()
    if not normalized_name:
        normalized_name = "unnamed"
    stat = _STATS.get(normalized_name)
    if stat is None:
        stat = PerfStat()
        _STATS[normalized_name] = stat
    return stat


def increment(name: str, *, amount: int = 1) -> None:
    """Increment an operation counter without recording a duration."""
    with _LOCK:
        _get_stat(name).count += max(0, int(amount))


def record_duration(name: str, *, elapsed_seconds: float) -> None:
    """Record one completed operation duration."""
    resolved_elapsed = max(0.0, float(elapsed_seconds))
    with _LOCK:
        stat = _get_stat(name)
        stat.count += 1
        stat.total_seconds += resolved_elapsed
        stat.last_seconds = resolved_elapsed


@contextmanager
def measure(name: str) -> Iterator[None]:
    """Measure one operation duration and store it in the process registry."""
    start = time.perf_counter()
    try:
        yield
    finally:
        record_duration(name, elapsed_seconds=time.perf_counter() - start)


def snapshot() -> dict[str, dict[str, float | int]]:
    """Return a JSON-serializable snapshot of current perf stats."""
    with _LOCK:
        return {
            name: stat.snapshot()
            for name, stat in sorted(_STATS.items(), key=lambda item: item[0])
        }


def reset() -> None:
    """Clear all in-process perf stats."""
    with _LOCK:
        _STATS.clear()
