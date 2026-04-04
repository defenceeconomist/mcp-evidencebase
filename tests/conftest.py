from __future__ import annotations

from collections.abc import Generator

import pytest

from mcp_evidencebase.api_modules.deps import reset_ingestion_service_cache_for_tests
from mcp_evidencebase.perf import reset as reset_perf_stats


@pytest.fixture(autouse=True)
def reset_runtime_singletons() -> Generator[None, None, None]:
    """Keep cached services and perf counters isolated across test cases."""
    reset_ingestion_service_cache_for_tests()
    reset_perf_stats()
    yield
    reset_ingestion_service_cache_for_tests()
    reset_perf_stats()
