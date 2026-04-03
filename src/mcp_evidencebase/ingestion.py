"""Compatibility facade for concern-split ingestion modules."""

from __future__ import annotations

import time

from mcp_evidencebase.ingestion_modules.chunking import *  # noqa: F403
from mcp_evidencebase.ingestion_modules.crossref import *  # noqa: F403
from mcp_evidencebase.ingestion_modules.metadata import *  # noqa: F403
from mcp_evidencebase.ingestion_modules.qdrant import QdrantIndexer
from mcp_evidencebase.ingestion_modules.repository import RedisDocumentRepository
from mcp_evidencebase.ingestion_modules.service import (
    IngestionService,
    PartitionClientLike,
    UnstructuredPartitionClient,
)
from mcp_evidencebase.ingestion_modules.wiring import (
    IngestionSettings,
    MinioObjectLike,
    build_ingestion_service,
    build_ingestion_settings,
)

__all__ = [
    "IngestionService",
    "IngestionSettings",
    "MinioObjectLike",
    "PartitionClientLike",
    "QdrantIndexer",
    "RedisDocumentRepository",
    "UnstructuredPartitionClient",
    "build_ingestion_service",
    "build_ingestion_settings",
    "time",
]
