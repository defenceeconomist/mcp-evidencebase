"""Command-line interface for ``mcp_evidencebase``."""

from __future__ import annotations

import argparse
import json

from mcp_evidencebase.core import healthcheck
from mcp_evidencebase.ingestion import SEARCH_MODES, build_ingestion_service


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser.

    Returns:
        Configured parser for the ``mcp-evidencebase`` command.
    """
    parser = argparse.ArgumentParser(prog="mcp-evidencebase")
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Run a basic package health check.",
    )
    parser.add_argument(
        "--purge-datastores",
        action="store_true",
        help="Purge Redis and Qdrant data for the configured application prefix.",
    )
    parser.add_argument(
        "--search-bucket",
        help="Bucket/collection name used for semantic, keyword, or hybrid search.",
    )
    parser.add_argument(
        "--search-query",
        help="Query text for semantic, keyword, or hybrid search.",
    )
    parser.add_argument(
        "--search-mode",
        default="hybrid",
        choices=SEARCH_MODES,
        help="Search mode: semantic, keyword, or hybrid (default: hybrid).",
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=10,
        help="Maximum search results to return (default: 10).",
    )
    parser.add_argument(
        "--search-rrf-k",
        type=int,
        default=60,
        help="Hybrid mode RRF rank constant k (default: 60).",
    )
    return parser


def main() -> int:
    """Run the CLI command.

    Returns:
        Process exit code. The current command set always returns ``0``.
    """
    parser = build_parser()
    args = parser.parse_args()
    if args.healthcheck:
        print(healthcheck())
        return 0
    if args.purge_datastores:
        summary = build_ingestion_service().purge_datastores()
        print(summary)
        return 0
    if args.search_bucket or args.search_query:
        if not args.search_bucket or not args.search_query:
            parser.error("--search-bucket and --search-query must be provided together.")

        results = build_ingestion_service().search_documents(
            bucket_name=args.search_bucket,
            query=args.search_query,
            limit=args.search_limit,
            mode=args.search_mode,
            rrf_k=args.search_rrf_k,
        )
        print(
            json.dumps(
                {
                    "bucket_name": args.search_bucket,
                    "query": args.search_query,
                    "mode": args.search_mode,
                    "limit": args.search_limit,
                    "rrf_k": args.search_rrf_k,
                    "results": results,
                },
                sort_keys=True,
            )
        )
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
