"""Command-line interface for ``mcp_evidencebase``."""

from __future__ import annotations

import argparse
import json

from mcp_evidencebase.core import healthcheck
from mcp_evidencebase.ingestion import SEARCH_MODES, build_ingestion_service
from mcp_evidencebase.ingestion_modules.service import (
    DependencyConfigurationError,
    DependencyDisabledError,
)
from mcp_evidencebase.runtime_diagnostics import collect_runtime_health


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser.

    Returns:
        Configured parser for the ``mcp-evidencebase`` command.
    """
    parser = argparse.ArgumentParser(prog="mcp-evidencebase")
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Run a dependency-aware health check and print `ok` or `error`.",
    )
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Print a full runtime dependency report and exit non-zero when required checks fail.",
    )
    parser.add_argument(
        "--purge-datastores",
        action="store_true",
        help="Purge Redis and Qdrant data for the configured application prefix.",
    )
    parser.add_argument(
        "--migrate-qdrant-to-shared-collection",
        action="store_true",
        help="Backfill legacy per-bucket Qdrant collections into the shared collection.",
    )
    parser.add_argument(
        "--relocate-prefix-to-root",
        action="store_true",
        help="Relocate one bucket prefix to the bucket root without reindexing.",
    )
    parser.add_argument(
        "--bucket",
        help="Bucket used by maintenance operations such as prefix relocation.",
    )
    parser.add_argument(
        "--source-prefix",
        default="articles/",
        help="Source prefix for relocation operations (default: articles/).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply a maintenance operation; otherwise run in dry-run mode.",
    )
    parser.add_argument(
        "--merge-buckets-into",
        help="Merge legacy physical buckets into one shared storage bucket.",
    )
    parser.add_argument(
        "--source-buckets",
        help="Comma-separated legacy physical bucket names to merge into shared storage.",
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
    if args.doctor:
        report = collect_runtime_health()
        print(json.dumps(report, sort_keys=True))
        return 0 if bool(report.get("ready")) else 1
    if args.healthcheck:
        status = healthcheck()
        print(status)
        return 0 if status == "ok" else 1
    if args.purge_datastores:
        try:
            summary = build_ingestion_service().purge_datastores()
        except (DependencyConfigurationError, DependencyDisabledError) as exc:
            print(str(exc))
            return 1
        print(summary)
        return 0
    if args.migrate_qdrant_to_shared_collection:
        try:
            summary = build_ingestion_service().migrate_legacy_qdrant_collections(
                dry_run=not bool(args.apply),
            )
        except (DependencyConfigurationError, DependencyDisabledError, ValueError) as exc:
            print(str(exc))
            return 1
        print(json.dumps(summary, sort_keys=True))
        return 0
    if args.relocate_prefix_to_root:
        if not args.bucket:
            parser.error("--bucket is required with --relocate-prefix-to-root.")
        try:
            summary = build_ingestion_service().relocate_prefix_to_bucket_root(
                bucket_name=args.bucket,
                source_prefix=args.source_prefix,
                dry_run=not bool(args.apply),
            )
        except (DependencyConfigurationError, DependencyDisabledError, ValueError) as exc:
            print(str(exc))
            return 1
        print(json.dumps(summary, sort_keys=True))
        return 0
    if args.merge_buckets_into:
        if not args.source_buckets:
            parser.error("--source-buckets is required with --merge-buckets-into.")
        source_bucket_names = [
            bucket_name.strip()
            for bucket_name in str(args.source_buckets).split(",")
            if bucket_name.strip()
        ]
        if not source_bucket_names:
            parser.error("--source-buckets must include at least one bucket name.")
        try:
            summary = build_ingestion_service().merge_buckets_into_storage(
                source_bucket_names=source_bucket_names,
                target_bucket_name=args.merge_buckets_into,
                dry_run=not bool(args.apply),
            )
        except (DependencyConfigurationError, DependencyDisabledError, ValueError) as exc:
            print(str(exc))
            return 1
        print(json.dumps(summary, sort_keys=True))
        return 0
    if args.search_bucket or args.search_query:
        if not args.search_bucket or not args.search_query:
            parser.error("--search-bucket and --search-query must be provided together.")

        try:
            results = build_ingestion_service().search_documents(
                bucket_name=args.search_bucket,
                query=args.search_query,
                limit=args.search_limit,
                mode=args.search_mode,
                rrf_k=args.search_rrf_k,
            )
        except (DependencyConfigurationError, DependencyDisabledError) as exc:
            print(str(exc))
            return 1
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
