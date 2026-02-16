"""Command-line interface for ``mcp_evidencebase``."""

from __future__ import annotations

import argparse

from mcp_evidencebase.core import healthcheck
from mcp_evidencebase.ingestion import build_ingestion_service


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

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
