"""CLI entrypoint for mcp_evidencebase."""

from __future__ import annotations

import argparse

from mcp_evidencebase.core import healthcheck


def build_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(prog="mcp-evidencebase")
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Run a basic package health check.",
    )
    return parser


def main() -> int:
    """Run the CLI and return process exit code."""
    parser = build_parser()
    args = parser.parse_args()
    if args.healthcheck:
        print(healthcheck())
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
