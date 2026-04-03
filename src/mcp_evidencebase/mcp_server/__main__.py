"""Enable `python -m mcp_evidencebase.mcp_server`."""

from __future__ import annotations

from mcp_evidencebase.mcp_server.server import build_server


def main() -> None:
    """Run the Evidence Base MCP server over stdio."""
    build_server().run()


if __name__ == "__main__":
    main()
