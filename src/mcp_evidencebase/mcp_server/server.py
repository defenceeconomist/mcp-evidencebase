"""FastMCP bootstrap for the Evidence Base stdio server."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from mcp_evidencebase.mcp_server.tools import EvidenceBaseMcpTools


def register_tools(server: FastMCP, tools: EvidenceBaseMcpTools) -> None:
    """Register the read-only Evidence Base tool surface."""

    @server.tool()
    def healthcheck() -> dict[str, object]:
        """Return dependency-aware runtime readiness for the current process."""
        return tools.healthcheck()

    @server.tool()
    def list_buckets() -> dict[str, object]:
        """List all configured Evidence Base buckets."""
        return tools.list_buckets()

    @server.tool()
    def list_documents(bucket_name: str) -> dict[str, object]:
        """List all documents for one bucket."""
        return tools.list_documents(bucket_name)

    @server.tool()
    def search_collection(
        bucket_name: str,
        query: str,
        limit: int = 10,
        mode: str = "hybrid",
        rrf_k: int = 60,
    ) -> dict[str, object]:
        """Search one bucket using semantic, keyword, or hybrid retrieval."""
        return tools.search_collection(
            bucket_name=bucket_name,
            query=query,
            limit=limit,
            mode=mode,
            rrf_k=rrf_k,
        )

    @server.tool()
    def list_document_sections(bucket_name: str, document_id: str) -> dict[str, object]:
        """List all stored sections for one document."""
        return tools.list_document_sections(bucket_name=bucket_name, document_id=document_id)

    @server.tool()
    def get_document_section(
        bucket_name: str,
        document_id: str,
        section_id: str,
    ) -> dict[str, object]:
        """Return one stored section for one document."""
        return tools.get_document_section(
            bucket_name=bucket_name,
            document_id=document_id,
            section_id=section_id,
        )

    @server.tool()
    def get_metadata_schema() -> dict[str, object]:
        """Return the shared citation and metadata schema."""
        return tools.get_metadata_schema()


def build_server(
    tools: EvidenceBaseMcpTools | None = None,
) -> FastMCP:
    """Build the stdio-first MCP server."""
    server = FastMCP(
        name="mcp-evidencebase",
        instructions=(
            "Read-only access to Evidence Base runtime health, buckets, documents, "
            "search results, sections, and metadata schema."
        ),
        json_response=True,
    )
    register_tools(server, tools=tools or EvidenceBaseMcpTools())
    return server
