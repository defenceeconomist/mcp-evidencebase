# GPT Actions API

This article documents the GPT-facing API endpoints exposed by `mcp-evidencebase`, with emphasis on the `searchCollection` GPT Action function.

## Base URLs

Use one of these API bases depending on environment:

- Local via NGINX proxy: `http://localhost:52180/api`
- Private Meshnet host: `http://<meshnet-hostname-or-ip>:52180/api`

Replace `<meshnet-hostname-or-ip>` with the NordVPN Meshnet hostname or Meshnet
IP of the machine running the stack.

## Endpoints

- `GET /gpt/openapi.json`: minimal OpenAPI schema for ChatGPT Actions
- `GET /gpt/ping`: auth-protected connectivity check
- `POST /gpt/search`: GPT Action search wrapper (`operationId: searchCollection`)

## Authentication

Set server env var:

```bash
GPT_ACTIONS_API_KEY=<your-api-key>
```

All GPT endpoints require this key. The API accepts any of these request auth forms:

- `Authorization: Bearer <key>` (recommended for ChatGPT Actions)
- HTTP Basic (`username=<key>` or `password=<key>`)
- `X-API-Key: <key>`

If the key is missing on the server, endpoints return `503`.
If a supplied key is invalid, endpoints return `401`.

## Private Client Setup

Use:

- OpenAPI schema URL: `http://<meshnet-hostname-or-ip>:52180/api/gpt/openapi.json`
- Authentication type: `API key`
- Auth type: `Bearer`
- API key value: same value as `GPT_ACTIONS_API_KEY`

Hosted ChatGPT Actions require a public HTTPS endpoint. A Meshnet-only/private
host works for your own devices, but not for ChatGPT's cloud-hosted Actions runtime.

## `searchCollection` Function

The GPT Action function is exposed as:

- `POST /gpt/search`
- OpenAPI `operationId`: `searchCollection`

### Request body

Required:

- `query` (`string`)

Optional:

- `bucket_name` (`string`): required only when multiple buckets exist
- `limit` (`int`, default `10`)
- `mode` (`semantic|keyword|hybrid`, default `hybrid`)
- `rrf_k` (`int`, default `60`)
- `use_staged_retrieval` (`bool`, default `true`)
- `query_variant_limit` (`int`, default `6`, clamped to `3..8`)
- `wide_limit_per_variant` (`int`, default `75`, clamped to `50..100`)
- `section_shortlist_limit` (`int`, default `20`, clamped to `10..30`)
- `max_section_text_chars` (`int`, default `2500`, clamped to `250..12000`)
- `minimal_response` (`bool`, default `true`)
- `minimal_result_text_chars` (`int`, default `500`, clamped to `25..2000`)

### Staged retrieval behavior

`/gpt/search` enables staged retrieval by default (`use_staged_retrieval=true`).

When enabled, retrieval runs as:

1. Query variant expansion (`query_variant_limit`, default `6`, clamped to `3..8`)
2. Wide chunk recall per variant (`wide_limit_per_variant`, default `75`, clamped to `50..100`)
3. Section grouping and shortlist (`section_shortlist_limit`, default `20`, clamped to `10..30`)
4. Section-level rerank with hard-filter boosts (country/year/programme signals from the query)
5. Final top `limit` results with section/chunk citation anchors

When disabled (`use_staged_retrieval=false`), the endpoint uses one-pass collection
search (semantic/keyword/hybrid) without variant expansion, section shortlist, or
section-level reranking.

### Response modes

When `minimal_response=true` (default), response is compact for token efficiency:

- Top-level fields: `bucket_name`, `query`, `mode`, `limit`, `rrf_k`, `results`
- Optional `citations` appears when available
- Each result is reduced to core citation/link/text fields

When `minimal_response=false`, response includes richer diagnostics:

- `query_variants`
- `hard_filters` (country/year/programme signals extracted from query)
- `stage_stats` (wide recall and shortlist stats)
- `citations` with section and chunk anchors
- Full section-oriented result payloads

### Bucket resolution behavior

If `bucket_name` is omitted:

- one bucket available: auto-selected
- zero buckets: `404`
- multiple buckets: `400` with available bucket preview

### Link fields in results

Each result can include:

- `source_material_url`: HTTP URL to retrieve the source PDF
- `resolver_link_url`: HTTP URL to open `resolver.html`
- `resolver_url`: normalized clickable URL
- `resolver_reference`: original internal `docs://...` reference (when present)

If `GPT_ACTIONS_LINK_BASE_URL` is set, relative links are expanded against that base.
Otherwise links are expanded from the incoming request base URL.

## `ping` Function

Use `GET /gpt/ping` to validate connectivity and auth.

Example:

```bash
curl -sS -H "Authorization: Bearer <your-api-key>" \
  "http://<meshnet-hostname-or-ip>:52180/api/gpt/ping?message=hello"
```

Response shape:

```json
{
  "status": "ok",
  "reply": "pong",
  "echo": "hello",
  "timestamp_utc": "2026-02-23T12:00:00Z"
}
```

## Example: `searchCollection`

```bash
curl -sS -X POST "http://<meshnet-hostname-or-ip>:52180/api/gpt/search" \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "research-raw",
    "query": "UK offsets programme 2020",
    "mode": "hybrid",
    "limit": 5
  }'
```

Example fallback to one-pass retrieval:

```bash
curl -sS -X POST "http://<meshnet-hostname-or-ip>:52180/api/gpt/search" \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "research-raw",
    "query": "UK offsets programme 2020",
    "mode": "hybrid",
    "limit": 5,
    "use_staged_retrieval": false
  }'
```

## Error map

- `400`: invalid input (for example invalid `mode`, missing `bucket_name` with multiple buckets)
- `401`: invalid or missing auth token
- `404`: no buckets available when `bucket_name` is omitted
- `503`: `GPT_ACTIONS_API_KEY` not configured server-side
