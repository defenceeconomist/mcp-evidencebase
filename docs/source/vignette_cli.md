# Command-Line Vignette: End-To-End Workflow

This vignette shows an end-to-end run using only command-line tools against the
compose stack.

## 1. Start the stack

```bash
docker compose up -d --build
```

Check API readiness:

```bash
curl -sS http://localhost:52180/api/healthz
# {"status":"ok"}
```

## 2. Set workflow variables

Use your own PDF path for `PDF_PATH`.

```bash
export BASE_URL="http://localhost:52180/api"
export BUCKET="research-raw"
export PDF_PATH="./paper.pdf"
```

Compute deterministic `document_id` (SHA-256 of bytes):

```bash
export DOC_ID="$( (shasum -a 256 "$PDF_PATH" 2>/dev/null || sha256sum "$PDF_PATH") | awk '{print $1}' )"
```

## 3. Create a bucket (and paired Qdrant collection)

```bash
curl -sS -X POST "$BASE_URL/buckets" \
  -H "Content-Type: application/json" \
  -d "{\"bucket_name\":\"$BUCKET\"}"
```

Expected shape:

```json
{"bucket_name":"research-raw","created":true,"qdrant_collection_created":true}
```

## 4. Upload a document and queue processing

```bash
curl -sS -X POST \
  "$BASE_URL/collections/$BUCKET/documents/upload?file_name=$(basename "$PDF_PATH")" \
  -H "Content-Type: application/pdf" \
  --data-binary "@$PDF_PATH"
```

This stores the object in MinIO and queues a Celery task for:
- Unstructured partition extraction (raw partition JSON persisted in Redis)
- metadata extraction (PDF metadata + first-page DOI/ISBN/ISSN heuristics)
- chunking with overlap (`CHUNK_SIZE_CHARS`, `CHUNK_OVERLAP_CHARS`) using internal chunk logic
- Qdrant upsert (chunk text + `page_numbers` + `bounding_boxes`)

## 5. Monitor processing from the API

Inspect all documents:

```bash
curl -sS "$BASE_URL/collections/$BUCKET/documents"
```

If `jq` is installed, filter one record:

```bash
curl -sS "$BASE_URL/collections/$BUCKET/documents" \
  | jq --arg doc "$DOC_ID" '.documents[] | select(.document_id == $doc) |
    {document_id, processing_state, processing_progress, partitions_count, chunks_count}'
```

Wait until `processing_state` becomes `processed` (or inspect `error` if `failed`).

## 6. Update metadata

```bash
curl -sS -X PUT "$BASE_URL/collections/$BUCKET/documents/$DOC_ID/metadata" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "title": "Example Paper",
      "author": "A. Author and B. Author",
      "year": "2026",
      "document_type": "article",
      "doi": "10.1000/example-doi"
    }
  }'
```

Fetch metadata from Crossref (DOI -> ISBN -> ISSN -> title, high-confidence only):

```bash
curl -sS -X POST "$BASE_URL/collections/$BUCKET/documents/$DOC_ID/metadata/fetch"
```

## 7. Trigger an on-demand scan (optional)

Use this when files are dropped into MinIO outside the upload API.

```bash
curl -sS -X POST "$BASE_URL/collections/$BUCKET/scan"
```

## 8. Run hybrid search

```bash
curl -sS "$BASE_URL/collections/$BUCKET/search?query=causal%20inference&mode=hybrid&limit=5&rrf_k=80"
```

You can also run the same flow from the package CLI:

```bash
python -m mcp_evidencebase \
  --search-bucket "$BUCKET" \
  --search-query "causal inference" \
  --search-mode hybrid \
  --search-limit 5 \
  --search-rrf-k 80
```

## 9. Delete document and bucket

Delete the document by `document_id`:

```bash
curl -sS -X DELETE "$BASE_URL/collections/$BUCKET/documents/$DOC_ID"
```

Delete the bucket:

```bash
curl -sS -X DELETE "$BASE_URL/buckets/$BUCKET"
```

## 10. Celery task reference

Tasks are defined in `src/mcp_evidencebase/tasks.py`:

- `mcp_evidencebase.ping`: simple worker health task.
- `mcp_evidencebase.scan_minio_objects(bucket_name=None)`: scans MinIO and enqueues
  `partition_minio_object` for objects that are new or have changed ETag.
- `mcp_evidencebase.partition_minio_object(bucket_name, object_name, etag=None)`: performs
  partition + metadata extraction and then enqueues `chunk_minio_object`.
- `mcp_evidencebase.chunk_minio_object(partition_payload)`: performs chunking + Qdrant upsert.
- `mcp_evidencebase.process_minio_object(bucket_name, object_name, etag=None)`: backward-compatible
  wrapper that runs both stages inline.

Current workflow shape:

- The task pipeline is linear but stage-specific: partition first, then chunk/index.
- `IngestionService` exposes `partition_object()` and `chunk_object()` independently, so chunking
  strategy can evolve without coupling to Unstructured API partition calls.
- Use stage-specific reruns when needed (for example re-chunk/re-index without re-partition).

## 11. Stop the stack

```bash
docker compose down
```
