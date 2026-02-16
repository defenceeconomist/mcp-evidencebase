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
- Unstructured partition extraction
- metadata extraction
- chunking and Qdrant upsert

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

## 7. Trigger an on-demand scan (optional)

Use this when files are dropped into MinIO outside the upload API.

```bash
curl -sS -X POST "$BASE_URL/collections/$BUCKET/scan"
```

## 8. Delete document and bucket

Delete the document by `document_id`:

```bash
curl -sS -X DELETE "$BASE_URL/collections/$BUCKET/documents/$DOC_ID"
```

Delete the bucket:

```bash
curl -sS -X DELETE "$BASE_URL/buckets/$BUCKET"
```

## 9. Stop the stack

```bash
docker compose down
```
