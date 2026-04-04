import test from "node:test";
import assert from "node:assert/strict";

import { createCollectionCache } from "../../js/collection-cache.mjs";
import { createDocumentDebugLoader } from "../../js/document-debug-loader.mjs";

test("document debug loader fetches payload once and caches it", async () => {
  const cache = createCollectionCache();
  let requestCount = 0;
  const loader = createDocumentDebugLoader({
    apiRequest: async () => {
      requestCount += 1;
      return {
        document_id: "doc-1",
        partitions_tree: { partitions: [{ text: "Section A" }] },
        chunks_tree: { chunks: [{ chunk_id: "chunk-1" }] },
      };
    },
    cache,
  });

  const firstPayload = await loader.loadDocumentDebugPayload({
    bucketName: "alpha",
    documentId: "doc-1",
  });
  const secondPayload = await loader.loadDocumentDebugPayload({
    bucketName: "alpha",
    documentId: "doc-1",
  });

  assert.equal(requestCount, 1);
  assert.deepEqual(secondPayload, firstPayload);
});

test("document debug loader stores fallback payload without fetching", async () => {
  const cache = createCollectionCache();
  let requestCount = 0;
  const loader = createDocumentDebugLoader({
    apiRequest: async () => {
      requestCount += 1;
      return {};
    },
    cache,
  });

  const payload = await loader.loadDocumentDebugPayload({
    bucketName: "alpha",
    documentId: "doc-2",
    fallbackPayload: {
      partitions_tree: { partitions: [{ text: "Fallback" }] },
      chunks_tree: { chunks: [{ chunk_id: "fallback-chunk" }] },
    },
  });

  assert.equal(requestCount, 0);
  assert.deepEqual(payload, {
    document_id: "doc-2",
    partitions_tree: { partitions: [{ text: "Fallback" }] },
    chunks_tree: { chunks: [{ chunk_id: "fallback-chunk" }] },
  });
});
