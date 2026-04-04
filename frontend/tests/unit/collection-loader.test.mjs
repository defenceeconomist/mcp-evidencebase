import test from "node:test";
import assert from "node:assert/strict";

import { createCollectionCache } from "../../js/collection-cache.mjs";
import { createCollectionLoader } from "../../js/collection-loader.mjs";

test("collection loader returns cached documents immediately before stale refresh resolves", async () => {
  const applied = [];
  let resolveRequest;
  let activeBucketName = "alpha";
  const cache = createCollectionCache({ ttlMs: 30_000, now: () => 0 });
  cache.setDocuments("alpha", [{ document_id: "cached-doc", processing_state: "processed" }], {
    loadedAt: 0,
    dirty: true,
  });
  cache.getEntry("alpha").loadedAt = 1;

  const loader = createCollectionLoader({
    apiRequest: async () =>
      await new Promise((resolve) => {
        resolveRequest = resolve;
      }),
    buildDocumentsPath: (bucketName) => `/collections/${bucketName}/documents`,
    normalizeDocument: (rawDocument) => rawDocument,
    applyDocuments(payload) {
      applied.push(payload);
    },
    isBucketActive(bucketName) {
      return bucketName === activeBucketName;
    },
    cache,
    now: () => 40_000,
  });

  const pendingPromise = loader.loadCollectionDocuments({ bucketName: "alpha" });

  assert.equal(applied.length, 1);
  assert.deepEqual(applied[0].documents, [{ document_id: "cached-doc", processing_state: "processed" }]);

  resolveRequest({ documents: [{ document_id: "fresh-doc", processing_state: "processed" }] });
  const documents = await pendingPromise;

  assert.deepEqual(documents, [{ document_id: "fresh-doc", processing_state: "processed" }]);
  assert.equal(applied.length, 2);
  assert.deepEqual(applied[1].documents, [{ document_id: "fresh-doc", processing_state: "processed" }]);
});

test("collection loader dedupes concurrent requests for the same collection", async () => {
  let requestCount = 0;
  const loader = createCollectionLoader({
    apiRequest: async () => {
      requestCount += 1;
      return { documents: [{ document_id: "doc-1", processing_state: "processed" }] };
    },
    buildDocumentsPath: (bucketName) => `/collections/${bucketName}/documents`,
    normalizeDocument: (rawDocument) => rawDocument,
    applyDocuments() {},
    isBucketActive() {
      return true;
    },
  });

  const firstPromise = loader.loadCollectionDocuments({ bucketName: "alpha", force: true });
  const secondPromise = loader.loadCollectionDocuments({ bucketName: "alpha" });

  const [firstDocuments, secondDocuments] = await Promise.all([firstPromise, secondPromise]);
  assert.equal(requestCount, 1);
  assert.deepEqual(firstDocuments, [{ document_id: "doc-1", processing_state: "processed" }]);
  assert.deepEqual(secondDocuments, firstDocuments);
});

test("collection loader ignores stale responses after switching collections", async () => {
  const applied = [];
  let resolveAlpha;
  let activeBucketName = "alpha";
  const loader = createCollectionLoader({
    apiRequest: async (path) =>
      await new Promise((resolve) => {
        if (path.includes("/alpha/")) {
          resolveAlpha = resolve;
          return;
        }
        resolve({ documents: [{ document_id: "doc-b", processing_state: "processed" }] });
      }),
    buildDocumentsPath: (bucketName) => `/collections/${bucketName}/documents`,
    normalizeDocument: (rawDocument) => rawDocument,
    applyDocuments(payload) {
      applied.push(payload.bucketName);
    },
    isBucketActive(bucketName) {
      return bucketName === activeBucketName;
    },
  });

  const alphaPromise = loader.loadCollectionDocuments({ bucketName: "alpha", force: true });
  activeBucketName = "beta";
  await loader.loadCollectionDocuments({ bucketName: "beta", force: true });
  resolveAlpha({ documents: [{ document_id: "doc-a", processing_state: "processed" }] });
  await alphaPromise;

  assert.deepEqual(applied, ["beta"]);
});

test("marking a collection dirty forces the next load to refetch", async () => {
  let requestCount = 0;
  const cache = createCollectionCache({ ttlMs: 30_000, now: () => 1 });
  cache.setDocuments("alpha", [{ document_id: "doc-1", processing_state: "processed" }], {
    loadedAt: 1,
    dirty: false,
  });
  const loader = createCollectionLoader({
    apiRequest: async () => {
      requestCount += 1;
      return { documents: [{ document_id: `doc-${requestCount}`, processing_state: "processed" }] };
    },
    buildDocumentsPath: (bucketName) => `/collections/${bucketName}/documents`,
    normalizeDocument: (rawDocument) => rawDocument,
    applyDocuments() {},
    isBucketActive() {
      return true;
    },
    cache,
    now: () => 2,
  });

  const firstDocuments = await loader.loadCollectionDocuments({ bucketName: "alpha" });
  assert.deepEqual(firstDocuments, [{ document_id: "doc-1", processing_state: "processed" }]);
  assert.equal(requestCount, 0);

  loader.markCollectionDirty("alpha");
  const secondDocuments = await loader.loadCollectionDocuments({ bucketName: "alpha" });

  assert.equal(requestCount, 1);
  assert.deepEqual(secondDocuments, [{ document_id: "doc-1", processing_state: "processed" }]);
});
