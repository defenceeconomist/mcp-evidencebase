export const DEFAULT_COLLECTION_CACHE_TTL_MS = 30_000;

const createEmptyEntry = () => ({
  documents: [],
  loadedAt: 0,
  dirty: false,
  pendingPromise: null,
  abortController: null,
  debugByDocumentId: new Map(),
  requestId: 0,
});

export const hasProcessingDocuments = (documents) => {
  if (!Array.isArray(documents)) {
    return false;
  }
  return documents.some(
    (documentRecord) => String(documentRecord?.processing_state || "").trim().toLowerCase() === "processing"
  );
};

export const shouldRefreshCollectionEntry = (
  entry,
  { now = Date.now(), ttlMs = DEFAULT_COLLECTION_CACHE_TTL_MS } = {}
) => {
  if (!entry || !Number.isFinite(entry.loadedAt) || entry.loadedAt <= 0) {
    return true;
  }
  if (entry.dirty) {
    return true;
  }
  if (hasProcessingDocuments(entry.documents)) {
    return true;
  }
  return now - entry.loadedAt >= ttlMs;
};

export const createCollectionCache = ({
  ttlMs = DEFAULT_COLLECTION_CACHE_TTL_MS,
  now = () => Date.now(),
} = {}) => {
  const entries = new Map();

  const ensureEntry = (bucketName) => {
    const normalizedBucketName = String(bucketName || "").trim();
    if (!normalizedBucketName) {
      return null;
    }
    if (!entries.has(normalizedBucketName)) {
      entries.set(normalizedBucketName, createEmptyEntry());
    }
    return entries.get(normalizedBucketName) || null;
  };

  return {
    ttlMs,
    getEntry(bucketName) {
      return ensureEntry(bucketName);
    },
    setDocuments(bucketName, documents, { loadedAt = now(), dirty = false } = {}) {
      const entry = ensureEntry(bucketName);
      if (!entry) {
        return null;
      }
      entry.documents = Array.isArray(documents) ? documents : [];
      entry.loadedAt = loadedAt;
      entry.dirty = dirty;
      return entry;
    },
    markDirty(bucketName, dirty = true) {
      const entry = ensureEntry(bucketName);
      if (!entry) {
        return null;
      }
      entry.dirty = Boolean(dirty);
      return entry;
    },
    setPending(bucketName, pendingPromise, abortController, requestId) {
      const entry = ensureEntry(bucketName);
      if (!entry) {
        return null;
      }
      entry.pendingPromise = pendingPromise || null;
      entry.abortController = abortController || null;
      entry.requestId = Number.isFinite(requestId) ? requestId : entry.requestId;
      return entry;
    },
    clearPending(bucketName, pendingPromise = null) {
      const entry = ensureEntry(bucketName);
      if (!entry) {
        return null;
      }
      if (pendingPromise && entry.pendingPromise !== pendingPromise) {
        return entry;
      }
      entry.pendingPromise = null;
      entry.abortController = null;
      return entry;
    },
    abortPending(bucketName) {
      const entry = ensureEntry(bucketName);
      if (!entry || !entry.abortController) {
        return false;
      }
      entry.abortController.abort();
      entry.pendingPromise = null;
      entry.abortController = null;
      return true;
    },
    abortAllPendingExcept(activeBucketName = "") {
      const normalizedActiveBucketName = String(activeBucketName || "").trim();
      for (const [bucketName, entry] of entries.entries()) {
        if (bucketName === normalizedActiveBucketName || !entry.abortController) {
          continue;
        }
        entry.abortController.abort();
        entry.pendingPromise = null;
        entry.abortController = null;
      }
    },
    getDebugPayload(bucketName, documentId) {
      const entry = ensureEntry(bucketName);
      if (!entry) {
        return null;
      }
      return entry.debugByDocumentId.get(String(documentId || "").trim()) || null;
    },
    setDebugPayload(bucketName, documentId, payload) {
      const entry = ensureEntry(bucketName);
      const normalizedDocumentId = String(documentId || "").trim();
      if (!entry || !normalizedDocumentId) {
        return null;
      }
      entry.debugByDocumentId.set(normalizedDocumentId, payload);
      return payload;
    },
  };
};
