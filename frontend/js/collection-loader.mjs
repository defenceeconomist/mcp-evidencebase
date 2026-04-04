import { createCollectionCache, shouldRefreshCollectionEntry } from "./collection-cache.mjs";

export const isAbortError = (error) => {
  return Boolean(
    error &&
      (error.name === "AbortError" ||
        error.code === 20 ||
        /abort/i.test(String(error.message || "")))
  );
};

export const createCollectionLoader = ({
  apiRequest,
  buildDocumentsPath,
  normalizeDocument,
  applyDocuments,
  isBucketActive,
  onLoadError = () => {},
  cache = createCollectionCache(),
  now = () => Date.now(),
} = {}) => {
  let nextRequestId = 0;

  const loadCollectionDocuments = async ({
    bucketName = "",
    force = false,
    background = false,
    preserveSelection = true,
  } = {}) => {
    const normalizedBucketName = String(bucketName || "").trim();
    if (!normalizedBucketName) {
      return [];
    }

    const entry = cache.getEntry(normalizedBucketName);
    const hasCachedResponse = entry.loadedAt > 0;

    if (!force && entry.pendingPromise) {
      if (hasCachedResponse && !background && isBucketActive(normalizedBucketName)) {
        applyDocuments({
          bucketName: normalizedBucketName,
          documents: entry.documents,
          preserveSelection,
        });
      }
      return entry.pendingPromise;
    }

    if (hasCachedResponse && !background && isBucketActive(normalizedBucketName)) {
      applyDocuments({
        bucketName: normalizedBucketName,
        documents: entry.documents,
        preserveSelection,
      });
    }

    if (!force && hasCachedResponse && !shouldRefreshCollectionEntry(entry, { now: now(), ttlMs: cache.ttlMs })) {
      return entry.documents;
    }

    cache.abortPending(normalizedBucketName);

    const requestId = nextRequestId + 1;
    nextRequestId = requestId;
    const abortController = new AbortController();

    let pendingPromise = null;
    pendingPromise = (async () => {
      try {
        const payload = await apiRequest(buildDocumentsPath(normalizedBucketName), {
          method: "GET",
          signal: abortController.signal,
        });
        const rawDocuments = Array.isArray(payload?.documents) ? payload.documents : [];
        const normalizedDocuments = rawDocuments.map((rawDocument, index) =>
          normalizeDocument(rawDocument, index)
        );
        const latestEntry = cache.setDocuments(normalizedBucketName, normalizedDocuments, {
          loadedAt: now(),
          dirty: false,
        });
        if (
          latestEntry &&
          latestEntry.requestId === requestId &&
          isBucketActive(normalizedBucketName)
        ) {
          applyDocuments({
            bucketName: normalizedBucketName,
            documents: normalizedDocuments,
            preserveSelection,
          });
        }
        return normalizedDocuments;
      } catch (error) {
        if (isAbortError(error)) {
          return entry.documents;
        }
        onLoadError({
          bucketName: normalizedBucketName,
          error,
          background,
        });
        throw error;
      } finally {
        cache.clearPending(normalizedBucketName, pendingPromise);
      }
    })();

    cache.setPending(normalizedBucketName, pendingPromise, abortController, requestId);
    return pendingPromise;
  };

  return {
    cache,
    loadCollectionDocuments,
    markCollectionDirty(bucketName, dirty = true) {
      return cache.markDirty(bucketName, dirty);
    },
    abortAllPendingExcept(bucketName = "") {
      cache.abortAllPendingExcept(bucketName);
    },
  };
};
