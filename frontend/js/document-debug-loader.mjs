const normalizeDebugPayload = (payload, documentId) => ({
  document_id: String(payload?.document_id || documentId || "").trim(),
  partitions_tree:
    payload?.partitions_tree && typeof payload.partitions_tree === "object"
      ? payload.partitions_tree
      : { partitions: [] },
  chunks_tree:
    payload?.chunks_tree && typeof payload.chunks_tree === "object"
      ? payload.chunks_tree
      : { chunks: [] },
});

export const createDocumentDebugLoader = ({
  apiRequest,
  cache,
} = {}) => {
  return {
    async loadDocumentDebugPayload({
      bucketName = "",
      documentId = "",
      fallbackPayload = null,
    } = {}) {
      const normalizedBucketName = String(bucketName || "").trim();
      const normalizedDocumentId = String(documentId || "").trim();
      if (!normalizedBucketName || !normalizedDocumentId) {
        return normalizeDebugPayload({}, normalizedDocumentId);
      }

      const cachedPayload = cache.getDebugPayload(normalizedBucketName, normalizedDocumentId);
      if (cachedPayload) {
        return cachedPayload;
      }

      if (fallbackPayload && typeof fallbackPayload === "object") {
        const normalizedFallback = normalizeDebugPayload(fallbackPayload, normalizedDocumentId);
        cache.setDebugPayload(normalizedBucketName, normalizedDocumentId, normalizedFallback);
        return normalizedFallback;
      }

      const payload = await apiRequest(
        `/collections/${encodeURIComponent(normalizedBucketName)}/documents/${encodeURIComponent(
          normalizedDocumentId
        )}/debug`,
        { method: "GET" }
      );
      const normalizedPayload = normalizeDebugPayload(payload, normalizedDocumentId);
      cache.setDebugPayload(normalizedBucketName, normalizedDocumentId, normalizedPayload);
      return normalizedPayload;
    },
  };
};
