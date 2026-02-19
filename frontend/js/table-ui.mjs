export const filterDocumentsByTitle = ({ documents, titleSearchQuery, normalizeText, filenameFromPath }) => {
  return documents.filter((documentRecord) => {
    if (!titleSearchQuery) {
      return true;
    }
    const documentTitle = normalizeText(
      documentRecord.title || filenameFromPath(documentRecord.file_path)
    ).toLowerCase();
    return documentTitle.includes(titleSearchQuery);
  });
};

export const ensureSelectedDocumentId = ({ candidateDocuments, selectedDocumentId }) => {
  if (!Array.isArray(candidateDocuments) || candidateDocuments.length === 0) {
    return null;
  }
  const selectedStillVisible = candidateDocuments.some(
    (documentRecord) => documentRecord.document_id === selectedDocumentId
  );
  if (!selectedStillVisible) {
    return candidateDocuments[0].document_id;
  }
  return selectedDocumentId;
};
