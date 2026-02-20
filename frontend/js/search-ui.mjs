export const formatSearchPages = (pageStart, pageEnd) => {
  const start = Number.parseInt(pageStart, 10);
  const end = Number.parseInt(pageEnd, 10);
  const hasStart = Number.isFinite(start) && start > 0;
  const hasEnd = Number.isFinite(end) && end > 0;
  if (!hasStart && !hasEnd) {
    return "n/a";
  }
  if (hasStart && hasEnd) {
    return start === end ? String(start) : `${start}-${end}`;
  }
  if (hasStart) {
    return String(start);
  }
  return String(end);
};

export const formatSearchPageLabel = (pageStart, pageEnd) => {
  const start = Number.parseInt(pageStart, 10);
  const end = Number.parseInt(pageEnd, 10);
  const hasStart = Number.isFinite(start) && start > 0;
  const hasEnd = Number.isFinite(end) && end > 0;
  if (hasStart && hasEnd) {
    return start === end ? "Page" : "Pages";
  }
  if (hasStart || hasEnd) {
    return "Page";
  }
  return "Pages";
};

export const formatSearchTitle = (result, filenameFromPath, normalizeText) => {
  return (
    normalizeText(result.title) ||
    normalizeText(filenameFromPath(result.file_path || "")) ||
    normalizeText(result.document_id) ||
    "Untitled chunk"
  );
};

export const formatSearchLocation = (result, normalizeText) => {
  return normalizeText(result.minio_location) || normalizeText(result.file_path) || "n/a";
};

export const formatSearchAuthorYear = (result, normalizeText) => {
  const author = normalizeText(result.author);
  const year = normalizeText(result.year);
  if (author && year) {
    return `${author} (${year})`;
  }
  return author || year || "n/a";
};
