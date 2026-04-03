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

export const getFolderKeyFromFilePath = (filePath, normalizeText) => {
  const normalizedPath = normalizeText(filePath).replace(/^\/+|\/+$/g, "");
  if (!normalizedPath) {
    return "";
  }
  const segments = normalizedPath.split("/").filter(Boolean);
  if (segments.length < 2) {
    return "";
  }
  return segments[0];
};

const compareMetadataValue = (value) => {
  if (Array.isArray(value) || (value && typeof value === "object")) {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return String(value);
    }
  }
  return String(value || "").trim();
};

const deriveUniformValue = (values) => {
  const normalizedEntries = values
    .map((value) => ({
      rawValue: value,
      comparableValue: compareMetadataValue(value),
    }))
    .filter((entry) => entry.comparableValue);

  if (normalizedEntries.length <= 0) {
    return { value: "", mixed: false };
  }

  const [firstEntry] = normalizedEntries;
  const hasConflict = normalizedEntries.some(
    (entry) => entry.comparableValue !== firstEntry.comparableValue
  );
  if (hasConflict) {
    return { value: "", mixed: true };
  }
  return { value: firstEntry.rawValue, mixed: false };
};

export const deriveFolderMetadata = ({
  childDocuments,
  folderKey,
  folderName,
  bibtexFields,
  getRecordBibtexFieldValue,
}) => {
  const baseBibtexFields = {};
  bibtexFields.forEach((fieldName) => {
    baseBibtexFields[fieldName] = "";
  });

  const derivedAuthors = deriveUniformValue(childDocuments.map((documentRecord) => documentRecord.authors || []));
  const derivedAuthorText = deriveUniformValue(
    childDocuments.map((documentRecord) => getRecordBibtexFieldValue(documentRecord, "author"))
  );

  const mixedFields = {};
  const record = {
    item_id: `folder:${folderKey}`,
    kind: "folder",
    folder_key: folderKey,
    folder_name: folderName,
    child_document_ids: childDocuments.map((documentRecord) => documentRecord.document_id),
    child_documents: childDocuments,
    file_path: folderKey,
    document_id: "",
    document_type: "book",
    citation_key: "",
    bulk_selected: false,
    selection_state: "none",
    depth: 0,
    expanded: false,
    authors: Array.isArray(derivedAuthors.value) ? derivedAuthors.value : [],
    author: typeof derivedAuthorText.value === "string" ? derivedAuthorText.value : "",
    bibtex_fields: { ...baseBibtexFields },
    mixed_fields: mixedFields,
  };

  if (derivedAuthors.mixed) {
    mixedFields.author = true;
    mixedFields.authors = true;
  }

  bibtexFields.forEach((fieldName) => {
    let sourceFieldName = fieldName;
    if (fieldName === "title") {
      sourceFieldName = "booktitle";
    }

    const derivedField = deriveUniformValue(
      childDocuments.map((documentRecord) => getRecordBibtexFieldValue(documentRecord, sourceFieldName))
    );
    if (derivedField.mixed) {
      mixedFields[fieldName] = true;
    }
    record.bibtex_fields[fieldName] = typeof derivedField.value === "string" ? derivedField.value : "";
    record[fieldName] = record.bibtex_fields[fieldName];
  });

  record.title = record.bibtex_fields.title || folderName;
  record.publication = record.bibtex_fields.publisher || record.bibtex_fields.title || folderName;
  record.year = record.bibtex_fields.year || "";
  return record;
};

export const computeFolderSelectionState = (childDocuments) => {
  const selectedCount = childDocuments.filter((documentRecord) => Boolean(documentRecord?.bulk_selected)).length;
  if (selectedCount <= 0) {
    return { bulkSelected: false, selectionState: "none" };
  }
  if (selectedCount === childDocuments.length) {
    return { bulkSelected: true, selectionState: "all" };
  }
  return { bulkSelected: false, selectionState: "partial" };
};

export const buildGroupedDocumentItems = ({
  documents,
  titleSearchQuery,
  normalizeText,
  filenameFromPath,
  bibtexFields,
  getRecordBibtexFieldValue,
  expandedFolderKeys,
}) => {
  const normalizedQuery = normalizeText(titleSearchQuery).toLowerCase();
  const folderGroups = new Map();
  documents.forEach((documentRecord) => {
    const folderKey = getFolderKeyFromFilePath(documentRecord.file_path, normalizeText);
    documentRecord.kind = "document";
    documentRecord.item_id = `document:${documentRecord.document_id}`;
    documentRecord.depth = 0;
    documentRecord.parent_folder_key = "";
    if (!folderKey) {
      return;
    }
    const group = folderGroups.get(folderKey) || [];
    group.push(documentRecord);
    folderGroups.set(folderKey, group);
  });

  const groupedFolderKeys = new Set(
    [...folderGroups.entries()]
      .filter(([, childDocuments]) => childDocuments.length >= 2)
      .map(([folderKey]) => folderKey)
  );

  const visibleItems = [];
  const folderItems = [];
  const visibleDocuments = [];
  const emittedFolders = new Set();

  const documentMatchesTitleQuery = (documentRecord) => {
    if (!normalizedQuery) {
      return true;
    }
    const documentTitle = normalizeText(
      documentRecord.title || filenameFromPath(documentRecord.file_path)
    ).toLowerCase();
    return documentTitle.includes(normalizedQuery);
  };

  const folderMatchesTitleQuery = (folderItem) => {
    if (!normalizedQuery) {
      return true;
    }
    const label = normalizeText(folderItem.title || folderItem.folder_name).toLowerCase();
    return label.includes(normalizedQuery);
  };

  documents.forEach((documentRecord) => {
    const folderKey = getFolderKeyFromFilePath(documentRecord.file_path, normalizeText);
    if (!groupedFolderKeys.has(folderKey)) {
      if (!documentMatchesTitleQuery(documentRecord)) {
        return;
      }
      visibleItems.push(documentRecord);
      visibleDocuments.push(documentRecord);
      return;
    }

    if (emittedFolders.has(folderKey)) {
      return;
    }
    emittedFolders.add(folderKey);

    const childDocuments = folderGroups.get(folderKey) || [];
    const folderItem = deriveFolderMetadata({
      childDocuments,
      folderKey,
      folderName: folderKey,
      bibtexFields,
      getRecordBibtexFieldValue,
    });
    const folderSelection = computeFolderSelectionState(childDocuments);
    folderItem.bulk_selected = folderSelection.bulkSelected;
    folderItem.selection_state = folderSelection.selectionState;
    folderItem.expanded = normalizedQuery ? true : expandedFolderKeys.has(folderKey);
    folderItems.push(folderItem);

    const matchingChildren = childDocuments.filter((childDocument) => documentMatchesTitleQuery(childDocument));
    const showFolder = folderMatchesTitleQuery(folderItem) || matchingChildren.length > 0;
    if (!showFolder) {
      return;
    }

    visibleItems.push(folderItem);
    const childItemsToRender = normalizedQuery ? matchingChildren : folderItem.expanded ? childDocuments : [];
    childItemsToRender.forEach((childDocument) => {
      childDocument.depth = 1;
      childDocument.parent_folder_key = folderKey;
      visibleItems.push(childDocument);
      visibleDocuments.push(childDocument);
    });
  });

  return { visibleItems, visibleDocuments, folderItems };
};

export const buildFolderMetadataPatch = ({ folderRecord, normalizeText }) => {
  return {
    authors:
      Array.isArray(folderRecord?.authors) && folderRecord.authors.length > 0 ? folderRecord.authors : [],
    author: normalizeText(folderRecord?.author),
    booktitle: normalizeText(folderRecord?.title),
    editor: normalizeText(folderRecord?.editor),
    year: normalizeText(folderRecord?.year),
    publisher: normalizeText(folderRecord?.publisher),
    address: normalizeText(folderRecord?.address),
    edition: normalizeText(folderRecord?.edition),
    series: normalizeText(folderRecord?.series),
    volume: normalizeText(folderRecord?.volume),
    number: normalizeText(folderRecord?.number),
    month: normalizeText(folderRecord?.month),
    note: normalizeText(folderRecord?.note),
    doi: normalizeText(folderRecord?.doi),
    isbn: normalizeText(folderRecord?.isbn),
  };
};

export const ensureSelectedItemId = ({ candidateItems, selectedItemId }) => {
  if (!Array.isArray(candidateItems) || candidateItems.length === 0) {
    return null;
  }
  const selectedStillVisible = candidateItems.some((candidateItem) => candidateItem.item_id === selectedItemId);
  if (!selectedStillVisible) {
    return candidateItems[0].item_id;
  }
  return selectedItemId;
};

export const ensureSelectedDocumentId = ({ candidateDocuments, selectedDocumentId }) => {
  return ensureSelectedItemId({
    candidateItems: Array.isArray(candidateDocuments)
      ? candidateDocuments.map((documentRecord) => ({
          item_id: documentRecord.document_id,
        }))
      : [],
    selectedItemId: selectedDocumentId,
  });
};
