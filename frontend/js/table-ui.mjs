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

const DEFAULT_CHILD_SORT = Object.freeze({
  key: "pages",
  direction: "asc",
});

const DEFAULT_TABLE_SORT = Object.freeze({
  key: "object_type",
  direction: "asc",
});

const ROMAN_NUMERAL_VALUES = Object.freeze({
  i: 1,
  v: 5,
  x: 10,
  l: 50,
  c: 100,
  d: 500,
  m: 1000,
});

const normalizeSortKey = (value) => String(value || "").trim();

const normalizeSortDirection = (value) => (String(value || "").toLowerCase() === "desc" ? "desc" : "asc");

const normalizeSortConfig = (sortConfig) => {
  const key = normalizeSortKey(sortConfig?.key);
  if (!key) {
    return null;
  }
  return {
    key,
    direction: normalizeSortDirection(sortConfig?.direction),
  };
};

const isDefaultTableSort = (sortConfig) => {
  const normalizedSortConfig = normalizeSortConfig(sortConfig);
  return Boolean(
    normalizedSortConfig &&
      normalizedSortConfig.key === DEFAULT_TABLE_SORT.key &&
      normalizedSortConfig.direction === DEFAULT_TABLE_SORT.direction
  );
};

const parseRomanNumeral = (value) => {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized || /[^ivxlcdm]/.test(normalized)) {
    return Number.NaN;
  }
  let total = 0;
  let previous = 0;
  for (let index = normalized.length - 1; index >= 0; index -= 1) {
    const numeralValue = ROMAN_NUMERAL_VALUES[normalized[index]];
    if (!numeralValue) {
      return Number.NaN;
    }
    if (numeralValue < previous) {
      total -= numeralValue;
    } else {
      total += numeralValue;
      previous = numeralValue;
    }
  }
  return total;
};

const parseLeadingPageNumber = (value) => {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return Number.POSITIVE_INFINITY;
  }

  const numericMatch = normalized.match(/-?\d+/);
  if (numericMatch) {
    return Number.parseInt(numericMatch[0], 10);
  }

  const romanMatch = normalized.match(/[ivxlcdm]+/i);
  if (romanMatch) {
    const parsedRoman = parseRomanNumeral(romanMatch[0]);
    if (Number.isFinite(parsedRoman)) {
      return parsedRoman;
    }
  }

  return Number.POSITIVE_INFINITY;
};

const buildSortAccessor = ({
  record,
  sortKey,
  filenameFromPath,
  getRecordBibtexFieldValue,
}) => {
  const normalizedSortKey = normalizeSortKey(sortKey);
  if (!normalizedSortKey) {
    return {
      rank: 1,
      numericValue: Number.POSITIVE_INFINITY,
      textValue: "",
    };
  }

  if (normalizedSortKey === "pages") {
    const rawPages = getRecordBibtexFieldValue(record, "pages");
    const numericValue = parseLeadingPageNumber(rawPages);
    return {
      rank: Number.isFinite(numericValue) ? 0 : 1,
      numericValue,
      textValue: rawPages.toLowerCase(),
    };
  }

  if (normalizedSortKey === "object_type") {
    const objectType =
      record?.kind === "folder"
        ? "folder"
        : String(filenameFromPath(record?.file_path || "").split(".").pop() || "file").trim().toLowerCase();
    return {
      rank: objectType ? 0 : 1,
      numericValue: Number.NaN,
      textValue: objectType,
    };
  }

  const rawValue =
    normalizedSortKey === "title"
      ? record?.kind === "folder"
        ? record.title || record.folder_name
        : record?.title || filenameFromPath(record?.file_path)
      : normalizedSortKey === "author"
        ? getRecordBibtexFieldValue(record, "author") || record?.author || ""
        : normalizedSortKey === "publication"
          ? record?.publication || ""
          : normalizedSortKey === "file_path"
            ? record?.file_path || ""
            : normalizedSortKey === "year"
              ? record?.year || getRecordBibtexFieldValue(record, "year")
              : normalizedSortKey in (record || {})
                ? record?.[normalizedSortKey]
                : getRecordBibtexFieldValue(record, normalizedSortKey);

  const textValue = String(rawValue || "").trim();
  const numericValue =
    normalizedSortKey === "year" && textValue
      ? Number.parseInt(textValue.replace(/[^\d-]+/g, ""), 10)
      : Number.NaN;

  return {
    rank: textValue ? 0 : 1,
    numericValue,
    textValue: textValue.toLowerCase(),
  };
};

const compareSortAccessors = (leftAccessor, rightAccessor, direction) => {
  if (leftAccessor.rank !== rightAccessor.rank) {
    return leftAccessor.rank - rightAccessor.rank;
  }

  if (Number.isFinite(leftAccessor.numericValue) && Number.isFinite(rightAccessor.numericValue)) {
    if (leftAccessor.numericValue !== rightAccessor.numericValue) {
      return direction === "desc"
        ? rightAccessor.numericValue - leftAccessor.numericValue
        : leftAccessor.numericValue - rightAccessor.numericValue;
    }
  }

  if (leftAccessor.textValue !== rightAccessor.textValue) {
    return direction === "desc"
      ? rightAccessor.textValue.localeCompare(leftAccessor.textValue, undefined, {
          numeric: true,
          sensitivity: "base",
        })
      : leftAccessor.textValue.localeCompare(rightAccessor.textValue, undefined, {
          numeric: true,
          sensitivity: "base",
        });
  }

  return 0;
};

const compareRecordsBySortConfig = ({
  leftRecord,
  rightRecord,
  sortConfig,
  filenameFromPath,
  getRecordBibtexFieldValue,
}) => {
  const normalizedSortConfig = normalizeSortConfig(sortConfig);
  if (!normalizedSortConfig) {
    return 0;
  }

  const primaryComparison = compareSortAccessors(
    buildSortAccessor({
      record: leftRecord,
      sortKey: normalizedSortConfig.key,
      filenameFromPath,
      getRecordBibtexFieldValue,
    }),
    buildSortAccessor({
      record: rightRecord,
      sortKey: normalizedSortConfig.key,
      filenameFromPath,
      getRecordBibtexFieldValue,
    }),
    normalizedSortConfig.direction
  );
  if (primaryComparison !== 0) {
    return primaryComparison;
  }

  const tieBreakerKeys = ["title", "file_path"];
  for (const tieBreakerKey of tieBreakerKeys) {
    if (tieBreakerKey === normalizedSortConfig.key) {
      continue;
    }
    const tieBreakerComparison = compareSortAccessors(
      buildSortAccessor({
        record: leftRecord,
        sortKey: tieBreakerKey,
        filenameFromPath,
        getRecordBibtexFieldValue,
      }),
      buildSortAccessor({
        record: rightRecord,
        sortKey: tieBreakerKey,
        filenameFromPath,
        getRecordBibtexFieldValue,
      }),
      "asc"
    );
    if (tieBreakerComparison !== 0) {
      return tieBreakerComparison;
    }
  }

  return String(leftRecord?.document_id || leftRecord?.item_id || "").localeCompare(
    String(rightRecord?.document_id || rightRecord?.item_id || ""),
    undefined,
    {
      numeric: true,
      sensitivity: "base",
    }
  );
};

const sortRecords = ({
  records,
  sortConfig,
  filenameFromPath,
  getRecordBibtexFieldValue,
}) => {
  const normalizedSortConfig = normalizeSortConfig(sortConfig);
  if (!normalizedSortConfig) {
    return [...records];
  }
  return [...records]
    .map((record, index) => ({ record, index }))
    .sort((leftEntry, rightEntry) => {
      const comparison = compareRecordsBySortConfig({
        leftRecord: leftEntry.record,
        rightRecord: rightEntry.record,
        sortConfig: normalizedSortConfig,
        filenameFromPath,
        getRecordBibtexFieldValue,
      });
      return comparison || leftEntry.index - rightEntry.index;
    })
    .map((entry) => entry.record);
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
  sortConfig = DEFAULT_TABLE_SORT,
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

  const sortedFolderChildren = new Map(
    [...folderGroups.entries()].map(([folderKey, childDocuments]) => [
      folderKey,
      sortRecords({
        records: childDocuments,
        sortConfig: isDefaultTableSort(sortConfig)
          ? DEFAULT_CHILD_SORT
          : normalizeSortConfig(sortConfig) || DEFAULT_CHILD_SORT,
        filenameFromPath,
        getRecordBibtexFieldValue,
      }),
    ])
  );

  const topLevelEntries = [];
  const emittedFolders = new Set();
  documents.forEach((documentRecord) => {
    const folderKey = getFolderKeyFromFilePath(documentRecord.file_path, normalizeText);
    if (!groupedFolderKeys.has(folderKey)) {
      topLevelEntries.push({
        item: documentRecord,
        originalIndex: topLevelEntries.length,
      });
      return;
    }

    if (emittedFolders.has(folderKey)) {
      return;
    }
    emittedFolders.add(folderKey);

    const childDocuments = sortedFolderChildren.get(folderKey) || [];
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
    topLevelEntries.push({
      item: folderItem,
      originalIndex: topLevelEntries.length,
    });
  });

  const sortedTopLevelEntries = normalizeSortConfig(sortConfig)
    ? [...topLevelEntries].sort((leftEntry, rightEntry) => {
        const comparison = compareRecordsBySortConfig({
          leftRecord: leftEntry.item,
          rightRecord: rightEntry.item,
          sortConfig,
          filenameFromPath,
          getRecordBibtexFieldValue,
        });
        return comparison || leftEntry.originalIndex - rightEntry.originalIndex;
      })
    : topLevelEntries;

  sortedTopLevelEntries.forEach(({ item }) => {
    if (!item || typeof item !== "object") {
      return;
    }

    if (item.kind !== "folder") {
      if (!documentMatchesTitleQuery(item)) {
        return;
      }
      visibleItems.push(item);
      visibleDocuments.push(item);
      return;
    }

    const matchingChildren = item.child_documents.filter((childDocument) => documentMatchesTitleQuery(childDocument));
    const showFolder = folderMatchesTitleQuery(item) || matchingChildren.length > 0;
    if (!showFolder) {
      return;
    }

    visibleItems.push(item);
    const childItemsToRender = normalizedQuery ? matchingChildren : item.expanded ? item.child_documents : [];
    childItemsToRender.forEach((childDocument) => {
      childDocument.depth = 1;
      childDocument.parent_folder_key = item.folder_key;
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
