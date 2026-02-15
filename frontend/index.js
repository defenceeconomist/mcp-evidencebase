(function () {
  const origin = window.location.origin;
  const apiBasePath = origin + "/api";
  const bucketList = document.getElementById("bucket-list");
  const bucketEmptyState = document.getElementById("bucket-empty-state");
  const collectionsFilterInput = document.getElementById("collections-filter-input");
  const collectionsListScroll = document.getElementById("collections-list-scroll");
  const addCollectionButton = document.getElementById("add-collection-btn");
  const removeCollectionButton = document.getElementById("remove-collection-btn");
  const appShell = document.getElementById("app-shell");
  const sidebar = document.getElementById("collections-sidebar");
  const mainPanel = document.getElementById("main-panel");
  const collectionToggles = document.querySelectorAll('[data-action="toggle-collections"]');
  const uploadPdfButton = document.getElementById("upload-pdf-btn");
  const fetchMetaButton = document.getElementById("fetch-meta-btn");
  const documentCardHeader = document.getElementById("document-card-header");
  const detailViewContainer = document.getElementById("detail-view-container");
  const detailTableScroll = document.getElementById("detail-table-scroll");
  const detailDocumentTable = document.getElementById("detail-document-table");
  const detailDocumentTableBody = document.getElementById("detail-document-tbody");
  const detailFieldsForm = document.getElementById("detail-fields-form");
  const detailSelectedDocument = document.getElementById("detail-selected-document");
  const bulkViewContainer = document.getElementById("bulk-view-container");
  const documentHotWrapper = document.getElementById("document-hot-wrapper");
  const documentHotContainer = document.getElementById("document-hot-container");
  const documentMetaCount = document.getElementById("document-meta-count");
  const documentTitleSearch = document.getElementById("document-title-search");
  const tableViewModeSwitch = document.getElementById("table-view-mode-switch");
  const detailViewLabel = document.getElementById("detail-view-label");
  const bulkViewLabel = document.getElementById("bulk-view-label");
  const docJsonModalElement = document.getElementById("doc-json-modal");
  const docJsonModalTitle = document.getElementById("doc-json-modal-title");
  const docJsonModalSubtitle = document.getElementById("doc-json-modal-subtitle");
  const docJsonModalTree = document.getElementById("doc-json-modal-tree");

  const selectedBucketCookieName = "evidencebase_selected_bucket";
  const documentTypes = [
    "article",
    "book",
    "booklet",
    "conference",
    "inbook",
    "incollection",
    "inproceedings",
    "manual",
    "mastersthesis",
    "misc",
    "phdthesis",
    "proceedings",
    "techreport",
    "unpublished",
  ];
  const bibtexFields = [
    "address",
    "annote",
    "author",
    "booktitle",
    "chapter",
    "crossref",
    "doi",
    "edition",
    "editor",
    "email",
    "howpublished",
    "institution",
    "journal",
    "month",
    "note",
    "number",
    "organization",
    "pages",
    "publisher",
    "school",
    "series",
    "title",
    "type",
    "volume",
    "year",
  ];
  const bibtexTypeRules = {
    article: {
      required: ["author", "title", "journal", "year"],
      recommended: ["volume", "number", "pages", "month", "note", "doi"],
    },
    book: {
      required: ["title", "author", "year", "publisher", "address"],
      recommended: ["editor", "volume", "number", "series", "edition", "month", "note", "doi"],
    },
    booklet: {
      required: ["title"],
      recommended: ["author", "howpublished", "address", "month", "year", "note"],
    },
    conference: {
      required: ["author", "title", "booktitle", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "pages",
        "address",
        "month",
        "organization",
        "publisher",
        "note",
      ],
    },
    inbook: {
      required: ["author", "title", "booktitle", "publisher", "year"],
      recommended: [
        "editor",
        "chapter",
        "pages",
        "volume",
        "number",
        "series",
        "address",
        "edition",
        "month",
        "note",
      ],
    },
    incollection: {
      required: ["author", "title", "booktitle", "publisher", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "chapter",
        "pages",
        "address",
        "edition",
        "month",
        "organization",
        "note",
      ],
    },
    inproceedings: {
      required: ["author", "title", "booktitle", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "pages",
        "address",
        "month",
        "organization",
        "publisher",
        "note",
      ],
    },
    manual: {
      required: ["title"],
      recommended: ["author", "organization", "address", "edition", "month", "year", "note"],
    },
    mastersthesis: {
      required: ["author", "title", "school", "year"],
      recommended: ["type", "address", "month", "note"],
    },
    misc: {
      required: [],
      recommended: ["author", "title", "howpublished", "month", "year", "note"],
    },
    phdthesis: {
      required: ["author", "title", "school", "year"],
      recommended: ["type", "address", "month", "note"],
    },
    proceedings: {
      required: ["title", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "address",
        "month",
        "publisher",
        "organization",
        "note",
      ],
    },
    techreport: {
      required: ["author", "title", "institution", "year"],
      recommended: ["type", "number", "address", "month", "note"],
    },
    unpublished: {
      required: ["author", "title", "note"],
      recommended: ["month", "year"],
    },
  };
  const getBulkBibtexFieldOrder = () => {
    const metrics = new Map();
    bibtexFields.forEach((fieldName) => {
      metrics.set(fieldName, { requiredCount: 0, recommendedCount: 0 });
    });
    Object.values(bibtexTypeRules).forEach((rules) => {
      (rules.required || []).forEach((fieldName) => {
        if (metrics.has(fieldName)) {
          metrics.get(fieldName).requiredCount += 1;
        }
      });
      (rules.recommended || []).forEach((fieldName) => {
        if (metrics.has(fieldName)) {
          metrics.get(fieldName).recommendedCount += 1;
        }
      });
    });
    return [...bibtexFields].sort((leftField, rightField) => {
      const left = metrics.get(leftField);
      const right = metrics.get(rightField);
      if (left.requiredCount !== right.requiredCount) {
        return right.requiredCount - left.requiredCount;
      }
      if (left.recommendedCount !== right.recommendedCount) {
        return right.recommendedCount - left.recommendedCount;
      }
      return leftField.localeCompare(rightField);
    });
  };
  const bulkBibtexFieldOrder = getBulkBibtexFieldOrder();
  const bibtexFieldLabelOverrides = {
    doi: "DOI",
    month: "Month",
    year: "Year",
  };
  const statusBadgeMeta = {
    required: { label: "Required", className: "bibtex-status-required" },
    recommended: { label: "Recommended", className: "bibtex-status-recommended" },
    optional: { label: "Optional", className: "bibtex-status-optional" },
  };

  const getBibtexFieldLabel = (fieldName) => {
    if (bibtexFieldLabelOverrides[fieldName]) {
      return bibtexFieldLabelOverrides[fieldName];
    }
    return fieldName
      .split("_")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  };

  const getBibtexFieldStatus = (documentType, fieldName) => {
    const rules = bibtexTypeRules[normalizeDocumentType(documentType)] || {
      required: [],
      recommended: [],
    };
    if ((rules.required || []).includes(fieldName)) {
      return "required";
    }
    if ((rules.recommended || []).includes(fieldName)) {
      return "recommended";
    }
    return "optional";
  };

  const getOrderedDetailBibtexFields = (documentType) => {
    const rules = bibtexTypeRules[normalizeDocumentType(documentType)] || {
      required: [],
      recommended: [],
    };
    const ordered = [];
    const seen = new Set();

    (rules.required || []).forEach((fieldName) => {
      if (bibtexFields.includes(fieldName) && !seen.has(fieldName)) {
        ordered.push(fieldName);
        seen.add(fieldName);
      }
    });
    (rules.recommended || []).forEach((fieldName) => {
      if (bibtexFields.includes(fieldName) && !seen.has(fieldName)) {
        ordered.push(fieldName);
        seen.add(fieldName);
      }
    });
    bulkBibtexFieldOrder.forEach((fieldName) => {
      if (!seen.has(fieldName)) {
        ordered.push(fieldName);
        seen.add(fieldName);
      }
    });

    return ordered;
  };

  let minioBuckets = [];
  let selectedBucketName = null;
  let collectionsCollapsed = false;
  let documents = [];
  let visibleDocuments = [];
  let titleSearchQuery = "";
  let collectionsFilterQuery = "";
  let selectedDocumentId = null;
  let bulkEditEnabled = false;
  let wasMobileLayout = window.matchMedia("(max-width: 991.98px)").matches;
  let documentTable = null;
  let docJsonModalInstance = null;
  const isMobileViewport = () => window.matchMedia("(max-width: 991.98px)").matches;

  const setCookie = (name, value, maxAgeSeconds) => {
    document.cookie = [
      `${name}=${encodeURIComponent(value)}`,
      "Path=/",
      "SameSite=Lax",
      `Max-Age=${maxAgeSeconds}`,
    ].join("; ");
  };

  const getCookie = (name) => {
    const cookiePrefix = `${name}=`;
    const cookieValue = document.cookie
      .split(";")
      .map((item) => item.trim())
      .find((item) => item.startsWith(cookiePrefix));

    if (!cookieValue) {
      return null;
    }

    return decodeURIComponent(cookieValue.slice(cookiePrefix.length));
  };

  const clearCookie = (name) => {
    document.cookie = `${name}=; Path=/; SameSite=Lax; Max-Age=0`;
  };

  const setSelectedBucketName = (bucketName) => {
    selectedBucketName = bucketName;
    if (bucketName) {
      setCookie(selectedBucketCookieName, bucketName, 60 * 60 * 24 * 365);
    } else {
      clearCookie(selectedBucketCookieName);
    }
  };

  const normalizeText = (value) => {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value).trim();
  };

  const normalizeDocumentType = (value) => {
    const normalized = normalizeText(value).toLowerCase();
    if (!normalized) {
      return "misc";
    }
    return documentTypes.includes(normalized) ? normalized : "misc";
  };

  const filenameFromPath = (filePath) => {
    const normalized = normalizeText(filePath);
    if (!normalized) {
      return "document.pdf";
    }
    const splitParts = normalized.split("/");
    return splitParts[splitParts.length - 1] || normalized;
  };

  const normalizeAuthor = (rawDocument) => {
    if (Array.isArray(rawDocument.author)) {
      return rawDocument.author
        .map((authorPart) => normalizeText(authorPart))
        .filter((authorPart) => authorPart.length > 0)
        .join(", ");
    }
    return normalizeText(rawDocument.author || rawDocument.authors);
  };

  const normalizeBibtexFields = (rawDocument) => {
    if (!rawDocument || typeof rawDocument !== "object") {
      return {};
    }
    const source =
      (rawDocument.bibtex_fields && typeof rawDocument.bibtex_fields === "object"
        ? rawDocument.bibtex_fields
        : rawDocument.bibtex && typeof rawDocument.bibtex === "object"
          ? rawDocument.bibtex
          : {}) || {};

    const normalized = {};
    Object.entries(source).forEach(([fieldName, fieldValue]) => {
      const normalizedFieldName = normalizeText(fieldName).toLowerCase();
      if (normalizedFieldName) {
        normalized[normalizedFieldName] = normalizeText(fieldValue);
      }
    });
    return normalized;
  };

  const derivePublicationFromBibtex = (bibtexFieldsMap) => {
    return (
      normalizeText(bibtexFieldsMap.journal) ||
      normalizeText(bibtexFieldsMap.booktitle) ||
      normalizeText(bibtexFieldsMap.publisher) ||
      normalizeText(bibtexFieldsMap.institution) ||
      normalizeText(bibtexFieldsMap.school) ||
      ""
    );
  };

  const setPublicationHintField = (record, publicationValue) => {
    const nextPublication = normalizeText(publicationValue);
    if (!nextPublication || !record || typeof record !== "object") {
      return;
    }

    const entryType = normalizeDocumentType(record.document_type);
    if (entryType === "article") {
      record.bibtex_fields.journal = record.bibtex_fields.journal || nextPublication;
      record.journal = record.bibtex_fields.journal;
      return;
    }
    if (entryType === "inbook" || entryType === "incollection" || entryType === "inproceedings") {
      record.bibtex_fields.booktitle = record.bibtex_fields.booktitle || nextPublication;
      record.booktitle = record.bibtex_fields.booktitle;
      return;
    }
    if (entryType === "techreport") {
      record.bibtex_fields.institution = record.bibtex_fields.institution || nextPublication;
      record.institution = record.bibtex_fields.institution;
      return;
    }
    if (entryType === "phdthesis" || entryType === "mastersthesis") {
      record.bibtex_fields.school = record.bibtex_fields.school || nextPublication;
      record.school = record.bibtex_fields.school;
      return;
    }
    record.bibtex_fields.publisher = record.bibtex_fields.publisher || nextPublication;
    record.publisher = record.bibtex_fields.publisher;
  };

  const syncRecordCoreFields = (record) => {
    if (!record || typeof record !== "object") {
      return;
    }
    if (!record.bibtex_fields || typeof record.bibtex_fields !== "object") {
      record.bibtex_fields = {};
    }

    bibtexFields.forEach((fieldName) => {
      const fieldValue = normalizeText(record.bibtex_fields[fieldName]);
      record.bibtex_fields[fieldName] = fieldValue;
      record[fieldName] = fieldValue;
    });

    const titleFromBibtex = normalizeText(record.bibtex_fields.title);
    record.title = titleFromBibtex || filenameFromPath(record.file_path);
    record.year = normalizeText(record.bibtex_fields.year);
    record.author = normalizeText(record.bibtex_fields.author);
    record.publication = derivePublicationFromBibtex(record.bibtex_fields);
  };

  const setBibtexFieldValue = (record, fieldName, value) => {
    if (!record || !bibtexFields.includes(fieldName)) {
      return;
    }
    const normalizedValue = normalizeText(value);
    if (!record.bibtex_fields || typeof record.bibtex_fields !== "object") {
      record.bibtex_fields = {};
    }
    record[fieldName] = normalizedValue;
    record.bibtex_fields[fieldName] = normalizedValue;
    syncRecordCoreFields(record);
  };

  const buildFallbackCitationKey = (filename, index) => {
    const withoutExtension = filenameFromPath(filename).replace(/\.[^/.]+$/, "");
    const slug = withoutExtension
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+/, "")
      .replace(/-+$/, "");
    return slug || `document-${index + 1}`;
  };

  const normalizeDocumentRecord = (rawDocument, index) => {
    const filePath =
      normalizeText(rawDocument.file_path) ||
      normalizeText(rawDocument.filepath) ||
      normalizeText(rawDocument.file_name) ||
      normalizeText(rawDocument.filename) ||
      normalizeText(rawDocument.name) ||
      `document-${index + 1}.pdf`;

    const filename = filenameFromPath(filePath);
    const sourceBibtexFields = normalizeBibtexFields(rawDocument);
    const normalizedCitationKey =
      normalizeText(rawDocument.citation_key) ||
      normalizeText(rawDocument.citationKey) ||
      normalizeText(rawDocument.citekey) ||
      normalizeText(rawDocument.key) ||
      normalizeText(sourceBibtexFields.citation_key) ||
      normalizeText(sourceBibtexFields.citekey) ||
      buildFallbackCitationKey(filePath, index);

    const normalizedProcessingState =
      normalizeText(rawDocument.processing_state).toLowerCase() === "processing"
        ? "processing"
        : "processed";
    const rawProgress = Number.parseInt(normalizeText(rawDocument.processing_progress), 10);
    const normalizedProcessingProgress = Number.isNaN(rawProgress)
      ? 100
      : Math.min(100, Math.max(0, rawProgress));
    const normalizedPartitionsCount = Number.parseInt(
      normalizeText(rawDocument.partitions_count),
      10
    );
    const normalizedChunksCount = Number.parseInt(normalizeText(rawDocument.chunks_count), 10);
    const partitionsTree =
      rawDocument.partitions_tree && typeof rawDocument.partitions_tree === "object"
        ? rawDocument.partitions_tree
        : { partitions: [] };
    const chunksTree =
      rawDocument.chunks_tree && typeof rawDocument.chunks_tree === "object"
        ? rawDocument.chunks_tree
        : { chunks: [] };

    const record = {
      document_id: normalizeText(rawDocument.id) || `${filename}-${index}`,
      file_path: filePath,
      citation_key: normalizedCitationKey,
      document_type: normalizeDocumentType(
        rawDocument.document_type || rawDocument.entrytype || sourceBibtexFields.entrytype
      ),
      processing_state: normalizedProcessingState,
      processing_progress: normalizedProcessingProgress,
      partitions_count: Number.isNaN(normalizedPartitionsCount) ? 0 : normalizedPartitionsCount,
      chunks_count: Number.isNaN(normalizedChunksCount) ? 0 : normalizedChunksCount,
      partitions_tree: partitionsTree,
      chunks_tree: chunksTree,
      parse_status: normalizedProcessingState === "processing" ? "processing" : "processed",
      bibtex_fields: {},
    };

    bibtexFields.forEach((fieldName) => {
      const rawValue =
        fieldName === "author"
          ? normalizeAuthor(rawDocument) || normalizeText(sourceBibtexFields[fieldName]) || ""
          : normalizeText(rawDocument[fieldName]) || normalizeText(sourceBibtexFields[fieldName]) || "";
      record[fieldName] = rawValue;
      record.bibtex_fields[fieldName] = rawValue;
    });

    if (!record.bibtex_fields.title) {
      record.bibtex_fields.title = normalizeText(rawDocument.title);
      record.title = record.bibtex_fields.title;
    }
    if (!record.bibtex_fields.author) {
      record.bibtex_fields.author = normalizeAuthor(rawDocument);
      record.author = record.bibtex_fields.author;
    }
    if (!record.bibtex_fields.year) {
      record.bibtex_fields.year = normalizeText(rawDocument.year);
      record.year = record.bibtex_fields.year;
    }

    const normalizedPublication = normalizeText(rawDocument.publication);
    if (normalizedPublication) {
      setPublicationHintField(record, normalizedPublication);
    }
    syncRecordCoreFields(record);

    return record;
  };

  const setCollectionsCollapsed = (collapsed) => {
    collectionsCollapsed = collapsed;
    sidebar.classList.toggle("d-none", collapsed);
    mainPanel.classList.toggle("col-lg-10", !collapsed);
    mainPanel.classList.toggle("col-lg-12", collapsed);

    collectionToggles.forEach((toggle) => {
      toggle.setAttribute("aria-expanded", String(!collapsed));
    });
  };

  const enforceViewportSidebarState = () => {
    if (isMobileViewport() && collectionsCollapsed) {
      setCollectionsCollapsed(false);
    }
  };

  const getTooltipInstance = (element) => bootstrap.Tooltip.getOrCreateInstance(element);

  const setTooltipContent = (element, text) => {
    element.setAttribute("data-bs-title", text);
    getTooltipInstance(element).setContent({ ".tooltip-inner": text });
  };

  const setEmptyState = (message) => {
    bucketEmptyState.textContent = message;
    bucketEmptyState.classList.toggle("d-none", message.length === 0);
  };

  const parseErrorMessage = async (response) => {
    try {
      const payload = await response.json();
      if (payload && typeof payload.detail === "string") {
        return payload.detail;
      }
    } catch (error) {
      // Ignore JSON parse failures and use status text fallback.
    }
    return `${response.status} ${response.statusText}`;
  };

  const apiRequest = async (path, options = {}) => {
    const response = await fetch(apiBasePath + path, {
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
      ...options,
    });
    if (!response.ok) {
      throw new Error(await parseErrorMessage(response));
    }
    return response.json();
  };

  const syncTableViewLabels = () => {
    if (!detailViewLabel || !bulkViewLabel || !tableViewModeSwitch) {
      return;
    }
    const isBulkMode = Boolean(tableViewModeSwitch.checked);
    detailViewLabel.classList.toggle("text-body-secondary", isBulkMode);
    bulkViewLabel.classList.toggle("text-body-secondary", !isBulkMode);
  };

  const setIndependentScrollHeights = () => {
    const isMobileLayout = window.matchMedia("(max-width: 991.98px)").matches;
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
    const clearElementHeight = (element) => {
      if (!element) {
        return;
      }
      element.style.height = "";
      element.style.maxHeight = "";
    };
    const calculateHeight = (element) => {
      if (!element) {
        return null;
      }
      const bounds = element.getBoundingClientRect();
      if (bounds.width === 0 && bounds.height === 0) {
        return null;
      }
      const availableHeight = Math.floor(viewportHeight - bounds.top - 12);
      const nextHeight = Math.max(160, availableHeight);
      element.style.height = `${nextHeight}px`;
      element.style.maxHeight = `${nextHeight}px`;
      return nextHeight;
    };

    if (wasMobileLayout && !isMobileLayout) {
      if (appShell) {
        appShell.scrollTop = 0;
      }
      if (collectionsListScroll) {
        collectionsListScroll.scrollTop = 0;
      }
      if (detailTableScroll) {
        detailTableScroll.scrollTop = 0;
      }
      if (detailFieldsForm) {
        detailFieldsForm.scrollTop = 0;
      }
      if (documentHotWrapper) {
        documentHotWrapper.scrollTop = 0;
      }
      window.scrollTo(0, 0);
    }

    if (isMobileLayout) {
      clearElementHeight(collectionsListScroll);
      clearElementHeight(detailTableScroll);
      clearElementHeight(detailFieldsForm);
      clearElementHeight(detailViewContainer);
      clearElementHeight(documentHotWrapper);
      if (documentTable) {
        documentTable.updateSettings({ height: 420 });
        documentTable.render();
      }
      wasMobileLayout = isMobileLayout;
      return;
    }

    calculateHeight(collectionsListScroll);
    if (bulkEditEnabled) {
      const tableHeight = calculateHeight(documentHotWrapper);
      if (documentTable && Number.isFinite(tableHeight)) {
        documentTable.updateSettings({ height: tableHeight });
        documentTable.render();
      }
      wasMobileLayout = isMobileLayout;
      return;
    }

    calculateHeight(detailTableScroll);
    calculateHeight(detailFieldsForm);
    const detailViewHeight = calculateHeight(detailViewContainer);
    if (Number.isFinite(detailViewHeight) && detailViewContainer) {
      detailViewContainer.style.maxHeight = `${detailViewHeight}px`;
    }
    wasMobileLayout = isMobileLayout;
  };

  const updateRemoveTooltip = () => {
    if (selectedBucketName) {
      setTooltipContent(
        removeCollectionButton,
        `Remove selected collection bucket: ${selectedBucketName}.`
      );
      return;
    }
    setTooltipContent(
      removeCollectionButton,
      "Select a collection first, then remove its MinIO bucket."
    );
  };

  const renderBuckets = () => {
    bucketList.innerHTML = "";
    if (minioBuckets.length === 0) {
      setSelectedBucketName(null);
      setEmptyState("No collections found. Use + to create one.");
      updateRemoveTooltip();
      return;
    }

    const visibleBuckets = minioBuckets.filter((bucketName) =>
      normalizeText(bucketName).toLowerCase().includes(collectionsFilterQuery)
    );

    if (visibleBuckets.length === 0) {
      setEmptyState("No collections match the current filter.");
      updateRemoveTooltip();
      return;
    }

    visibleBuckets.forEach((bucketName) => {
      const item = document.createElement("li");
      item.className =
        "list-group-item list-group-item-action user-select-none" +
        (bucketName === selectedBucketName ? " active" : "");
      item.textContent = bucketName;
      item.setAttribute("role", "button");
      item.setAttribute("tabindex", "0");
      item.addEventListener("click", () => {
        setSelectedBucketName(bucketName);
        renderBuckets();
        void refreshDocuments();
      });
      item.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          setSelectedBucketName(bucketName);
          renderBuckets();
          void refreshDocuments();
        }
      });
      bucketList.appendChild(item);
    });

    setEmptyState("");
    updateRemoveTooltip();
  };

  const formatJsonPrimitive = (value) => {
    if (typeof value === "string") {
      return `"${value}"`;
    }
    if (value === null) {
      return "null";
    }
    return String(value);
  };

  const renderJsonTreeNode = (key, value, depth = 0) => {
    const item = document.createElement("li");
    const header = document.createElement("div");
    header.className = "json-tree-node-header";

    const keyLabel = document.createElement("span");
    keyLabel.className = "text-primary";
    keyLabel.textContent = key;

    if (
      value === null ||
      typeof value !== "object" ||
      (Array.isArray(value) && value.length === 0) ||
      (!Array.isArray(value) && Object.keys(value).length === 0)
    ) {
      const valueLabel = document.createElement("span");
      valueLabel.className = "text-body-secondary";
      if (Array.isArray(value)) {
        valueLabel.textContent = "[]";
      } else if (value && typeof value === "object") {
        valueLabel.textContent = "{}";
      } else {
        valueLabel.textContent = formatJsonPrimitive(value);
      }
      header.appendChild(keyLabel);
      header.appendChild(document.createTextNode(": "));
      header.appendChild(valueLabel);
      item.appendChild(header);
      return item;
    }

    const isArray = Array.isArray(value);
    const childEntries = isArray
      ? value.map((entry, index) => [String(index), entry])
      : Object.entries(value);
    const summary = isArray ? `Array(${childEntries.length})` : `Object(${childEntries.length})`;
    const expandedByDefault = depth < 2;

    const toggleButton = document.createElement("button");
    toggleButton.type = "button";
    toggleButton.className = "json-tree-toggle";
    toggleButton.textContent = expandedByDefault ? "▾" : "▸";
    toggleButton.setAttribute("aria-label", `Toggle ${key}`);

    const summaryLabel = document.createElement("span");
    summaryLabel.className = "text-body-secondary";
    summaryLabel.textContent = summary;

    const childrenList = document.createElement("ul");
    if (!expandedByDefault) {
      childrenList.classList.add("d-none");
    }
    childEntries.forEach(([childKey, childValue]) => {
      childrenList.appendChild(renderJsonTreeNode(childKey, childValue, depth + 1));
    });

    const setExpanded = (expanded) => {
      toggleButton.textContent = expanded ? "▾" : "▸";
      childrenList.classList.toggle("d-none", !expanded);
    };

    header.addEventListener("click", () => {
      setExpanded(childrenList.classList.contains("d-none"));
    });

    header.appendChild(toggleButton);
    header.appendChild(keyLabel);
    header.appendChild(document.createTextNode(": "));
    header.appendChild(summaryLabel);
    item.appendChild(header);
    item.appendChild(childrenList);
    return item;
  };

  const openDocJsonModal = (documentRecord, payloadType) => {
    if (!docJsonModalElement || !docJsonModalTitle || !docJsonModalSubtitle || !docJsonModalTree) {
      return;
    }

    const isPartitions = payloadType === "partitions";
    const payload = isPartitions ? documentRecord.partitions_tree : documentRecord.chunks_tree;
    const count = isPartitions ? documentRecord.partitions_count : documentRecord.chunks_count;
    docJsonModalTitle.textContent = isPartitions ? "Partitions JSON Tree" : "Chunks JSON Tree";
    docJsonModalSubtitle.textContent =
      `${filenameFromPath(documentRecord.file_path)} | ${isPartitions ? "Partitions" : "Chunks"}: ${count}`;
    docJsonModalTree.innerHTML = "";

    const rootList = document.createElement("ul");
    rootList.appendChild(renderJsonTreeNode("root", payload));
    docJsonModalTree.appendChild(rootList);

    if (!docJsonModalInstance) {
      docJsonModalInstance = bootstrap.Modal.getOrCreateInstance(docJsonModalElement);
    }
    docJsonModalInstance.show();
  };

  const parseActionsRenderer = (instance, td, row, col, prop, value, cellProperties) => {
    Handsontable.renderers.TextRenderer(instance, td, row, col, prop, value, cellProperties);
    td.classList.add("hot-parse-cell");
    td.innerHTML = "";

    const physicalRow = instance.toPhysicalRow(row);
    const record = instance.getSourceDataAtRow(physicalRow);
    if (!record) {
      return td;
    }

    if (record.processing_state === "processing") {
      const progressWrap = document.createElement("div");
      progressWrap.className = "progress";

      const progressBar = document.createElement("div");
      progressBar.className = "progress-bar progress-bar-striped progress-bar-animated";
      progressBar.style.width = `${record.processing_progress}%`;
      progressBar.textContent = `${record.processing_progress}%`;

      progressWrap.appendChild(progressBar);
      td.appendChild(progressWrap);
      return td;
    }

    const parseActions = document.createElement("div");
    parseActions.className = "btn-group";
    parseActions.role = "group";

    const partitionsButton = document.createElement("button");
    partitionsButton.type = "button";
    partitionsButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    partitionsButton.textContent = `P: ${record.partitions_count}`;
    partitionsButton.dataset.jsonAction = "partitions";
    partitionsButton.dataset.documentId = record.document_id;

    const chunksButton = document.createElement("button");
    chunksButton.type = "button";
    chunksButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    chunksButton.textContent = `C: ${record.chunks_count}`;
    chunksButton.dataset.jsonAction = "chunks";
    chunksButton.dataset.documentId = record.document_id;

    parseActions.appendChild(partitionsButton);
    parseActions.appendChild(chunksButton);
    td.appendChild(parseActions);

    return td;
  };

  const initializeDocumentTable = () => {
    if (!documentHotContainer || documentTable) {
      return;
    }

    if (typeof Handsontable === "undefined") {
      documentHotContainer.innerHTML =
        '<p class="m-3 text-danger">Handsontable failed to load. Check network access to the CDN.</p>';
      return;
    }

    const bulkColumns = [
      { data: "document_id", type: "text", readOnly: true, width: 240 },
      { data: "file_path", type: "text", readOnly: true, width: 320 },
      {
        data: "document_type",
        type: "dropdown",
        source: documentTypes,
        strict: false,
        allowInvalid: false,
        width: 130,
      },
      { data: "citation_key", type: "text", width: 170 },
      ...bulkBibtexFieldOrder.map((fieldName) => ({
        data: fieldName,
        type: "text",
        width:
          fieldName === "title"
            ? 260
            : fieldName === "author"
              ? 220
              : fieldName === "note" || fieldName === "annote"
                ? 260
                : 140,
      })),
      { data: "parse_status", readOnly: true, renderer: parseActionsRenderer, width: 170 },
    ];
    const bulkHeaders = [
      "Document ID",
      "File Path",
      "Document Type",
      "Citation Key",
      ...bulkBibtexFieldOrder.map((fieldName) => getBibtexFieldLabel(fieldName)),
      "",
    ];

    documentTable = new Handsontable(documentHotContainer, {
      data: [],
      columns: bulkColumns,
      colHeaders: bulkHeaders,
      rowHeaders: true,
      width: "100%",
      stretchH: "none",
      columnSorting: false,
      dropdownMenu: true,
      filters: true,
      manualColumnResize: true,
      contextMenu: true,
      licenseKey: "non-commercial-and-evaluation",
      afterOnCellMouseDown(event, coords) {
        if (coords.row < 0) {
          return;
        }
        const physicalRow = documentTable.toPhysicalRow(coords.row);
        const record = documentTable.getSourceDataAtRow(physicalRow);
        if (!record) {
          return;
        }
        selectedDocumentId = record.document_id;
      },
      afterChange(changes, source) {
        if (!changes || source === "loadData") {
          return;
        }

        let shouldRefreshDetailTable = false;
        let shouldRefreshDetailForm = false;
        changes.forEach(([visualRow, property, oldValue, newValue]) => {
          if (oldValue === newValue) {
            return;
          }

          const physicalRow = documentTable.toPhysicalRow(visualRow);
          const record = documentTable.getSourceDataAtRow(physicalRow);
          if (!record) {
            return;
          }

          const normalizedValue = normalizeText(newValue);
          if (property === "document_type") {
            const nextType = normalizeDocumentType(normalizedValue);
            record.document_type = nextType;
            if (nextType !== newValue) {
              documentTable.setDataAtRowProp(visualRow, "document_type", nextType, "normalize");
            }
            shouldRefreshDetailForm = shouldRefreshDetailForm || record.document_id === selectedDocumentId;
            shouldRefreshDetailTable = true;
            return;
          }

          if (property === "citation_key") {
            record.citation_key = normalizedValue;
            shouldRefreshDetailForm = shouldRefreshDetailForm || record.document_id === selectedDocumentId;
            return;
          }

          if (bibtexFields.includes(property)) {
            setBibtexFieldValue(record, property, normalizedValue);
            shouldRefreshDetailForm = shouldRefreshDetailForm || record.document_id === selectedDocumentId;
            shouldRefreshDetailTable = true;
            return;
          }

          record[property] = normalizedValue;
        });

        if (shouldRefreshDetailTable) {
          renderDetailTable();
        }
        if (shouldRefreshDetailForm) {
          renderDetailFields();
        }
      },
    });
  };

  const getFilteredDocuments = () => {
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

  const getSelectedDocument = () => {
    if (!selectedDocumentId) {
      return null;
    }
    return documents.find((documentRecord) => documentRecord.document_id === selectedDocumentId) || null;
  };

  const ensureSelectedDocument = (candidateDocuments) => {
    if (!Array.isArray(candidateDocuments) || candidateDocuments.length === 0) {
      selectedDocumentId = null;
      return;
    }
    const selectedStillVisible = candidateDocuments.some(
      (documentRecord) => documentRecord.document_id === selectedDocumentId
    );
    if (!selectedStillVisible) {
      selectedDocumentId = candidateDocuments[0].document_id;
    }
  };

  const renderParseStatusIntoCell = (cell, documentRecord) => {
    if (!cell) {
      return;
    }
    cell.innerHTML = "";
    cell.className = "text-nowrap";

    if (documentRecord.processing_state === "processing") {
      const progressWrap = document.createElement("div");
      progressWrap.className = "progress";
      progressWrap.style.minWidth = "8rem";

      const progressBar = document.createElement("div");
      progressBar.className = "progress-bar progress-bar-striped progress-bar-animated";
      progressBar.style.width = `${documentRecord.processing_progress}%`;
      progressBar.textContent = `${documentRecord.processing_progress}%`;

      progressWrap.appendChild(progressBar);
      cell.appendChild(progressWrap);
      return;
    }

    const parseActions = document.createElement("div");
    parseActions.className = "btn-group";
    parseActions.role = "group";

    const partitionsButton = document.createElement("button");
    partitionsButton.type = "button";
    partitionsButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    partitionsButton.textContent = `P: ${documentRecord.partitions_count}`;
    partitionsButton.dataset.jsonAction = "partitions";
    partitionsButton.dataset.documentId = documentRecord.document_id;

    const chunksButton = document.createElement("button");
    chunksButton.type = "button";
    chunksButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    chunksButton.textContent = `C: ${documentRecord.chunks_count}`;
    chunksButton.dataset.jsonAction = "chunks";
    chunksButton.dataset.documentId = documentRecord.document_id;

    parseActions.appendChild(partitionsButton);
    parseActions.appendChild(chunksButton);
    cell.appendChild(parseActions);
  };

  const updateDocumentCount = (visibleCount) => {
    if (!documentMetaCount) {
      return;
    }
    if (titleSearchQuery) {
      documentMetaCount.textContent = `${visibleCount} of ${documents.length} documents`;
      return;
    }
    documentMetaCount.textContent = `${visibleCount} documents`;
  };

  const renderDetailTable = () => {
    if (!detailDocumentTableBody) {
      return;
    }

    detailDocumentTableBody.innerHTML = "";
    if (visibleDocuments.length === 0) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 5;
      cell.className = "text-body-secondary py-3";
      cell.textContent = "No document metadata available.";
      row.appendChild(cell);
      detailDocumentTableBody.appendChild(row);
      return;
    }

    visibleDocuments.forEach((documentRecord) => {
      const row = document.createElement("tr");
      row.dataset.documentId = documentRecord.document_id;
      if (documentRecord.document_id === selectedDocumentId) {
        row.classList.add("table-active");
      }

      const titleCell = document.createElement("td");
      titleCell.textContent = documentRecord.title || filenameFromPath(documentRecord.file_path);

      const yearCell = document.createElement("td");
      yearCell.textContent = documentRecord.year || "n/a";

      const authorCell = document.createElement("td");
      authorCell.textContent = documentRecord.author || "n/a";

      const publicationCell = document.createElement("td");
      publicationCell.textContent = documentRecord.publication || "n/a";

      const parseCell = document.createElement("td");
      renderParseStatusIntoCell(parseCell, documentRecord);

      row.appendChild(titleCell);
      row.appendChild(yearCell);
      row.appendChild(authorCell);
      row.appendChild(publicationCell);
      row.appendChild(parseCell);
      detailDocumentTableBody.appendChild(row);
    });
  };

  const createFieldRow = (
    labelText,
    value,
    {
      readOnly = false,
      placeholder = "",
      type = "text",
      onInput = null,
      selectOptions = [],
      status = "",
    } = {}
  ) => {
    const row = document.createElement("div");
    row.className = "detail-field-row";
    if (status && statusBadgeMeta[status]) {
      row.classList.add(`detail-field-row-${status}`);
    }

    const labelWrap = document.createElement("div");
    labelWrap.className = "d-flex align-items-center justify-content-between gap-2 mb-1";

    const label = document.createElement("label");
    label.className = "form-label mb-0 fw-semibold small";
    label.textContent = labelText;
    labelWrap.appendChild(label);

    if (status && statusBadgeMeta[status]) {
      const badge = document.createElement("span");
      badge.className = `badge rounded-pill bibtex-status-tag ${statusBadgeMeta[status].className}`;
      badge.textContent = statusBadgeMeta[status].label;
      labelWrap.appendChild(badge);
    }

    let control;
    if (type === "select") {
      control = document.createElement("select");
      control.className = "form-select form-select-sm";
      selectOptions.forEach((optionValue) => {
        const option = document.createElement("option");
        option.value = optionValue;
        option.textContent = optionValue;
        control.appendChild(option);
      });
      control.value = value || "misc";
    } else {
      control = document.createElement("input");
      control.type = "text";
      control.className = "form-control form-control-sm";
      control.value = value || "";
      control.placeholder = placeholder;
      control.readOnly = readOnly;
    }

    if (readOnly) {
      control.classList.add("bg-body-tertiary");
    }

    if (onInput) {
      const eventName = type === "select" ? "change" : "input";
      control.addEventListener(eventName, () => onInput(control.value));
    }

    row.appendChild(labelWrap);
    row.appendChild(control);
    return row;
  };

  const renderDetailFields = () => {
    if (!detailFieldsForm || !detailSelectedDocument) {
      return;
    }

    const selectedDocument = getSelectedDocument();
    detailFieldsForm.innerHTML = "";
    if (!selectedDocument) {
      detailSelectedDocument.textContent = "Select a document to edit its metadata fields.";
      return;
    }

    detailSelectedDocument.textContent = `Selected: ${filenameFromPath(selectedDocument.file_path)}`;

    detailFieldsForm.appendChild(
      createFieldRow("Document Type", selectedDocument.document_type, {
        type: "select",
        selectOptions: documentTypes,
        onInput(nextValue) {
          selectedDocument.document_type = normalizeDocumentType(nextValue);
          renderDetailFields();
          if (bulkEditEnabled && documentTable) {
            documentTable.render();
          }
        },
      })
    );

    detailFieldsForm.appendChild(
      createFieldRow("Citation Key", selectedDocument.citation_key, {
        placeholder: "citation_key",
        onInput(nextValue) {
          selectedDocument.citation_key = normalizeText(nextValue);
        },
      })
    );

    getOrderedDetailBibtexFields(selectedDocument.document_type).forEach((fieldName) => {
      detailFieldsForm.appendChild(
        createFieldRow(getBibtexFieldLabel(fieldName), selectedDocument[fieldName], {
          placeholder: fieldName,
          status: getBibtexFieldStatus(selectedDocument.document_type, fieldName),
          onInput(nextValue) {
            setBibtexFieldValue(selectedDocument, fieldName, nextValue);
            renderDetailTable();
          },
        })
      );
    });
  };

  const renderBulkTable = () => {
    initializeDocumentTable();
    if (!documentTable) {
      return;
    }

    documentTable.loadData(visibleDocuments);
  };

  const renderDocumentTable = () => {
    visibleDocuments = getFilteredDocuments();
    ensureSelectedDocument(visibleDocuments);
    updateDocumentCount(visibleDocuments.length);

    if (bulkEditEnabled) {
      detailViewContainer?.classList.add("d-none");
      bulkViewContainer?.classList.remove("d-none");
      renderBulkTable();
    } else {
      bulkViewContainer?.classList.add("d-none");
      detailViewContainer?.classList.remove("d-none");
      renderDetailTable();
      renderDetailFields();
    }

    window.requestAnimationFrame(setIndependentScrollHeights);
  };

  const renderDocumentMeta = () => {
    renderDocumentTable();
  };

  const refreshDocuments = async () => {
    if (!selectedBucketName) {
      documents = [];
      selectedDocumentId = null;
      renderDocumentMeta();
      return;
    }

    documents = [];
    selectedDocumentId = null;
    renderDocumentMeta();
  };

  const refreshBuckets = async () => {
    try {
      const payload = await apiRequest("/buckets");
      minioBuckets = Array.isArray(payload.buckets)
        ? payload.buckets.map((bucketName) => normalizeText(bucketName)).filter(Boolean)
        : [];
      minioBuckets.sort((leftBucket, rightBucket) => leftBucket.localeCompare(rightBucket));

      if (selectedBucketName && !minioBuckets.includes(selectedBucketName)) {
        setSelectedBucketName(null);
      }

      if (!selectedBucketName) {
        const storedBucketName = getCookie(selectedBucketCookieName);
        if (storedBucketName && minioBuckets.includes(storedBucketName)) {
          setSelectedBucketName(storedBucketName);
        } else if (storedBucketName) {
          clearCookie(selectedBucketCookieName);
        } else if (minioBuckets.length > 0) {
          setSelectedBucketName(minioBuckets[0]);
        }
      }

      renderBuckets();
      await refreshDocuments();
    } catch (error) {
      minioBuckets = [];
      setSelectedBucketName(null);
      renderBuckets();
      setEmptyState("Failed to load collections from API.");
      documents = [];
      selectedDocumentId = null;
      renderDocumentMeta();
      window.alert(`Could not load collections: ${error.message}`);
    }
  };

  const addBucket = async () => {
    const requestedBucketName = window.prompt("Enter a new collection bucket name:");
    if (requestedBucketName === null) {
      return;
    }

    const normalizedBucketName = requestedBucketName.trim();
    if (normalizedBucketName.length === 0) {
      window.alert("Collection name cannot be empty.");
      return;
    }

    try {
      const payload = await apiRequest("/buckets", {
        method: "POST",
        body: JSON.stringify({ bucket_name: normalizedBucketName }),
      });
      if (!payload.created) {
        window.alert(`Collection '${normalizedBucketName}' already exists.`);
      }
      setSelectedBucketName(normalizedBucketName);
      await refreshBuckets();
    } catch (error) {
      window.alert(`Could not add collection: ${error.message}`);
    }
  };

  const removeBucket = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }

    if (!window.confirm(`Remove collection '${selectedBucketName}'?`)) {
      return;
    }

    try {
      await apiRequest(`/buckets/${encodeURIComponent(selectedBucketName)}`, {
        method: "DELETE",
      });
      setSelectedBucketName(null);
      await refreshBuckets();
    } catch (error) {
      window.alert(`Could not remove collection: ${error.message}`);
    }
  };

  document.querySelectorAll("[data-path]").forEach((anchor) => {
    const path = anchor.getAttribute("data-path") || "";
    anchor.href = origin + path;
    anchor.target = "_blank";
    anchor.rel = "noopener noreferrer";
  });

  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
    new bootstrap.Tooltip(el);
  });
  setTooltipContent(addCollectionButton, "Create a new collection bucket in MinIO.");

  addCollectionButton.addEventListener("click", () => {
    void addBucket();
  });
  removeCollectionButton.addEventListener("click", () => {
    void removeBucket();
  });

  uploadPdfButton?.addEventListener("click", () => {
    window.alert("Upload PDF is not wired yet.");
  });
  fetchMetaButton?.addEventListener("click", () => {
    window.alert("Fetch Meta is not wired yet.");
  });

  collectionToggles.forEach((toggle) => {
    toggle.addEventListener("click", () => {
      if (isMobileViewport()) {
        return;
      }
      setCollectionsCollapsed(!collectionsCollapsed);
    });
  });

  documentTitleSearch?.addEventListener("input", () => {
    titleSearchQuery = normalizeText(documentTitleSearch.value).toLowerCase();
    renderDocumentMeta();
  });
  tableViewModeSwitch?.addEventListener("change", () => {
    bulkEditEnabled = Boolean(tableViewModeSwitch.checked);
    syncTableViewLabels();
    renderDocumentMeta();
  });

  collectionsFilterInput?.addEventListener("input", () => {
    collectionsFilterQuery = normalizeText(collectionsFilterInput.value).toLowerCase();
    renderBuckets();
    window.requestAnimationFrame(setIndependentScrollHeights);
  });

  detailDocumentTable?.addEventListener("click", (event) => {
    const actionButton = event.target.closest("button[data-json-action]");
    if (actionButton) {
      event.preventDefault();
      event.stopPropagation();
      const documentId = actionButton.dataset.documentId;
      const payloadType = actionButton.dataset.jsonAction;
      if (!documentId || !payloadType) {
        return;
      }
      const targetDocument = documents.find(
        (documentRecord) => documentRecord.document_id === documentId
      );
      if (targetDocument) {
        openDocJsonModal(targetDocument, payloadType);
      }
      return;
    }

    const row = event.target.closest("tr[data-document-id]");
    if (!row) {
      return;
    }
    const nextDocumentId = row.dataset.documentId;
    if (!nextDocumentId || nextDocumentId === selectedDocumentId) {
      return;
    }
    selectedDocumentId = nextDocumentId;
    renderDetailTable();
    renderDetailFields();
  });

  documentHotContainer?.addEventListener("click", (event) => {
    const actionButton = event.target.closest("button[data-json-action]");
    if (!actionButton || !documentHotContainer.contains(actionButton)) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();

    const documentId = actionButton.dataset.documentId;
    const payloadType = actionButton.dataset.jsonAction;
    if (!documentId || !payloadType) {
      return;
    }

    const targetDocument = documents.find((documentRecord) => documentRecord.document_id === documentId);
    if (!targetDocument) {
      return;
    }
    openDocJsonModal(targetDocument, payloadType);
  });

  window.addEventListener("resize", () => {
    enforceViewportSidebarState();
    setIndependentScrollHeights();
  });

  setCollectionsCollapsed(false);
  enforceViewportSidebarState();
  if (documentCardHeader) {
    documentCardHeader.style.minHeight = `${Math.ceil(documentCardHeader.getBoundingClientRect().height)}px`;
  }

  minioBuckets = [];
  documents = [];
  visibleDocuments = [];
  selectedDocumentId = null;
  bulkEditEnabled = false;
  if (tableViewModeSwitch) {
    tableViewModeSwitch.checked = false;
  }
  syncTableViewLabels();

  updateRemoveTooltip();
  window.requestAnimationFrame(setIndependentScrollHeights);
  void refreshBuckets();
})();
