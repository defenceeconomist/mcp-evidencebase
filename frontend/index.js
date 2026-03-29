import { createApiRequest } from "./js/api-client.mjs";
import {
  buildFallbackCitationKey as buildFallbackCitationKeyFromModule,
  getBulkBibtexFieldOrder as getBulkBibtexFieldOrderFromModule,
  getDefaultCitationSchema,
  getBibtexFieldStatus as getBibtexFieldStatusFromModule,
  normalizeCitationSchema,
  normalizeDocumentType as normalizeDocumentTypeFromModule,
} from "./js/metadata-editor.mjs";
import {
  formatSearchAuthorYear as formatSearchAuthorYearFromModule,
  formatSearchLocation as formatSearchLocationFromModule,
  formatSearchPageLabel as formatSearchPageLabelFromModule,
  formatSearchPages as formatSearchPagesFromModule,
  formatSearchTitle as formatSearchTitleFromModule,
} from "./js/search-ui.mjs";
import {
  clearCookieValue,
  createStateStore,
  getCookieValue,
  setCookieValue,
} from "./js/state-store.mjs";
import {
  ensureSelectedDocumentId,
  filterDocumentsByTitle,
} from "./js/table-ui.mjs";

(function () {
  window.__EVIDENCEBASE_UI_BUILD__ = "2026-03-29-detail-reindex-a";
  const origin = window.location.origin;
  const deriveAppBasePath = (pathname) => {
    const normalizedPath = String(pathname || "/").trim() || "/";
    if (normalizedPath === "/") {
      return "";
    }
    if (/\.[^/]+$/.test(normalizedPath)) {
      const lastSlashIndex = normalizedPath.lastIndexOf("/");
      return lastSlashIndex > 0 ? normalizedPath.slice(0, lastSlashIndex) : "";
    }
    return normalizedPath.replace(/\/+$/, "");
  };
  const appBasePath = deriveAppBasePath(window.location.pathname);
  const appBaseUrl = origin + appBasePath;
  const apiBasePath = appBaseUrl + "/api";
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
  const uploadPdfFolderButton = document.getElementById("upload-pdf-folder-btn");
  const downloadBibtexButton = document.getElementById("download-bibtex-btn");
  const fetchMetaButton = document.getElementById("fetch-meta-btn");
  const updateCitationKeyButton = document.getElementById("update-citation-key-btn");
  const removeSelectedDocumentsButton = document.getElementById("remove-selected-docs-btn");
  const bulkFetchProgress = document.getElementById("bulk-fetch-progress");
  const bulkFetchProgressBar = document.getElementById("bulk-fetch-progress-bar");
  const bulkFetchProgressText = document.getElementById("bulk-fetch-progress-text");
  const removeDocumentButton = document.getElementById("remove-doc-btn");
  const documentCardHeader = document.getElementById("document-card-header");
  const detailViewContainer = document.getElementById("detail-view-container");
  const detailTableScroll = document.getElementById("detail-table-scroll");
  const detailDocumentTable = document.getElementById("detail-document-table");
  const detailDocumentTableBody = document.getElementById("detail-document-tbody");
  const detailFieldsForm = document.getElementById("detail-fields-form");
  const detailSelectedDocument = document.getElementById("detail-selected-document");
  const detailFetchMetaButton = document.getElementById("detail-fetch-meta-btn");
  const detailUpdateCitationKeyButton = document.getElementById("detail-update-citation-key-btn");
  const detailReindexButton = document.getElementById("detail-reindex-btn");
  const detailClearMetaButton = document.getElementById("detail-clear-meta-btn");
  const bulkViewContainer = document.getElementById("bulk-view-container");
  const documentHotWrapper = document.getElementById("document-hot-wrapper");
  const documentHotContainer = document.getElementById("document-hot-container");
  const documentMetaCount = document.getElementById("document-meta-count");
  const documentTitleSearch = document.getElementById("document-title-search");
  const tableViewModeSwitch = document.getElementById("table-view-mode-switch");
  const detailViewLabel = document.getElementById("detail-view-label");
  const bulkViewLabel = document.getElementById("bulk-view-label");
  const documentMetaTab = document.getElementById("document-meta-tab");
  const documentMetaPane = document.getElementById("document-meta-pane");
  const docJsonModalElement = document.getElementById("doc-json-modal");
  const docJsonModalTitle = document.getElementById("doc-json-modal-title");
  const docJsonModalSubtitle = document.getElementById("doc-json-modal-subtitle");
  const docJsonModalTree = document.getElementById("doc-json-modal-tree");
  const searchSectionModalElement = document.getElementById("search-section-modal");
  const searchSectionModalTitle = document.getElementById("search-section-modal-title");
  const searchSectionModalSubtitle = document.getElementById("search-section-modal-subtitle");
  const searchSectionModalBody = document.getElementById("search-section-modal-body");
  const searchSectionPrevButton = document.getElementById("search-section-prev-btn");
  const searchSectionNextButton = document.getElementById("search-section-next-btn");
  const pdfSplitModalElement = document.getElementById("pdf-split-modal");
  const pdfSplitModalTitle = document.getElementById("pdf-split-modal-title");
  const pdfSplitModalSubtitle = document.getElementById("pdf-split-modal-subtitle");
  const pdfSplitPdfTitle = document.getElementById("pdf-split-pdf-title");
  const pdfSplitPageCount = document.getElementById("pdf-split-page-count");
  const pdfSplitFolderInput = document.getElementById("pdf-split-folder-input");
  const pdfSplitLevelOptions = document.getElementById("pdf-split-level-options");
  const pdfSplitStatus = document.getElementById("pdf-split-status");
  const pdfSplitPreviewTab = document.getElementById("pdf-split-preview-tab");
  const pdfSplitMetadataTab = document.getElementById("pdf-split-metadata-tab");
  const pdfSplitMetadataFields = document.getElementById("pdf-split-metadata-fields");
  const pdfSplitPreviewCount = document.getElementById("pdf-split-preview-count");
  const pdfSplitPreviewBody = document.getElementById("pdf-split-preview-body");
  const pdfSplitSelectAllFiles = document.getElementById("pdf-split-select-all-files");
  const pdfSplitUploadButton = document.getElementById("pdf-split-upload-btn");
  const semanticSearchTab = document.getElementById("semantic-search-tab");
  const semanticSearchPane = document.getElementById("semantic-search-pane");
  const semanticSearchForm = document.getElementById("semantic-search-form");
  const semanticSearchQueryInput = document.getElementById("semantic-search-query");
  const semanticSearchModeSelect = document.getElementById("semantic-search-mode");
  const semanticSearchLimitInput = document.getElementById("semantic-search-limit");
  const semanticRrfKField = document.getElementById("semantic-rrf-k-field");
  const rrfKInput = document.getElementById("rrf-k-input");
  const semanticSearchSubmit = document.getElementById("semantic-search-submit");
  const semanticSearchStatus = document.getElementById("semantic-search-status");
  const semanticSearchCount = document.getElementById("semantic-search-count");
  const semanticSearchResultsScroll = document.getElementById("semantic-search-results-scroll");
  const semanticSearchResultsContainer = document.getElementById("semantic-search-results");
  const mainViewTabList = document.querySelector('ul[role="tablist"][aria-label="Main views"]');

  // Keep the JSON modal outside tab panes so it can open from any active view.
  if (docJsonModalElement && docJsonModalElement.parentElement !== document.body) {
    document.body.appendChild(docJsonModalElement);
  }
  if (searchSectionModalElement && searchSectionModalElement.parentElement !== document.body) {
    document.body.appendChild(searchSectionModalElement);
  }
  if (pdfSplitModalElement && pdfSplitModalElement.parentElement !== document.body) {
    document.body.appendChild(pdfSplitModalElement);
  }

  const selectedBucketCookieName = "evidencebase_selected_bucket";
  const uiState = createStateStore({
    selectedBucketName: null,
    documents: [],
  });

  let citationSchema = getDefaultCitationSchema();
  let documentTypes = [...citationSchema.document_types];
  let bibtexFields = [...citationSchema.bibtex_fields];
  let bibtexTypeRules = { ...citationSchema.bibtex_type_rules };
  let crossrefLookupSeedFields = [...citationSchema.crossref_lookup_seed_fields];
  let minimalMetadataIdentityFields = [...citationSchema.minimal_metadata_identity_fields];
  let knownAuthorSuffixes = new Set(citationSchema.known_author_suffixes);
  let bulkBibtexFieldOrder = getBulkBibtexFieldOrderFromModule(bibtexFields, bibtexTypeRules);

  const applyCitationSchema = (rawSchema) => {
    citationSchema = normalizeCitationSchema(rawSchema);
    documentTypes = [...citationSchema.document_types];
    bibtexFields = [...citationSchema.bibtex_fields];
    bibtexTypeRules = { ...citationSchema.bibtex_type_rules };
    crossrefLookupSeedFields = [...citationSchema.crossref_lookup_seed_fields];
    minimalMetadataIdentityFields = [...citationSchema.minimal_metadata_identity_fields];
    knownAuthorSuffixes = new Set(citationSchema.known_author_suffixes);
    bulkBibtexFieldOrder = getBulkBibtexFieldOrderFromModule(bibtexFields, bibtexTypeRules);
  };
  const bibtexFieldLabelOverrides = {
    doi: "DOI",
    isbn: "ISBN",
    issn: "ISSN",
    month: "Month",
    year: "Year",
  };
  const statusBadgeMeta = {
    required: { label: "Required", className: "bibtex-status-required" },
    recommended: { label: "Recommended", className: "bibtex-status-recommended" },
    optional: { label: "Optional", className: "bibtex-status-optional" },
  };
  const processingStageLabels = {
    queued: "Queued",
    partition: "Partition",
    meta: "Metadata",
    section: "Section",
    chunk: "Chunk",
    upsert: "Upsert",
    processed: "Done",
    failed: "Failed",
  };
  const processingStageRanges = {
    partition: [0, 20],
    meta: [20, 40],
    section: [40, 60],
    chunk: [60, 80],
    upsert: [80, 100],
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

  const getRecordBibtexFieldValue = (record, fieldName) => {
    if (!record || typeof record !== "object") {
      return "";
    }
    const hasBibtexField =
      record.bibtex_fields &&
      typeof record.bibtex_fields === "object" &&
      Object.prototype.hasOwnProperty.call(record.bibtex_fields, fieldName);
    return normalizeText(hasBibtexField ? record.bibtex_fields[fieldName] : record[fieldName]);
  };

  const hasMissingRequiredBibtexFields = (record) => {
    const rules = bibtexTypeRules[normalizeDocumentType(record?.document_type)] || {
      required: [],
      recommended: [],
    };
    return (rules.required || []).some((fieldName) => !getRecordBibtexFieldValue(record, fieldName));
  };

  const hasOnlyLookupIdentityFields = (record) => {
    const nonEmptyBibtexFieldNames = bibtexFields.filter((fieldName) => getRecordBibtexFieldValue(record, fieldName));
    if (nonEmptyBibtexFieldNames.length === 0) {
      return false;
    }
    return nonEmptyBibtexFieldNames.every((fieldName) => minimalMetadataIdentityFields.includes(fieldName));
  };

  const hasCrossrefLookupSeed = (record) => {
    return crossrefLookupSeedFields.some((fieldName) => getRecordBibtexFieldValue(record, fieldName));
  };

  const shouldLookupMissingMetadataForRecord = (record) => {
    const missingRequiredFields = hasMissingRequiredBibtexFields(record);
    const hasOnlyIdentityFields = hasOnlyLookupIdentityFields(record);
    const hasLookupSeed = hasCrossrefLookupSeed(record);
    const matchedCriteria = missingRequiredFields || hasOnlyIdentityFields;
    return {
      matchedCriteria,
      shouldLookup: matchedCriteria && hasLookupSeed,
      missingRequiredFields,
      hasOnlyIdentityFields,
      hasLookupSeed,
    };
  };

  const getBibtexFieldStatus = (documentType, fieldName) => {
    return getBibtexFieldStatusFromModule({
      documentType,
      fieldName,
      bibtexTypeRules,
    });
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
  let searchSectionModalInstance = null;
  let pdfSplitModalInstance = null;
  let documentRefreshTimerId = null;
  const metadataSaveTimers = new Map();
  const metadataSaveDelayMs = 650;
  let bulkFetchMetaInProgress = false;
  let bulkCitationKeyUpdateInProgress = false;
  let semanticSearchResults = [];
  let searchSectionModalLoadToken = 0;
  let searchSectionNavigationState = {
    bucketName: "",
    documentId: "",
    sourceResult: null,
    sections: [],
    currentIndex: -1,
  };
  let pdfSplitState = {
    file: null,
    preview: null,
    selectedLevel: 1,
    folderName: "",
    folderNameTouched: false,
    metadataRecord: null,
    selectedOutputFiles: new Set(),
    uploadInProgress: false,
  };
  const isMobileViewport = () => window.matchMedia("(max-width: 991.98px)").matches;

  const setCookie = (name, value, maxAgeSeconds) => {
    setCookieValue(name, value, maxAgeSeconds);
  };

  const getCookie = (name) => {
    return getCookieValue(name);
  };

  const clearCookie = (name) => {
    clearCookieValue(name);
  };

  const setSelectedBucketName = (bucketName) => {
    flushPendingMetadataSaveTimers();
    selectedBucketName = bucketName;
    uiState.set("selectedBucketName", bucketName);
    if (bucketName) {
      setCookie(selectedBucketCookieName, bucketName, 60 * 60 * 24 * 365);
      setSemanticSearchStatus(`Selected collection: ${bucketName}.`);
    } else {
      clearCookie(selectedBucketCookieName);
      setSemanticSearchStatus("Select a collection and run a query.");
    }
    semanticSearchResults = [];
    renderSemanticSearchResults();
  };

  const normalizeText = (value) => {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value).trim();
  };

  const clampPercent = (value, fallback = 0) => {
    const parsed = Number.parseInt(normalizeText(value), 10);
    if (Number.isNaN(parsed)) {
      return fallback;
    }
    return Math.min(100, Math.max(0, parsed));
  };

  const inferProcessingStageFromProgress = (processingProgress) => {
    const percent = Number.isFinite(processingProgress)
      ? Math.min(100, Math.max(0, processingProgress))
      : 0;
    if (percent <= 20) {
      return "partition";
    }
    if (percent <= 40) {
      return "meta";
    }
    if (percent <= 60) {
      return "section";
    }
    if (percent <= 80) {
      return "chunk";
    }
    return "upsert";
  };

  const normalizeProcessingStage = ({ rawStage, processingState, processingProgress }) => {
    const normalizedState = normalizeText(processingState).toLowerCase();
    const normalizedRawStage = normalizeText(rawStage).toLowerCase();
    if (normalizedRawStage && Object.prototype.hasOwnProperty.call(processingStageLabels, normalizedRawStage)) {
      return normalizedRawStage;
    }
    if (normalizedState === "processed") {
      return "processed";
    }
    if (normalizedState === "failed") {
      return "failed";
    }
    return inferProcessingStageFromProgress(processingProgress);
  };

  const normalizeProcessingStageProgress = ({
    rawStageProgress,
    processingStage,
    processingProgress,
  }) => {
    const directStageProgress = clampPercent(rawStageProgress, -1);
    if (directStageProgress >= 0) {
      return directStageProgress;
    }
    if (processingStage === "processed" || processingStage === "failed") {
      return 100;
    }
    const stageRange = processingStageRanges[processingStage];
    if (!Array.isArray(stageRange) || stageRange.length !== 2) {
      return 0;
    }
    const start = stageRange[0];
    const end = stageRange[1];
    const width = Math.max(1, end - start);
    const normalizedProgress = Math.min(100, Math.max(0, processingProgress));
    const stageProgress = Math.round(((normalizedProgress - start) / width) * 100);
    return Math.min(100, Math.max(0, stageProgress));
  };

  const getProcessingStageLabel = (stageName) => {
    const normalizedStageName = normalizeText(stageName).toLowerCase();
    if (Object.prototype.hasOwnProperty.call(processingStageLabels, normalizedStageName)) {
      return processingStageLabels[normalizedStageName];
    }
    return "Processing";
  };

  const createDocumentProgressBar = (record, { minWidth = "" } = {}) => {
    const progressWrap = document.createElement("div");
    progressWrap.className = "progress document-processing-progress";
    if (minWidth) {
      progressWrap.style.minWidth = minWidth;
    }

    const progressBar = document.createElement("div");
    progressBar.className = "progress-bar progress-bar-striped progress-bar-animated";
    progressBar.style.width = `${record.processing_progress}%`;
    const stageLabel = getProcessingStageLabel(record.processing_stage);
    const barLabel = `${stageLabel} ${record.processing_progress}%`;
    progressBar.textContent = barLabel;
    progressBar.setAttribute("aria-valuetext", barLabel);
    progressBar.title = `Stage ${stageLabel} (${record.processing_stage_progress}%)`;

    progressWrap.appendChild(progressBar);
    return progressWrap;
  };

  const normalizeObjectPath = (value) => {
    return normalizeText(value)
      .replace(/\\/g, "/")
      .replace(/^\/+/, "")
      .replace(/\/+$/, "")
      .replace(/\/{2,}/g, "/");
  };

  const isPdfFile = (file) => {
    if (!file || typeof file !== "object") {
      return false;
    }
    return normalizeText(file.name).toLowerCase().endsWith(".pdf");
  };

  const normalizeDocumentType = (value) => {
    return normalizeDocumentTypeFromModule(value, documentTypes);
  };

  const filenameFromPath = (filePath) => {
    const normalized = normalizeText(filePath);
    if (!normalized) {
      return "document.pdf";
    }
    const splitParts = normalized.split("/");
    return splitParts[splitParts.length - 1] || normalized;
  };

  const getPdfSplitLevelPreview = (preview, level) => {
    if (!preview || !Array.isArray(preview.levels)) {
      return null;
    }
    return preview.levels.find((entry) => Number.parseInt(entry?.level, 10) === level) || null;
  };

  const parseMinioLocation = (value) => {
    const normalizedLocation = normalizeText(value).replace(/^\/+/, "");
    if (!normalizedLocation || !normalizedLocation.includes("/")) {
      return null;
    }
    const splitIndex = normalizedLocation.indexOf("/");
    const bucketName = normalizeText(normalizedLocation.slice(0, splitIndex));
    const filePath = normalizeText(normalizedLocation.slice(splitIndex + 1));
    if (!bucketName || !filePath) {
      return null;
    }
    return { bucketName, filePath };
  };

  const buildResolverHref = ({ bucketName, filePath, page, highlightText }) => {
    const normalizedBucketName = normalizeText(bucketName);
    const normalizedFilePath = normalizeText(filePath).replace(/^\/+/, "");
    if (!normalizedBucketName || !normalizedFilePath) {
      return "";
    }
    const params = new URLSearchParams({
      bucket: normalizedBucketName,
      file_path: normalizedFilePath,
    });
    const resolvedPage = Number.parseInt(page, 10);
    if (Number.isFinite(resolvedPage) && resolvedPage > 0) {
      params.set("page", String(resolvedPage));
    }
    const normalizedHighlight = normalizeText(highlightText).replace(/\s+/g, " ");
    if (normalizedHighlight) {
      const maxHighlightLength = 420;
      const truncatedHighlight =
        normalizedHighlight.length > maxHighlightLength
          ? `${normalizedHighlight.slice(0, maxHighlightLength - 1).trim()}…`
          : normalizedHighlight;
      params.set("highlight", truncatedHighlight);
    }
    return `${appBaseUrl}/resolver.html?${params.toString()}`;
  };

  const buildResolverHrefForSelectedDocument = (record) => {
    if (!record || typeof record !== "object") {
      return "";
    }
    return buildResolverHref({
      bucketName: selectedBucketName,
      filePath: record.file_path,
    });
  };

  const buildResolverHrefForDocumentRecord = (record) => {
    if (!record || typeof record !== "object") {
      return "";
    }
    const resolvedFromLocation = parseMinioLocation(record.minio_location);
    return buildResolverHref({
      bucketName: resolvedFromLocation?.bucketName || selectedBucketName,
      filePath: resolvedFromLocation?.filePath || record.file_path,
    });
  };

  const buildResolverHrefForSearchResult = (result) => {
    if (!result || typeof result !== "object") {
      return "";
    }
    const resolvedFromLocation = parseMinioLocation(result.minio_location);
    return buildResolverHref({
      bucketName: resolvedFromLocation?.bucketName || selectedBucketName,
      filePath: resolvedFromLocation?.filePath || result.file_path,
      page: result.page_start,
      highlightText: result.text,
    });
  };

  const isLikelyAuthorSuffix = (value) => {
    const normalizedValue = normalizeText(value).toLowerCase();
    if (!normalizedValue) {
      return false;
    }
    return knownAuthorSuffixes.has(normalizedValue);
  };

  const normalizeAuthorNameParts = (firstName, lastName, suffix) => {
    const normalized = {
      first_name: normalizeText(firstName),
      last_name: normalizeText(lastName),
      suffix: normalizeText(suffix),
    };
    if (!normalized.first_name && !normalized.last_name && !normalized.suffix) {
      return null;
    }
    return normalized;
  };

  const stripOuterBraces = (value) => {
    let normalizedValue = normalizeText(value);
    while (
      normalizedValue.startsWith("{") &&
      normalizedValue.endsWith("}") &&
      normalizedValue.length >= 2
    ) {
      normalizedValue = normalizeText(normalizedValue.slice(1, -1));
    }
    return normalizedValue;
  };

  const parseAuthorFromText = (value) => {
    const normalizedValue = stripOuterBraces(value);
    if (!normalizedValue) {
      return null;
    }

    if (normalizedValue.includes(",")) {
      const commaParts = normalizedValue
        .split(",")
        .map((part) => stripOuterBraces(part))
        .filter(Boolean);
      if (commaParts.length >= 3) {
        const lastName = commaParts[0];
        const suffix = commaParts[1];
        const firstName = commaParts.slice(2).join(", ");
        return normalizeAuthorNameParts(firstName, lastName, suffix);
      }
      if (commaParts.length === 2) {
        const lastName = commaParts[0];
        const firstName = commaParts[1];
        return normalizeAuthorNameParts(firstName, lastName, "");
      }
    }

    const tokens = normalizedValue.split(/\s+/).filter(Boolean);
    if (!tokens.length) {
      return null;
    }
    if (tokens.length === 1) {
      return normalizeAuthorNameParts("", tokens[0], "");
    }

    let suffix = "";
    let bodyTokens = [...tokens];
    const trailingToken = tokens[tokens.length - 1];
    if (isLikelyAuthorSuffix(trailingToken)) {
      suffix = trailingToken;
      bodyTokens = tokens.slice(0, -1);
    }
    if (!bodyTokens.length) {
      return normalizeAuthorNameParts("", "", suffix);
    }

    const lastName = bodyTokens[bodyTokens.length - 1];
    const firstName = bodyTokens.slice(0, -1).join(" ");
    return normalizeAuthorNameParts(firstName, lastName, suffix);
  };

  const normalizeAuthorEntry = (rawAuthor) => {
    if (!rawAuthor) {
      return null;
    }
    if (typeof rawAuthor === "string") {
      return parseAuthorFromText(rawAuthor);
    }
    if (typeof rawAuthor !== "object") {
      return null;
    }

    return normalizeAuthorNameParts(
      rawAuthor.first_name || rawAuthor.firstName || rawAuthor.first || rawAuthor.given || "",
      rawAuthor.last_name || rawAuthor.lastName || rawAuthor.last || rawAuthor.family || "",
      rawAuthor.suffix || rawAuthor.suffix_name || rawAuthor.suf || ""
    );
  };

  const normalizeAuthorEntries = (rawAuthors) => {
    if (!Array.isArray(rawAuthors)) {
      return [];
    }
    return rawAuthors
      .map((rawAuthor) => normalizeAuthorEntry(rawAuthor))
      .filter((authorEntry) => Boolean(authorEntry));
  };

  const parseStructuredAuthors = (value) => {
    if (Array.isArray(value)) {
      return normalizeAuthorEntries(value);
    }
    if (typeof value !== "string") {
      return [];
    }
    const normalizedValue = normalizeText(value);
    if (!normalizedValue) {
      return [];
    }
    try {
      const parsedValue = JSON.parse(normalizedValue);
      return normalizeAuthorEntries(parsedValue);
    } catch (error) {
      return [];
    }
  };

  const parseAuthorsFromText = (value) => {
    const normalizedValue = normalizeText(value);
    if (!normalizedValue) {
      return [];
    }
    const splitPattern = /\s+(?:and|&)\s+|\s*;\s*|\s*\n+\s*/i;
    return normalizedValue
      .split(splitPattern)
      .map((authorText) => parseAuthorFromText(authorText))
      .filter((authorEntry) => Boolean(authorEntry));
  };

  const resolveRecordAuthorEntries = (record) => {
    if (!record || typeof record !== "object") {
      return [];
    }

    const normalizedEntries = normalizeAuthorEntries(record.authors);
    if (normalizedEntries.length > 0) {
      return normalizedEntries;
    }

    const authorCandidates = [
      record.bibtex_fields && typeof record.bibtex_fields === "object"
        ? record.bibtex_fields.author
        : "",
      record.author,
    ];

    for (const candidate of authorCandidates) {
      const structuredEntries = parseStructuredAuthors(candidate);
      if (structuredEntries.length > 0) {
        return structuredEntries;
      }
      const parsedFromText = parseAuthorsFromText(candidate);
      if (parsedFromText.length > 0) {
        return parsedFromText;
      }
    }

    return [];
  };

  const getRecordAuthorDisplayHarvard = (record) => {
    const normalizedEntries = resolveRecordAuthorEntries(record);
    const harvardDisplay = formatAuthorsHarvard(normalizedEntries);
    if (harvardDisplay) {
      return harvardDisplay;
    }
    return normalizeText(record?.author);
  };

  const getAuthorInitials = (firstName) => {
    return normalizeText(firstName)
      .split(/\s+/)
      .filter(Boolean)
      .map((token) => {
        const firstLetter = token.match(/[A-Za-z]/);
        return firstLetter ? `${firstLetter[0].toUpperCase()}.` : "";
      })
      .filter(Boolean)
      .join(" ");
  };

  const formatAuthorHarvard = (authorEntry) => {
    if (!authorEntry || typeof authorEntry !== "object") {
      return "";
    }
    const firstName = normalizeText(authorEntry.first_name);
    const lastName = normalizeText(authorEntry.last_name);
    const suffix = normalizeText(authorEntry.suffix);
    const initials = getAuthorInitials(firstName);

    let formattedName = "";
    if (lastName && initials) {
      formattedName = `${lastName}, ${initials}`;
    } else if (lastName) {
      formattedName = lastName;
    } else if (firstName) {
      formattedName = firstName;
    }
    if (!formattedName) {
      return "";
    }
    if (suffix) {
      return `${formattedName}, ${suffix}`;
    }
    return formattedName;
  };

  const formatAuthorsHarvard = (authorEntries) => {
    const normalizedEntries = normalizeAuthorEntries(authorEntries);
    if (!normalizedEntries.length) {
      return "";
    }
    const formattedAuthors = normalizedEntries
      .map((authorEntry) => formatAuthorHarvard(authorEntry))
      .filter(Boolean);
    if (!formattedAuthors.length) {
      return "";
    }
    if (formattedAuthors.length === 1) {
      return formattedAuthors[0];
    }
    if (formattedAuthors.length === 2) {
      return `${formattedAuthors[0]} & ${formattedAuthors[1]}`;
    }
    return `${formattedAuthors.slice(0, -1).join(", ")} & ${formattedAuthors[formattedAuthors.length - 1]}`;
  };

  const formatAuthorBibtex = (authorEntry) => {
    if (!authorEntry || typeof authorEntry !== "object") {
      return "";
    }
    const firstName = normalizeText(authorEntry.first_name);
    const lastName = normalizeText(authorEntry.last_name);
    const suffix = normalizeText(authorEntry.suffix);

    if (lastName && suffix && firstName) {
      return `${lastName}, ${suffix}, ${firstName}`;
    }
    if (lastName && firstName) {
      return `${lastName}, ${firstName}`;
    }
    if (lastName && suffix) {
      return `${lastName}, ${suffix}`;
    }
    if (lastName) {
      return lastName;
    }
    if (firstName) {
      return firstName;
    }
    return "";
  };

  const formatAuthorsBibtex = (authorEntries) => {
    const normalizedEntries = normalizeAuthorEntries(authorEntries);
    if (!normalizedEntries.length) {
      return "";
    }
    return normalizedEntries.map((authorEntry) => formatAuthorBibtex(authorEntry)).filter(Boolean).join(" and ");
  };

  const normalizeAuthorEntriesFromDocument = (rawDocument, sourceBibtexFields) => {
    const candidates = [
      rawDocument.authors,
      rawDocument.author_entries,
      sourceBibtexFields.authors,
      sourceBibtexFields.author_entries,
      rawDocument.author,
      sourceBibtexFields.author,
    ];

    for (const candidate of candidates) {
      const structured = parseStructuredAuthors(candidate);
      if (structured.length) {
        return structured;
      }

      if (typeof candidate === "string") {
        const parsedFromText = parseAuthorsFromText(candidate);
        if (parsedFromText.length) {
          return parsedFromText;
        }
      }
      if (Array.isArray(candidate)) {
        const parsedFromArray = normalizeAuthorEntries(candidate);
        if (parsedFromArray.length) {
          return parsedFromArray;
        }
      }
    }

    return [];
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

  const normalizeBibtexFieldKey = (fieldName) => {
    return normalizeText(fieldName)
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");
  };

  const getBibtexFieldValueByAliases = (bibtexFieldsMap, aliases) => {
    if (!bibtexFieldsMap || typeof bibtexFieldsMap !== "object") {
      return "";
    }

    for (const alias of aliases) {
      const directValue = normalizeText(bibtexFieldsMap[alias]);
      if (directValue) {
        return directValue;
      }
    }

    const normalizedAliases = new Set(aliases.map((alias) => normalizeBibtexFieldKey(alias)));
    for (const [fieldName, fieldValue] of Object.entries(bibtexFieldsMap)) {
      if (!normalizedAliases.has(normalizeBibtexFieldKey(fieldName))) {
        continue;
      }
      const normalizedFieldValue = normalizeText(fieldValue);
      if (normalizedFieldValue) {
        return normalizedFieldValue;
      }
    }
    return "";
  };

  const derivePublicationFromBibtex = (bibtexFieldsMap) => {
    return (
      getBibtexFieldValueByAliases(bibtexFieldsMap, ["journal"]) ||
      getBibtexFieldValueByAliases(bibtexFieldsMap, ["booktitle", "book_title", "book title"]) ||
      getBibtexFieldValueByAliases(bibtexFieldsMap, ["publisher"]) ||
      getBibtexFieldValueByAliases(bibtexFieldsMap, ["institution"]) ||
      getBibtexFieldValueByAliases(bibtexFieldsMap, ["school"]) ||
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

    const normalizedAuthors = normalizeAuthorEntries(record.authors);
    if (normalizedAuthors.length > 0) {
      record.authors = normalizedAuthors;
      const bibtexAuthorDisplay = formatAuthorsBibtex(normalizedAuthors);
      record.author = bibtexAuthorDisplay;
      record.bibtex_fields.author = bibtexAuthorDisplay;
    } else {
      record.authors = parseAuthorsFromText(record.bibtex_fields.author);
      record.author = normalizeText(record.bibtex_fields.author);
    }

    const titleFromBibtex = normalizeText(record.bibtex_fields.title);
    record.title = titleFromBibtex || filenameFromPath(record.file_path);
    record.year = normalizeText(record.bibtex_fields.year);
    record.author = normalizeText(record.bibtex_fields.author);
    record.publication = derivePublicationFromBibtex(record.bibtex_fields);
  };

  const setBibtexFieldValue = (
    record,
    fieldName,
    value,
    { preserveStructuredAuthors = false } = {}
  ) => {
    if (!record || !bibtexFields.includes(fieldName)) {
      return;
    }
    const normalizedValue = normalizeText(value);
    if (!record.bibtex_fields || typeof record.bibtex_fields !== "object") {
      record.bibtex_fields = {};
    }
    if (fieldName === "author" && !preserveStructuredAuthors) {
      const structuredEntries = parseStructuredAuthors(normalizedValue);
      record.authors =
        structuredEntries.length > 0
          ? structuredEntries
          : parseAuthorsFromText(normalizedValue);
    }
    record[fieldName] = normalizedValue;
    record.bibtex_fields[fieldName] = normalizedValue;
    syncRecordCoreFields(record);
  };

  const normalizeCitationToken = (value) => {
    return normalizeText(value).toLowerCase().replace(/[^a-z0-9]+/g, "");
  };

  const extractCitationYearToken = (value) => {
    const normalizedValue = normalizeText(value);
    if (!normalizedValue) {
      return "";
    }
    const yearMatch = normalizedValue.match(/(?:19|20)\d{2}/);
    if (yearMatch) {
      return yearMatch[0];
    }
    const fallbackYearMatch = normalizedValue.match(/\d{4}/);
    return fallbackYearMatch ? fallbackYearMatch[0] : "";
  };

  const extractCitationFirstTitleWord = (value) => {
    const normalizedValue = stripOuterBraces(value);
    if (!normalizedValue) {
      return "";
    }
    const titleWordMatch = normalizedValue.match(/[A-Za-z0-9]+/);
    return titleWordMatch ? titleWordMatch[0] : "";
  };

  const extractCitationChapterTitleToken = (value) => {
    const normalizedValue = stripOuterBraces(value);
    if (!normalizedValue) {
      return "";
    }
    const chapterMatch = normalizedValue.match(
      /\bchapter\s*([0-9]+|[ivxlcdm]+)\b(?:\s*[-_:,.;)]*\s*([A-Za-z0-9]+))?/i
    );
    if (!chapterMatch) {
      return "";
    }
    const chapterNumber = normalizeCitationToken(chapterMatch[1]);
    const trailingWord = normalizeCitationToken(chapterMatch[2] || "");
    const chapterToken = `ch${chapterNumber}${trailingWord}`;
    return chapterToken === "ch" ? "" : chapterToken;
  };

  const extractCitationFirstAuthorLastName = (authorEntries, authorText) => {
    const normalizedEntries = normalizeAuthorEntries(authorEntries);
    if (normalizedEntries.length > 0) {
      const firstAuthor = normalizedEntries[0];
      return normalizeText(firstAuthor.last_name) || normalizeText(firstAuthor.first_name);
    }

    const normalizedAuthorText = normalizeText(authorText);
    if (!normalizedAuthorText) {
      return "";
    }
    const firstAuthorSegment = normalizedAuthorText.split(/\s+and\s+|\s*&\s*|\s*;\s*/i)[0];
    const parsedAuthor = parseAuthorFromText(firstAuthorSegment);
    if (!parsedAuthor) {
      return "";
    }
    return normalizeText(parsedAuthor.last_name) || normalizeText(parsedAuthor.first_name);
  };

  const buildFallbackCitationKey = ({
    filePath,
    index,
    authorEntries,
    authorText,
    title,
    year,
  }) => {
    return buildFallbackCitationKeyFromModule({
      filePath,
      index,
      authorLastName: extractCitationFirstAuthorLastName(authorEntries, authorText),
      title,
      year,
      filenameFromPath,
    });
  };

  const buildDefaultCitationKeyForRecord = (record) => {
    if (!record || typeof record !== "object") {
      return "";
    }
    const authorEntries = normalizeAuthorEntries(record.authors);
    const authorText = getRecordBibtexFieldValue(record, "author") || normalizeText(record.author);
    const yearText = getRecordBibtexFieldValue(record, "year") || normalizeText(record.year);
    const titleText =
      getRecordBibtexFieldValue(record, "title") ||
      normalizeText(record.title) ||
      filenameFromPath(record.file_path || "");
    const fileTitleText = filenameFromPath(record.file_path || "").replace(/\.[^/.]+$/, "");
    const authorToken = normalizeCitationToken(
      extractCitationFirstAuthorLastName(authorEntries, authorText)
    );
    const yearToken = extractCitationYearToken(yearText);
    const fileTitleToken = extractCitationChapterTitleToken(fileTitleText);
    const metadataTitleToken = normalizeCitationToken(extractCitationFirstTitleWord(titleText));
    const titleToken = fileTitleToken || metadataTitleToken;
    return `${authorToken}${yearToken}${titleToken}`;
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
    const normalizedAuthorEntries = normalizeAuthorEntriesFromDocument(rawDocument, sourceBibtexFields);
    const formattedAuthorDisplay = formatAuthorsHarvard(normalizedAuthorEntries);
    const citationFallbackAuthor = normalizeText(rawDocument.author) || sourceBibtexFields.author;
    const citationFallbackYear = normalizeText(rawDocument.year) || sourceBibtexFields.year;
    const citationFallbackTitle =
      normalizeText(rawDocument.title) ||
      sourceBibtexFields.title ||
      filenameFromPath(filePath).replace(/\.[^/.]+$/, "");
    const normalizedCitationKey =
      normalizeText(rawDocument.citation_key) ||
      normalizeText(rawDocument.citationKey) ||
      normalizeText(rawDocument.citekey) ||
      normalizeText(rawDocument.key) ||
      normalizeText(sourceBibtexFields.citation_key) ||
      normalizeText(sourceBibtexFields.citekey) ||
      buildFallbackCitationKey({
        filePath,
        index,
        authorEntries: normalizedAuthorEntries,
        authorText: citationFallbackAuthor,
        title: citationFallbackTitle,
        year: citationFallbackYear,
      });

    const processingStateSource = normalizeText(rawDocument.processing_state).toLowerCase();
    const normalizedProcessingState =
      processingStateSource === "processing" || processingStateSource === "failed"
        ? processingStateSource
        : "processed";
    const normalizedProcessingProgress = clampPercent(
      rawDocument.processing_progress,
      normalizedProcessingState === "processing" ? 0 : 100
    );
    const normalizedProcessingStage = normalizeProcessingStage({
      rawStage: rawDocument.processing_stage,
      processingState: normalizedProcessingState,
      processingProgress: normalizedProcessingProgress,
    });
    const normalizedProcessingStageProgress = normalizeProcessingStageProgress({
      rawStageProgress: rawDocument.processing_stage_progress,
      processingStage: normalizedProcessingStage,
      processingProgress: normalizedProcessingProgress,
    });
    const normalizedPartitionsCount = Number.parseInt(
      normalizeText(rawDocument.partitions_count),
      10
    );
    const normalizedChunksCount = Number.parseInt(normalizeText(rawDocument.chunks_count), 10);
    const normalizedSectionsCount = Number.parseInt(normalizeText(rawDocument.sections_count), 10);
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
      bulk_selected: false,
      file_path: filePath,
      citation_key: normalizedCitationKey,
      document_type: normalizeDocumentType(
        rawDocument.document_type || rawDocument.entrytype || sourceBibtexFields.entrytype
      ),
      processing_state: normalizedProcessingState,
      processing_stage: normalizedProcessingStage,
      processing_stage_progress: normalizedProcessingStageProgress,
      processing_progress: normalizedProcessingProgress,
      partitions_count: Number.isNaN(normalizedPartitionsCount) ? 0 : normalizedPartitionsCount,
      chunks_count: Number.isNaN(normalizedChunksCount) ? 0 : normalizedChunksCount,
      sections_count: Number.isNaN(normalizedSectionsCount) ? 0 : normalizedSectionsCount,
      partitions_tree: partitionsTree,
      chunks_tree: chunksTree,
      parse_status: normalizedProcessingState,
      bibtex_fields: {},
      authors: normalizedAuthorEntries,
    };

    bibtexFields.forEach((fieldName) => {
      const rawValue =
        fieldName === "author"
          ? formattedAuthorDisplay || normalizeText(sourceBibtexFields[fieldName]) || ""
          : normalizeText(rawDocument[fieldName]) || normalizeText(sourceBibtexFields[fieldName]) || "";
      record[fieldName] = rawValue;
      record.bibtex_fields[fieldName] = rawValue;
    });

    if (!record.bibtex_fields.title) {
      record.bibtex_fields.title = normalizeText(rawDocument.title);
      record.title = record.bibtex_fields.title;
    }
    if (!record.bibtex_fields.author) {
      record.bibtex_fields.author = formattedAuthorDisplay;
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

  const apiRequest = createApiRequest({ apiBasePath });

  const buildMetadataUpdatePayload = (record) => {
    if (!record || typeof record !== "object") {
      return {};
    }

    const payload = {
      document_type: normalizeDocumentType(record.document_type),
      citation_key: normalizeText(record.citation_key),
      authors: "",
    };

    const normalizedAuthors = normalizeAuthorEntries(record.authors);
    if (normalizedAuthors.length > 0) {
      payload.authors = JSON.stringify(normalizedAuthors);
    }

    bibtexFields.forEach((fieldName) => {
      const hasBibtexField =
        record.bibtex_fields &&
        typeof record.bibtex_fields === "object" &&
        Object.prototype.hasOwnProperty.call(record.bibtex_fields, fieldName);
      payload[fieldName] = normalizeText(
        hasBibtexField ? record.bibtex_fields[fieldName] : record[fieldName]
      );
    });
    return payload;
  };

  const metadataDiffFields = ["document_type", "citation_key", "authors", ...bibtexFields];

  const getMetadataFieldLabel = (fieldName) => {
    if (fieldName === "document_type") {
      return "Document Type";
    }
    if (fieldName === "citation_key") {
      return "Citation Key";
    }
    if (fieldName === "authors") {
      return "Authors";
    }
    return getBibtexFieldLabel(fieldName);
  };

  const toMetadataComparableValue = (value) => {
    if (value === null || value === undefined) {
      return "";
    }
    if (typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch (error) {
        return String(value);
      }
    }
    return normalizeText(value);
  };

  const buildMetadataSnapshotFromRecord = (record) => {
    const payload = buildMetadataUpdatePayload(record);
    const snapshot = {};
    metadataDiffFields.forEach((fieldName) => {
      snapshot[fieldName] = toMetadataComparableValue(payload[fieldName]);
    });
    return snapshot;
  };

  const buildMetadataSnapshotFromPayload = (metadataPayload) => {
    const source =
      metadataPayload && typeof metadataPayload === "object" ? metadataPayload : {};
    const snapshot = {};
    metadataDiffFields.forEach((fieldName) => {
      snapshot[fieldName] = toMetadataComparableValue(source[fieldName]);
    });
    return snapshot;
  };

  const getChangedMetadataFieldNames = (beforeSnapshot, afterSnapshot) => {
    return metadataDiffFields.filter((fieldName) => {
      return toMetadataComparableValue(beforeSnapshot?.[fieldName]) !== toMetadataComparableValue(afterSnapshot?.[fieldName]);
    });
  };

  const persistDocumentMetadata = async (record, { silent = true } = {}) => {
    if (
      !record ||
      typeof record !== "object" ||
      !selectedBucketName ||
      !record.document_id
    ) {
      return false;
    }
    const metadata = buildMetadataUpdatePayload(record);
    try {
      await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents/${encodeURIComponent(
          record.document_id
        )}/metadata`,
        {
          method: "PUT",
          body: JSON.stringify({ metadata }),
        }
      );
      return true;
    } catch (error) {
      if (!silent) {
        window.alert(`Could not save metadata: ${error.message}`);
      } else {
        console.error("Could not save metadata:", error);
      }
      return false;
    }
  };

  const persistDefaultCitationKeyForRecord = async (record, { silent = true } = {}) => {
    if (!record || typeof record !== "object") {
      return false;
    }
    const nextCitationKey = buildDefaultCitationKeyForRecord(record);
    const currentCitationKey = normalizeText(record.citation_key);
    if (!nextCitationKey || nextCitationKey === currentCitationKey) {
      return false;
    }
    record.citation_key = nextCitationKey;
    return persistDocumentMetadata(record, { silent });
  };

  const clearPendingMetadataSaveTimers = () => {
    metadataSaveTimers.forEach((pendingSave) => {
      window.clearTimeout(pendingSave.timerId);
    });
    metadataSaveTimers.clear();
  };

  const flushPendingMetadataSaveTimers = () => {
    metadataSaveTimers.forEach((pendingSave, documentId) => {
      window.clearTimeout(pendingSave.timerId);
      metadataSaveTimers.delete(documentId);
      void persistDocumentMetadata(pendingSave.record, { silent: true });
    });
  };

  const scheduleDocumentMetadataSave = (record, { delayMs = metadataSaveDelayMs } = {}) => {
    if (!record || typeof record !== "object" || !record.document_id) {
      return;
    }
    const documentId = record.document_id;
    const existingTimer = metadataSaveTimers.get(documentId);
    if (existingTimer) {
      window.clearTimeout(existingTimer.timerId);
      metadataSaveTimers.delete(documentId);
    }

    const timerId = window.setTimeout(() => {
      metadataSaveTimers.delete(documentId);
      void persistDocumentMetadata(record, { silent: true });
    }, Math.max(0, delayMs));
    metadataSaveTimers.set(documentId, { timerId, record });
  };

  const flushPendingMetadataSaveForRecord = async (record) => {
    if (!record || typeof record !== "object" || !record.document_id) {
      return true;
    }
    const pendingTimer = metadataSaveTimers.get(record.document_id);
    if (!pendingTimer) {
      return true;
    }
    window.clearTimeout(pendingTimer.timerId);
    metadataSaveTimers.delete(record.document_id);
    return persistDocumentMetadata(pendingTimer.record, { silent: true });
  };

  const setSemanticSearchStatus = (message) => {
    if (semanticSearchStatus) {
      semanticSearchStatus.textContent = message;
    }
  };

  const setSemanticSearchCount = (count) => {
    if (semanticSearchCount) {
      semanticSearchCount.textContent = `${count} result${count === 1 ? "" : "s"}`;
    }
  };

  const formatSearchPages = (pageStart, pageEnd) => {
    return formatSearchPagesFromModule(pageStart, pageEnd);
  };

  const formatSearchPageLabel = (pageStart, pageEnd) => {
    return formatSearchPageLabelFromModule(pageStart, pageEnd);
  };

  const formatSearchTitle = (result) => {
    return formatSearchTitleFromModule(result, filenameFromPath, normalizeText);
  };

  const formatSearchLocation = (result) => {
    return formatSearchLocationFromModule(result, normalizeText);
  };

  const formatSearchAuthorYear = (result) => {
    return formatSearchAuthorYearFromModule(result, normalizeText);
  };

  const getSearchResultQdrantPayload = (result) => {
    if (!result || typeof result !== "object") {
      return null;
    }
    const payload = result.qdrant_payload;
    return payload && typeof payload === "object" ? payload : null;
  };

  const resolveSearchResultParentSectionId = (result) => {
    const direct = normalizeText(result?.parent_section_id);
    if (direct) {
      return direct;
    }
    return normalizeText(getSearchResultQdrantPayload(result)?.parent_section_id);
  };

  const resolveSearchResultParentSectionIndex = (result) => {
    const rawValue =
      result?.parent_section_index ?? getSearchResultQdrantPayload(result)?.parent_section_index;
    const parsed = Number.parseInt(rawValue, 10);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return null;
    }
    return parsed;
  };

  const resolveSearchResultParentSectionTitle = (result) => {
    const direct = normalizeText(result?.parent_section_title);
    if (direct) {
      return direct;
    }
    const payloadTitle = normalizeText(getSearchResultQdrantPayload(result)?.parent_section_title);
    if (payloadTitle) {
      return payloadTitle;
    }
    return normalizeText(result?.section_title);
  };

  const resolveSearchResultParentSectionText = (result) => {
    const directMarkdown = result?.parent_section_markdown;
    if (directMarkdown !== null && directMarkdown !== undefined) {
      const text = String(directMarkdown).trim();
      if (text) {
        return text;
      }
    }
    const direct = result?.parent_section_text;
    if (direct !== null && direct !== undefined) {
      const text = String(direct).trim();
      if (text) {
        return text;
      }
    }
    const payloadText = getSearchResultQdrantPayload(result)?.parent_section_text;
    if (payloadText !== null && payloadText !== undefined) {
      const text = String(payloadText).trim();
      if (text) {
        return text;
      }
    }
    return "";
  };

  const escapeHtml = (value) => {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  };

  const renderSectionMarkdownHtml = (markdownText) => {
    const normalizedText = normalizeText(markdownText);
    if (!normalizedText) {
      return '<p class="text-body-secondary mb-0">Section text is unavailable.</p>';
    }

    const markedParser = window.marked;
    const domPurify = window.DOMPurify;
    if (
      markedParser &&
      typeof markedParser.parse === "function" &&
      domPurify &&
      typeof domPurify.sanitize === "function"
    ) {
      try {
        const rendered = markedParser.parse(normalizedText, {
          gfm: true,
          breaks: false,
          headerIds: false,
          mangle: false,
        });
        return domPurify.sanitize(String(rendered), {
          USE_PROFILES: { html: true },
          ADD_DATA_URI_TAGS: ["img"],
        });
      } catch (error) {
        console.warn("Failed to render markdown for section text.", error);
      }
    }

    return `<p>${escapeHtml(normalizedText).replace(/\n/g, "<br />")}</p>`;
  };

  const parseOptionalNonNegativeInt = (value) => {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return null;
    }
    return parsed;
  };

  const resolveSearchResultDocumentId = (result) => {
    const direct = normalizeText(result?.document_id);
    if (direct) {
      return direct;
    }
    const payload = getSearchResultQdrantPayload(result);
    const payloadDocumentId = normalizeText(payload?.document_id || payload?.doc_id);
    if (payloadDocumentId) {
      return payloadDocumentId;
    }
    return "";
  };

  const resolveSearchResultBucketName = (result) => {
    const fromLocation = parseMinioLocation(result?.minio_location);
    return normalizeText(fromLocation?.bucketName || selectedBucketName);
  };

  const normalizeDocumentSectionRecord = (section) => {
    if (!section || typeof section !== "object") {
      return null;
    }
    const sectionId = normalizeText(section.section_id || section.id);
    if (!sectionId) {
      return null;
    }
    return {
      section_id: sectionId,
      section_index: parseOptionalNonNegativeInt(section.section_index),
      section_title: normalizeText(section.section_title || section.title),
      section_markdown: normalizeText(section.section_markdown || section.markdown),
      section_text: normalizeText(section.section_text || section.text),
      page_start: parseOptionalNonNegativeInt(section.page_start),
      page_end: parseOptionalNonNegativeInt(section.page_end),
    };
  };

  const normalizeDocumentSectionsPayload = (payload) => {
    if (!payload || typeof payload !== "object" || !Array.isArray(payload.sections)) {
      return [];
    }
    return payload.sections
      .map((section, index) => {
        const normalized = normalizeDocumentSectionRecord(section);
        if (!normalized) {
          return null;
        }
        return { ...normalized, _inputOrder: index };
      })
      .filter((section) => Boolean(section))
      .sort((a, b) => {
        const aIndex = Number.isFinite(a.section_index) ? a.section_index : Number.MAX_SAFE_INTEGER;
        const bIndex = Number.isFinite(b.section_index) ? b.section_index : Number.MAX_SAFE_INTEGER;
        if (aIndex !== bIndex) {
          return aIndex - bIndex;
        }
        return a._inputOrder - b._inputOrder;
      })
      .map(({ _inputOrder, ...section }) => section);
  };

  const clearSearchSectionNavigationState = () => {
    searchSectionNavigationState = {
      bucketName: "",
      documentId: "",
      sourceResult: null,
      sections: [],
      currentIndex: -1,
    };
  };

  const updateSearchSectionModalNavigationButtons = () => {
    if (!searchSectionPrevButton || !searchSectionNextButton) {
      return;
    }
    const hasSections =
      Array.isArray(searchSectionNavigationState.sections) &&
      searchSectionNavigationState.sections.length > 0 &&
      searchSectionNavigationState.currentIndex >= 0;
    searchSectionPrevButton.disabled = !hasSections || searchSectionNavigationState.currentIndex <= 0;
    searchSectionNextButton.disabled =
      !hasSections ||
      searchSectionNavigationState.currentIndex >= searchSectionNavigationState.sections.length - 1;
  };

  const buildSearchModalSectionIdentifier = (sectionId, sectionIndex) => {
    if (sectionIndex === null) {
      return `Section ID: ${sectionId || "n/a"}`;
    }
    return `Section #${sectionIndex + 1} | Section ID: ${sectionId || "n/a"}`;
  };

  const renderSearchSectionModalSection = ({ section, result }) => {
    if (!searchSectionModalTitle || !searchSectionModalSubtitle || !searchSectionModalBody) {
      return;
    }
    const sectionTitle =
      normalizeText(section?.section_title) || resolveSearchResultParentSectionTitle(result) || "n/a";
    const sectionId = normalizeText(section?.section_id) || resolveSearchResultParentSectionId(result) || "n/a";
    const sectionIndex =
      parseOptionalNonNegativeInt(section?.section_index) ?? resolveSearchResultParentSectionIndex(result);
    const sectionIdentifier = buildSearchModalSectionIdentifier(sectionId, sectionIndex);
    const pages = formatSearchPages(
      section?.page_start ?? result?.page_start,
      section?.page_end ?? result?.page_end
    );
    const pageLabel = formatSearchPageLabel(
      section?.page_start ?? result?.page_start,
      section?.page_end ?? result?.page_end
    );
    const sectionText =
      normalizeText(section?.section_markdown || section?.section_text) ||
      resolveSearchResultParentSectionText(result) ||
      "Parent section text is unavailable for this chunk. Re-index this collection to populate parent sections.";

    searchSectionModalTitle.textContent = `Section: ${sectionTitle}`;
    searchSectionModalSubtitle.textContent = `${formatSearchTitle(result)} | ${sectionIdentifier} | ${pageLabel}: ${pages}`;
    searchSectionModalBody.innerHTML = renderSectionMarkdownHtml(sectionText);
    searchSectionModalBody.querySelectorAll("a[href]").forEach((anchor) => {
      anchor.setAttribute("target", "_blank");
      anchor.setAttribute("rel", "noopener noreferrer");
    });
  };

  const resolveCurrentSectionIndex = ({ sections, result }) => {
    if (!Array.isArray(sections) || !sections.length) {
      return -1;
    }

    const parentSectionId = resolveSearchResultParentSectionId(result);
    if (parentSectionId) {
      const byIdIndex = sections.findIndex((section) => section.section_id === parentSectionId);
      if (byIdIndex >= 0) {
        return byIdIndex;
      }
    }

    const parentSectionIndex = resolveSearchResultParentSectionIndex(result);
    if (parentSectionIndex !== null) {
      const byIndex = sections.findIndex((section) => section.section_index === parentSectionIndex);
      if (byIndex >= 0) {
        return byIndex;
      }
      if (parentSectionIndex >= 0 && parentSectionIndex < sections.length) {
        return parentSectionIndex;
      }
    }

    const parentSectionTitle = normalizeText(resolveSearchResultParentSectionTitle(result)).toLowerCase();
    if (parentSectionTitle) {
      const byTitle = sections.findIndex(
        (section) => normalizeText(section.section_title).toLowerCase() === parentSectionTitle
      );
      if (byTitle >= 0) {
        return byTitle;
      }
    }

    return 0;
  };

  const fetchDocumentSectionsForModal = async ({ bucketName, documentId }) => {
    if (!bucketName || !documentId) {
      return [];
    }
    const response = await apiRequest(
      `/collections/${encodeURIComponent(bucketName)}/documents/${encodeURIComponent(documentId)}/sections`
    );
    return normalizeDocumentSectionsPayload(response);
  };

  const navigateSearchSectionModal = (offset) => {
    const sections = searchSectionNavigationState.sections;
    if (!Array.isArray(sections) || !sections.length || !searchSectionNavigationState.sourceResult) {
      updateSearchSectionModalNavigationButtons();
      return;
    }
    const nextIndex = searchSectionNavigationState.currentIndex + offset;
    if (nextIndex < 0 || nextIndex >= sections.length) {
      updateSearchSectionModalNavigationButtons();
      return;
    }
    searchSectionNavigationState.currentIndex = nextIndex;
    renderSearchSectionModalSection({
      section: sections[nextIndex],
      result: searchSectionNavigationState.sourceResult,
    });
    updateSearchSectionModalNavigationButtons();
  };

  const renderSemanticSearchResults = () => {
    if (!semanticSearchResultsContainer) {
      return;
    }
    semanticSearchResultsContainer.innerHTML = "";

    if (!semanticSearchResults.length) {
      const emptyState = document.createElement("div");
      emptyState.className = "semantic-result-empty";
      emptyState.textContent = "No search results yet.";
      semanticSearchResultsContainer.appendChild(emptyState);
      setSemanticSearchCount(0);
      return;
    }

    semanticSearchResults.forEach((result) => {
      const card = document.createElement("article");
      card.className = "semantic-result-card";

      const header = document.createElement("header");
      header.className = "semantic-result-header";

      const title = document.createElement("div");
      title.className = "semantic-result-title";
      title.textContent = formatSearchTitle(result);

      const authorYear = document.createElement("div");
      authorYear.className = "semantic-result-meta-line text-body-secondary";
      authorYear.textContent = formatSearchAuthorYear(result);

      const location = document.createElement("div");
      location.className = "semantic-result-location text-body-secondary";
      const locationText = formatSearchLocation(result);
      const locationResolverHref = buildResolverHrefForSearchResult(result);
      if (locationResolverHref) {
        const locationLink = document.createElement("a");
        locationLink.href = locationResolverHref;
        locationLink.target = "_blank";
        locationLink.rel = "noopener noreferrer";
        locationLink.className = "link-secondary text-decoration-underline";
        locationLink.textContent = locationText;
        location.appendChild(locationLink);
      } else {
        location.textContent = locationText;
      }

      header.appendChild(title);
      header.appendChild(authorYear);
      header.appendChild(location);

      const body = document.createElement("div");
      body.className = "semantic-result-body";

      const sectionTitle = document.createElement("p");
      sectionTitle.className = "semantic-search-section-title";
      sectionTitle.textContent = `Section: ${normalizeText(result.section_title) || "n/a"} | ${formatSearchPageLabel(result.page_start, result.page_end)}: ${formatSearchPages(result.page_start, result.page_end)}`;
      body.appendChild(sectionTitle);

      const snippet = document.createElement("p");
      snippet.className = "semantic-search-snippet";
      snippet.textContent = normalizeText(result.text || "");
      body.appendChild(snippet);

      const footer = document.createElement("footer");
      footer.className = "semantic-result-footer d-flex justify-content-between align-items-center";

      const meta = document.createElement("div");
      meta.className = "d-flex align-items-center gap-3 flex-wrap";

      const score = document.createElement("span");
      score.className = "text-body-secondary";
      const scoreValue = Number.parseFloat(result.score);
      score.textContent = `Score: ${Number.isFinite(scoreValue) ? scoreValue.toFixed(4) : "0.0000"}`;

      meta.appendChild(score);

      const actions = document.createElement("div");
      actions.className = "d-flex align-items-center gap-2 flex-wrap";

      const sectionText = resolveSearchResultParentSectionText(result);
      const sectionButton = document.createElement("button");
      sectionButton.type = "button";
      sectionButton.className = "btn btn-outline-primary btn-sm";
      sectionButton.textContent = "View Section";
      if (sectionText) {
        sectionButton.addEventListener("click", () => {
          openSearchParentSectionModal(result);
        });
      } else {
        sectionButton.setAttribute("disabled", "disabled");
        sectionButton.title = "Section text unavailable. Re-index this collection to populate parent sections.";
      }
      actions.appendChild(sectionButton);

      const metadataButton = document.createElement("button");
      metadataButton.type = "button";
      metadataButton.className = "btn btn-outline-secondary btn-sm";
      metadataButton.textContent = "View Qdrant Metadata";
      metadataButton.addEventListener("click", () => {
        openSearchChunkMetadataModal(result);
      });
      actions.appendChild(metadataButton);

      footer.appendChild(meta);
      footer.appendChild(actions);
      card.appendChild(header);
      card.appendChild(body);
      card.appendChild(footer);
      semanticSearchResultsContainer.appendChild(card);
    });

    setSemanticSearchCount(semanticSearchResults.length);
    window.requestAnimationFrame(setIndependentScrollHeights);
  };

  const searchCollection = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const query = normalizeText(semanticSearchQueryInput?.value || "");
    if (!query) {
      window.alert("Enter a search query.");
      return;
    }

    const mode = normalizeText(semanticSearchModeSelect?.value || "hybrid").toLowerCase();
    const limit = Number.parseInt(semanticSearchLimitInput?.value || "10", 10);
    const params = new URLSearchParams({
      query,
      mode,
      limit: String(Number.isFinite(limit) ? limit : 10),
    });
    if (mode === "hybrid") {
      const rrfKValue = Number.parseInt(rrfKInput?.value || "60", 10);
      params.set("rrf_k", String(Number.isFinite(rrfKValue) ? Math.max(1, rrfKValue) : 60));
    }

    if (semanticSearchSubmit) {
      semanticSearchSubmit.setAttribute("disabled", "disabled");
    }
    setSemanticSearchStatus("Searching...");
    try {
      const payload = await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/search?${params.toString()}`
      );
      semanticSearchResults = Array.isArray(payload.results) ? payload.results : [];
      renderSemanticSearchResults();
      setSemanticSearchStatus(
        `Query "${query}" ran on ${selectedBucketName} using ${normalizeText(payload.mode || mode)} mode.`
      );
    } catch (error) {
      semanticSearchResults = [];
      renderSemanticSearchResults();
      setSemanticSearchStatus("Search failed.");
      window.alert(`Could not run search: ${error.message}`);
    } finally {
      if (semanticSearchSubmit) {
        semanticSearchSubmit.removeAttribute("disabled");
      }
    }
  };

  const syncSemanticSearchModeFields = () => {
    if (!semanticSearchModeSelect || !semanticRrfKField || !rrfKInput) {
      return;
    }
    const hybridEnabled = semanticSearchModeSelect.value === "hybrid";
    semanticRrfKField.classList.toggle("d-none", !hybridEnabled);
    rrfKInput.disabled = !hybridEnabled;
  };

  const resetSemanticSearchScroll = () => {
    if (semanticSearchPane) {
      semanticSearchPane.scrollTop = 0;
    }
    if (mainPanel) {
      mainPanel.scrollTop = 0;
    }
    if (appShell) {
      appShell.scrollTop = 0;
    }
    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;
    window.scrollTo(0, 0);
  };

  const setMainTabState = (nextPaneId) => {
    const tabDefinitions = [
      { tabButton: documentMetaTab, tabPane: documentMetaPane, paneId: "document-meta-pane" },
      { tabButton: semanticSearchTab, tabPane: semanticSearchPane, paneId: "semantic-search-pane" },
    ];

    tabDefinitions.forEach(({ tabButton, tabPane, paneId }) => {
      const isActive = paneId === nextPaneId;
      if (tabButton) {
        tabButton.classList.toggle("active", isActive);
        tabButton.setAttribute("aria-selected", String(isActive));
      }
      if (tabPane) {
        tabPane.classList.toggle("is-active", isActive);
        tabPane.hidden = !isActive;
        tabPane.setAttribute("aria-hidden", String(!isActive));
        tabPane.style.display = isActive ? "flex" : "none";
      }
    });
  };

  const clearDocumentRefreshTimer = () => {
    if (documentRefreshTimerId !== null) {
      window.clearTimeout(documentRefreshTimerId);
      documentRefreshTimerId = null;
    }
  };

  const scheduleDocumentRefresh = () => {
    clearDocumentRefreshTimer();
    if (!selectedBucketName) {
      return;
    }
    const hasInFlightDocument = documents.some(
      (documentRecord) => documentRecord.processing_state === "processing"
    );
    if (!hasInFlightDocument) {
      return;
    }
    documentRefreshTimerId = window.setTimeout(() => {
      void refreshDocuments({ silent: true, preserveSelection: true });
    }, 2500);
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
      if (semanticSearchResultsScroll) {
        semanticSearchResultsScroll.scrollTop = 0;
      }
      window.scrollTo(0, 0);
    }

    if (isMobileLayout) {
      clearElementHeight(collectionsListScroll);
      clearElementHeight(detailTableScroll);
      clearElementHeight(detailFieldsForm);
      clearElementHeight(detailViewContainer);
      clearElementHeight(documentHotWrapper);
      clearElementHeight(semanticSearchResultsScroll);
      if (documentTable) {
        documentTable.updateSettings({ height: 420 });
        documentTable.render();
      }
      wasMobileLayout = isMobileLayout;
      return;
    }

    calculateHeight(collectionsListScroll);
    calculateHeight(semanticSearchResultsScroll);
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

    toggleButton.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
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

  const openJsonModal = ({ title, subtitle, payload }) => {
    if (!docJsonModalElement || !docJsonModalTitle || !docJsonModalSubtitle || !docJsonModalTree) {
      return;
    }

    docJsonModalTitle.textContent = normalizeText(title) || "JSON Tree";
    docJsonModalSubtitle.textContent = normalizeText(subtitle);
    docJsonModalTree.innerHTML = "";

    const rootList = document.createElement("ul");
    rootList.appendChild(renderJsonTreeNode("root", payload));
    docJsonModalTree.appendChild(rootList);

    if (!docJsonModalInstance) {
      docJsonModalInstance = bootstrap.Modal.getOrCreateInstance(docJsonModalElement);
    }
    docJsonModalInstance.show();
  };

  const openDocJsonModal = (documentRecord, payloadType) => {
    const isPartitions = payloadType === "partitions";
    const payload = isPartitions ? documentRecord.partitions_tree : documentRecord.chunks_tree;
    const count = isPartitions ? documentRecord.partitions_count : documentRecord.chunks_count;
    openJsonModal({
      title: isPartitions ? "Partitions JSON Tree" : "Chunks JSON Tree",
      subtitle: `${filenameFromPath(documentRecord.file_path)} | ${
        isPartitions ? "Partitions" : "Chunks"
      }: ${count}`,
      payload,
    });
  };

  const buildDocumentSectionModalResult = (documentRecord) => {
    if (!documentRecord || typeof documentRecord !== "object") {
      return {};
    }
    return {
      document_id: documentRecord.document_id,
      file_path: documentRecord.file_path,
      title: documentRecord.title,
      minio_location: `${selectedBucketName}/${normalizeText(documentRecord.file_path).replace(/^\/+/, "")}`,
    };
  };

  const openDocumentSectionsModal = (documentRecord) => {
    if (
      !documentRecord ||
      typeof documentRecord !== "object" ||
      !searchSectionModalElement ||
      !searchSectionModalTitle ||
      !searchSectionModalSubtitle ||
      !searchSectionModalBody
    ) {
      return;
    }

    const sourceResult = buildDocumentSectionModalResult(documentRecord);
    searchSectionNavigationState = {
      bucketName: selectedBucketName,
      documentId: documentRecord.document_id,
      sourceResult,
      sections: [],
      currentIndex: -1,
    };
    searchSectionModalTitle.textContent = `Sections: ${filenameFromPath(documentRecord.file_path)}`;
    searchSectionModalSubtitle.textContent = `${normalizeText(documentRecord.title) || "n/a"} | Loading sections...`;
    searchSectionModalBody.innerHTML =
      '<p class="text-body-secondary mb-0">Loading section markdown from the document mapping...</p>';
    updateSearchSectionModalNavigationButtons();

    if (!searchSectionModalInstance) {
      searchSectionModalInstance = bootstrap.Modal.getOrCreateInstance(searchSectionModalElement);
    }
    searchSectionModalInstance.show();

    const requestToken = searchSectionModalLoadToken + 1;
    searchSectionModalLoadToken = requestToken;
    void (async () => {
      try {
        const sections = await fetchDocumentSectionsForModal({
          bucketName: searchSectionNavigationState.bucketName,
          documentId: searchSectionNavigationState.documentId,
        });
        if (searchSectionModalLoadToken !== requestToken) {
          return;
        }
        searchSectionNavigationState.sections = sections;
        if (!sections.length) {
          searchSectionNavigationState.currentIndex = -1;
          searchSectionModalTitle.textContent = `Sections: ${filenameFromPath(documentRecord.file_path)}`;
          searchSectionModalSubtitle.textContent = "No sections found for this document.";
          searchSectionModalBody.innerHTML =
            '<p class="text-body-secondary mb-0">Section markdown is unavailable. Re-index this document to populate section mappings.</p>';
          return;
        }
        searchSectionNavigationState.currentIndex = 0;
        renderSearchSectionModalSection({
          section: sections[0],
          result: searchSectionNavigationState.sourceResult,
        });
      } catch (error) {
        if (searchSectionModalLoadToken !== requestToken) {
          return;
        }
        console.warn("Failed to load sections for document modal.", error);
        searchSectionNavigationState.sections = [];
        searchSectionNavigationState.currentIndex = -1;
        searchSectionModalTitle.textContent = `Sections: ${filenameFromPath(documentRecord.file_path)}`;
        searchSectionModalSubtitle.textContent = "Failed to load document sections.";
        searchSectionModalBody.innerHTML =
          '<p class="text-danger mb-0">Could not load sections for this document. Try again after refreshing.</p>';
      } finally {
        if (searchSectionModalLoadToken === requestToken) {
          updateSearchSectionModalNavigationButtons();
        }
      }
    })();
  };

  const openSearchParentSectionModal = (result) => {
    if (
      !searchSectionModalElement ||
      !searchSectionModalTitle ||
      !searchSectionModalSubtitle ||
      !searchSectionModalBody
    ) {
      return;
    }
    const fallbackSection = {
      section_id: resolveSearchResultParentSectionId(result),
      section_index: resolveSearchResultParentSectionIndex(result),
      section_title: resolveSearchResultParentSectionTitle(result),
      section_markdown: normalizeText(result?.parent_section_markdown),
      section_text: resolveSearchResultParentSectionText(result),
      page_start: parseOptionalNonNegativeInt(result?.page_start),
      page_end: parseOptionalNonNegativeInt(result?.page_end),
    };
    const fallbackSections = fallbackSection.section_text ? [fallbackSection] : [];
    searchSectionNavigationState = {
      bucketName: resolveSearchResultBucketName(result),
      documentId: resolveSearchResultDocumentId(result),
      sourceResult: result,
      sections: fallbackSections,
      currentIndex: fallbackSections.length > 0 ? 0 : -1,
    };
    renderSearchSectionModalSection({
      section: fallbackSections.length > 0 ? fallbackSections[0] : null,
      result,
    });
    updateSearchSectionModalNavigationButtons();

    if (!searchSectionModalInstance) {
      searchSectionModalInstance = bootstrap.Modal.getOrCreateInstance(searchSectionModalElement);
    }
    searchSectionModalInstance.show();

    const requestToken = searchSectionModalLoadToken + 1;
    searchSectionModalLoadToken = requestToken;
    void (async () => {
      try {
        const sections = await fetchDocumentSectionsForModal({
          bucketName: searchSectionNavigationState.bucketName,
          documentId: searchSectionNavigationState.documentId,
        });
        if (searchSectionModalLoadToken !== requestToken || !sections.length) {
          return;
        }
        const currentIndex = resolveCurrentSectionIndex({ sections, result });
        searchSectionNavigationState.sections = sections;
        searchSectionNavigationState.currentIndex = currentIndex;
        if (currentIndex >= 0 && sections[currentIndex]) {
          renderSearchSectionModalSection({
            section: sections[currentIndex],
            result: searchSectionNavigationState.sourceResult,
          });
        }
      } catch (error) {
        console.warn("Failed to load section navigation data.", error);
      } finally {
        if (searchSectionModalLoadToken === requestToken) {
          updateSearchSectionModalNavigationButtons();
        }
      }
    })();
  };

  const openSearchChunkMetadataModal = (result) => {
    if (!result || typeof result !== "object") {
      return;
    }
    const rawPayload = result.qdrant_payload;
    const payload =
      rawPayload && typeof rawPayload === "object"
        ? rawPayload
        : {
            id: result.id,
            document_id: result.document_id,
            title: result.title,
            author: result.author,
            year: result.year,
            file_path: result.file_path,
            chunk_index: result.chunk_index,
            section_title: result.section_title,
            parent_section_id: result.parent_section_id,
            parent_section_index: result.parent_section_index,
            parent_section_title: result.parent_section_title,
            parent_section_text: result.parent_section_text,
            page_start: result.page_start,
            page_end: result.page_end,
            resolver_url: result.resolver_url,
            minio_location: result.minio_location,
          };
    openJsonModal({
      title: "Qdrant Chunk Metadata",
      subtitle: `${formatSearchTitle(result)} | Chunk ID: ${normalizeText(result.id) || "n/a"}`,
      payload,
    });
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
      td.appendChild(createDocumentProgressBar(record));
      return td;
    }

    if (record.processing_state === "failed") {
      const failedBadge = document.createElement("span");
      failedBadge.className = "badge text-bg-danger";
      failedBadge.textContent = "Failed";
      td.appendChild(failedBadge);
      return td;
    }

    const parseActions = document.createElement("div");
    parseActions.className = "btn-group";
    parseActions.role = "group";

    const pdfButton = document.createElement("button");
    pdfButton.type = "button";
    pdfButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    pdfButton.textContent = "pdf";
    pdfButton.dataset.pdfAction = "open";
    pdfButton.dataset.documentId = record.document_id;

    const partitionsButton = document.createElement("button");
    partitionsButton.type = "button";
    partitionsButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    partitionsButton.textContent = `P: ${record.partitions_count}`;
    partitionsButton.dataset.jsonAction = "partitions";
    partitionsButton.dataset.documentId = record.document_id;

    const sectionsButton = document.createElement("button");
    sectionsButton.type = "button";
    sectionsButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    sectionsButton.textContent = `S: ${record.sections_count}`;
    sectionsButton.dataset.sectionAction = "open";
    sectionsButton.dataset.documentId = record.document_id;

    const chunksButton = document.createElement("button");
    chunksButton.type = "button";
    chunksButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    chunksButton.textContent = `C: ${record.chunks_count}`;
    chunksButton.dataset.jsonAction = "chunks";
    chunksButton.dataset.documentId = record.document_id;

    parseActions.appendChild(pdfButton);
    parseActions.appendChild(partitionsButton);
    parseActions.appendChild(sectionsButton);
    parseActions.appendChild(chunksButton);
    td.appendChild(parseActions);

    return td;
  };

  const getBulkSelectAllHeaderCheckboxes = () => {
    if (!documentHotContainer) {
      return [];
    }
    return Array.from(documentHotContainer.querySelectorAll("input.bulk-select-all-checkbox"));
  };

  const setBulkSelectAllHeaderCheckboxState = ({
    checked = false,
    indeterminate = false,
    disabled = false,
  } = {}) => {
    const headerCheckboxes = getBulkSelectAllHeaderCheckboxes();
    if (headerCheckboxes.length <= 0) {
      return;
    }
    headerCheckboxes.forEach((checkbox) => {
      checkbox.disabled = disabled;
      checkbox.checked = checked;
      checkbox.indeterminate = indeterminate;
    });
  };

  const syncBulkSelectAllHeaderCheckbox = () => {
    const sourceData =
      documentTable && typeof documentTable.getSourceData === "function"
        ? documentTable.getSourceData()
        : [];
    const totalRows = Array.isArray(sourceData) ? sourceData.length : 0;
    const selectedRows =
      totalRows > 0
        ? sourceData.filter((record) => record && typeof record === "object" && Boolean(record.bulk_selected))
            .length
        : 0;
    setBulkSelectAllHeaderCheckboxState({
      disabled: totalRows <= 0,
      checked: totalRows > 0 && selectedRows === totalRows,
      indeterminate: selectedRows > 0 && selectedRows < totalRows,
    });
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
      { data: "bulk_selected", type: "checkbox", width: 118, className: "htCenter htMiddle" },
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
    ];
    const bulkHeaders = [
      "",
      "File Path",
      "Document Type",
      "Citation Key",
      ...bulkBibtexFieldOrder.map((fieldName) => getBibtexFieldLabel(fieldName)),
    ];

    documentTable = new Handsontable(documentHotContainer, {
      data: [],
      columns: bulkColumns,
      colHeaders: bulkHeaders,
      rowHeaders: true,
      width: "100%",
      stretchH: "none",
      columnSorting: true,
      dropdownMenu: ["filter_by_condition", "filter_by_value", "filter_action_bar"],
      filters: true,
      manualColumnResize: true,
      contextMenu: ["filter_by_condition", "filter_by_value", "filter_action_bar"],
      licenseKey: "non-commercial-and-evaluation",
      afterGetColHeader(col, TH) {
        if (col !== 0) {
          return;
        }
        const headerLabelHost = TH.querySelector(".colHeader") || TH;
        if (!headerLabelHost.querySelector("input.bulk-select-all-checkbox")) {
          headerLabelHost.textContent = "";
          const wrapper = document.createElement("div");
          wrapper.className = "d-flex justify-content-start align-items-center";

          const label = document.createElement("label");
          label.className = "d-inline-flex align-items-center gap-1 m-0";
          const stopHeaderInteraction = (event) => {
            event.stopPropagation();
          };
          wrapper.addEventListener("mousedown", stopHeaderInteraction);
          wrapper.addEventListener("click", stopHeaderInteraction);
          label.addEventListener("mousedown", stopHeaderInteraction);
          label.addEventListener("click", stopHeaderInteraction);

          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.className = "form-check-input m-0 bulk-select-all-checkbox";
          checkbox.setAttribute("aria-label", "Select all records");
          checkbox.addEventListener("mousedown", (event) => {
            event.stopPropagation();
          });
          checkbox.addEventListener("click", (event) => {
            event.stopPropagation();
          });

          const text = document.createElement("span");
          text.className = "small text-body-secondary";
          text.textContent = "Select all";

          label.appendChild(checkbox);
          label.appendChild(text);
          wrapper.appendChild(label);
          headerLabelHost.appendChild(wrapper);
        }
        window.requestAnimationFrame(syncBulkSelectAllHeaderCheckbox);
      },
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
        updateRemoveDocumentButtonState();
      },
      afterChange(changes, source) {
        if (!changes || source === "loadData") {
          return;
        }

        let shouldRefreshDetailTable = false;
        let shouldRefreshDetailForm = false;
        let shouldSyncBulkSelectionHeader = false;
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
            selectedDocumentId = record.document_id;
            record.document_type = nextType;
            if (nextType !== newValue) {
              documentTable.setDataAtRowProp(visualRow, "document_type", nextType, "normalize");
            }
            scheduleDocumentMetadataSave(record);
            shouldRefreshDetailForm = true;
            shouldRefreshDetailTable = true;
            return;
          }

          if (property === "citation_key") {
            selectedDocumentId = record.document_id;
            record.citation_key = normalizedValue;
            scheduleDocumentMetadataSave(record);
            shouldRefreshDetailForm = true;
            return;
          }

          if (property === "bulk_selected") {
            record.bulk_selected = Boolean(newValue);
            shouldSyncBulkSelectionHeader = true;
            return;
          }

          if (bibtexFields.includes(property)) {
            selectedDocumentId = record.document_id;
            setBibtexFieldValue(record, property, normalizedValue);
            scheduleDocumentMetadataSave(record);
            shouldRefreshDetailForm = true;
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
        if (shouldSyncBulkSelectionHeader) {
          syncBulkSelectAllHeaderCheckbox();
          updateRemoveDocumentButtonState();
        }
      },
    });
  };

  const getFilteredDocuments = () => {
    return filterDocumentsByTitle({
      documents,
      titleSearchQuery,
      normalizeText,
      filenameFromPath,
    });
  };

  const getSelectedDocument = () => {
    if (!selectedDocumentId) {
      return null;
    }
    return documents.find((documentRecord) => documentRecord.document_id === selectedDocumentId) || null;
  };

  const getActionTargetDocument = () => {
    const selectedDocument = getSelectedDocument();
    if (selectedDocument) {
      return selectedDocument;
    }
    if (!Array.isArray(visibleDocuments) || visibleDocuments.length <= 0) {
      return null;
    }
    const fallback = visibleDocuments[0];
    if (!fallback || !fallback.document_id) {
      return null;
    }
    selectedDocumentId = fallback.document_id;
    renderDetailTable();
    renderDetailFields();
    return fallback;
  };

  const ensureSelectedDocument = (candidateDocuments) => {
    selectedDocumentId = ensureSelectedDocumentId({
      candidateDocuments,
      selectedDocumentId,
    });
  };

  const renderParseStatusIntoCell = (cell, documentRecord) => {
    if (!cell) {
      return;
    }
    cell.innerHTML = "";
    cell.className = "text-nowrap";

    if (documentRecord.processing_state === "processing") {
      cell.appendChild(createDocumentProgressBar(documentRecord, { minWidth: "10.5rem" }));
      return;
    }

    if (documentRecord.processing_state === "failed") {
      const failedBadge = document.createElement("span");
      failedBadge.className = "badge text-bg-danger";
      failedBadge.textContent = "Failed";
      cell.appendChild(failedBadge);
      return;
    }

    const parseActions = document.createElement("div");
    parseActions.className = "btn-group";
    parseActions.role = "group";

    const pdfButton = document.createElement("button");
    pdfButton.type = "button";
    pdfButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    pdfButton.textContent = "pdf";
    pdfButton.dataset.pdfAction = "open";
    pdfButton.dataset.documentId = documentRecord.document_id;

    const partitionsButton = document.createElement("button");
    partitionsButton.type = "button";
    partitionsButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    partitionsButton.textContent = `P: ${documentRecord.partitions_count}`;
    partitionsButton.dataset.jsonAction = "partitions";
    partitionsButton.dataset.documentId = documentRecord.document_id;

    const sectionsButton = document.createElement("button");
    sectionsButton.type = "button";
    sectionsButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    sectionsButton.textContent = `S: ${documentRecord.sections_count}`;
    sectionsButton.dataset.sectionAction = "open";
    sectionsButton.dataset.documentId = documentRecord.document_id;

    const chunksButton = document.createElement("button");
    chunksButton.type = "button";
    chunksButton.className = "btn btn-outline-secondary btn-sm parse-action-btn";
    chunksButton.textContent = `C: ${documentRecord.chunks_count}`;
    chunksButton.dataset.jsonAction = "chunks";
    chunksButton.dataset.documentId = documentRecord.document_id;

    parseActions.appendChild(pdfButton);
    parseActions.appendChild(partitionsButton);
    parseActions.appendChild(sectionsButton);
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

  const getBulkSelectedDocuments = () => {
    if (!Array.isArray(documents) || documents.length <= 0) {
      return [];
    }
    return documents.filter((documentRecord) => Boolean(documentRecord?.bulk_selected));
  };

  const setBulkFetchProgress = (completedCount, totalCount) => {
    if (!bulkFetchProgress || !bulkFetchProgressBar || !bulkFetchProgressText) {
      return;
    }
    const normalizedTotalCount = Number.isFinite(totalCount) ? Math.max(0, totalCount) : 0;
    const safeTotalCount = normalizedTotalCount > 0 ? normalizedTotalCount : 1;
    const normalizedCompletedCount = Number.isFinite(completedCount)
      ? Math.max(0, Math.min(completedCount, safeTotalCount))
      : 0;
    const progressPercent = Math.round((normalizedCompletedCount / safeTotalCount) * 100);

    bulkFetchProgress.classList.remove("d-none");
    bulkFetchProgressBar.style.width = `${progressPercent}%`;
    bulkFetchProgressBar.textContent = `${progressPercent}%`;
    bulkFetchProgressBar.setAttribute("aria-valuenow", String(progressPercent));
    bulkFetchProgressText.textContent =
      normalizedTotalCount > 0
        ? `${normalizedCompletedCount} of ${normalizedTotalCount} processed`
        : "0 of 0 processed";
  };

  const resetBulkFetchProgress = () => {
    if (!bulkFetchProgress || !bulkFetchProgressBar || !bulkFetchProgressText) {
      return;
    }
    bulkFetchProgress.classList.add("d-none");
    bulkFetchProgressBar.style.width = "0%";
    bulkFetchProgressBar.textContent = "0%";
    bulkFetchProgressBar.setAttribute("aria-valuenow", "0");
    bulkFetchProgressText.textContent = "";
  };

  const updateRemoveDocumentButtonState = () => {
    const hasActionTarget =
      Boolean(getSelectedDocument()) || (Array.isArray(visibleDocuments) && visibleDocuments.length > 0);
    const actionsDisabled = !selectedBucketName || !hasActionTarget;
    const bulkSelectedCount = getBulkSelectedDocuments().length;
    const bulkMetaActionsDisabled =
      bulkFetchMetaInProgress || bulkCitationKeyUpdateInProgress || !selectedBucketName || bulkSelectedCount <= 0;
    const bulkRemoveSelectedDisabled = bulkCitationKeyUpdateInProgress || !selectedBucketName || bulkSelectedCount <= 0;
    if (removeDocumentButton) {
      removeDocumentButton.disabled = actionsDisabled;
    }
    if (fetchMetaButton) {
      fetchMetaButton.disabled = bulkMetaActionsDisabled;
    }
    if (updateCitationKeyButton) {
      updateCitationKeyButton.disabled = bulkMetaActionsDisabled;
    }
    if (removeSelectedDocumentsButton) {
      removeSelectedDocumentsButton.disabled = bulkRemoveSelectedDisabled;
    }
    if (downloadBibtexButton) {
      downloadBibtexButton.disabled = !selectedBucketName;
    }
    if (detailFetchMetaButton) {
      detailFetchMetaButton.disabled = actionsDisabled;
    }
    if (detailUpdateCitationKeyButton) {
      detailUpdateCitationKeyButton.disabled = actionsDisabled;
    }
    if (detailReindexButton) {
      detailReindexButton.disabled = actionsDisabled;
    }
    if (detailClearMetaButton) {
      detailClearMetaButton.disabled = actionsDisabled;
    }
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
      authorCell.textContent = getRecordAuthorDisplayHarvard(documentRecord) || "n/a";

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

  const setRecordAuthors = (record, nextAuthors) => {
    if (!record || typeof record !== "object") {
      return;
    }
    record.authors = normalizeAuthorEntries(nextAuthors);
    const bibtexAuthorDisplay = formatAuthorsBibtex(record.authors);
    setBibtexFieldValue(record, "author", bibtexAuthorDisplay, {
      preserveStructuredAuthors: true,
    });
    scheduleDocumentMetadataSave(record);
  };

  const createAuthorNameInput = (labelText, value, placeholder, onInput) => {
    const col = document.createElement("div");
    col.className = "col-12 col-md-4";

    const label = document.createElement("label");
    label.className = "form-label mb-1 small";
    label.textContent = labelText;

    const input = document.createElement("input");
    input.type = "text";
    input.className = "form-control form-control-sm";
    input.value = normalizeText(value);
    input.placeholder = placeholder;
    input.addEventListener("input", () => onInput(input.value));

    col.appendChild(label);
    col.appendChild(input);
    return col;
  };

  const createAuthorsFieldRow = (record, status) => {
    const row = document.createElement("div");
    row.className = "detail-field-row";
    if (status && statusBadgeMeta[status]) {
      row.classList.add(`detail-field-row-${status}`);
    }

    const labelWrap = document.createElement("div");
    labelWrap.className = "d-flex align-items-center justify-content-between gap-2 mb-1";

    const label = document.createElement("label");
    label.className = "form-label mb-0 fw-semibold small";
    label.textContent = "Authors";
    labelWrap.appendChild(label);

    if (status && statusBadgeMeta[status]) {
      const badge = document.createElement("span");
      badge.className = `badge rounded-pill bibtex-status-tag ${statusBadgeMeta[status].className}`;
      badge.textContent = statusBadgeMeta[status].label;
      labelWrap.appendChild(badge);
    }
    row.appendChild(labelWrap);

    const hint = document.createElement("p");
    hint.className = "small text-body-secondary mb-2";
    hint.textContent =
      "Uses BibTeX author format: Lastname, Suffix, Firstname and Lastname, Firstname.";
    row.appendChild(hint);

    const listWrap = document.createElement("div");
    listWrap.className = "authors-editor-list d-flex flex-column gap-2";
    row.appendChild(listWrap);

    const addAuthorButton = document.createElement("button");
    addAuthorButton.type = "button";
    addAuthorButton.className = "btn btn-outline-secondary btn-sm align-self-start mt-2";
    addAuthorButton.textContent = "Add Author";
    let authorEntries = [...resolveRecordAuthorEntries(record)];

    const renderAuthorRows = () => {
      listWrap.innerHTML = "";
      if (!authorEntries.length) {
        const emptyState = document.createElement("p");
        emptyState.className = "small text-body-secondary mb-0";
        emptyState.textContent = "No authors added.";
        listWrap.appendChild(emptyState);
        return;
      }

      authorEntries.forEach((authorEntry, index) => {
        const authorRow = document.createElement("div");
        authorRow.className = "authors-editor-row row g-2 align-items-end";

        authorRow.appendChild(
          createAuthorNameInput(
            "First Name",
            authorEntry.first_name,
            "First name",
            (nextValue) => {
              authorEntries[index].first_name = normalizeText(nextValue);
              setRecordAuthors(record, authorEntries);
              renderDetailTable();
            }
          )
        );
        authorRow.appendChild(
          createAuthorNameInput(
            "Last Name",
            authorEntry.last_name,
            "Last name",
            (nextValue) => {
              authorEntries[index].last_name = normalizeText(nextValue);
              setRecordAuthors(record, authorEntries);
              renderDetailTable();
            }
          )
        );
        authorRow.appendChild(
          createAuthorNameInput("Suffix", authorEntry.suffix, "Suffix", (nextValue) => {
            authorEntries[index].suffix = normalizeText(nextValue);
            setRecordAuthors(record, authorEntries);
            renderDetailTable();
          })
        );

        const actionsCol = document.createElement("div");
        actionsCol.className = "col-12 d-flex justify-content-end";
        const removeButton = document.createElement("button");
        removeButton.type = "button";
        removeButton.className = "btn btn-outline-danger btn-sm";
        removeButton.textContent = "Remove";
        removeButton.addEventListener("click", () => {
          authorEntries.splice(index, 1);
          setRecordAuthors(record, authorEntries);
          renderAuthorRows();
          renderDetailTable();
        });
        actionsCol.appendChild(removeButton);
        authorRow.appendChild(actionsCol);

        listWrap.appendChild(authorRow);
      });
    };

    addAuthorButton.addEventListener("click", () => {
      authorEntries.push({ first_name: "", last_name: "", suffix: "" });
      renderAuthorRows();
    });

    row.appendChild(addAuthorButton);
    renderAuthorRows();
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

    detailSelectedDocument.textContent = "";
    const selectedPrefix = document.createElement("span");
    selectedPrefix.textContent = "Selected: ";
    detailSelectedDocument.appendChild(selectedPrefix);
    const selectedFileName = filenameFromPath(selectedDocument.file_path);
    const selectedResolverHref = buildResolverHrefForSelectedDocument(selectedDocument);
    if (selectedResolverHref) {
      const selectedLink = document.createElement("a");
      selectedLink.href = selectedResolverHref;
      selectedLink.target = "_blank";
      selectedLink.rel = "noopener noreferrer";
      selectedLink.className = "link-secondary text-decoration-underline";
      selectedLink.textContent = selectedFileName;
      selectedLink.title = normalizeText(selectedDocument.file_path);
      detailSelectedDocument.appendChild(selectedLink);
    } else {
      detailSelectedDocument.append(selectedFileName);
    }

    detailFieldsForm.appendChild(
      createFieldRow("Document Type", selectedDocument.document_type, {
        type: "select",
        selectOptions: documentTypes,
        onInput(nextValue) {
          selectedDocument.document_type = normalizeDocumentType(nextValue);
          scheduleDocumentMetadataSave(selectedDocument);
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
          scheduleDocumentMetadataSave(selectedDocument);
        },
      })
    );

    getOrderedDetailBibtexFields(selectedDocument.document_type).forEach((fieldName) => {
      if (fieldName === "author") {
        detailFieldsForm.appendChild(
          createAuthorsFieldRow(
            selectedDocument,
            getBibtexFieldStatus(selectedDocument.document_type, fieldName)
          )
        );
        return;
      }
      detailFieldsForm.appendChild(
        createFieldRow(getBibtexFieldLabel(fieldName), selectedDocument[fieldName], {
          placeholder: fieldName,
          status: getBibtexFieldStatus(selectedDocument.document_type, fieldName),
          onInput(nextValue) {
            setBibtexFieldValue(selectedDocument, fieldName, nextValue);
            scheduleDocumentMetadataSave(selectedDocument);
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
    window.requestAnimationFrame(syncBulkSelectAllHeaderCheckbox);
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

    updateRemoveDocumentButtonState();
    window.requestAnimationFrame(setIndependentScrollHeights);
  };

  const renderDocumentMeta = () => {
    renderDocumentTable();
  };

  const refreshDocuments = async ({ silent = false, preserveSelection = true } = {}) => {
    if (!selectedBucketName) {
      clearDocumentRefreshTimer();
      documents = [];
      uiState.set("documents", []);
      selectedDocumentId = null;
      renderDocumentMeta();
      return;
    }

    try {
      const payload = await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents`
      );
      const rawDocuments = Array.isArray(payload.documents) ? payload.documents : [];
      documents = rawDocuments.map((rawDocument, index) => normalizeDocumentRecord(rawDocument, index));
      uiState.set("documents", documents);

      if (!preserveSelection) {
        selectedDocumentId = null;
      } else if (
        selectedDocumentId &&
        !documents.some((documentRecord) => documentRecord.document_id === selectedDocumentId)
      ) {
        selectedDocumentId = null;
      }

      renderDocumentMeta();
      scheduleDocumentRefresh();
    } catch (error) {
      clearDocumentRefreshTimer();
      if (!silent) {
        window.alert(`Could not load documents: ${error.message}`);
      }
    }
  };

  const loadCitationSchema = async () => {
    try {
      const payload = await apiRequest("/metadata/schema");
      applyCitationSchema(payload);
    } catch (error) {
      console.warn("Could not load metadata schema from API, using frontend defaults.", error);
      applyCitationSchema(getDefaultCitationSchema());
    }
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
      uiState.set("documents", []);
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

  const uploadDocumentFile = async (
    file,
    { objectName = "", refreshDocumentsAfterUpload = true, suppressErrorAlert = false } = {}
  ) => {
    if (!selectedBucketName) {
      if (!suppressErrorAlert) {
        window.alert("Select a collection first.");
      }
      return {
        ok: false,
        objectName: "",
        error: new Error("Select a collection first."),
      };
    }
    if (!file) {
      return {
        ok: false,
        objectName: "",
        error: new Error("No file was selected."),
      };
    }

    const normalizedObjectName = normalizeObjectPath(objectName || file.name);
    if (!normalizedObjectName) {
      const fileName = normalizeText(file.name) || "file";
      const error = new Error(`Invalid upload path for '${fileName}'.`);
      if (!suppressErrorAlert) {
        window.alert(error.message);
      }
      return {
        ok: false,
        objectName: normalizedObjectName,
        error,
      };
    }

    try {
      await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents/upload?file_name=${encodeURIComponent(
          normalizedObjectName
        )}`,
        {
          method: "POST",
          body: file,
          headers: {
            "Content-Type": file.type || "application/octet-stream",
          },
        }
      );
      if (refreshDocumentsAfterUpload) {
        await refreshDocuments({ silent: true, preserveSelection: true });
      }
      return {
        ok: true,
        objectName: normalizedObjectName,
        error: null,
      };
    } catch (error) {
      if (!suppressErrorAlert) {
        const fileName = filenameFromPath(normalizedObjectName);
        window.alert(`Could not upload file '${fileName}': ${error.message}`);
      }
      return {
        ok: false,
        objectName: normalizedObjectName,
        error,
      };
    }
  };

  const sanitizePdfSplitSegment = (value, fallback = "document") => {
    const cleanedValue = normalizeText(value)
      .replace(/[\x00-\x1f\x7f/\\]+/g, " ")
      .replace(/\s+/g, " ")
      .replace(/^[.\s]+|[.\s]+$/g, "");
    if (cleanedValue) {
      return cleanedValue;
    }
    const cleanedFallback = normalizeText(fallback)
      .replace(/[\x00-\x1f\x7f/\\]+/g, " ")
      .replace(/\s+/g, " ")
      .replace(/^[.\s]+|[.\s]+$/g, "");
    return cleanedFallback || "document";
  };

  const getPdfSplitFallbackFolderName = () => {
    if (pdfSplitState.preview) {
      const previewFolderName = normalizeText(pdfSplitState.preview.folder_name);
      if (previewFolderName) {
        return previewFolderName;
      }
    }
    const fileName = pdfSplitState.file ? filenameFromPath(pdfSplitState.file.name) : "document.pdf";
    const fileStem = normalizeText(fileName.replace(/\.[^.]+$/, ""));
    return fileStem || "document";
  };

  const getResolvedPdfSplitFolderName = () => {
    return sanitizePdfSplitSegment(pdfSplitState.folderName, getPdfSplitFallbackFolderName());
  };

  const createPdfSplitMetadataRecord = ({ file, preview }) => {
    const filePath = normalizeText(file?.name) || "document.pdf";
    const metadataSeed = preview && typeof preview.metadata_seed === "object" ? preview.metadata_seed : {};
    const record = {
      document_type: "incollection",
      citation_key: "",
      authors: [],
      file_path: filePath,
      bibtex_fields: {},
    };
    bibtexFields.forEach((fieldName) => {
      record[fieldName] = "";
      record.bibtex_fields[fieldName] = "";
    });
    const defaultBookTitle =
      normalizeText(metadataSeed.booktitle) ||
      normalizeText(metadataSeed.title) ||
      normalizeText(preview?.pdf_title) ||
      normalizeText(filenameFromPath(filePath).replace(/\.[^.]+$/, ""));
    setBibtexFieldValue(record, "booktitle", defaultBookTitle);
    const structuredAuthors = parseStructuredAuthors(metadataSeed.authors);
    if (structuredAuthors.length) {
      record.authors = structuredAuthors;
      setBibtexFieldValue(record, "author", formatAuthorsBibtex(structuredAuthors), {
        preserveStructuredAuthors: true,
      });
    } else if (normalizeText(metadataSeed.author)) {
      setBibtexFieldValue(record, "author", metadataSeed.author);
    }
    bibtexFields
      .filter((fieldName) => !["title", "author", "booktitle"].includes(fieldName))
      .forEach((fieldName) => {
      const fieldValue = normalizeText(metadataSeed[fieldName]);
      if (fieldValue) {
        setBibtexFieldValue(record, fieldName, fieldValue);
      }
    });
    syncRecordCoreFields(record);
    return record;
  };

  const getPdfSplitSelectedFilesForCurrentLevel = () => {
    const levelPreview = getPdfSplitLevelPreview(pdfSplitState.preview, pdfSplitState.selectedLevel);
    const splits = Array.isArray(levelPreview?.splits) ? levelPreview.splits : [];
    return splits
      .map((split) => normalizeText(split.file_name))
      .filter((fileName) => pdfSplitState.selectedOutputFiles.has(fileName));
  };

  const selectAllPdfSplitFilesForCurrentLevel = () => {
    const levelPreview = getPdfSplitLevelPreview(pdfSplitState.preview, pdfSplitState.selectedLevel);
    const splits = Array.isArray(levelPreview?.splits) ? levelPreview.splits : [];
    pdfSplitState.selectedOutputFiles = new Set(
      splits.map((split) => normalizeText(split.file_name)).filter(Boolean)
    );
  };

  const clearAllPdfSplitFilesForCurrentLevel = () => {
    const levelPreview = getPdfSplitLevelPreview(pdfSplitState.preview, pdfSplitState.selectedLevel);
    const splits = Array.isArray(levelPreview?.splits) ? levelPreview.splits : [];
    splits.forEach((split) => {
      const splitFileName = normalizeText(split.file_name);
      if (splitFileName) {
        pdfSplitState.selectedOutputFiles.delete(splitFileName);
      }
    });
  };

  const getResolvedPdfSplitBookTitle = () => {
    const record = pdfSplitState.metadataRecord;
    const recordBookTitle = record ? getRecordBibtexFieldValue(record, "booktitle") : "";
    const recordTitle = record ? getRecordBibtexFieldValue(record, "title") : "";
    const fallbackTitle =
      normalizeText(pdfSplitState.preview?.pdf_title) ||
      normalizeText(pdfSplitState.file ? filenameFromPath(pdfSplitState.file.name).replace(/\.[^.]+$/, "") : "");
    return normalizeText(recordBookTitle) || normalizeText(recordTitle) || fallbackTitle || "document";
  };

  const updatePdfSplitModalSummary = () => {
    if (pdfSplitModalSubtitle) {
      const fileName = normalizeText(pdfSplitState.file?.name);
      const folderName = getResolvedPdfSplitFolderName();
      pdfSplitModalSubtitle.textContent =
        fileName && selectedBucketName ? `${fileName} -> ${selectedBucketName}/${folderName}` : "";
    }
    if (pdfSplitPdfTitle) {
      pdfSplitPdfTitle.textContent = getResolvedPdfSplitBookTitle();
    }
    if (pdfSplitFolderInput) {
      pdfSplitFolderInput.value = normalizeText(pdfSplitState.folderName);
    }
  };

  const setPdfSplitModalBusyState = (busy, statusText = "") => {
    pdfSplitState.uploadInProgress = busy;
    if (pdfSplitUploadButton) {
      pdfSplitUploadButton.disabled = busy || !pdfSplitState.preview;
      pdfSplitUploadButton.textContent = busy ? "Creating Split Folder..." : "Create Split Folder";
    }
    if (pdfSplitFolderInput) {
      pdfSplitFolderInput.disabled = busy;
    }
    if (pdfSplitMetadataTab) {
      pdfSplitMetadataTab.disabled = busy;
    }
    if (pdfSplitMetadataFields) {
      pdfSplitMetadataFields.querySelectorAll("input, select, textarea, button").forEach((control) => {
        control.disabled = busy;
      });
    }
    if (pdfSplitLevelOptions) {
      pdfSplitLevelOptions.querySelectorAll('input[name="pdf-split-level"]').forEach((input) => {
        input.disabled = busy || input.dataset.available !== "true";
      });
    }
    if (pdfSplitStatus) {
      pdfSplitStatus.textContent = normalizeText(statusText);
    }
  };

  const clearPdfSplitState = () => {
    pdfSplitState = {
      file: null,
      preview: null,
      selectedLevel: 1,
      folderName: "",
      folderNameTouched: false,
      metadataRecord: null,
      selectedOutputFiles: new Set(),
      uploadInProgress: false,
    };
    if (pdfSplitModalTitle) {
      pdfSplitModalTitle.textContent = "Split PDF by Chapter";
    }
    if (pdfSplitModalSubtitle) {
      pdfSplitModalSubtitle.textContent = "";
    }
    if (pdfSplitPdfTitle) {
      pdfSplitPdfTitle.textContent = "";
    }
    if (pdfSplitFolderInput) {
      pdfSplitFolderInput.value = "";
    }
    if (pdfSplitPageCount) {
      pdfSplitPageCount.textContent = "";
    }
    if (pdfSplitPreviewCount) {
      pdfSplitPreviewCount.textContent = "";
    }
    if (pdfSplitPreviewBody) {
      pdfSplitPreviewBody.innerHTML = "";
    }
    if (pdfSplitSelectAllFiles) {
      pdfSplitSelectAllFiles.checked = false;
      pdfSplitSelectAllFiles.indeterminate = false;
      pdfSplitSelectAllFiles.disabled = false;
    }
    if (pdfSplitLevelOptions) {
      pdfSplitLevelOptions.innerHTML = "";
    }
    if (pdfSplitMetadataFields) {
      pdfSplitMetadataFields.innerHTML = "";
    }
    if (pdfSplitPreviewTab) {
      bootstrap.Tab.getOrCreateInstance(pdfSplitPreviewTab).show();
    }
    setPdfSplitModalBusyState(false, "");
  };

  const setPdfSplitMetadataFieldValue = (fieldName, value) => {
    const record = pdfSplitState.metadataRecord;
    if (!record) {
      return;
    }
    if (fieldName === "document_type") {
      record.document_type = normalizeDocumentType(value);
      syncRecordCoreFields(record);
      renderPdfSplitMetadataFields();
      return;
    }
    if (fieldName === "citation_key") {
      record.citation_key = normalizeText(value);
    } else {
      setBibtexFieldValue(record, fieldName, value);
      if (fieldName === "booktitle" && !pdfSplitState.folderNameTouched) {
        pdfSplitState.folderName = sanitizePdfSplitSegment(value, getPdfSplitFallbackFolderName());
      }
    }
    updatePdfSplitModalSummary();
    renderPdfSplitPreview();
  };

  const setPdfSplitMetadataAuthors = (authorEntries) => {
    const record = pdfSplitState.metadataRecord;
    if (!record) {
      return;
    }
    record.authors = normalizeAuthorEntries(authorEntries);
    const bibtexAuthorDisplay = formatAuthorsBibtex(record.authors);
    setBibtexFieldValue(record, "author", bibtexAuthorDisplay, {
      preserveStructuredAuthors: true,
    });
    updatePdfSplitModalSummary();
  };

  const createPdfSplitAuthorsFieldRow = (record, status) => {
    const row = document.createElement("div");
    row.className = "detail-field-row";
    if (status && statusBadgeMeta[status]) {
      row.classList.add(`detail-field-row-${status}`);
    }

    const labelWrap = document.createElement("div");
    labelWrap.className = "d-flex align-items-center justify-content-between gap-2 mb-1";

    const label = document.createElement("label");
    label.className = "form-label mb-0 fw-semibold small";
    label.textContent = "Authors";
    labelWrap.appendChild(label);

    if (status && statusBadgeMeta[status]) {
      const badge = document.createElement("span");
      badge.className = `badge rounded-pill bibtex-status-tag ${statusBadgeMeta[status].className}`;
      badge.textContent = statusBadgeMeta[status].label;
      labelWrap.appendChild(badge);
    }
    row.appendChild(labelWrap);

    const hint = document.createElement("p");
    hint.className = "small text-body-secondary mb-2";
    hint.textContent =
      "Uses BibTeX author format: Lastname, Suffix, Firstname and Lastname, Firstname.";
    row.appendChild(hint);

    const listWrap = document.createElement("div");
    listWrap.className = "authors-editor-list d-flex flex-column gap-2";
    row.appendChild(listWrap);

    const addAuthorButton = document.createElement("button");
    addAuthorButton.type = "button";
    addAuthorButton.className = "btn btn-outline-secondary btn-sm align-self-start mt-2";
    addAuthorButton.textContent = "Add Author";
    let authorEntries = [...resolveRecordAuthorEntries(record)];

    const renderAuthorRows = () => {
      listWrap.innerHTML = "";
      if (!authorEntries.length) {
        const emptyState = document.createElement("p");
        emptyState.className = "small text-body-secondary mb-0";
        emptyState.textContent = "No authors added.";
        listWrap.appendChild(emptyState);
        return;
      }

      authorEntries.forEach((authorEntry, index) => {
        const authorRow = document.createElement("div");
        authorRow.className = "authors-editor-row row g-2 align-items-end";

        authorRow.appendChild(
          createAuthorNameInput("First Name", authorEntry.first_name, "First name", (nextValue) => {
            authorEntries[index].first_name = normalizeText(nextValue);
            setPdfSplitMetadataAuthors(authorEntries);
          })
        );
        authorRow.appendChild(
          createAuthorNameInput("Last Name", authorEntry.last_name, "Last name", (nextValue) => {
            authorEntries[index].last_name = normalizeText(nextValue);
            setPdfSplitMetadataAuthors(authorEntries);
          })
        );
        authorRow.appendChild(
          createAuthorNameInput("Suffix", authorEntry.suffix, "Suffix", (nextValue) => {
            authorEntries[index].suffix = normalizeText(nextValue);
            setPdfSplitMetadataAuthors(authorEntries);
          })
        );

        const actionsCol = document.createElement("div");
        actionsCol.className = "col-12 d-flex justify-content-end";
        const removeButton = document.createElement("button");
        removeButton.type = "button";
        removeButton.className = "btn btn-outline-danger btn-sm";
        removeButton.textContent = "Remove";
        removeButton.addEventListener("click", () => {
          authorEntries.splice(index, 1);
          setPdfSplitMetadataAuthors(authorEntries);
          renderAuthorRows();
        });
        actionsCol.appendChild(removeButton);
        authorRow.appendChild(actionsCol);

        listWrap.appendChild(authorRow);
      });
    };

    addAuthorButton.addEventListener("click", () => {
      authorEntries.push({ first_name: "", last_name: "", suffix: "" });
      setPdfSplitMetadataAuthors(authorEntries);
      renderAuthorRows();
    });

    row.appendChild(addAuthorButton);
    renderAuthorRows();
    return row;
  };

  const renderPdfSplitMetadataFields = () => {
    if (!pdfSplitMetadataFields) {
      return;
    }
    const record = pdfSplitState.metadataRecord;
    pdfSplitMetadataFields.innerHTML = "";
    if (!record) {
      return;
    }

    const documentTypeRow = createFieldRow("Document Type", record.document_type, {
      type: "select",
      selectOptions: documentTypes,
      onInput(nextValue) {
        setPdfSplitMetadataFieldValue("document_type", nextValue);
      },
    });
    pdfSplitMetadataFields.appendChild(documentTypeRow);

    const citationKeyRow = createFieldRow("Citation Key", record.citation_key, {
      placeholder: "citation_key",
      onInput(nextValue) {
        setPdfSplitMetadataFieldValue("citation_key", nextValue);
      },
    });
    pdfSplitMetadataFields.appendChild(citationKeyRow);

    getOrderedDetailBibtexFields(record.document_type).forEach((fieldName) => {
      if (fieldName === "author") {
        pdfSplitMetadataFields.appendChild(
          createPdfSplitAuthorsFieldRow(record, getBibtexFieldStatus(record.document_type, fieldName))
        );
        return;
      }
      const fieldRow = createFieldRow(getBibtexFieldLabel(fieldName), record[fieldName], {
        placeholder: fieldName,
        status: getBibtexFieldStatus(record.document_type, fieldName),
        onInput(nextValue) {
          setPdfSplitMetadataFieldValue(fieldName, nextValue);
        },
      });
      pdfSplitMetadataFields.appendChild(fieldRow);
    });
  };

  const buildPdfSplitMetadataOverrides = () => {
    const record = pdfSplitState.metadataRecord;
    if (!record) {
      return {};
    }
    const fullPayload = buildMetadataUpdatePayload(record);
    const metadata = {};
    metadataDiffFields.forEach((fieldName) => {
      const fieldValue = fullPayload[fieldName];
      if (fieldName === "authors") {
        if (normalizeAuthorEntries(record.authors).length > 0) {
          metadata[fieldName] = fieldValue;
        }
        return;
      }
      if (normalizeText(fieldValue)) {
        metadata[fieldName] = fieldValue;
      }
    });
    return metadata;
  };

  const renderPdfSplitPreview = () => {
    if (!pdfSplitState.preview || !pdfSplitPreviewBody || !pdfSplitPreviewCount) {
      return;
    }
    const levelPreview = getPdfSplitLevelPreview(pdfSplitState.preview, pdfSplitState.selectedLevel);
    const splits = Array.isArray(levelPreview?.splits) ? levelPreview.splits : [];
    const resolvedFolderName = getResolvedPdfSplitFolderName();

    pdfSplitPreviewBody.innerHTML = "";
    if (!splits.length) {
      if (pdfSplitSelectAllFiles) {
        pdfSplitSelectAllFiles.checked = false;
        pdfSplitSelectAllFiles.indeterminate = false;
        pdfSplitSelectAllFiles.disabled = true;
      }
      const emptyRow = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 5;
      cell.className = "text-body-secondary small px-3 py-3";
      cell.textContent = `No level ${pdfSplitState.selectedLevel} headings were detected in this PDF.`;
      emptyRow.appendChild(cell);
      pdfSplitPreviewBody.appendChild(emptyRow);
      pdfSplitPreviewCount.textContent = "0 chapter files";
      return;
    }

    const selectedFiles = getPdfSplitSelectedFilesForCurrentLevel();
    if (pdfSplitSelectAllFiles) {
      pdfSplitSelectAllFiles.disabled = pdfSplitState.uploadInProgress;
      pdfSplitSelectAllFiles.checked = selectedFiles.length === splits.length;
      pdfSplitSelectAllFiles.indeterminate =
        selectedFiles.length > 0 && selectedFiles.length < splits.length;
    }
    pdfSplitPreviewCount.textContent =
      `${selectedFiles.length} of ${splits.length} chapter file${splits.length === 1 ? "" : "s"} selected`;
    splits.forEach((split, index) => {
      const row = document.createElement("tr");

      const includeCell = document.createElement("td");
      includeCell.className = "ps-3 text-nowrap";
      const includeInput = document.createElement("input");
      includeInput.type = "checkbox";
      includeInput.className = "form-check-input pdf-split-file-toggle";
      includeInput.checked = pdfSplitState.selectedOutputFiles.has(normalizeText(split.file_name));
      includeInput.disabled = pdfSplitState.uploadInProgress;
      includeInput.addEventListener("change", () => {
        const splitFileName = normalizeText(split.file_name);
        if (includeInput.checked) {
          pdfSplitState.selectedOutputFiles.add(splitFileName);
        } else {
          pdfSplitState.selectedOutputFiles.delete(splitFileName);
        }
        renderPdfSplitPreview();
        setPdfSplitModalBusyState(false, `Previewing level ${pdfSplitState.selectedLevel} chapter splits.`);
      });
      includeCell.appendChild(includeInput);
      row.appendChild(includeCell);

      const indexCell = document.createElement("td");
      indexCell.className = "ps-3 text-body-secondary text-nowrap";
      indexCell.textContent = String(index + 1);
      row.appendChild(indexCell);

      const chapterCell = document.createElement("td");
      const chapterTitle = document.createElement("div");
      chapterTitle.className = "pdf-split-chapter-title";
      chapterTitle.textContent = normalizeText(split.chapter_title) || `Chapter ${index + 1}`;
      chapterCell.appendChild(chapterTitle);
      if (Number(split.heading_page_start) > Number(split.page_start)) {
        const detail = document.createElement("div");
        detail.className = "pdf-split-option-meta";
        detail.textContent = `Heading starts on page ${split.heading_page_start}`;
        chapterCell.appendChild(detail);
      }
      row.appendChild(chapterCell);

      const pagesCell = document.createElement("td");
      pagesCell.className = "text-nowrap";
      pagesCell.textContent = `${split.page_start}-${split.page_end} (${split.page_count} page${
        Number(split.page_count) === 1 ? "" : "s"
      })`;
      row.appendChild(pagesCell);

      const fileCell = document.createElement("td");
      fileCell.className = "pe-3";
      const fileName = document.createElement("div");
      fileName.className = "pdf-split-file-name";
      fileName.textContent = `${resolvedFolderName}/${normalizeText(split.file_name)}`;
      fileCell.appendChild(fileName);
      row.appendChild(fileCell);

      pdfSplitPreviewBody.appendChild(row);
    });
  };

  const renderPdfSplitLevelOptions = () => {
    if (!pdfSplitState.preview || !pdfSplitLevelOptions) {
      return;
    }
    pdfSplitLevelOptions.innerHTML = "";
    pdfSplitState.preview.levels.forEach((levelPreview) => {
      const wrapper = document.createElement("div");
      wrapper.className = `form-check${levelPreview.available ? "" : " disabled"}`;

      const input = document.createElement("input");
      input.className = "form-check-input";
      input.type = "radio";
      input.name = "pdf-split-level";
      input.id = `pdf-split-level-${levelPreview.level}`;
      input.value = String(levelPreview.level);
      input.dataset.available = levelPreview.available ? "true" : "false";
      input.checked = levelPreview.level === pdfSplitState.selectedLevel;
      input.disabled = !levelPreview.available || pdfSplitState.uploadInProgress;
      input.addEventListener("change", () => {
        pdfSplitState.selectedLevel = levelPreview.level;
        selectAllPdfSplitFilesForCurrentLevel();
        renderPdfSplitPreview();
        setPdfSplitModalBusyState(false, `Previewing level ${levelPreview.level} chapter splits.`);
      });

      const label = document.createElement("label");
      label.className = "form-check-label";
      label.htmlFor = input.id;

      const title = document.createElement("div");
      title.className = "pdf-split-option-title";
      title.textContent = `Level ${levelPreview.level} headings`;
      label.appendChild(title);

      const meta = document.createElement("div");
      meta.className = "pdf-split-option-meta";
      meta.textContent = levelPreview.available
        ? `${levelPreview.split_count} split file${levelPreview.split_count === 1 ? "" : "s"}`
        : "No headings found at this level";
      label.appendChild(meta);

      wrapper.appendChild(input);
      wrapper.appendChild(label);
      pdfSplitLevelOptions.appendChild(wrapper);
    });
  };

  const openPdfSplitModal = ({ file, preview }) => {
    if (
      !file ||
      !preview ||
      !pdfSplitModalElement ||
      !pdfSplitPdfTitle ||
      !pdfSplitFolderInput ||
      !pdfSplitPageCount
    ) {
      return;
    }

    pdfSplitState.file = file;
    pdfSplitState.preview = preview;
    pdfSplitState.selectedLevel = Number.parseInt(preview.default_heading_level, 10) || 1;
    pdfSplitState.folderName = sanitizePdfSplitSegment(preview.folder_name, getPdfSplitFallbackFolderName());
    pdfSplitState.folderNameTouched = false;
    pdfSplitState.metadataRecord = createPdfSplitMetadataRecord({ file, preview });
    pdfSplitState.selectedOutputFiles = new Set();
    selectAllPdfSplitFilesForCurrentLevel();

    if (pdfSplitModalTitle) {
      pdfSplitModalTitle.textContent = "Split PDF by Chapter";
    }
    pdfSplitPageCount.textContent = `${preview.page_count} page${Number(preview.page_count) === 1 ? "" : "s"}`;

    updatePdfSplitModalSummary();
    renderPdfSplitLevelOptions();
    renderPdfSplitMetadataFields();
    renderPdfSplitPreview();
    setPdfSplitModalBusyState(false, `Previewing level ${pdfSplitState.selectedLevel} chapter splits.`);

    if (!pdfSplitModalInstance) {
      pdfSplitModalInstance = bootstrap.Modal.getOrCreateInstance(pdfSplitModalElement);
    }
    pdfSplitModalInstance.show();
  };

  const previewPdfSplitUpload = async (file) => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    if (!file || !isPdfFile(file)) {
      window.alert("Select a PDF file first.");
      return;
    }

    const originalUploadPdfDisabled = uploadPdfButton ? uploadPdfButton.disabled : false;
    const originalUploadFolderDisabled = uploadPdfFolderButton ? uploadPdfFolderButton.disabled : false;
    const originalUploadFolderLabel = uploadPdfFolderButton
      ? normalizeText(uploadPdfFolderButton.textContent) || "Upload PDF as Folder"
      : "Upload PDF as Folder";

    if (uploadPdfButton) {
      uploadPdfButton.disabled = true;
    }
    if (uploadPdfFolderButton) {
      uploadPdfFolderButton.disabled = true;
      uploadPdfFolderButton.textContent = "Analyzing PDF...";
    }

    try {
      const preview = await apiRequest(
        `/collections/${encodeURIComponent(
          selectedBucketName
        )}/documents/split/preview?file_name=${encodeURIComponent(normalizeText(file.name))}`,
        {
          method: "POST",
          body: file,
          headers: {
            "Content-Type": file.type || "application/pdf",
          },
        }
      );
      openPdfSplitModal({ file, preview });
    } catch (error) {
      window.alert(`Could not preview split chapters for '${normalizeText(file.name)}': ${error.message}`);
    } finally {
      if (uploadPdfButton) {
        uploadPdfButton.disabled = originalUploadPdfDisabled;
      }
      if (uploadPdfFolderButton) {
        uploadPdfFolderButton.disabled = originalUploadFolderDisabled;
        uploadPdfFolderButton.textContent = originalUploadFolderLabel;
      }
    }
  };

  const createPdfSplitFolder = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    if (!pdfSplitState.file || !pdfSplitState.preview) {
      window.alert("Preview a PDF split before uploading.");
      return;
    }

    const levelPreview = getPdfSplitLevelPreview(pdfSplitState.preview, pdfSplitState.selectedLevel);
    if (!levelPreview?.available) {
      window.alert(`No level ${pdfSplitState.selectedLevel} headings are available for this PDF.`);
      return;
    }
    const selectedOutputFiles = getPdfSplitSelectedFilesForCurrentLevel();
    if (!selectedOutputFiles.length) {
      window.alert("Select at least one output file to create.");
      return;
    }

    setPdfSplitModalBusyState(
      true,
      `Creating chapter files from level ${pdfSplitState.selectedLevel} headings...`
    );

    try {
      const requestParams = new URLSearchParams({
        file_name: normalizeText(pdfSplitState.file.name),
        heading_level: String(pdfSplitState.selectedLevel),
        folder_name: getResolvedPdfSplitFolderName(),
      });
      const metadataOverrides = buildPdfSplitMetadataOverrides();
      const payload = await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents/split/upload?${requestParams.toString()}`,
        {
          method: "POST",
          body: pdfSplitState.file,
          headers: {
            "Content-Type": pdfSplitState.file.type || "application/pdf",
            "X-Evidencebase-Split-Metadata": JSON.stringify(metadataOverrides),
            "X-Evidencebase-Split-Selected-Files": selectedOutputFiles.join(","),
          },
        }
      );

      await refreshDocuments({ silent: true, preserveSelection: true });
      pdfSplitModalInstance?.hide();

      const baseLocation = `${selectedBucketName}/${payload.folder_name}`;
      if (payload.failure_count > 0) {
        const failureLines = (Array.isArray(payload.failures) ? payload.failures : [])
          .slice(0, 8)
          .map((failure) => `- ${normalizeText(failure.object_name)}: ${normalizeText(failure.error)}`);
        if (Array.isArray(payload.failures) && payload.failures.length > 8) {
          failureLines.push(`- ...and ${payload.failures.length - 8} more`);
        }
        window.alert(
          `Created ${payload.uploaded_count} split PDF${payload.uploaded_count === 1 ? "" : "s"} in '${baseLocation}'.\n` +
            "Some chapter uploads failed.\n\n" +
            `Failures:\n${failureLines.join("\n")}`
        );
        return;
      }

      window.alert(
        `Created ${payload.uploaded_count} split PDF${payload.uploaded_count === 1 ? "" : "s"} in '${baseLocation}'.\n` +
          "Celery tasks were queued for processing."
      );
    } catch (error) {
      setPdfSplitModalBusyState(false, `Could not create split folder: ${error.message}`);
      window.alert(`Could not create the split PDF folder: ${error.message}`);
    }
  };

  const downloadCollectionBibtex = () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const downloadUrl =
      `${apiBasePath}/collections/${encodeURIComponent(selectedBucketName)}/bibliography.bib`;
    const anchor = document.createElement("a");
    anchor.href = downloadUrl;
    anchor.download = `${selectedBucketName}-bibliography.bib`;
    anchor.rel = "noopener";
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
  };

  const fetchDocumentMetadataFromCrossref = async (record) => {
    if (!record || typeof record !== "object" || !record.document_id || !selectedBucketName) {
      return {
        ok: false,
        errorMessage: "Missing record, document ID, or selected collection.",
      };
    }

    try {
      const payload = await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents/${encodeURIComponent(
          record.document_id
        )}/metadata/fetch`,
        {
          method: "POST",
        }
      );
      return {
        ok: true,
        payload,
      };
    } catch (error) {
      const message = normalizeText(error?.message);
      if (message.includes("404")) {
        return {
          ok: false,
          errorMessage:
            "Could not fetch metadata from Crossref: API metadata fetch endpoint is unavailable (404). Rebuild/restart the API container.",
        };
      }
      return {
        ok: false,
        errorMessage: `Could not fetch metadata from Crossref: ${message || "Unknown error"}`,
      };
    }
  };

  const getBulkOverwriteDecision = (record, currentDecisionMode, reasonText = "") => {
    if (currentDecisionMode === "yes_to_all") {
      return { shouldFetch: true, nextDecisionMode: currentDecisionMode };
    }
    if (currentDecisionMode === "no_to_all") {
      return { shouldFetch: false, nextDecisionMode: currentDecisionMode };
    }

    const fileName = filenameFromPath(record?.file_path);
    while (true) {
      const response = window.prompt(
        `Fetch missing metadata for '${fileName}'?\nReason: ${reasonText || "Matched missing metadata criteria."}\nThis may overwrite existing metadata.\nType one: yes, yes to all, no, no to all.\n(Cancel = no to all)`,
        "yes"
      );
      if (response === null) {
        return { shouldFetch: false, nextDecisionMode: "no_to_all" };
      }
      const normalizedResponse = normalizeText(response).toLowerCase();
      if (normalizedResponse === "yes" || normalizedResponse === "y") {
        return { shouldFetch: true, nextDecisionMode: "ask_each" };
      }
      if (
        normalizedResponse === "yes to all" ||
        normalizedResponse === "yes-all" ||
        normalizedResponse === "yes all" ||
        normalizedResponse === "yes_to_all"
      ) {
        return { shouldFetch: true, nextDecisionMode: "yes_to_all" };
      }
      if (normalizedResponse === "no" || normalizedResponse === "n") {
        return { shouldFetch: false, nextDecisionMode: "ask_each" };
      }
      if (
        normalizedResponse === "no to all" ||
        normalizedResponse === "no-all" ||
        normalizedResponse === "no all" ||
        normalizedResponse === "no_to_all"
      ) {
        return { shouldFetch: false, nextDecisionMode: "no_to_all" };
      }
      window.alert("Please type: yes, yes to all, no, or no to all.");
    }
  };

  const fetchMissingMetadataForAllRecords = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    if (bulkFetchMetaInProgress) {
      return;
    }
    const selectedRecords = getBulkSelectedDocuments();
    if (selectedRecords.length <= 0) {
      window.alert("Select at least one record in Bulk Edit.");
      return;
    }
    const selectedCount = selectedRecords.length;
    if (
      !window.confirm(
        `Fetch missing metadata for ${selectedCount} selected record${selectedCount === 1 ? "" : "s"}?` +
          "\nThis uses the existing Crossref DOI-first lookup logic and may overwrite metadata fields."
      )
    ) {
      return;
    }

    bulkFetchMetaInProgress = true;
    updateRemoveDocumentButtonState();
    let fetchedCount = 0;
    let updatedCount = 0;
    let unchangedCount = 0;
    let citationKeyUpdatedCount = 0;
    let failedCount = 0;
    let processedCount = 0;
    const failureMessages = [];
    const fetchedDocumentIds = new Set();

    setBulkFetchProgress(0, selectedCount);
    try {
      for (const record of selectedRecords) {
        try {
          const fileName = filenameFromPath(record?.file_path);
          const pendingSaveSuccessful = await flushPendingMetadataSaveForRecord(record);
          if (!pendingSaveSuccessful) {
            failedCount += 1;
            failureMessages.push(`${fileName}: could not save pending metadata edits.`);
            continue;
          }

          const beforeSnapshot = buildMetadataSnapshotFromRecord(record);
          const fetchResult = await fetchDocumentMetadataFromCrossref(record);
          if (fetchResult.ok) {
            fetchedCount += 1;
            fetchedDocumentIds.add(record.document_id);
            const payload = fetchResult.payload || {};
            const afterSnapshot = buildMetadataSnapshotFromPayload(payload.metadata);
            const changedFields = getChangedMetadataFieldNames(beforeSnapshot, afterSnapshot);
            if (changedFields.length > 0) {
              updatedCount += 1;
            } else {
              unchangedCount += 1;
            }
            continue;
          }
          failedCount += 1;
          failureMessages.push(`${fileName}: ${fetchResult.errorMessage}`);
        } finally {
          processedCount += 1;
          setBulkFetchProgress(processedCount, selectedCount);
        }
      }

      if (fetchedCount > 0) {
        await refreshDocuments({ silent: true, preserveSelection: true });
        for (const documentId of fetchedDocumentIds) {
          const refreshedRecord = documents.find((documentRecord) => documentRecord.document_id === documentId);
          if (!refreshedRecord) {
            continue;
          }
          const citationKeyUpdated = await persistDefaultCitationKeyForRecord(refreshedRecord, {
            silent: true,
          });
          if (citationKeyUpdated) {
            citationKeyUpdatedCount += 1;
          }
        }
        if (citationKeyUpdatedCount > 0) {
          await refreshDocuments({ silent: true, preserveSelection: true });
        }
      }
    } finally {
      bulkFetchMetaInProgress = false;
      resetBulkFetchProgress();
      updateRemoveDocumentButtonState();
    }

    const summaryLines = [
      "Missing metadata fetch finished.",
      `Selected records: ${selectedCount}`,
      `Fetched successfully: ${fetchedCount}`,
      `Updated records: ${updatedCount}`,
      `Citation keys updated: ${citationKeyUpdatedCount}`,
      `Fetched with no field changes: ${unchangedCount}`,
      `Failed: ${failedCount}`,
    ];
    if (failureMessages.length > 0) {
      summaryLines.push("");
      summaryLines.push("Failures:");
      failureMessages.slice(0, 5).forEach((failureMessage) => {
        summaryLines.push(`- ${failureMessage}`);
      });
      if (failureMessages.length > 5) {
        summaryLines.push(`- ...and ${failureMessages.length - 5} more`);
      }
    }
    window.alert(summaryLines.join("\n"));
  };

  const updateCitationKeysForBulkSelectedDocuments = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    if (bulkFetchMetaInProgress || bulkCitationKeyUpdateInProgress) {
      return;
    }
    const selectedRecords = getBulkSelectedDocuments();
    if (selectedRecords.length <= 0) {
      window.alert("Select at least one record in Bulk Edit.");
      return;
    }
    const selectedCount = selectedRecords.length;
    if (
      !window.confirm(
        `Update citation key for ${selectedCount} selected record${selectedCount === 1 ? "" : "s"}?` +
          "\nThis uses the author+year+title-word format and saves each record."
      )
    ) {
      return;
    }

    bulkCitationKeyUpdateInProgress = true;
    updateRemoveDocumentButtonState();
    let updatedCount = 0;
    let unchangedCount = 0;
    let failedCount = 0;
    let processedCount = 0;
    const failureMessages = [];

    setBulkFetchProgress(0, selectedCount);
    try {
      for (const record of selectedRecords) {
        try {
          const fileName = filenameFromPath(record?.file_path);
          const pendingSaveSuccessful = await flushPendingMetadataSaveForRecord(record);
          if (!pendingSaveSuccessful) {
            failedCount += 1;
            failureMessages.push(`${fileName}: could not save pending metadata edits.`);
            continue;
          }

          const previousCitationKey = normalizeText(record.citation_key);
          const nextCitationKey = buildDefaultCitationKeyForRecord(record);
          if (!nextCitationKey) {
            failedCount += 1;
            failureMessages.push(`${fileName}: could not build a citation key from current metadata.`);
            continue;
          }
          if (nextCitationKey === previousCitationKey) {
            unchangedCount += 1;
            continue;
          }

          record.citation_key = nextCitationKey;
          const saveSuccessful = await persistDocumentMetadata(record, { silent: true });
          if (saveSuccessful) {
            updatedCount += 1;
          } else {
            record.citation_key = previousCitationKey;
            failedCount += 1;
            failureMessages.push(`${fileName}: could not save updated citation key.`);
          }
        } finally {
          processedCount += 1;
          setBulkFetchProgress(processedCount, selectedCount);
        }
      }

      if (updatedCount > 0) {
        await refreshDocuments({ silent: true, preserveSelection: true });
      }
    } finally {
      bulkCitationKeyUpdateInProgress = false;
      resetBulkFetchProgress();
      updateRemoveDocumentButtonState();
    }

    const summaryLines = [
      "Citation key update finished.",
      `Selected records: ${selectedCount}`,
      `Updated records: ${updatedCount}`,
      `Already up to date: ${unchangedCount}`,
      `Failed: ${failedCount}`,
    ];
    if (failureMessages.length > 0) {
      summaryLines.push("");
      summaryLines.push("Failures:");
      failureMessages.slice(0, 5).forEach((failureMessage) => {
        summaryLines.push(`- ${failureMessage}`);
      });
      if (failureMessages.length > 5) {
        summaryLines.push(`- ...and ${failureMessages.length - 5} more`);
      }
    }
    window.alert(summaryLines.join("\n"));
  };

  const fetchSelectedDocumentMetadata = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const selectedDocument = getActionTargetDocument();
    if (!selectedDocument) {
      window.alert("Select a document first.");
      return;
    }
    if (
      !window.confirm(
        `Fetch metadata for '${filenameFromPath(selectedDocument.file_path)}'?\nThis may overwrite existing metadata fields.`
      )
    ) {
      return;
    }

    const pendingSaveSuccessful = await flushPendingMetadataSaveForRecord(selectedDocument);
    if (!pendingSaveSuccessful) {
      return;
    }

    const selectedDocumentIdForFetch = selectedDocument.document_id;
    const beforeSnapshot = buildMetadataSnapshotFromRecord(selectedDocument);
    const fetchResult = await fetchDocumentMetadataFromCrossref(selectedDocument);
    if (!fetchResult.ok) {
      window.alert(fetchResult.errorMessage);
      return;
    }
    const payload = fetchResult.payload || {};
    const afterSnapshot = buildMetadataSnapshotFromPayload(payload.metadata);
    const changedFields = getChangedMetadataFieldNames(beforeSnapshot, afterSnapshot);
    const changedFieldLabels = changedFields.map((fieldName) => getMetadataFieldLabel(fieldName));
    await refreshDocuments({ silent: true, preserveSelection: true });
    let citationKeyUpdated = false;
    const refreshedDocument = documents.find(
      (documentRecord) => documentRecord.document_id === selectedDocumentIdForFetch
    );
    if (refreshedDocument) {
      citationKeyUpdated = await persistDefaultCitationKeyForRecord(refreshedDocument, {
        silent: true,
      });
      if (citationKeyUpdated) {
        await refreshDocuments({ silent: true, preserveSelection: true });
      }
    }
    const lookupField = normalizeText(payload.lookup_field).toUpperCase();
    const confidence = Number.parseFloat(normalizeText(payload.confidence));
    const confidenceSuffix = Number.isFinite(confidence) ? ` (${confidence.toFixed(2)})` : "";
    const citationKeySuffix = citationKeyUpdated
      ? " Citation key updated to author+year+title-word format."
      : "";
    if (lookupField) {
      if (changedFieldLabels.length > 0) {
        window.alert(
          `Crossref metadata fetched via ${lookupField}${confidenceSuffix}. Updated fields: ${changedFieldLabels.join(", ")}.${citationKeySuffix}`
        );
      } else {
        window.alert(
          `Crossref metadata fetched via ${lookupField}${confidenceSuffix}. No metadata fields changed.${citationKeySuffix}`
        );
      }
    } else {
      if (changedFieldLabels.length > 0) {
        window.alert(
          `Crossref metadata fetched. Updated fields: ${changedFieldLabels.join(", ")}.${citationKeySuffix}`
        );
      } else {
        window.alert(`Crossref metadata fetched. No metadata fields changed.${citationKeySuffix}`);
      }
    }
  };

  const updateCitationKeyForSelectedDocument = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const selectedDocument = getActionTargetDocument();
    if (!selectedDocument) {
      window.alert("Select a document first.");
      return;
    }
    const fileName = filenameFromPath(selectedDocument.file_path);
    const currentCitationKey = normalizeText(selectedDocument.citation_key);
    const nextCitationKey = buildDefaultCitationKeyForRecord(selectedDocument);
    if (!nextCitationKey) {
      window.alert(`Could not build a citation key for '${fileName}' from the current metadata.`);
      return;
    }
    if (nextCitationKey === currentCitationKey) {
      window.alert(`Citation key for '${fileName}' is already up to date.`);
      return;
    }
    if (
      !window.confirm(
        `Update citation key for '${fileName}'?\nCurrent: ${currentCitationKey || "(empty)"}\nNew: ${nextCitationKey}`
      )
    ) {
      return;
    }

    const pendingSaveSuccessful = await flushPendingMetadataSaveForRecord(selectedDocument);
    if (!pendingSaveSuccessful) {
      window.alert(`Could not save pending metadata edits for '${fileName}'.`);
      return;
    }

    selectedDocument.citation_key = nextCitationKey;
    const saveSuccessful = await persistDocumentMetadata(selectedDocument, { silent: true });
    if (!saveSuccessful) {
      selectedDocument.citation_key = currentCitationKey;
      window.alert(`Could not save updated citation key for '${fileName}'.`);
      return;
    }

    await refreshDocuments({ silent: true, preserveSelection: true });
    window.alert(`Citation key updated to '${nextCitationKey}'.`);
  };

  const clearDocumentMetadata = (record) => {
    if (!record || typeof record !== "object") {
      return;
    }
    record.document_type = "misc";
    record.authors = [];
    if (!record.bibtex_fields || typeof record.bibtex_fields !== "object") {
      record.bibtex_fields = {};
    }
    bibtexFields.forEach((fieldName) => {
      record[fieldName] = "";
      record.bibtex_fields[fieldName] = "";
    });
    syncRecordCoreFields(record);
  };

  const clearSelectedDocumentMetadata = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const selectedDocument = getActionTargetDocument();
    if (!selectedDocument) {
      window.alert("Select a document first.");
      return;
    }
    const fileName = filenameFromPath(selectedDocument.file_path);
    if (!window.confirm(`Clear metadata for '${fileName}'?`)) {
      return;
    }

    const pendingTimer = metadataSaveTimers.get(selectedDocument.document_id);
    if (pendingTimer) {
      window.clearTimeout(pendingTimer.timerId);
      metadataSaveTimers.delete(selectedDocument.document_id);
    }

    clearDocumentMetadata(selectedDocument);
    const saveSuccessful = await persistDocumentMetadata(selectedDocument, { silent: false });
    if (!saveSuccessful) {
      return;
    }
    await refreshDocuments({ silent: true, preserveSelection: true });
    window.alert("Metadata cleared.");
  };

  const reindexSelectedDocument = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const selectedDocument = getActionTargetDocument();
    if (!selectedDocument) {
      window.alert("Select a document first.");
      return;
    }

    const fileName = filenameFromPath(selectedDocument.file_path);
    if (!window.confirm(`Reindex '${fileName}'?`)) {
      return;
    }

    const pendingSaveSuccessful = await flushPendingMetadataSaveForRecord(selectedDocument);
    if (!pendingSaveSuccessful) {
      window.alert(`Could not save pending metadata edits for '${fileName}'.`);
      return;
    }

    try {
      const payload = await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents/${encodeURIComponent(
          selectedDocument.document_id
        )}/reindex`,
        { method: "POST" }
      );
      await refreshDocuments({ silent: true, preserveSelection: true });
      if (payload.queued) {
        window.alert(`Reindex queued for '${fileName}'.`);
        return;
      }
      window.alert(
        `Reindex requested for '${fileName}', but queueing failed: ${
          normalizeText(payload.queue_error) || "Unknown error"
        }`
      );
    } catch (error) {
      window.alert(`Could not reindex '${fileName}': ${error.message}`);
    }
  };

  const removeSelectedDocument = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const selectedDocument = getSelectedDocument();
    if (!selectedDocument) {
      window.alert("Select a document first.");
      return;
    }

    const fileName = filenameFromPath(selectedDocument.file_path);
    if (!window.confirm(`Remove file '${fileName}'?`)) {
      return;
    }
    const pendingTimer = metadataSaveTimers.get(selectedDocument.document_id);
    if (pendingTimer) {
      window.clearTimeout(pendingTimer.timerId);
      metadataSaveTimers.delete(selectedDocument.document_id);
    }

    try {
      await apiRequest(
        `/collections/${encodeURIComponent(selectedBucketName)}/documents/${encodeURIComponent(
          selectedDocument.document_id
        )}`,
        { method: "DELETE" }
      );
      selectedDocumentId = null;
      await refreshDocuments({ silent: true, preserveSelection: false });
    } catch (error) {
      window.alert(`Could not remove file: ${error.message}`);
    }
  };

  const removeBulkSelectedDocuments = async () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }

    const selectedRecords = getBulkSelectedDocuments();
    if (selectedRecords.length <= 0) {
      window.alert("Select at least one file in Bulk Edit.");
      return;
    }

    const selectedCount = selectedRecords.length;
    const fileLabel = selectedCount === 1 ? "file" : "files";
    if (!window.confirm(`Remove ${selectedCount} selected ${fileLabel}?`)) {
      return;
    }

    let removedCount = 0;
    const failureMessages = [];
    for (const record of selectedRecords) {
      if (!record || !record.document_id) {
        continue;
      }
      const pendingTimer = metadataSaveTimers.get(record.document_id);
      if (pendingTimer) {
        window.clearTimeout(pendingTimer.timerId);
        metadataSaveTimers.delete(record.document_id);
      }
      try {
        await apiRequest(
          `/collections/${encodeURIComponent(selectedBucketName)}/documents/${encodeURIComponent(
            record.document_id
          )}`,
          { method: "DELETE" }
        );
        removedCount += 1;
      } catch (error) {
        failureMessages.push(`${filenameFromPath(record.file_path)}: ${error.message}`);
      }
    }

    if (removedCount > 0) {
      selectedDocumentId = null;
      await refreshDocuments({ silent: true, preserveSelection: false });
    } else {
      updateRemoveDocumentButtonState();
    }

    if (failureMessages.length > 0) {
      const failureLines = failureMessages.slice(0, 5).map((message) => `- ${message}`);
      if (failureMessages.length > 5) {
        failureLines.push(`- ...and ${failureMessages.length - 5} more`);
      }
      window.alert(
        `Removed ${removedCount} of ${selectedCount} selected ${fileLabel}.\n\nFailures:\n${failureLines.join("\n")}`
      );
      return;
    }
    window.alert(`Removed ${removedCount} selected ${fileLabel}.`);
  };

  document.querySelectorAll("[data-path]").forEach((anchor) => {
    const path = anchor.getAttribute("data-path") || "";
    anchor.href = `${appBaseUrl}${path}`;
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
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const picker = document.createElement("input");
    picker.type = "file";
    picker.accept = ".pdf,.txt,.md,.docx";
    picker.addEventListener("change", () => {
      const nextFile = picker.files && picker.files[0] ? picker.files[0] : null;
      if (nextFile) {
        void uploadDocumentFile(nextFile);
      }
    });
    picker.click();
  });
  uploadPdfFolderButton?.addEventListener("click", () => {
    if (!selectedBucketName) {
      window.alert("Select a collection first.");
      return;
    }
    const picker = document.createElement("input");
    picker.type = "file";
    picker.accept = ".pdf,application/pdf";
    picker.addEventListener("change", () => {
      const nextFile = picker.files && picker.files[0] ? picker.files[0] : null;
      if (nextFile) {
        void previewPdfSplitUpload(nextFile);
      }
    });
    picker.click();
  });
  downloadBibtexButton?.addEventListener("click", () => {
    downloadCollectionBibtex();
  });
  fetchMetaButton?.addEventListener("click", () => {
    void fetchMissingMetadataForAllRecords();
  });
  updateCitationKeyButton?.addEventListener("click", () => {
    void updateCitationKeysForBulkSelectedDocuments();
  });
  detailFetchMetaButton?.addEventListener("click", () => {
    void fetchSelectedDocumentMetadata();
  });
  detailUpdateCitationKeyButton?.addEventListener("click", () => {
    void updateCitationKeyForSelectedDocument();
  });
  detailReindexButton?.addEventListener("click", () => {
    void reindexSelectedDocument();
  });
  detailClearMetaButton?.addEventListener("click", () => {
    void clearSelectedDocumentMetadata();
  });
  removeSelectedDocumentsButton?.addEventListener("click", () => {
    void removeBulkSelectedDocuments();
  });
  removeDocumentButton?.addEventListener("click", () => {
    void removeSelectedDocument();
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
  semanticSearchForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    void searchCollection();
  });
  searchSectionPrevButton?.addEventListener("click", () => {
    navigateSearchSectionModal(-1);
  });
  searchSectionNextButton?.addEventListener("click", () => {
    navigateSearchSectionModal(1);
  });
  pdfSplitFolderInput?.addEventListener("input", () => {
    const nextFolderName = sanitizePdfSplitSegment(pdfSplitFolderInput.value, getPdfSplitFallbackFolderName());
    pdfSplitState.folderName = nextFolderName;
    pdfSplitState.folderNameTouched = true;
    updatePdfSplitModalSummary();
    renderPdfSplitPreview();
  });
  pdfSplitSelectAllFiles?.addEventListener("change", () => {
    if (pdfSplitSelectAllFiles.checked) {
      selectAllPdfSplitFilesForCurrentLevel();
    } else {
      clearAllPdfSplitFilesForCurrentLevel();
    }
    renderPdfSplitPreview();
    setPdfSplitModalBusyState(false, `Previewing level ${pdfSplitState.selectedLevel} chapter splits.`);
  });
  pdfSplitUploadButton?.addEventListener("click", () => {
    void createPdfSplitFolder();
  });
  searchSectionModalElement?.addEventListener("hidden.bs.modal", () => {
    searchSectionModalLoadToken += 1;
    clearSearchSectionNavigationState();
    updateSearchSectionModalNavigationButtons();
  });
  pdfSplitModalElement?.addEventListener("hidden.bs.modal", () => {
    clearPdfSplitState();
  });
  semanticSearchModeSelect?.addEventListener("change", () => {
    syncSemanticSearchModeFields();
  });
  const handleMainTabClick = (nextPaneId) => {
    setMainTabState(nextPaneId);
    if (nextPaneId === "semantic-search-pane") {
      resetSemanticSearchScroll();
    }
    window.requestAnimationFrame(setIndependentScrollHeights);
  };

  documentMetaTab?.addEventListener("click", (event) => {
    event.preventDefault();
    handleMainTabClick("document-meta-pane");
  });
  semanticSearchTab?.addEventListener("click", (event) => {
    event.preventDefault();
    handleMainTabClick("semantic-search-pane");
  });
  mainViewTabList?.addEventListener("click", (event) => {
    const tabTrigger = event.target.closest("button[data-tab-target]");
    if (!tabTrigger) {
      return;
    }
    event.preventDefault();
    const targetSelector = tabTrigger.getAttribute("data-tab-target");
    if (targetSelector === "#semantic-search-pane") {
      handleMainTabClick("semantic-search-pane");
      return;
    }
    handleMainTabClick("document-meta-pane");
  });

  collectionsFilterInput?.addEventListener("input", () => {
    collectionsFilterQuery = normalizeText(collectionsFilterInput.value).toLowerCase();
    renderBuckets();
    window.requestAnimationFrame(setIndependentScrollHeights);
  });

  detailDocumentTable?.addEventListener("click", (event) => {
    const pdfButton = event.target.closest("button[data-pdf-action]");
    if (pdfButton) {
      event.preventDefault();
      event.stopPropagation();
      const documentId = pdfButton.dataset.documentId;
      if (!documentId) {
        return;
      }
      const targetDocument = documents.find(
        (documentRecord) => documentRecord.document_id === documentId
      );
      if (!targetDocument) {
        return;
      }
      const resolverHref = buildResolverHrefForDocumentRecord(targetDocument);
      if (resolverHref) {
        window.open(resolverHref, "_blank", "noopener,noreferrer");
      }
      return;
    }

    const sectionButton = event.target.closest("button[data-section-action]");
    if (sectionButton) {
      event.preventDefault();
      event.stopPropagation();
      const documentId = sectionButton.dataset.documentId;
      if (!documentId) {
        return;
      }
      const targetDocument = documents.find(
        (documentRecord) => documentRecord.document_id === documentId
      );
      if (targetDocument) {
        openDocumentSectionsModal(targetDocument);
      }
      return;
    }

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
    updateRemoveDocumentButtonState();
  });

  documentHotContainer?.addEventListener("click", (event) => {
    const pdfButton = event.target.closest("button[data-pdf-action]");
    if (pdfButton && documentHotContainer.contains(pdfButton)) {
      event.preventDefault();
      event.stopPropagation();

      const documentId = pdfButton.dataset.documentId;
      if (!documentId) {
        return;
      }
      const targetDocument = documents.find((documentRecord) => documentRecord.document_id === documentId);
      if (!targetDocument) {
        return;
      }
      const resolverHref = buildResolverHrefForDocumentRecord(targetDocument);
      if (resolverHref) {
        window.open(resolverHref, "_blank", "noopener,noreferrer");
      }
      return;
    }

    const sectionButton = event.target.closest("button[data-section-action]");
    if (sectionButton && documentHotContainer.contains(sectionButton)) {
      event.preventDefault();
      event.stopPropagation();

      const documentId = sectionButton.dataset.documentId;
      if (!documentId) {
        return;
      }
      const targetDocument = documents.find((documentRecord) => documentRecord.document_id === documentId);
      if (!targetDocument) {
        return;
      }
      openDocumentSectionsModal(targetDocument);
      return;
    }

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
  documentHotContainer?.addEventListener("change", (event) => {
    const selectAllCheckbox = event.target.closest("input.bulk-select-all-checkbox");
    if (!selectAllCheckbox || !documentTable || !documentHotContainer.contains(selectAllCheckbox)) {
      return;
    }
    event.stopPropagation();

    const nextChecked = Boolean(selectAllCheckbox.checked);
    setBulkSelectAllHeaderCheckboxState({
      disabled: false,
      checked: nextChecked,
      indeterminate: false,
    });
    const changes = [];
    const rowCount = documentTable.countRows();
    for (let visualRow = 0; visualRow < rowCount; visualRow += 1) {
      const physicalRow = documentTable.toPhysicalRow(visualRow);
      const record = documentTable.getSourceDataAtRow(physicalRow);
      if (!record || Boolean(record.bulk_selected) === nextChecked) {
        continue;
      }
      changes.push([visualRow, "bulk_selected", nextChecked]);
    }

    if (changes.length > 0) {
      documentTable.setDataAtRowProp(changes, "bulk-select-all");
      return;
    }
    syncBulkSelectAllHeaderCheckbox();
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
  setMainTabState("document-meta-pane");
  syncSemanticSearchModeFields();
  renderSemanticSearchResults();
  updateSearchSectionModalNavigationButtons();
  setSemanticSearchStatus("Select a collection and run a query.");

  updateRemoveTooltip();
  updateRemoveDocumentButtonState();
  window.requestAnimationFrame(setIndependentScrollHeights);
  void (async () => {
    await loadCitationSchema();
    await refreshBuckets();
  })();
})();
