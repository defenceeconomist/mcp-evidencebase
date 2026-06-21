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

export const processingStageLabels = {
  queued: "Queued",
  partition: "Partition",
  meta: "Metadata",
  section: "Section",
  chunk: "Chunk",
  upsert: "Upsert",
  processed: "Done",
  failed: "Failed",
};

export const processingStageRanges = {
  partition: [0, 20],
  meta: [20, 40],
  section: [40, 60],
  chunk: [60, 80],
  upsert: [80, 100],
};

const processingStageDetails = {
  queued: "Uploaded. Waiting for a worker to start processing.",
  partition: "Stage 1 of 5. Extracting document structure and text.",
  meta: "Stage 2 of 5. Partitioning complete. Reading document metadata.",
  section: "Stage 3 of 5. Metadata saved. Building the section map.",
  chunk: "Stage 4 of 5. Sections ready. Splitting the document into search chunks.",
  upsert: "Stage 5 of 5. Chunking complete. Embedding chunks and indexing for search.",
  processed: "Processing complete.",
  failed: "Processing failed.",
};

export const inferProcessingStageFromProgress = (processingProgress) => {
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

export const normalizeProcessingStage = ({ rawStage, processingState, processingProgress }) => {
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

export const normalizeProcessingStageProgress = ({
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

export const getProcessingStageLabel = (stageName) => {
  const normalizedStageName = normalizeText(stageName).toLowerCase();
  if (Object.prototype.hasOwnProperty.call(processingStageLabels, normalizedStageName)) {
    return processingStageLabels[normalizedStageName];
  }
  return "Processing";
};

export const getProcessingStatusPresentation = ({
  processingState,
  processingStage,
  processingProgress,
  processingStageProgress,
}) => {
  const normalizedState = normalizeText(processingState).toLowerCase();
  const actualProgress = clampPercent(
    processingProgress,
    normalizedState === "processing" ? 0 : 100
  );
  const stageName = normalizeProcessingStage({
    rawStage: processingStage,
    processingState,
    processingProgress: actualProgress,
  });
  const stageProgress = normalizeProcessingStageProgress({
    rawStageProgress: processingStageProgress,
    processingStage: stageName,
    processingProgress: actualProgress,
  });
  const stageLabel = getProcessingStageLabel(stageName);
  const isActive = normalizedState === "processing";
  const visibleProgress = isActive ? Math.max(actualProgress, 12) : actualProgress;
  const progressText = `${stageLabel} ${actualProgress}%`;

  let detailText = processingStageDetails[stageName] || "Processing document.";
  if (isActive && stageProgress > 0 && stageProgress < 100) {
    detailText = `${detailText} ${stageProgress}% of this stage complete.`;
  }

  return {
    actualProgress,
    visibleProgress,
    stageName,
    stageLabel,
    stageProgress,
    progressText,
    detailText,
    title: `${progressText}. ${detailText}`,
  };
};
