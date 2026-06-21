import test from "node:test";
import assert from "node:assert/strict";

import {
  getProcessingStatusPresentation,
  normalizeProcessingStage,
  normalizeProcessingStageProgress,
} from "../../js/progress-ui.mjs";

test("normalizeProcessingStage prefers an explicit known stage", () => {
  assert.equal(
    normalizeProcessingStage({
      rawStage: "meta",
      processingState: "processing",
      processingProgress: 20,
    }),
    "meta"
  );
});

test("normalizeProcessingStage infers the stage from overall progress", () => {
  assert.equal(
    normalizeProcessingStage({
      rawStage: "",
      processingState: "processing",
      processingProgress: 67,
    }),
    "chunk"
  );
});

test("normalizeProcessingStageProgress derives local progress from the global range", () => {
  assert.equal(
    normalizeProcessingStageProgress({
      rawStageProgress: "",
      processingStage: "meta",
      processingProgress: 30,
    }),
    50
  );
});

test("getProcessingStatusPresentation keeps queued work visible and descriptive", () => {
  const presentation = getProcessingStatusPresentation({
    processingState: "processing",
    processingStage: "queued",
    processingProgress: 0,
    processingStageProgress: 0,
  });

  assert.equal(presentation.stageName, "queued");
  assert.equal(presentation.actualProgress, 0);
  assert.equal(presentation.visibleProgress, 12);
  assert.equal(presentation.progressText, "Queued 0%");
  assert.match(presentation.detailText, /waiting for a worker/i);
});

test("getProcessingStatusPresentation reports stage-local progress when available", () => {
  const presentation = getProcessingStatusPresentation({
    processingState: "processing",
    processingStage: "upsert",
    processingProgress: 86,
    processingStageProgress: 30,
  });

  assert.equal(presentation.stageName, "upsert");
  assert.equal(presentation.actualProgress, 86);
  assert.equal(presentation.stageProgress, 30);
  assert.match(presentation.detailText, /30% of this stage complete/i);
});
