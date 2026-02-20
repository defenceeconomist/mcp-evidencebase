import test from "node:test";
import assert from "node:assert/strict";

import {
  formatSearchAuthorYear,
  formatSearchLocation,
  formatSearchPageLabel,
  formatSearchPages,
  formatSearchTitle,
} from "../../js/search-ui.mjs";

const normalizeText = (value) => (value === null || value === undefined ? "" : String(value).trim());
const filenameFromPath = (value) => {
  const parts = String(value || "").split("/");
  return parts[parts.length - 1] || "";
};

test("formatSearchPages handles ranges and missing values", () => {
  assert.equal(formatSearchPages(4, 4), "4");
  assert.equal(formatSearchPages(4, 7), "4-7");
  assert.equal(formatSearchPages(null, null), "n/a");
});

test("formatSearchPageLabel handles single pages and ranges", () => {
  assert.equal(formatSearchPageLabel(4, 4), "Page");
  assert.equal(formatSearchPageLabel(4, 7), "Pages");
  assert.equal(formatSearchPageLabel(4, null), "Page");
  assert.equal(formatSearchPageLabel(null, null), "Pages");
});

test("search formatting helpers provide robust fallbacks", () => {
  const result = {
    title: "",
    file_path: "papers/evidence.pdf",
    document_id: "doc-1",
    minio_location: "",
    author: "Lovelace",
    year: "2024",
  };

  assert.equal(formatSearchTitle(result, filenameFromPath, normalizeText), "evidence.pdf");
  assert.equal(formatSearchLocation(result, normalizeText), "papers/evidence.pdf");
  assert.equal(formatSearchAuthorYear(result, normalizeText), "Lovelace (2024)");
});
