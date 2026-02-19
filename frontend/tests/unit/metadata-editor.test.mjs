import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFallbackCitationKey,
  formatAuthorsHarvard,
  getBulkBibtexFieldOrder,
  getDefaultCitationSchema,
  normalizeCitationSchema,
  parseAuthorFromText,
} from "../../js/metadata-editor.mjs";

test("normalizeCitationSchema falls back to defaults", () => {
  const schema = normalizeCitationSchema({});
  const defaults = getDefaultCitationSchema();
  assert.deepEqual(schema.document_types, defaults.document_types);
  assert.deepEqual(schema.bibtex_fields, defaults.bibtex_fields);
});

test("getBulkBibtexFieldOrder prioritizes required fields", () => {
  const order = getBulkBibtexFieldOrder(
    ["author", "title", "journal", "year", "note"],
    {
      article: {
        required: ["author", "title", "journal", "year"],
        recommended: ["note"],
      },
    }
  );

  assert.deepEqual(order.slice(0, 4), ["author", "journal", "title", "year"].sort());
  assert.equal(order.includes("note"), true);
});

test("parseAuthorFromText supports suffixes and Harvard formatting", () => {
  const knownAuthorSuffixes = new Set(["jr", "jr."]);
  const parsed = parseAuthorFromText("Doe, Jr., Jane", knownAuthorSuffixes);
  assert.deepEqual(parsed, {
    first_name: "Jane",
    last_name: "Doe",
    suffix: "Jr.",
  });

  const harvard = formatAuthorsHarvard([parsed], knownAuthorSuffixes);
  assert.equal(harvard, "Doe, J., Jr.");
});

test("buildFallbackCitationKey uses author+year+title then filename fallback", () => {
  const key = buildFallbackCitationKey({
    filePath: "papers/evidence.pdf",
    index: 0,
    authorLastName: "Lovelace",
    title: "Causal Inference",
    year: "2024",
    filenameFromPath: (value) => value.split("/").pop() || value,
  });
  assert.equal(key, "lovelace2024causal");

  const fallback = buildFallbackCitationKey({
    filePath: "papers/evidence.pdf",
    index: 3,
    authorLastName: "",
    title: "",
    year: "",
    filenameFromPath: (value) => value.split("/").pop() || value,
  });
  assert.equal(fallback, "evidence");
});
