import test from "node:test";
import assert from "node:assert/strict";

import { ensureSelectedDocumentId, filterDocumentsByTitle } from "../../js/table-ui.mjs";

const normalizeText = (value) => (value === null || value === undefined ? "" : String(value).trim());
const filenameFromPath = (value) => {
  const parts = String(value || "").split("/");
  return parts[parts.length - 1] || "";
};

test("filterDocumentsByTitle filters by normalized title", () => {
  const documents = [
    { document_id: "1", title: "Offset evidence", file_path: "a.pdf" },
    { document_id: "2", title: "Climate policy", file_path: "b.pdf" },
  ];

  const filtered = filterDocumentsByTitle({
    documents,
    titleSearchQuery: "offset",
    normalizeText,
    filenameFromPath,
  });

  assert.deepEqual(filtered.map((record) => record.document_id), ["1"]);
});

test("ensureSelectedDocumentId keeps existing selection when visible", () => {
  const candidateDocuments = [
    { document_id: "1" },
    { document_id: "2" },
  ];

  assert.equal(
    ensureSelectedDocumentId({ candidateDocuments, selectedDocumentId: "2" }),
    "2"
  );
  assert.equal(
    ensureSelectedDocumentId({ candidateDocuments, selectedDocumentId: "3" }),
    "1"
  );
  assert.equal(
    ensureSelectedDocumentId({ candidateDocuments: [], selectedDocumentId: "1" }),
    null
  );
});
