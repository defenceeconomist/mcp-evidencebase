import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFolderMetadataPatch,
  buildGroupedDocumentItems,
  computeFolderSelectionState,
  ensureSelectedDocumentId,
  ensureSelectedItemId,
  filterDocumentsByTitle,
} from "../../js/table-ui.mjs";

const normalizeText = (value) => (value === null || value === undefined ? "" : String(value).trim());
const filenameFromPath = (value) => {
  const parts = String(value || "").split("/");
  return parts[parts.length - 1] || "";
};
const bibtexFields = ["title", "author", "booktitle", "editor", "year", "publisher", "address", "edition", "series", "volume", "number", "month", "note", "doi", "isbn"];
const getRecordBibtexFieldValue = (record, fieldName) => {
  return normalizeText(record?.bibtex_fields?.[fieldName] ?? record?.[fieldName]);
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

test("ensureSelectedItemId keeps existing item selection when visible", () => {
  const candidateItems = [
    { item_id: "folder:book" },
    { item_id: "document:2" },
  ];

  assert.equal(ensureSelectedItemId({ candidateItems, selectedItemId: "document:2" }), "document:2");
  assert.equal(ensureSelectedItemId({ candidateItems, selectedItemId: "missing" }), "folder:book");
});

test("buildGroupedDocumentItems creates a folder parent for shared prefixes", () => {
  const documents = [
    {
      document_id: "1",
      file_path: "book/chapter-1.pdf",
      title: "Chapter 1",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      bulk_selected: false,
      bibtex_fields: { title: "Chapter 1", booktitle: "The Book", author: "Lovelace, Ada", year: "2024", publisher: "Press" },
    },
    {
      document_id: "2",
      file_path: "book/chapter-2.pdf",
      title: "Chapter 2",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      bulk_selected: false,
      bibtex_fields: { title: "Chapter 2", booktitle: "The Book", author: "Lovelace, Ada", year: "2024", publisher: "Press" },
    },
    {
      document_id: "3",
      file_path: "standalone.pdf",
      title: "Standalone",
      authors: [],
      bulk_selected: false,
      bibtex_fields: { title: "Standalone" },
    },
  ];

  const grouped = buildGroupedDocumentItems({
    documents,
    titleSearchQuery: "",
    normalizeText,
    filenameFromPath,
    bibtexFields,
    getRecordBibtexFieldValue,
    expandedFolderKeys: new Set(["book"]),
  });

  assert.deepEqual(grouped.visibleItems.map((item) => item.item_id), [
    "folder:book",
    "document:1",
    "document:2",
    "document:3",
  ]);
  assert.equal(grouped.visibleItems[0].title, "The Book");
});

test("buildGroupedDocumentItems keeps single-child prefixes flat", () => {
  const documents = [
    {
      document_id: "1",
      file_path: "book/chapter-1.pdf",
      title: "Chapter 1",
      authors: [],
      bulk_selected: false,
      bibtex_fields: { title: "Chapter 1", booktitle: "The Book" },
    },
  ];

  const grouped = buildGroupedDocumentItems({
    documents,
    titleSearchQuery: "",
    normalizeText,
    filenameFromPath,
    bibtexFields,
    getRecordBibtexFieldValue,
    expandedFolderKeys: new Set(["book"]),
  });

  assert.deepEqual(grouped.visibleItems.map((item) => item.item_id), ["document:1"]);
});

test("buildGroupedDocumentItems shows mixed parent metadata as blank", () => {
  const documents = [
    {
      document_id: "1",
      file_path: "book/chapter-1.pdf",
      title: "Chapter 1",
      authors: [],
      bulk_selected: false,
      bibtex_fields: { title: "Chapter 1", booktitle: "Book A", publisher: "Press 1" },
    },
    {
      document_id: "2",
      file_path: "book/chapter-2.pdf",
      title: "Chapter 2",
      authors: [],
      bulk_selected: false,
      bibtex_fields: { title: "Chapter 2", booktitle: "Book B", publisher: "Press 2" },
    },
  ];

  const grouped = buildGroupedDocumentItems({
    documents,
    titleSearchQuery: "",
    normalizeText,
    filenameFromPath,
    bibtexFields,
    getRecordBibtexFieldValue,
    expandedFolderKeys: new Set(),
  });

  assert.equal(grouped.visibleItems[0].title, "book");
  assert.equal(grouped.visibleItems[0].publisher, "");
  assert.equal(grouped.visibleItems[0].mixed_fields.title, true);
  assert.equal(grouped.visibleItems[0].mixed_fields.publisher, true);
});

test("buildFolderMetadataPatch maps parent book title to child booktitle payload", () => {
  const payload = buildFolderMetadataPatch({
    folderRecord: {
      title: "The Book",
      author: "Lovelace, Ada",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      year: "2024",
      publisher: "Press",
      isbn: "123",
    },
    normalizeText,
  });

  assert.deepEqual(payload, {
    authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
    author: "Lovelace, Ada",
    booktitle: "The Book",
    editor: "",
    year: "2024",
    publisher: "Press",
    address: "",
    edition: "",
    series: "",
    volume: "",
    number: "",
    month: "",
    note: "",
    doi: "",
    isbn: "123",
  });
});

test("computeFolderSelectionState reflects all, none, and partial child selection", () => {
  assert.deepEqual(
    computeFolderSelectionState([{ bulk_selected: false }, { bulk_selected: false }]),
    { bulkSelected: false, selectionState: "none" }
  );
  assert.deepEqual(
    computeFolderSelectionState([{ bulk_selected: true }, { bulk_selected: true }]),
    { bulkSelected: true, selectionState: "all" }
  );
  assert.deepEqual(
    computeFolderSelectionState([{ bulk_selected: true }, { bulk_selected: false }]),
    { bulkSelected: false, selectionState: "partial" }
  );
});

test("buildGroupedDocumentItems auto-expands matching parents and keeps matching children only", () => {
  const documents = [
    {
      document_id: "1",
      file_path: "book/chapter-1.pdf",
      title: "Chapter One",
      authors: [],
      bulk_selected: false,
      bibtex_fields: { title: "Chapter One", booktitle: "The Book" },
    },
    {
      document_id: "2",
      file_path: "book/chapter-2.pdf",
      title: "Appendix",
      authors: [],
      bulk_selected: false,
      bibtex_fields: { title: "Appendix", booktitle: "The Book" },
    },
  ];

  const grouped = buildGroupedDocumentItems({
    documents,
    titleSearchQuery: "chapter",
    normalizeText,
    filenameFromPath,
    bibtexFields,
    getRecordBibtexFieldValue,
    expandedFolderKeys: new Set(),
  });

  assert.deepEqual(grouped.visibleItems.map((item) => item.item_id), ["folder:book", "document:1"]);
  assert.equal(grouped.visibleItems[0].expanded, true);
});
