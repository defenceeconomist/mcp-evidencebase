const { test, expect } = require("@playwright/test");

test("renders collections, documents, and semantic search results", async ({ page }) => {
  let lastSearchUrl = null;
  let lastDocumentsUrl = null;

  await page.addInitScript(() => {
    window.bootstrap = window.bootstrap || {
      Tooltip: {
        getOrCreateInstance: () => ({ setContent: () => {} }),
      },
      Modal: {
        getOrCreateInstance: () => ({ show: () => {}, hide: () => {} }),
      },
    };
    window.alert = () => {};
    window.confirm = () => true;
    window.prompt = () => null;
  });

  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const pathname = url.pathname;

    if (method === "GET" && pathname.endsWith("/api/buckets")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ buckets: ["demo-collection"] }),
      });
    }

    if (
      method === "GET" &&
      pathname.endsWith("/api/collections/demo-collection/documents")
    ) {
      lastDocumentsUrl = url;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          documents: [
            {
              id: "doc-1",
              file_path: "offsets/evidence-paper.pdf",
              title: "Offset policy evidence",
              year: "2024",
              publication: "Journal of Evidence",
              authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
              processing_state: "processed",
              processing_progress: 100,
              partitions_count: 3,
              chunks_count: 7,
              bibtex_fields: {
                title: "Offset policy evidence",
                author: "Lovelace, Ada",
                year: "2024",
                journal: "Journal of Evidence",
              },
            },
          ],
        }),
      });
    }

    if (
      method === "GET" &&
      pathname.endsWith("/api/collections/demo-collection/search")
    ) {
      lastSearchUrl = url;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          mode: "hybrid",
          results: [
            {
              document_id: "doc-1",
              title: "Offset policy evidence",
              author: "Lovelace, A.",
              year: "2024",
              minio_location: "demo-collection/offsets/evidence-paper.pdf",
              section_title: "Findings",
              page_start: 4,
              page_end: 4,
              text: "Offsets reduced procurement delays in comparable programs.",
              score: 0.87654,
            },
          ],
        }),
      });
    }

    return route.fulfill({
      status: 404,
      contentType: "application/json",
      body: JSON.stringify({ detail: `Unhandled mock route: ${method} ${pathname}` }),
    });
  });

  await page.goto("/");

  await expect(page.getByRole("link", { name: "Evidence Base" })).toBeVisible();
  await expect(page.locator("#bucket-list")).toContainText("demo-collection");
  await expect(page.locator("#document-meta-count")).toHaveText("1 documents");
  await expect(page.locator("#detail-document-tbody")).toContainText("Offset policy evidence");
  await expect
    .poll(() => (lastDocumentsUrl ? lastDocumentsUrl.searchParams.get("include_debug") : null))
    .toBe("false");
  await expect
    .poll(() => (lastDocumentsUrl ? lastDocumentsUrl.searchParams.get("include_locations") : null))
    .toBe("false");

  await page.getByRole("tab", { name: "Semantic Search" }).click();
  await page.fill("#semantic-search-query", "offsets evidence");
  await page.click("#semantic-search-submit");

  await expect(page.locator("#semantic-search-count")).toHaveText("1 result");
  await expect(page.locator("#semantic-search-results")).toContainText(
    "Offsets reduced procurement delays in comparable programs."
  );
  await expect(page.locator("#semantic-search-status")).toHaveText(
    'Query "offsets evidence" ran on demo-collection using hybrid mode.'
  );
  await expect
    .poll(() => (lastSearchUrl ? lastSearchUrl.searchParams.get("rrf_k") : null))
    .toBe("60");
});

test("detail view groups split pdf chapters under a book parent and saves parent metadata to children", async ({ page }) => {
  const documents = [
    {
      id: "doc-1",
      file_path: "book/chapter-1.pdf",
      title: "Chapter 1",
      year: "2024",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      processing_state: "processed",
      processing_progress: 100,
      partitions_count: 3,
      chunks_count: 7,
      bibtex_fields: {
        title: "Chapter 1",
        author: "Lovelace, Ada",
        booktitle: "The Book",
        year: "2024",
        publisher: "Original Press",
      },
    },
    {
      id: "doc-2",
      file_path: "book/chapter-2.pdf",
      title: "Chapter 2",
      year: "2024",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      processing_state: "processed",
      processing_progress: 100,
      partitions_count: 2,
      chunks_count: 5,
      bibtex_fields: {
        title: "Chapter 2",
        author: "Lovelace, Ada",
        booktitle: "The Book",
        year: "2024",
        publisher: "Original Press",
      },
    },
  ];
  const metadataRequests = [];

  await page.addInitScript(() => {
    window.bootstrap = window.bootstrap || {
      Tooltip: {
        getOrCreateInstance: () => ({ setContent: () => {} }),
      },
    };
    window.alert = () => {};
    window.confirm = () => true;
    window.prompt = () => null;
  });

  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const pathname = url.pathname;

    if (method === "GET" && pathname.endsWith("/api/buckets")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ buckets: ["demo-collection"] }),
      });
    }

    if (method === "GET" && pathname.endsWith("/api/collections/demo-collection/documents")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ documents }),
      });
    }

    if (method === "PUT" && /\/api\/collections\/demo-collection\/documents\/doc-[12]\/metadata$/.test(pathname)) {
      const payload = JSON.parse(request.postData() || "{}");
      metadataRequests.push({ pathname, metadata: payload.metadata });
      const targetDocument = documents.find((documentRecord) => pathname.includes(documentRecord.id));
      if (targetDocument) {
        Object.assign(targetDocument.bibtex_fields, payload.metadata);
      }
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ metadata: payload.metadata }),
      });
    }

    return route.fulfill({
      status: 404,
      contentType: "application/json",
      body: JSON.stringify({ detail: `Unhandled mock route: ${method} ${pathname}` }),
    });
  });

  await page.goto("/");

  await expect(page.locator("#detail-document-tbody")).toContainText("The Book");
  await page.locator("#detail-document-tbody tr").first().click();
  await expect(page.locator("#detail-selected-document")).toContainText("Selected book: The Book");
  await expect(page.locator("#detail-document-actions")).toHaveClass(/d-none/);

  await page
    .locator("#detail-fields-form .detail-field-row")
    .filter({ has: page.locator("label", { hasText: /^Title$/ }) })
    .locator("input")
    .fill("Updated Book");
  await page.waitForTimeout(800);
  await expect.poll(() => metadataRequests.length).toBe(2);
  expect(metadataRequests[0].metadata.booktitle).toBe("Updated Book");
  expect(metadataRequests[1].metadata.booktitle).toBe("Updated Book");
});

test("bulk edit shows expandable parent rows for split pdf folders", async ({ page }) => {
  const documents = [
    {
      id: "doc-1",
      file_path: "book/chapter-1.pdf",
      title: "Chapter 1",
      year: "2024",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      processing_state: "processed",
      processing_progress: 100,
      partitions_count: 1,
      chunks_count: 2,
      bibtex_fields: {
        title: "Chapter 1",
        author: "Lovelace, Ada",
        booktitle: "The Book",
        year: "2024",
      },
    },
    {
      id: "doc-2",
      file_path: "book/chapter-2.pdf",
      title: "Chapter 2",
      year: "2024",
      authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
      processing_state: "processed",
      processing_progress: 100,
      partitions_count: 1,
      chunks_count: 2,
      bibtex_fields: {
        title: "Chapter 2",
        author: "Lovelace, Ada",
        booktitle: "The Book",
        year: "2024",
      },
    },
  ];
  const deleteRequests = [];

  await page.addInitScript(() => {
    window.bootstrap = window.bootstrap || {
      Tooltip: {
        getOrCreateInstance: () => ({ setContent: () => {} }),
      },
    };
    window.alert = () => {};
    window.confirm = () => true;
    window.prompt = () => null;
  });

  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const pathname = url.pathname;

    if (method === "GET" && pathname.endsWith("/api/buckets")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ buckets: ["demo-collection"] }),
      });
    }

    if (method === "GET" && pathname.endsWith("/api/collections/demo-collection/documents")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ documents }),
      });
    }

    if (method === "DELETE" && /\/api\/collections\/demo-collection\/documents\/doc-[12]$/.test(pathname)) {
      deleteRequests.push(pathname);
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ removed: true }),
      });
    }

    return route.fulfill({
      status: 404,
      contentType: "application/json",
      body: JSON.stringify({ detail: `Unhandled mock route: ${method} ${pathname}` }),
    });
  });

  await page.goto("/");
  await page.locator("#table-view-mode-switch").check();

  await expect(page.locator("#document-hot-container")).toContainText("The Book");
  await page.locator("#document-hot-container .hot-tree-chevron[data-folder-toggle='true']").first().click();
  await expect(page.locator("#document-hot-container")).toContainText("Chapter 1");

  const rowCheckbox = page.locator("#document-hot-container tbody tr").first().locator('input[type="checkbox"]').first();
  await rowCheckbox.check();
  await page.click("#remove-selected-docs-btn");
  await expect.poll(() => deleteRequests.length).toBe(2);
});

test("switching collections reuses cached data and loads debug JSON on demand", async ({ page }) => {
  let alphaRequestCount = 0;
  let debugRequestCount = 0;

  await page.addInitScript(() => {
    window.bootstrap = window.bootstrap || {
      Tooltip: {
        getOrCreateInstance: () => ({ setContent: () => {} }),
      },
      Modal: {
        getOrCreateInstance: () => ({ show: () => {}, hide: () => {} }),
      },
    };
    window.alert = () => {};
    window.confirm = () => true;
    window.prompt = () => null;
  });

  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const pathname = url.pathname;

    if (method === "GET" && pathname.endsWith("/api/buckets")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ buckets: ["alpha", "beta"] }),
      });
    }

    if (method === "GET" && pathname.endsWith("/api/collections/alpha/documents")) {
      alphaRequestCount += 1;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          documents: [
            {
              id: "doc-a",
              file_path: "alpha-paper.pdf",
              title: "Alpha Cached",
              year: "2024",
              authors: [{ first_name: "Ada", last_name: "Lovelace", suffix: "" }],
              processing_state: "processed",
              processing_progress: 100,
              partitions_count: 1,
              chunks_count: 1,
              bibtex_fields: {
                title: "Alpha Cached",
                author: "Lovelace, Ada",
                year: "2024",
              },
            },
          ],
        }),
      });
    }

    if (method === "GET" && pathname.endsWith("/api/collections/beta/documents")) {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          documents: [
            {
              id: "doc-b",
              file_path: "beta-paper.pdf",
              title: "Beta Fresh",
              year: "2025",
              authors: [{ first_name: "Grace", last_name: "Hopper", suffix: "" }],
              processing_state: "processed",
              processing_progress: 100,
              partitions_count: 1,
              chunks_count: 1,
              bibtex_fields: {
                title: "Beta Fresh",
                author: "Hopper, Grace",
                year: "2025",
              },
            },
          ],
        }),
      });
    }

    if (method === "GET" && pathname.endsWith("/api/collections/alpha/documents/doc-a/debug")) {
      debugRequestCount += 1;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          document_id: "doc-a",
          partitions_tree: { partitions: [{ text: "Alpha partition" }] },
          chunks_tree: { chunks: [{ chunk_id: "chunk-a", text: "Alpha chunk" }] },
        }),
      });
    }

    return route.fulfill({
      status: 404,
      contentType: "application/json",
      body: JSON.stringify({ detail: `Unhandled mock route: ${method} ${pathname}` }),
    });
  });

  await page.goto("/");
  await expect(page.locator("#detail-document-tbody")).toContainText("Alpha Cached");

  await page.locator("#bucket-list li").filter({ hasText: "beta" }).click();
  await expect(page.locator("#detail-document-tbody")).toContainText("Beta Fresh");

  await page.locator("#bucket-list li").filter({ hasText: "alpha" }).click();
  await expect(page.locator("#detail-document-tbody")).toContainText("Alpha Cached");
  await expect.poll(() => alphaRequestCount).toBe(1);

  await page.locator(".parse-action-btn[data-json-action='partitions']").first().click();
  await expect.poll(() => debugRequestCount).toBe(1);
  await page.locator("#doc-json-modal .btn-close").click();

  await page.locator(".parse-action-btn[data-json-action='partitions']").first().click();
  await expect.poll(() => debugRequestCount).toBe(1);
});
