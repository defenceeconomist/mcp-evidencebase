const { test, expect } = require("@playwright/test");

test("renders collections, documents, and semantic search results", async ({ page }) => {
  let lastSearchUrl = null;

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

    if (
      method === "GET" &&
      pathname.endsWith("/api/collections/demo-collection/documents")
    ) {
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
