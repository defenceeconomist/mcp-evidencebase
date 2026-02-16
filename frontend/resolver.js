(function () {
  const pdfjsLib = window.pdfjsLib;
  if (!pdfjsLib) {
    window.alert("PDF.js failed to load.");
    return;
  }

  pdfjsLib.GlobalWorkerOptions.workerSrc =
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";

  const sourceLabel = document.getElementById("resolver-source");
  const statusLabel = document.getElementById("resolver-status");
  const canvas = document.getElementById("resolver-canvas");
  const pageInput = document.getElementById("resolver-page-input");
  const pageTotal = document.getElementById("resolver-page-total");
  const prevButton = document.getElementById("resolver-prev");
  const nextButton = document.getElementById("resolver-next");
  const openRawLink = document.getElementById("resolver-open-raw");

  const context = canvas.getContext("2d", { alpha: false });
  const query = new URLSearchParams(window.location.search);
  const bucketName = (query.get("bucket") || "").trim();
  const filePath = (query.get("file_path") || "").trim().replace(/^\/+/, "");
  const requestedPage = Number.parseInt(query.get("page") || "1", 10);

  const encodePath = (value) => encodeURIComponent(value || "");
  const apiUrl = `/api/collections/${encodePath(bucketName)}/documents/resolve?file_path=${encodePath(filePath)}`;

  let pdfDocument = null;
  let currentPage = Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
  let renderToken = 0;

  const setStatus = (message) => {
    if (statusLabel) {
      statusLabel.textContent = message;
    }
  };

  const syncPageControls = () => {
    const totalPages = pdfDocument ? pdfDocument.numPages : 1;
    pageInput.value = String(currentPage);
    pageInput.max = String(totalPages);
    pageTotal.textContent = `/ ${totalPages}`;
    prevButton.disabled = currentPage <= 1;
    nextButton.disabled = currentPage >= totalPages;
  };

  const renderPage = async (pageNumber) => {
    if (!pdfDocument) {
      return;
    }
    const normalizedPage = Math.min(Math.max(1, pageNumber), pdfDocument.numPages);
    currentPage = normalizedPage;
    syncPageControls();

    const token = ++renderToken;
    setStatus(`Rendering page ${normalizedPage}...`);

    const page = await pdfDocument.getPage(normalizedPage);
    const viewport = page.getViewport({ scale: 1.2 });
    const outputScale = window.devicePixelRatio || 1;

    canvas.width = Math.floor(viewport.width * outputScale);
    canvas.height = Math.floor(viewport.height * outputScale);
    canvas.style.width = `${Math.floor(viewport.width)}px`;
    canvas.style.height = `${Math.floor(viewport.height)}px`;

    const renderContext = {
      canvasContext: context,
      viewport,
      transform: outputScale !== 1 ? [outputScale, 0, 0, outputScale, 0, 0] : null,
    };
    await page.render(renderContext).promise;

    if (token !== renderToken) {
      return;
    }

    setStatus(`Page ${normalizedPage} of ${pdfDocument.numPages}`);
  };

  const loadPdf = async () => {
    if (!bucketName || !filePath) {
      sourceLabel.textContent = "Missing bucket or file path in resolver URL.";
      setStatus("Resolver URL is missing required query params.");
      prevButton.disabled = true;
      nextButton.disabled = true;
      pageInput.disabled = true;
      openRawLink.setAttribute("href", "#");
      openRawLink.classList.add("disabled");
      return;
    }

    sourceLabel.textContent = `${bucketName}/${filePath}`;
    openRawLink.href = apiUrl;

    try {
      const loadingTask = pdfjsLib.getDocument({ url: apiUrl, withCredentials: true });
      pdfDocument = await loadingTask.promise;
      if (currentPage > pdfDocument.numPages) {
        currentPage = pdfDocument.numPages;
      }
      await renderPage(currentPage);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setStatus(`Could not load PDF: ${message}`);
      window.alert(`Could not load PDF: ${message}`);
    }
  };

  prevButton.addEventListener("click", () => {
    if (!pdfDocument || currentPage <= 1) {
      return;
    }
    void renderPage(currentPage - 1);
  });

  nextButton.addEventListener("click", () => {
    if (!pdfDocument || currentPage >= pdfDocument.numPages) {
      return;
    }
    void renderPage(currentPage + 1);
  });

  pageInput.addEventListener("change", () => {
    if (!pdfDocument) {
      return;
    }
    const requested = Number.parseInt(pageInput.value || "1", 10);
    if (!Number.isFinite(requested)) {
      pageInput.value = String(currentPage);
      return;
    }
    void renderPage(requested);
  });

  void loadPdf();
})();
