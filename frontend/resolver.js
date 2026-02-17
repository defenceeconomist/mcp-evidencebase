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
  const highlightCallout = document.getElementById("resolver-highlight");
  const highlightText = document.getElementById("resolver-highlight-text");
  const pagesContainer = document.getElementById("resolver-pages");
  const pagesShell = document.getElementById("resolver-canvas-shell");
  const pageInput = document.getElementById("resolver-page-input");
  const pageTotal = document.getElementById("resolver-page-total");
  const prevButton = document.getElementById("resolver-prev");
  const nextButton = document.getElementById("resolver-next");
  const openRawLink = document.getElementById("resolver-open-raw");

  const query = new URLSearchParams(window.location.search);
  const bucketName = (query.get("bucket") || "").trim();
  const filePath = (query.get("file_path") || "").trim().replace(/^\/+/, "");
  const requestedPage = Number.parseInt(query.get("page") || "1", 10);
  const requestedHighlight = (query.get("highlight") || "").trim();

  const encodePath = (value) => encodeURIComponent(value || "");
  const apiUrl = `/api/collections/${encodePath(bucketName)}/documents/resolve?file_path=${encodePath(filePath)}`;

  let pdfDocument = null;
  let currentPage = Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
  let pageNodes = [];
  let isProgrammaticScroll = false;
  let scrollUnlockTimer = 0;
  let highlightTimer = 0;
  const getDocumentScrollElement = () => document.scrollingElement || document.documentElement;

  const isElementScrollableY = (element) => {
    if (!element) {
      return false;
    }
    return element.scrollHeight - element.clientHeight > 1;
  };

  const getActiveScrollElement = () => {
    return isElementScrollableY(pagesShell) ? pagesShell : getDocumentScrollElement();
  };

  const getCurrentScrollTop = (element) => {
    if (element === getDocumentScrollElement()) {
      return Math.max(
        0,
        window.scrollY || document.documentElement.scrollTop || element.scrollTop || 0
      );
    }
    return Math.max(0, element.scrollTop || 0);
  };

  const scrollElementTo = (element, top, behavior) => {
    const targetTop = Math.max(0, top);
    if (element === getDocumentScrollElement()) {
      window.scrollTo({ top: targetTop, behavior });
      return;
    }
    element.scrollTo({ top: targetTop, behavior });
  };

  const setStatus = (message) => {
    if (statusLabel) {
      statusLabel.textContent = message;
    }
  };

  const showTemporaryHighlight = (value) => {
    if (!highlightCallout || !highlightText) {
      return;
    }
    const normalizedValue = String(value || "").trim();
    if (!normalizedValue) {
      highlightCallout.classList.add("d-none");
      highlightCallout.classList.remove("is-fading");
      highlightText.textContent = "";
      return;
    }

    window.clearTimeout(highlightTimer);
    highlightCallout.classList.remove("d-none");
    highlightCallout.classList.remove("is-fading");
    highlightText.textContent = normalizedValue;
    highlightTimer = window.setTimeout(() => {
      highlightCallout.classList.add("is-fading");
      window.setTimeout(() => {
        highlightCallout.classList.add("d-none");
        highlightCallout.classList.remove("is-fading");
      }, 320);
    }, 7000);
  };

  const emphasizeAfterPageJump = (value) => {
    const normalizedValue = String(value || "").trim();
    if (!normalizedValue) {
      showTemporaryHighlight("");
      return;
    }
    window.setTimeout(() => {
      showTemporaryHighlight(normalizedValue);
    }, 620);
  };

  const syncPageControls = () => {
    const totalPages = pdfDocument ? pdfDocument.numPages : 1;
    pageInput.value = String(currentPage);
    pageInput.max = String(totalPages);
    pageTotal.textContent = `/ ${totalPages}`;
    prevButton.disabled = currentPage <= 1;
    nextButton.disabled = currentPage >= totalPages;
  };

  const getPageScale = (page) => {
    const baseViewport = page.getViewport({ scale: 1 });
    const shellWidth = Math.max(320, (pagesShell?.clientWidth || window.innerWidth || 900) - 32);
    const fitScale = shellWidth / Math.max(1, baseViewport.width);
    return Math.max(0.3, Math.min(1.6, fitScale));
  };

  const renderOnePage = async (pageNumber, targetCanvas) => {
    const page = await pdfDocument.getPage(pageNumber);
    const scale = getPageScale(page);
    const viewport = page.getViewport({ scale });
    const outputScale = window.devicePixelRatio || 1;

    targetCanvas.width = Math.floor(viewport.width * outputScale);
    targetCanvas.height = Math.floor(viewport.height * outputScale);
    targetCanvas.style.width = `${Math.floor(viewport.width)}px`;
    targetCanvas.style.height = `${Math.floor(viewport.height)}px`;

    const renderContext = {
      canvasContext: targetCanvas.getContext("2d", { alpha: false }),
      viewport,
      transform: outputScale !== 1 ? [outputScale, 0, 0, outputScale, 0, 0] : null,
    };
    await page.render(renderContext).promise;
  };

  const getPageScrollTop = (pageNode, scrollElement) => {
    const pageRect = pageNode.getBoundingClientRect();
    const shellRectTop =
      scrollElement === getDocumentScrollElement()
        ? 0
        : scrollElement.getBoundingClientRect().top;
    return Math.max(
      0,
      getCurrentScrollTop(scrollElement) + (pageRect.top - shellRectTop)
    );
  };

  const updateCurrentPageFromScroll = () => {
    if (!pageNodes.length || isProgrammaticScroll) {
      return;
    }

    const scrollElement = getActiveScrollElement();
    const anchorY = getCurrentScrollTop(scrollElement) + 28;
    let nextCurrentPage = 1;

    for (let index = 0; index < pageNodes.length; index += 1) {
      const pageNode = pageNodes[index];
      if (getPageScrollTop(pageNode, scrollElement) <= anchorY) {
        nextCurrentPage = index + 1;
      } else {
        break;
      }
    }

    if (nextCurrentPage !== currentPage) {
      currentPage = nextCurrentPage;
      syncPageControls();
      setStatus(`Page ${currentPage} of ${pdfDocument.numPages}`);
    }
  };

  const scrollToPage = (pageNumber, { behavior = "smooth" } = {}) => {
    if (!pdfDocument || !pageNodes.length) {
      return;
    }

    const normalizedPage = Math.min(Math.max(1, pageNumber), pdfDocument.numPages);
    const pageNode = pageNodes[normalizedPage - 1];
    if (!pageNode) {
      return;
    }

    currentPage = normalizedPage;
    syncPageControls();
    setStatus(`Page ${currentPage} of ${pdfDocument.numPages}`);

    isProgrammaticScroll = true;
    window.clearTimeout(scrollUnlockTimer);
    const scrollElement = getActiveScrollElement();
    const scrollTop = getPageScrollTop(pageNode, scrollElement) - 6;
    scrollElementTo(scrollElement, scrollTop, behavior);
    scrollUnlockTimer = window.setTimeout(() => {
      isProgrammaticScroll = false;
    }, 500);
  };

  const renderAllPages = async () => {
    pagesContainer.innerHTML = "";
    pageNodes = [];

    for (let pageNumber = 1; pageNumber <= pdfDocument.numPages; pageNumber += 1) {
      const pageWrap = document.createElement("section");
      pageWrap.className = "resolver-page";
      pageWrap.dataset.page = String(pageNumber);

      const canvas = document.createElement("canvas");
      canvas.setAttribute("aria-label", `PDF page ${pageNumber}`);

      pageWrap.appendChild(canvas);
      pagesContainer.appendChild(pageWrap);
      pageNodes.push(pageWrap);

      setStatus(`Rendering page ${pageNumber} of ${pdfDocument.numPages}...`);
      await renderOnePage(pageNumber, canvas);
    }
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
      currentPage = Math.min(currentPage, Math.max(1, pdfDocument.numPages));
      syncPageControls();
      await renderAllPages();
      scrollToPage(currentPage, { behavior: "auto" });
      window.requestAnimationFrame(() => {
        scrollToPage(currentPage, { behavior: "auto" });
        emphasizeAfterPageJump(requestedHighlight);
      });
      setStatus(`Page ${currentPage} of ${pdfDocument.numPages}`);
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
    scrollToPage(currentPage - 1);
  });

  nextButton.addEventListener("click", () => {
    if (!pdfDocument || currentPage >= pdfDocument.numPages) {
      return;
    }
    scrollToPage(currentPage + 1);
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
    scrollToPage(requested);
  });

  const onAnyScroll = () => {
    updateCurrentPageFromScroll();
  };
  pagesShell.addEventListener("scroll", onAnyScroll, { passive: true });
  window.addEventListener("scroll", onAnyScroll, { passive: true });

  void loadPdf();
})();
