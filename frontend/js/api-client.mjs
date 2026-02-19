export const parseApiErrorMessage = async (response) => {
  try {
    const payload = await response.json();
    if (payload && typeof payload.detail === "string") {
      return payload.detail;
    }
  } catch {
    // Ignore JSON parse failures and use status text fallback.
  }
  return `${response.status} ${response.statusText}`;
};

export const createApiRequest = ({ apiBasePath, fetchImpl = fetch }) => {
  return async (path, options = {}) => {
    const headers = { ...(options.headers || {}) };
    const hasContentTypeHeader = Object.keys(headers).some(
      (headerName) => headerName.toLowerCase() === "content-type"
    );
    const isFormDataBody =
      typeof FormData !== "undefined" && options.body && options.body instanceof FormData;
    if (!hasContentTypeHeader && !isFormDataBody) {
      headers["Content-Type"] = "application/json";
    }

    const response = await fetchImpl(apiBasePath + path, {
      headers,
      ...options,
    });
    if (!response.ok) {
      throw new Error(await parseApiErrorMessage(response));
    }
    return response.json();
  };
};
