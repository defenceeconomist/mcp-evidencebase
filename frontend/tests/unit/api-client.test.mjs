import test from "node:test";
import assert from "node:assert/strict";

import { createApiRequest, parseApiErrorMessage } from "../../js/api-client.mjs";

test("parseApiErrorMessage returns detail when present", async () => {
  const response = {
    status: 400,
    statusText: "Bad Request",
    async json() {
      return { detail: "invalid payload" };
    },
  };

  const message = await parseApiErrorMessage(response);
  assert.equal(message, "invalid payload");
});

test("createApiRequest adds JSON content type and parses body", async () => {
  const calls = [];
  const apiRequest = createApiRequest({
    apiBasePath: "http://localhost/api",
    fetchImpl: async (url, options) => {
      calls.push({ url, options });
      return {
        ok: true,
        async json() {
          return { ok: true };
        },
      };
    },
  });

  const payload = await apiRequest("/buckets", { method: "GET" });
  assert.deepEqual(payload, { ok: true });
  assert.equal(calls[0].url, "http://localhost/api/buckets");
  assert.equal(calls[0].options.headers["Content-Type"], "application/json");
});

test("createApiRequest throws parsed API errors", async () => {
  const apiRequest = createApiRequest({
    apiBasePath: "http://localhost/api",
    fetchImpl: async () => ({
      ok: false,
      status: 404,
      statusText: "Not Found",
      async json() {
        return { detail: "missing" };
      },
    }),
  });

  await assert.rejects(() => apiRequest("/missing"), /missing/);
});
