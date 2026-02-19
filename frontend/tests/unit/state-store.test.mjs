import test from "node:test";
import assert from "node:assert/strict";

import { createStateStore } from "../../js/state-store.mjs";

test("createStateStore supports get/set/update", () => {
  const store = createStateStore({ selectedBucketName: null, count: 0 });

  assert.equal(store.get("selectedBucketName"), null);
  store.set("selectedBucketName", "research-raw");
  assert.equal(store.get("selectedBucketName"), "research-raw");

  const nextCount = store.update("count", (value) => (value || 0) + 1);
  assert.equal(nextCount, 1);
  assert.equal(store.get("count"), 1);

  assert.deepEqual(store.snapshot(), {
    selectedBucketName: "research-raw",
    count: 1,
  });
});
