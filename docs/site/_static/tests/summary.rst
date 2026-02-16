Generated: ``2026-02-16 11:45:33 UTC``

- Total tests: **37**
- Passed: **37**
- Failed: **0**
- Skipped: **0**

Coverage
~~~~~~~~

- Total line coverage: **63.4%** (593/935 lines)

.. list-table:: Module coverage (``src/mcp_evidencebase``)
   :header-rows: 1
   :widths: 52 12 12 12

   * - Module
     - Covered
     - Total
     - Coverage
   * - ``src/mcp_evidencebase/__init__.py``
     - 1
     - 1
     - 100.0%
   * - ``src/mcp_evidencebase/__main__.py``
     - 0
     - 3
     - 0.0%
   * - ``src/mcp_evidencebase/api.py``
     - 104
     - 123
     - 84.6%
   * - ``src/mcp_evidencebase/bucket_service.py``
     - 10
     - 13
     - 76.9%
   * - ``src/mcp_evidencebase/celery_app.py``
     - 11
     - 13
     - 84.6%
   * - ``src/mcp_evidencebase/cli.py``
     - 0
     - 17
     - 0.0%
   * - ``src/mcp_evidencebase/core.py``
     - 39
     - 44
     - 88.6%
   * - ``src/mcp_evidencebase/ingestion.py``
     - 400
     - 677
     - 59.1%
   * - ``src/mcp_evidencebase/minio_settings.py``
     - 19
     - 19
     - 100.0%
   * - ``src/mcp_evidencebase/tasks.py``
     - 9
     - 25
     - 36.0%

Grouped Test Results
~~~~~~~~~~~~~~~~~~~~

Reference: CLI And Package Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **1** | Passed: **1** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_cli.py::test_version_is_non_empty``
     - passed
     - 0.04
     - Check package metadata exports a non-empty version string.

Reference: Core Bucket Helpers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **7** | Passed: **7** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_core.py::test_add_minio_bucket_creates_when_missing``
     - passed
     - 0.07
     - Confirm missing buckets are created and propagate the requested region.
   * - ``tests/test_core.py::test_add_minio_bucket_rejects_empty_bucket_name``
     - passed
     - 0.15
     - Validate blank bucket names are rejected with a ``ValueError``.
   * - ``tests/test_core.py::test_add_minio_bucket_returns_false_when_exists``
     - passed
     - 0.04
     - Ensure creating an existing bucket is a no-op that returns ``False``.
   * - ``tests/test_core.py::test_healthcheck``
     - passed
     - 0.04
     - Verify the healthcheck helper returns the stable ``ok`` status.
   * - ``tests/test_core.py::test_list_minio_buckets_returns_sorted_names``
     - passed
     - 0.06
     - Verify bucket listing is returned in deterministic alphabetical order.
   * - ``tests/test_core.py::test_remove_minio_bucket_removes_when_exists``
     - passed
     - 0.05
     - Confirm existing buckets are removed and tracked by the client stub.
   * - ``tests/test_core.py::test_remove_minio_bucket_returns_false_when_missing``
     - passed
     - 0.04
     - Ensure removing a missing bucket returns ``False`` without side effects.

Reference: MinIO Settings
^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **4** | Passed: **4** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_minio_settings.py::test_build_minio_settings_reads_env_values``
     - passed
     - 0.04
     - Verify explicit environment values override MinIO defaults correctly.
   * - ``tests/test_minio_settings.py::test_build_minio_settings_uses_defaults``
     - passed
     - 0.05
     - Validate default MinIO settings when no environment values are provided.
   * - ``tests/test_minio_settings.py::test_to_bool_accepts_common_truthy_values``
     - passed
     - 0.04
     - Check canonical truthy strings are parsed into ``True`` values.
   * - ``tests/test_minio_settings.py::test_to_bool_returns_false_for_none_or_unknown_values``
     - passed
     - 0.04
     - Ensure unknown tokens and ``None`` map to ``False``.

Reference: API Endpoints
^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **16** | Passed: **16** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_api.py::test_create_bucket_maps_value_error_to_bad_request``
     - passed
     - 0.80
     - Confirm invalid bucket input is surfaced as HTTP 400 from ``POST /buckets``.
   * - ``tests/test_api.py::test_create_bucket_returns_bad_gateway_when_qdrant_sync_fails``
     - passed
     - 0.75
     - Check Qdrant sync failures during create map to HTTP 502.
   * - ``tests/test_api.py::test_create_bucket_returns_created_result``
     - passed
     - 1.09
     - Check bucket creation normalizes whitespace and reports Qdrant sync status.
   * - ``tests/test_api.py::test_delete_bucket_maps_s3_errors_to_bad_request``
     - passed
     - 0.74
     - Verify ``DELETE /buckets/{bucket}`` maps MinIO errors to HTTP 400.
   * - ``tests/test_api.py::test_delete_bucket_returns_bad_gateway_when_qdrant_sync_fails``
     - passed
     - 0.78
     - Check Qdrant sync failures during delete map to HTTP 502.
   * - ``tests/test_api.py::test_delete_bucket_returns_removed_result``
     - passed
     - 0.96
     - Ensure successful deletion returns both bucket and Qdrant removal flags.
   * - ``tests/test_api.py::test_delete_document_keeps_partitions``
     - passed
     - 0.87
     - Ensure delete endpoint preserves partitions by forcing ``keep_partitions=True``.
   * - ``tests/test_api.py::test_get_buckets_maps_s3_errors_to_bad_request``
     - passed
     - 0.66
     - Verify MinIO ``S3Error`` values are mapped to HTTP 400 responses.
   * - ``tests/test_api.py::test_get_buckets_returns_bucket_names``
     - passed
     - 0.96
     - Ensure ``GET /buckets`` returns bucket names from the bucket service.
   * - ``tests/test_api.py::test_get_documents_returns_documents``
     - passed
     - 0.81
     - Assert ``GET /collections/{bucket}/documents`` returns service records.
   * - ``tests/test_api.py::test_healthz_returns_ok``
     - passed
     - 1.82
     - Assert ``/healthz`` returns HTTP 200 with ``{"status": "ok"}``.
   * - ``tests/test_api.py::test_trigger_bucket_scan_queues_task``
     - passed
     - 0.68
     - Ensure manual scan endpoint queues a task and returns ``queued=true``.
   * - ``tests/test_api.py::test_trigger_bucket_scan_returns_queued_false_when_broker_is_unavailable``
     - passed
     - 0.91
     - Confirm scan queue failures return ``queued=false`` with queue error details.
   * - ``tests/test_api.py::test_update_document_metadata_returns_payload``
     - passed
     - 0.92
     - Check metadata update echoes normalized payload and service call arguments.
   * - ``tests/test_api.py::test_upload_document_queues_processing_task``
     - passed
     - 0.73
     - Verify uploads enqueue ``process_minio_object`` and return task metadata.
   * - ``tests/test_api.py::test_upload_document_returns_queued_false_when_broker_is_unavailable``
     - passed
     - 1.13
     - Confirm broker failures keep upload successful but report ``queued=false``.

Vignette: End-To-End Ingestion Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **9** | Passed: **9** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_ingestion.py::test_chunk_partition_texts_returns_overlapping_chunks``
     - passed
     - 0.07
     - Check chunking applies overlap so adjacent chunks share boundary context.
   * - ``tests/test_ingestion.py::test_compute_chunk_point_id_is_deterministic_uuid``
     - passed
     - 0.09
     - Ensure chunk point IDs are deterministic UUIDs and change by chunk index.
   * - ``tests/test_ingestion.py::test_compute_document_id_is_deterministic``
     - passed
     - 0.06
     - Verify document IDs are deterministic SHA-256 hashes of the same bytes.
   * - ``tests/test_ingestion.py::test_extract_metadata_can_parse_first_page_identifiers``
     - passed
     - 0.09
     - Verify first-page title, author, DOI, and ISBN values are extracted.
   * - ``tests/test_ingestion.py::test_extract_metadata_limits_doi_extraction_to_first_page``
     - passed
     - 0.25
     - Confirm DOI extraction ignores reference-page DOIs beyond first-page partitions.
   * - ``tests/test_ingestion.py::test_qdrant_payload_contains_document_partition_meta_and_resolver_keys``
     - passed
     - 0.18
     - Validate Qdrant payload contains document, partition, metadata, and resolver keys.
   * - ``tests/test_ingestion.py::test_remove_document_can_remove_partitions``
     - passed
     - 0.06
     - Verify partition payloads are deleted when ``keep_partitions`` is ``False``.
   * - ``tests/test_ingestion.py::test_remove_document_keeps_partitions_but_removes_other_redis_data``
     - passed
     - 0.26
     - Check document removal keeps partitions but clears source/meta mappings.
   * - ``tests/test_ingestion.py::test_repository_maps_document_to_multiple_minio_locations``
     - passed
     - 0.18
     - Ensure one document can map to multiple MinIO object locations deterministically.
