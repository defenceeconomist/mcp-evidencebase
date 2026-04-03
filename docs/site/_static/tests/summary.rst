Generated: ``2026-04-03 10:11:13 UTC``

- Total tests: **172**
- Passed: **169**
- Failed: **0**
- Skipped: **3**

Coverage
~~~~~~~~

- Total line coverage: **78.3%** (3544/4524 lines)

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
     - 35
     - 35
     - 100.0%
   * - ``src/mcp_evidencebase/api_modules/__init__.py``
     - 0
     - 0
     - 0.0%
   * - ``src/mcp_evidencebase/api_modules/deps.py``
     - 36
     - 43
     - 83.7%
   * - ``src/mcp_evidencebase/api_modules/errors.py``
     - 17
     - 23
     - 73.9%
   * - ``src/mcp_evidencebase/api_modules/models.py``
     - 21
     - 21
     - 100.0%
   * - ``src/mcp_evidencebase/api_modules/routers/__init__.py``
     - 0
     - 0
     - 0.0%
   * - ``src/mcp_evidencebase/api_modules/routers/buckets.py``
     - 32
     - 32
     - 100.0%
   * - ``src/mcp_evidencebase/api_modules/routers/collections.py``
     - 199
     - 245
     - 81.2%
   * - ``src/mcp_evidencebase/api_modules/routers/gpt.py``
     - 27
     - 27
     - 100.0%
   * - ``src/mcp_evidencebase/api_modules/services.py``
     - 454
     - 535
     - 84.9%
   * - ``src/mcp_evidencebase/api_modules/task_dispatch.py``
     - 13
     - 13
     - 100.0%
   * - ``src/mcp_evidencebase/bucket_service.py``
     - 10
     - 13
     - 76.9%
   * - ``src/mcp_evidencebase/celery_app.py``
     - 27
     - 29
     - 93.1%
   * - ``src/mcp_evidencebase/citation_schema.py``
     - 10
     - 10
     - 100.0%
   * - ``src/mcp_evidencebase/cli.py``
     - 44
     - 51
     - 86.3%
   * - ``src/mcp_evidencebase/core.py``
     - 41
     - 46
     - 89.1%
   * - ``src/mcp_evidencebase/ingestion.py``
     - 10
     - 10
     - 100.0%
   * - ``src/mcp_evidencebase/ingestion_modules/__init__.py``
     - 0
     - 0
     - 0.0%
   * - ``src/mcp_evidencebase/ingestion_modules/chunking.py``
     - 510
     - 707
     - 72.1%
   * - ``src/mcp_evidencebase/ingestion_modules/crossref.py``
     - 216
     - 273
     - 79.1%
   * - ``src/mcp_evidencebase/ingestion_modules/metadata.py``
     - 300
     - 368
     - 81.5%
   * - ``src/mcp_evidencebase/ingestion_modules/qdrant.py``
     - 289
     - 410
     - 70.5%
   * - ``src/mcp_evidencebase/ingestion_modules/repository.py``
     - 291
     - 373
     - 78.0%
   * - ``src/mcp_evidencebase/ingestion_modules/service.py``
     - 517
     - 751
     - 68.8%
   * - ``src/mcp_evidencebase/ingestion_modules/wiring.py``
     - 87
     - 98
     - 88.8%
   * - ``src/mcp_evidencebase/minio_settings.py``
     - 19
     - 19
     - 100.0%
   * - ``src/mcp_evidencebase/pdf_split.py``
     - 157
     - 175
     - 89.7%
   * - ``src/mcp_evidencebase/runtime_diagnostics.py``
     - 95
     - 123
     - 77.2%
   * - ``src/mcp_evidencebase/tasks.py``
     - 86
     - 90
     - 95.6%

Grouped Test Results
~~~~~~~~~~~~~~~~~~~~

Reference: CLI And Package Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **6** | Passed: **6** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_cli.py::test_main_doctor_prints_report_json``
     - passed
     - 0.22
     - Ensure ``--doctor`` prints the structured preflight report.
   * - ``tests/test_cli.py::test_main_healthcheck_exits_non_zero_when_runtime_is_not_ready``
     - passed
     - 0.23
     - Ensure ``--healthcheck`` reflects dependency readiness in its exit code.
   * - ``tests/test_cli.py::test_main_purge_datastores_prints_summary``
     - passed
     - 0.46
     - Ensure ``--purge-datastores`` runs purge and prints the summary payload.
   * - ``tests/test_cli.py::test_main_search_prints_results_json``
     - passed
     - 0.25
     - Ensure search flags call ingestion search and print serialized result payload.
   * - ``tests/test_cli.py::test_main_search_returns_non_zero_when_qdrant_is_disabled``
     - passed
     - 0.22
     - Search should fail cleanly with a dependency-disabled message.
   * - ``tests/test_cli.py::test_version_is_non_empty``
     - passed
     - 0.03
     - Check package metadata exports a non-empty version string.

Reference: Core Bucket Helpers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **8** | Passed: **8** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_core.py::test_add_minio_bucket_creates_when_missing``
     - passed
     - 0.08
     - Confirm missing buckets are created and propagate the requested region.
   * - ``tests/test_core.py::test_add_minio_bucket_rejects_empty_bucket_name``
     - passed
     - 0.08
     - Validate blank bucket names are rejected with a ``ValueError``.
   * - ``tests/test_core.py::test_add_minio_bucket_returns_false_when_exists``
     - passed
     - 0.03
     - Ensure creating an existing bucket is a no-op that returns ``False``.
   * - ``tests/test_core.py::test_healthcheck_returns_error_when_runtime_is_not_ready``
     - passed
     - 0.04
     - Verify the healthcheck helper reports ``error`` when required checks fail.
   * - ``tests/test_core.py::test_healthcheck_returns_ok_when_runtime_is_ready``
     - passed
     - 0.05
     - Verify the healthcheck helper reports ``ok`` when required checks pass.
   * - ``tests/test_core.py::test_list_minio_buckets_returns_sorted_names``
     - passed
     - 0.05
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
     - 0.03
     - Verify explicit environment values override MinIO defaults correctly.
   * - ``tests/test_minio_settings.py::test_build_minio_settings_uses_defaults``
     - passed
     - 0.03
     - Validate default MinIO settings when no environment values are provided.
   * - ``tests/test_minio_settings.py::test_to_bool_accepts_common_truthy_values``
     - passed
     - 0.03
     - Check canonical truthy strings are parsed into ``True`` values.
   * - ``tests/test_minio_settings.py::test_to_bool_returns_false_for_none_or_unknown_values``
     - passed
     - 0.03
     - Ensure unknown tokens and ``None`` map to ``False``.

Reference: API Endpoints
^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **58** | Passed: **58** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_api.py::test_api_startup_raises_when_required_dependencies_fail``
     - passed
     - 2.11
     - Startup should fail fast when required dependency checks are not ready.
   * - ``tests/test_api.py::test_create_bucket_maps_value_error_to_bad_request``
     - passed
     - 0.52
     - Confirm invalid bucket input is surfaced as HTTP 400 from ``POST /buckets``.
   * - ``tests/test_api.py::test_create_bucket_returns_created_result``
     - passed
     - 0.67
     - Check bucket creation normalizes whitespace without requiring Qdrant sync.
   * - ``tests/test_api.py::test_delete_bucket_maps_s3_errors_to_bad_request``
     - passed
     - 1.43
     - Verify ``DELETE /buckets/{bucket}`` maps MinIO errors to HTTP 400.
   * - ``tests/test_api.py::test_delete_bucket_returns_removed_result``
     - passed
     - 2.73
     - Ensure successful deletion returns bucket removal status only.
   * - ``tests/test_api.py::test_delete_document_keeps_partitions``
     - passed
     - 0.69
     - Ensure delete endpoint preserves partitions by forcing ``keep_partitions=True``.
   * - ``tests/test_api.py::test_download_collection_bibliography_dedupes_citation_keys``
     - passed
     - 0.83
     - Ensure duplicate citation keys are suffixed and structured authors are serialized.
   * - ``tests/test_api.py::test_download_collection_bibliography_returns_bibtex_attachment``
     - passed
     - 1.01
     - Ensure bibliography endpoint returns downloadable BibTeX built from metadata records.
   * - ``tests/test_api.py::test_fetch_document_metadata_from_crossref_returns_payload``
     - passed
     - 0.71
     - Check Crossref metadata fetch endpoint returns lookup/confidence payload.
   * - ``tests/test_api.py::test_get_buckets_maps_s3_errors_to_bad_request``
     - passed
     - 0.53
     - Verify MinIO ``S3Error`` values are mapped to HTTP 400 responses.
   * - ``tests/test_api.py::test_get_buckets_returns_bucket_names``
     - passed
     - 0.72
     - Ensure ``GET /buckets`` returns bucket names from the bucket service.
   * - ``tests/test_api.py::test_get_document_section_returns_section_payload``
     - passed
     - 0.72
     - Ensure section endpoint returns section mapping payload from ingestion service.
   * - ``tests/test_api.py::test_get_documents_returns_503_when_redis_is_disabled``
     - passed
     - 0.80
     - Disabled Redis-backed document listing should surface as HTTP 503.
   * - ``tests/test_api.py::test_get_documents_returns_documents``
     - passed
     - 2.02
     - Assert ``GET /collections/{bucket}/documents`` returns service records.
   * - ``tests/test_api.py::test_get_metadata_schema_returns_shared_rules``
     - passed
     - 1.82
     - Ensure metadata schema endpoint exposes shared citation/author rules.
   * - ``tests/test_api.py::test_gpt_openapi_exposes_ping_operation_with_bearer_auth_security``
     - passed
     - 0.71
     - Confirm GPT OpenAPI advertises Bearer Auth for ping.
   * - ``tests/test_api.py::test_gpt_ping_accepts_api_key_in_basic_username``
     - passed
     - 1.01
     - Assert GPT ping accepts API key in the Basic username field.
   * - ``tests/test_api.py::test_gpt_ping_accepts_bearer_api_key``
     - passed
     - 1.13
     - Assert GPT ping accepts API key via Bearer token.
   * - ``tests/test_api.py::test_gpt_ping_accepts_x_api_key_header``
     - passed
     - 16.24
     - Assert GPT ping accepts API key via X-API-Key header.
   * - ``tests/test_api.py::test_gpt_ping_rejects_invalid_basic_auth``
     - passed
     - 1.01
     - Assert GPT ping endpoint rejects invalid Basic Auth credentials.
   * - ``tests/test_api.py::test_gpt_ping_requires_basic_auth``
     - passed
     - 1.33
     - Assert GPT ping endpoint returns 401 when Basic Auth is missing.
   * - ``tests/test_api.py::test_gpt_ping_returns_pong_with_valid_basic_auth``
     - passed
     - 1.15
     - Assert GPT ping endpoint returns pong with valid API key via Basic Auth.
   * - ``tests/test_api.py::test_gpt_ping_returns_service_unavailable_when_auth_not_configured``
     - passed
     - 0.69
     - Assert GPT ping returns 503 if server API key is not configured.
   * - ``tests/test_api.py::test_gpt_ping_trims_whitespace_around_api_key``
     - passed
     - 1.01
     - Assert GPT ping auth comparisons are resilient to whitespace around key values.
   * - ``tests/test_api.py::test_gpt_search_defaults_to_minimal_response_shape``
     - passed
     - 1.01
     - Assert GPT search returns compact fields by default to reduce payload size.
   * - ``tests/test_api.py::test_gpt_search_honors_links_base_url_override``
     - passed
     - 1.09
     - Assert GPT search link fields use the configured public base URL when provided.
   * - ``tests/test_api.py::test_gpt_search_honors_links_base_url_override_without_scheme``
     - passed
     - 1.02
     - Assert GPT search base URL override defaults to HTTPS when scheme is omitted.
   * - ``tests/test_api.py::test_gpt_search_minimal_response_truncates_text_with_parameter``
     - passed
     - 1.02
     - Assert minimal response text is trimmed according to minimal_result_text_chars.
   * - ``tests/test_api.py::test_gpt_search_rejects_invalid_mode``
     - passed
     - 0.91
     - Assert GPT search wrapper validates mode and returns 400 for invalid values.
   * - ``tests/test_api.py::test_gpt_search_requires_bucket_name_when_multiple_buckets_exist``
     - passed
     - 0.88
     - Assert GPT search returns 400 when bucket_name is omitted and multiple buckets exist.
   * - ``tests/test_api.py::test_gpt_search_returns_results_with_bearer_auth``
     - passed
     - 1.81
     - Assert GPT search wrapper returns results and forwards search parameters.
   * - ``tests/test_api.py::test_gpt_search_staged_retrieval_returns_section_citations``
     - passed
     - 1.68
     - Assert default staged GPT search returns section-level citations with chunk anchors.
   * - ``tests/test_api.py::test_gpt_search_uses_single_bucket_when_bucket_name_omitted``
     - passed
     - 1.02
     - Assert GPT search auto-selects the only bucket when bucket_name is omitted.
   * - ``tests/test_api.py::test_healthz_returns_ok``
     - passed
     - 1.41
     - Assert ``/healthz`` returns a dependency-aware readiness payload.
   * - ``tests/test_api.py::test_list_document_sections_returns_sections_payload``
     - passed
     - 0.71
     - Ensure section listing endpoint returns ordered section mappings.
   * - ``tests/test_api.py::test_livez_returns_ok``
     - passed
     - 0.79
     - Assert ``/livez`` returns process liveness without dependency probes.
   * - ``tests/test_api.py::test_preview_document_split_enriches_metadata_seed_from_crossref``
     - passed
     - 1.68
     - Verify split preview merges Crossref-enriched metadata into the seed.
   * - ``tests/test_api.py::test_preview_document_split_prefers_chapter_like_headings_within_mixed_level``
     - passed
     - 1.48
     - Verify mixed heading levels prefer chapter-like split points.
   * - ``tests/test_api.py::test_preview_document_split_returns_heading_levels``
     - passed
     - 2.50
     - Verify split preview exposes chapter plans for outline heading levels 1-3.
   * - ``tests/test_api.py::test_readyz_returns_service_unavailable_when_not_ready``
     - passed
     - 0.52
     - Assert ``/readyz`` returns HTTP 503 when required dependencies fail.
   * - ``tests/test_api.py::test_rebuild_sections_can_target_single_document``
     - passed
     - 0.63
     - Ensure section rebuild endpoint supports document-specific mapping rebuild.
   * - ``tests/test_api.py::test_rebuild_sections_returns_503_when_redis_is_disabled``
     - passed
     - 0.56
     - Disabled Redis-backed section rebuild should surface as HTTP 503.
   * - ``tests/test_api.py::test_rebuild_sections_runs_bucket_rebuild_by_default``
     - passed
     - 0.72
     - Ensure section rebuild endpoint can rebuild all document mappings in a bucket.
   * - ``tests/test_api.py::test_reindex_document_queues_upsert_task``
     - passed
     - 0.78
     - Verify reindex endpoint queues the existing upsert stage for one document.
   * - ``tests/test_api.py::test_reindex_document_returns_queued_false_when_broker_is_unavailable``
     - passed
     - 0.81
     - Confirm reindex queue failures report queued=false without losing validation.
   * - ``tests/test_api.py::test_resolve_document_rejects_empty_file_path``
     - passed
     - 0.56
     - Verify resolver endpoint validates non-empty file_path query values.
   * - ``tests/test_api.py::test_resolve_document_returns_pdf_payload``
     - passed
     - 0.78
     - Ensure resolver endpoint streams bytes with inline PDF headers.
   * - ``tests/test_api.py::test_search_collection_rejects_invalid_mode``
     - passed
     - 0.69
     - Verify invalid search modes are mapped to HTTP 400 before service execution.
   * - ``tests/test_api.py::test_search_collection_returns_503_when_qdrant_is_disabled``
     - passed
     - 0.64
     - Disabled search dependencies should surface as HTTP 503.
   * - ``tests/test_api.py::test_search_collection_returns_results``
     - passed
     - 1.04
     - Ensure search endpoint returns ranked results from ingestion service.
   * - ``tests/test_api.py::test_trigger_bucket_scan_queues_task``
     - passed
     - 0.65
     - Ensure manual scan endpoint queues a task and returns ``queued=true``.
   * - ``tests/test_api.py::test_trigger_bucket_scan_returns_queued_false_when_broker_is_unavailable``
     - passed
     - 0.65
     - Confirm scan queue failures return ``queued=false`` with queue error details.
   * - ``tests/test_api.py::test_update_document_metadata_accepts_structured_authors``
     - passed
     - 0.64
     - Check metadata update accepts structured author entries.
   * - ``tests/test_api.py::test_update_document_metadata_returns_payload``
     - passed
     - 0.76
     - Check metadata update echoes normalized payload and service call arguments.
   * - ``tests/test_api.py::test_upload_document_queues_processing_task``
     - passed
     - 0.69
     - Verify uploads enqueue ``partition_minio_object`` and return task metadata.
   * - ``tests/test_api.py::test_upload_document_returns_queued_false_when_broker_is_unavailable``
     - passed
     - 0.96
     - Confirm broker failures keep upload successful but report ``queued=false``.
   * - ``tests/test_api.py::test_upload_split_document_honors_custom_folder_and_embeds_author_metadata``
     - passed
     - 1.55
     - Verify selected split files inherit schema-based metadata overrides.
   * - ``tests/test_api.py::test_upload_split_document_uploads_chapters_and_queues_tasks``
     - passed
     - 2.13
     - Verify chapter splits upload into the PDF title folder and queue processing per split.

Vignette: End-To-End Ingestion Workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Tests: **55** | Passed: **55** | Failed: **0** | Skipped: **0**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_ingestion.py::test_build_ingestion_service_allows_disabled_redis_and_qdrant``
     - passed
     - 0.11
     - Reduced-capability mode should build without Redis and Qdrant clients.
   * - ``tests/test_ingestion.py::test_build_ingestion_service_rejects_missing_required_qdrant_url``
     - passed
     - 0.09
     - Required Qdrant should fail fast before route/task execution.
   * - ``tests/test_ingestion.py::test_build_ingestion_service_rejects_missing_required_redis_url``
     - passed
     - 0.20
     - Required Redis should fail fast before route/task execution.
   * - ``tests/test_ingestion.py::test_build_ingestion_settings_do_not_default_redis_or_qdrant_urls``
     - passed
     - 0.05
     - Redis and Qdrant targets should be empty until explicitly configured.
   * - ``tests/test_ingestion.py::test_build_ingestion_settings_supports_chunking_element_overrides``
     - passed
     - 0.05
     - Confirm chunking element filters and text controls are parsed from env.
   * - ``tests/test_ingestion.py::test_build_ingestion_settings_supports_qdrant_timeout_override``
     - passed
     - 0.05
     - Confirm QDRANT_TIMEOUT_SECONDS is parsed and minimum-clamped.
   * - ``tests/test_ingestion.py::test_build_ingestion_settings_supports_unstructured_timeout_override``
     - passed
     - 0.15
     - Confirm UNSTRUCTURED_TIMEOUT_SECONDS is parsed and minimum-clamped.
   * - ``tests/test_ingestion.py::test_build_partition_chunks_attaches_page_numbers_and_bounding_boxes``
     - passed
     - 0.09
     - Verify chunk records include source page numbers and bounding-box metadata.
   * - ``tests/test_ingestion.py::test_chunk_object_reads_persisted_partitions_and_marks_processed``
     - passed
     - 0.55
     - Verify chunk stage reads stored partition payload and writes final processed state.
   * - ``tests/test_ingestion.py::test_chunk_partition_texts_preserves_element_boundaries_without_global_overlap``
     - passed
     - 0.14
     - Check chunking keeps whole-element boundaries and avoids global overlap.
   * - ``tests/test_ingestion.py::test_compute_chunk_point_id_is_deterministic_uuid``
     - passed
     - 0.09
     - Ensure chunk point IDs are deterministic UUIDs and change by chunk index.
   * - ``tests/test_ingestion.py::test_compute_document_id_is_deterministic``
     - passed
     - 0.04
     - Verify document IDs are deterministic SHA-256 hashes of the same bytes.
   * - ``tests/test_ingestion.py::test_crossref_rate_limit_enforces_list_and_single_intervals``
     - passed
     - 0.24
     - Verify list requests wait ~1s and single-record requests wait ~0.2s.
   * - ``tests/test_ingestion.py::test_crossref_request_kind_detects_single_and_list_requests``
     - passed
     - 0.04
     - Ensure Crossref request paths are classified correctly.
   * - ``tests/test_ingestion.py::test_extract_metadata_can_parse_first_page_identifiers``
     - passed
     - 0.10
     - Verify DOI/ISBN/ISSN are extracted from first-page text only.
   * - ``tests/test_ingestion.py::test_extract_metadata_limits_doi_extraction_to_first_page``
     - passed
     - 0.29
     - Confirm title/author come from PDF metadata and DOI from first-page only.
   * - ``tests/test_ingestion.py::test_extract_partition_bounding_box_reads_unstructured_coordinates``
     - passed
     - 0.04
     - Ensure partition coordinate metadata is normalized into a bounding-box payload.
   * - ``tests/test_ingestion.py::test_extract_pdf_title_author_reads_pdf_metadata``
     - passed
     - 0.08
     - Verify PDF title/author extraction reads embedded metadata values.
   * - ``tests/test_ingestion.py::test_fetch_metadata_from_crossref_falls_back_to_editors_when_author_missing``
     - passed
     - 0.17
     - Ensure editor-only Crossref records still populate displayable creator fields.
   * - ``tests/test_ingestion.py::test_fetch_metadata_from_crossref_parses_name_only_author_entries``
     - passed
     - 0.26
     - Ensure title lookups accept Crossref author entries that only expose ``name``.
   * - ``tests/test_ingestion.py::test_fetch_metadata_from_crossref_prefers_doi_and_updates_authors_and_entry_type``
     - passed
     - 0.96
     - Ensure DOI lookup is preferred and updates document_type/authors fields.
   * - ``tests/test_ingestion.py::test_fetch_metadata_from_crossref_prefers_isbn_before_issn_and_title``
     - passed
     - 0.25
     - Verify lookup order checks ISBN before ISSN/title when DOI is unavailable.
   * - ``tests/test_ingestion.py::test_fetch_metadata_from_crossref_rejects_low_confidence_title_match``
     - passed
     - 0.13
     - Ensure low-confidence title matches are not accepted.
   * - ``tests/test_ingestion.py::test_fetch_metadata_from_crossref_tries_next_candidate_when_first_has_no_updates``
     - passed
     - 0.25
     - Ensure title lookups continue when the top-scoring candidate adds no new fields.
   * - ``tests/test_ingestion.py::test_get_partitions_by_key_does_not_read_legacy_storage``
     - passed
     - 0.05
     - Verify legacy reverse-index/document partition keys are ignored.
   * - ``tests/test_ingestion.py::test_ingestion_service_purge_datastores_returns_deleted_counts``
     - passed
     - 0.06
     - Ensure service purge reports deleted Redis keys and Qdrant collections.
   * - ``tests/test_ingestion.py::test_ingestion_service_rebuild_document_section_mapping_from_partitions``
     - passed
     - 0.16
     - Ensure section mappings can be rebuilt independently from stored partitions.
   * - ``tests/test_ingestion.py::test_ingestion_service_search_documents_delegates_to_qdrant_indexer``
     - passed
     - 0.15
     - Ensure service search delegates to Qdrant indexer with unchanged parameters.
   * - ``tests/test_ingestion.py::test_ingestion_service_search_documents_hydrates_parent_section_from_redis``
     - passed
     - 0.12
     - Ensure search results are enriched from Redis metadata and section mappings.
   * - ``tests/test_ingestion.py::test_list_documents_includes_structured_authors_field``
     - passed
     - 0.11
     - Verify list_documents surfaces normalized structured author entries.
   * - ``tests/test_ingestion.py::test_list_documents_populates_chunks_tree_for_processed_documents``
     - passed
     - 0.47
     - Ensure list_documents returns computed chunk payloads for modal chunk viewing.
   * - ``tests/test_ingestion.py::test_partition_object_persists_partitions_and_metadata_without_chunking``
     - passed
     - 1.55
     - Ensure partition stage stores partitions/metadata and does not upsert vectors.
   * - ``tests/test_ingestion.py::test_qdrant_indexer_hybrid_search_merges_dense_and_keyword_results``
     - passed
     - 0.30
     - Verify hybrid search merges dense and keyword ranks with RRF.
   * - ``tests/test_ingestion.py::test_qdrant_indexer_purge_prefixed_collections``
     - passed
     - 0.07
     - Validate only collections with the configured prefix are deleted.
   * - ``tests/test_ingestion.py::test_qdrant_payload_contains_document_partition_meta_and_resolver_keys``
     - passed
     - 0.37
     - Validate Qdrant payload contains resolver, partition, and spatial metadata keys.
   * - ``tests/test_ingestion.py::test_qdrant_upsert_skips_image_chunks_for_embedding``
     - passed
     - 0.08
     - Ensure image chunks are excluded from embedding/index payload writes.
   * - ``tests/test_ingestion.py::test_remove_document_can_remove_partitions``
     - passed
     - 0.07
     - Verify partition payloads are deleted when ``keep_partitions`` is ``False``.
   * - ``tests/test_ingestion.py::test_remove_document_keeps_partitions_but_removes_other_redis_data``
     - passed
     - 0.14
     - Check document removal keeps partitions but clears source/meta mappings.
   * - ``tests/test_ingestion.py::test_repository_defaults_citation_key_from_author_year_and_title``
     - passed
     - 0.15
     - Ensure missing citation keys default to ``firstAuthorLastName + year + firstTitleWord``.
   * - ``tests/test_ingestion.py::test_repository_defaults_citation_key_from_chapter_filename_token``
     - passed
     - 0.07
     - Ensure chapter-marked filenames become ``chN<word>`` citation title tokens.
   * - ``tests/test_ingestion.py::test_repository_defaults_citation_key_from_structured_authors``
     - passed
     - 0.08
     - Ensure structured authors are preferred when deriving the citation key.
   * - ``tests/test_ingestion.py::test_repository_maps_document_to_multiple_minio_locations``
     - passed
     - 0.05
     - Ensure one document can map to multiple MinIO object locations deterministically.
   * - ``tests/test_ingestion.py::test_repository_persists_isbn_metadata_field``
     - passed
     - 0.05
     - Ensure ISBN is stored and returned through metadata payload fields.
   * - ``tests/test_ingestion.py::test_repository_persists_issn_metadata_field``
     - passed
     - 0.05
     - Ensure ISSN is stored and returned through metadata payload fields.
   * - ``tests/test_ingestion.py::test_repository_persists_structured_authors_metadata_field``
     - passed
     - 0.08
     - Ensure structured author entries are serialized and returned from metadata payload.
   * - ``tests/test_ingestion.py::test_repository_purge_prefix_data_deletes_only_prefixed_keys``
     - passed
     - 0.09
     - Ensure purge removes only keys under the configured Redis prefix.
   * - ``tests/test_ingestion.py::test_set_metadata_for_location_stores_payload_under_source_meta_key``
     - passed
     - 0.06
     - Ensure metadata payloads are nested under source keys.
   * - ``tests/test_ingestion.py::test_set_partitions_stores_payload_under_document_partition_key``
     - passed
     - 0.05
     - Ensure partition payloads are nested under the owning document key.
   * - ``tests/test_ingestion.py::test_unstructured_partition_client_wraps_read_timeout``
     - passed
     - 0.11
     - Ensure read timeouts raise a clear TimeoutError with tuning guidance.
   * - ``tests/test_ingestion.py::test_update_metadata_clears_stale_upsert_state_when_no_reindex_is_needed``
     - passed
     - 0.11
     - Ensure citation-key-only updates do not leave documents parked at upsert 80%.
   * - ``tests/test_ingestion.py::test_update_metadata_does_not_reindex_when_non_indexed_metadata_changes``
     - passed
     - 0.10
     - Ensure fields not persisted in Qdrant chunk payload don't force re-upsert.
   * - ``tests/test_ingestion.py::test_update_metadata_does_not_reindex_when_non_year_index_fields_change[metadata_update0]``
     - passed
     - 0.11
     - Validates update metadata does not reindex when non year index fields change metadata update0.
   * - ``tests/test_ingestion.py::test_update_metadata_does_not_reindex_when_non_year_index_fields_change[metadata_update1]``
     - passed
     - 0.11
     - Validates update metadata does not reindex when non year index fields change metadata update1.
   * - ``tests/test_ingestion.py::test_update_metadata_does_not_reindex_when_non_year_index_fields_change[metadata_update2]``
     - passed
     - 0.10
     - Validates update metadata does not reindex when non year index fields change metadata update2.
   * - ``tests/test_ingestion.py::test_update_metadata_reindexes_vectors_when_year_changes``
     - passed
     - 0.18
     - Ensure year updates refresh Qdrant payloads for existing chunks.

Other
^^^^^

- Tests: **41** | Passed: **38** | Failed: **0** | Skipped: **3**

.. list-table:: What each test validates
   :header-rows: 1
   :widths: 40 10 10 40

   * - Test
     - Status
     - Duration (ms)
     - What is tested and expected result
   * - ``tests/test_celery_app.py::test_validate_beat_runtime_dependencies_uses_beat_component_name``
     - passed
     - 0.04
     - Beat init hook should validate runtime dependencies as celery-beat.
   * - ``tests/test_celery_app.py::test_validate_runtime_dependencies_raises_for_failed_required_checks``
     - passed
     - 0.13
     - Celery startup validation should raise with the Celery component label.
   * - ``tests/test_celery_app.py::test_validate_worker_runtime_dependencies_uses_worker_component_name``
     - passed
     - 0.05
     - Worker init hook should validate runtime dependencies as celery-worker.
   * - ``tests/test_chunking.py::test_chunking_annotates_parent_section_metadata``
     - passed
     - 0.08
     - Validates chunking annotates parent section metadata.
   * - ``tests/test_chunking.py::test_chunking_chunk_id_is_deterministic``
     - passed
     - 0.10
     - Validates chunking chunk id is deterministic.
   * - ``tests/test_chunking.py::test_chunking_element_inclusion_is_configurable``
     - passed
     - 0.08
     - Validates chunking element inclusion is configurable.
   * - ``tests/test_chunking.py::test_chunking_excludes_headers_footers_and_parser_noise``
     - passed
     - 0.10
     - Validates chunking excludes headers footers and parser noise.
   * - ``tests/test_chunking.py::test_chunking_excludes_uncategorized_text``
     - passed
     - 0.06
     - Validates chunking excludes uncategorized text.
   * - ``tests/test_chunking.py::test_chunking_image_ocr_stays_searchable_while_section_text_uses_placeholder``
     - passed
     - 0.06
     - Validates chunking image ocr stays searchable while section text uses placeholder.
   * - ``tests/test_chunking.py::test_chunking_oversized_split_overlap_is_internal_only``
     - passed
     - 0.17
     - Validates chunking oversized split overlap is internal only.
   * - ``tests/test_chunking.py::test_chunking_page_range_metadata_is_correct``
     - passed
     - 0.10
     - Validates chunking page range metadata is correct.
   * - ``tests/test_chunking.py::test_chunking_parent_section_text_avoids_spurious_paragraph_breaks``
     - passed
     - 0.08
     - Validates chunking parent section text avoids spurious paragraph breaks.
   * - ``tests/test_chunking.py::test_chunking_parent_section_text_keeps_page_break_paragraphs``
     - passed
     - 0.06
     - Validates chunking parent section text keeps page break paragraphs.
   * - ``tests/test_chunking.py::test_chunking_parent_section_text_prefers_table_html_and_image_markdown``
     - passed
     - 0.07
     - Validates chunking parent section text prefers table html and image markdown.
   * - ``tests/test_chunking.py::test_chunking_size_limits_respected``
     - passed
     - 0.12
     - Validates chunking size limits respected.
   * - ``tests/test_chunking.py::test_chunking_small_chunk_merging``
     - passed
     - 0.17
     - Validates chunking small chunk merging.
   * - ``tests/test_chunking.py::test_chunking_tables_are_isolated``
     - passed
     - 0.08
     - Validates chunking tables are isolated.
   * - ``tests/test_chunking.py::test_chunking_title_creates_section_boundary``
     - passed
     - 0.55
     - Validates chunking title creates section boundary.
   * - ``tests/test_live_datastores_integration.py::test_live_delete_document_cleans_minio_redis_and_qdrant``
     - skipped
     - 0.05
     - Ensure delete_document removes source bytes plus Redis/Qdrant references for one document.
   * - ``tests/test_live_datastores_integration.py::test_live_etag_changes_require_reprocessing``
     - skipped
     - 0.06
     - Confirm Redis mapping state detects ETag changes from MinIO updates.
   * - ``tests/test_live_datastores_integration.py::test_live_partition_chunk_and_search_round_trip``
     - skipped
     - 0.11
     - Verify partition/chunk flow persists state and returns Qdrant-backed search hits.
   * - ``tests/test_runtime_diagnostics.py::test_build_runtime_contract_reads_explicit_flags``
     - passed
     - 0.04
     - Verify runtime dependency requirement flags are loaded from environment values.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_marks_missing_required_celery_broker_as_error``
     - passed
     - 0.06
     - Missing required broker config should fail readiness immediately.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_marks_missing_required_celery_result_backend_as_error``
     - passed
     - 0.06
     - Missing required result backend config should fail readiness immediately.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_marks_missing_required_qdrant_as_error``
     - passed
     - 0.07
     - Missing required Qdrant config should fail readiness without fallbacks.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_marks_missing_required_redis_as_error``
     - passed
     - 0.06
     - Missing required targets should fail readiness without localhost fallbacks.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_marks_optional_missing_dependency_as_disabled``
     - passed
     - 0.07
     - Optional dependencies with no configured target should be marked disabled.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_returns_ready_when_optional_qdrant_fails``
     - passed
     - 0.17
     - Optional dependency failures should not make the overall runtime unready.
   * - ``tests/test_runtime_diagnostics.py::test_collect_runtime_health_returns_unready_when_required_qdrant_fails``
     - passed
     - 0.07
     - Required dependency failures should make the readiness report fail.
   * - ``tests/test_runtime_diagnostics.py::test_log_runtime_health_includes_celery_result_backend``
     - passed
     - 0.11
     - Startup contract logging should include the result backend fragment.
   * - ``tests/test_runtime_diagnostics.py::test_raise_for_failed_required_checks_includes_component_details``
     - passed
     - 0.05
     - Startup failures should include the failing dependency and target.
   * - ``tests/test_tasks.py::test_chunk_task_enqueues_upsert_stage``
     - passed
     - 0.11
     - Ensure chunk task runs chunk stage and queues upsert stage.
   * - ``tests/test_tasks.py::test_meta_task_update_meta_fetches_crossref``
     - passed
     - 0.16
     - Verify metadata task can enrich from Crossref before queueing sections.
   * - ``tests/test_tasks.py::test_meta_task_update_meta_ignores_crossref_errors``
     - passed
     - 0.09
     - Ensure Crossref failures do not block downstream ingestion stages.
   * - ``tests/test_tasks.py::test_partition_task_enqueues_meta_stage``
     - passed
     - 0.17
     - Verify partition task runs partition stage and schedules metadata stage.
   * - ``tests/test_tasks.py::test_process_task_update_meta_fetches_crossref_before_tail_stages``
     - passed
     - 0.14
     - Verify compatibility wrapper still executes all stages inline.
   * - ``tests/test_tasks.py::test_scan_minio_objects_enqueues_partition_stage``
     - passed
     - 5.55
     - Ensure scan task queues only changed objects into the partition stage.
   * - ``tests/test_tasks.py::test_section_task_enqueues_chunk_stage``
     - passed
     - 0.11
     - Ensure section task runs section stage and queues chunk stage.
   * - ``tests/test_tasks.py::test_section_task_propagates_disabled_redis_dependency``
     - passed
     - 0.12
     - Redis-disabled section rebuilds should fail explicitly in worker tasks.
   * - ``tests/test_tasks.py::test_upsert_task_calls_upsert_stage``
     - passed
     - 0.11
     - Ensure upsert task runs upsert stage directly.
   * - ``tests/test_tasks.py::test_upsert_task_propagates_disabled_qdrant_dependency``
     - passed
     - 0.09
     - Qdrant-disabled upserts should fail explicitly in worker tasks.
