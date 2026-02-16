Reference
=========

CLI Module
----------

.. automodule:: mcp_evidencebase.cli
   :members:

Core Module
-----------

.. automodule:: mcp_evidencebase.core
   :members:
   :exclude-members: BucketSummaryLike, MinioClientLike

Bucket Service
--------------

.. automodule:: mcp_evidencebase.bucket_service
   :members:

MinIO Settings
--------------

.. automodule:: mcp_evidencebase.minio_settings
   :members:

API Module
----------

.. automodule:: mcp_evidencebase.api
   :members:
   :exclude-members: app, logger

Ingestion Models And Service
----------------------------

.. autoclass:: mcp_evidencebase.ingestion.IngestionSettings
   :members:

.. autoclass:: mcp_evidencebase.ingestion.IngestionService
   :members:

.. autofunction:: mcp_evidencebase.ingestion.build_ingestion_settings

.. autofunction:: mcp_evidencebase.ingestion.build_ingestion_service

Tasks And Worker App
--------------------

.. automodule:: mcp_evidencebase.tasks
   :members:

.. automodule:: mcp_evidencebase.celery_app
   :members:
