Reference
=========

Command Line Examples
---------------------

Use the bucket API through the proxy:

.. code-block:: bash

   BASE_URL="http://localhost:52180/api"

List buckets:

.. code-block:: bash

   curl -sS "$BASE_URL/buckets"

Add a bucket:

.. code-block:: bash

   curl -sS -X POST "$BASE_URL/buckets" \
     -H "Content-Type: application/json" \
     -d '{"bucket_name":"research-raw"}'

Remove a bucket:

.. code-block:: bash

   curl -sS -X DELETE "$BASE_URL/buckets/research-raw"

Core API
--------

.. autofunction:: mcp_evidencebase.core.add_minio_bucket

.. autofunction:: mcp_evidencebase.core.remove_minio_bucket

.. autofunction:: mcp_evidencebase.core.list_minio_buckets

.. autofunction:: mcp_evidencebase.core.healthcheck
