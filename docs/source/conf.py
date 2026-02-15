"""Sphinx configuration for mcp-evidencebase."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("../../src"))

project = "mcp-evidencebase"
author = "mcp-evidencebase contributors"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns: list[str] = []

html_theme = "alabaster"
html_static_path = ["_static"]

napoleon_google_docstring = True
napoleon_numpy_docstring = False

autodoc_member_order = "bysource"
autodoc_typehints = "description"

autodoc_mock_imports = ["minio"]
