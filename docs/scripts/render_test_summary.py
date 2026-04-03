#!/usr/bin/env python3
"""Render a grouped test-and-coverage summary for standalone test artifacts."""

from __future__ import annotations

import argparse
import ast
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

AREA_BY_FILE: dict[str, str] = {
    "tests/test_cli.py": "Reference: CLI And Package Metadata",
    "tests/test_core.py": "Reference: Core Bucket Helpers",
    "tests/test_minio_settings.py": "Reference: MinIO Settings",
    "tests/test_api.py": "Reference: API Endpoints",
    "tests/test_ingestion.py": "Vignette: End-To-End Ingestion Workflow",
}

AREA_ORDER: list[str] = list(AREA_BY_FILE.values())


def _escape_cell(value: str) -> str:
    """Escape content for Markdown table cells."""
    sanitized = " ".join(value.split())
    return sanitized.replace("|", r"\|")


def _load_test_docstrings(tests_dir: Path, repo_root: Path) -> dict[str, str]:
    """Read test function docstrings keyed by ``nodeid``."""
    resolved_tests_dir = tests_dir.resolve()
    resolved_repo_root = repo_root.resolve()
    docstrings: dict[str, str] = {}
    for path in sorted(resolved_tests_dir.glob("test_*.py")):
        module = ast.parse(path.read_text(encoding="utf-8"))
        absolute_prefix = path.as_posix()
        relative_prefix = path.relative_to(resolved_repo_root).as_posix()
        for node in module.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            if not node.name.startswith("test_"):
                continue
            doc = ast.get_docstring(node) or ""
            normalized = " ".join(doc.split())
            docstrings[f"{absolute_prefix}::{node.name}"] = normalized
            docstrings[f"{relative_prefix}::{node.name}"] = normalized
    return docstrings


def _parse_coverage(coverage_xml_path: Path) -> tuple[int, int, list[tuple[str, int, int, float]]]:
    """Parse coverage XML into totals and per-module line coverage."""
    if not coverage_xml_path.exists():
        return 0, 0, []

    root = ElementTree.parse(coverage_xml_path).getroot()
    covered_total = int(root.attrib.get("lines-covered", "0"))
    valid_total = int(root.attrib.get("lines-valid", "0"))

    module_lines: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for class_element in root.findall(".//class"):
        filename = class_element.attrib.get("filename", "")
        if not filename.startswith("src/mcp_evidencebase/"):
            continue
        lines = class_element.findall("./lines/line")
        valid = len(lines)
        covered = sum(1 for line in lines if int(line.attrib.get("hits", "0")) > 0)
        module_lines[filename][0] += covered
        module_lines[filename][1] += valid

    modules: list[tuple[str, int, int, float]] = []
    for filename, (covered, valid) in sorted(module_lines.items()):
        percent = (covered / valid * 100.0) if valid else 0.0
        modules.append((filename, covered, valid, percent))
    return covered_total, valid_total, modules


def _load_results(report_json_path: Path) -> list[dict[str, Any]]:
    """Load test results from ``pytest-html-plus`` JSON output."""
    if not report_json_path.exists():
        return []
    payload = json.loads(report_json_path.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    if isinstance(results, list):
        return [entry for entry in results if isinstance(entry, dict)]
    return []


def _commentary_for_result(result: dict[str, Any], docstrings: dict[str, str]) -> str:
    """Resolve commentary for one result row."""
    nodeid = str(result.get("nodeid", ""))
    test_name = str(result.get("test", ""))
    file_name = str(result.get("file", ""))
    doc = docstrings.get(nodeid)
    if doc:
        return doc

    fallback_key = f"{file_name}::{test_name}"
    doc = docstrings.get(fallback_key)
    if doc:
        return doc

    humanized = test_name.replace("test_", "").replace("_", " ").strip()
    if not humanized:
        return "No commentary available."
    return f"Validates {humanized}."


def render_summary(
    *,
    report_json_path: Path,
    coverage_xml_path: Path,
    tests_dir: Path,
    repo_root: Path,
    output_path: Path,
) -> None:
    """Render Markdown summary to ``output_path``."""
    results = _load_results(report_json_path)
    docstrings = _load_test_docstrings(tests_dir, repo_root)
    covered_total, valid_total, modules = _parse_coverage(coverage_xml_path)

    status_counts = Counter(str(result.get("status", "unknown")) for result in results)
    total = len(results)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = ["# Test Report Summary", "", f"Generated: `{generated_at}`", ""]

    if total == 0:
        lines.append(
            "No test results were found. Run `tests/build_test_reports.sh` to regenerate test artifacts."
        )
        lines.append("")
    else:
        lines.append("## Overview")
        lines.append("")
        lines.append(f"- Total tests: **{total}**")
        lines.append(f"- Passed: **{status_counts.get('passed', 0)}**")
        lines.append(f"- Failed: **{status_counts.get('failed', 0)}**")
        lines.append(f"- Skipped: **{status_counts.get('skipped', 0)}**")
        lines.append("")

    lines.append("## Coverage")
    lines.append("")
    if valid_total > 0:
        total_percent = covered_total / valid_total * 100.0
        lines.append(
            f"- Total line coverage: **{total_percent:.1f}%** "
            f"({covered_total}/{valid_total} lines)"
        )
    else:
        lines.append("- Coverage data unavailable.")
    lines.append("")

    if modules:
        lines.append("| Module | Covered | Total | Coverage |")
        lines.append("| --- | ---: | ---: | ---: |")
        for filename, covered, valid, percent in modules:
            lines.append(f"| `{_escape_cell(filename)}` | {covered} | {valid} | {percent:.1f}% |")
        lines.append("")

    grouped_results: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for result in results:
        file_name = str(result.get("file", ""))
        area = AREA_BY_FILE.get(file_name, "Other")
        grouped_results[area].append(result)

    lines.append("## Grouped Test Results")
    lines.append("")
    for area in AREA_ORDER + sorted(set(grouped_results) - set(AREA_ORDER)):
        area_results = grouped_results.get(area, [])
        if not area_results:
            continue
        area_counts = Counter(str(result.get("status", "unknown")) for result in area_results)
        lines.append(f"### {area}")
        lines.append("")
        lines.append(
            f"- Tests: **{len(area_results)}** | "
            f"Passed: **{area_counts.get('passed', 0)}** | "
            f"Failed: **{area_counts.get('failed', 0)}** | "
            f"Skipped: **{area_counts.get('skipped', 0)}**"
        )
        lines.append("")
        lines.append("| Test | Status | Duration (ms) | What is tested and expected result |")
        lines.append("| --- | --- | ---: | --- |")
        for result in sorted(area_results, key=lambda item: str(item.get("nodeid", ""))):
            nodeid = str(result.get("nodeid", ""))
            status = str(result.get("status", "unknown"))
            duration_ms = float(result.get("duration", 0.0)) * 1000.0
            commentary = _commentary_for_result(result, docstrings)
            lines.append(
                f"| `{_escape_cell(nodeid)}` | {status} | {duration_ms:.2f} | {_escape_cell(commentary)} |"
            )
        lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    root = Path(__file__).resolve().parents[2]
    report_dir = root / "build/test-reports"
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-json", type=Path, default=report_dir / "final_report.json")
    parser.add_argument("--coverage-xml", type=Path, default=report_dir / "coverage.xml")
    parser.add_argument("--tests-dir", type=Path, default=root / "tests")
    parser.add_argument("--output", type=Path, default=report_dir / "summary.md")
    return parser.parse_args()


def main() -> int:
    """Run summary rendering with repository-default paths."""
    root = Path(__file__).resolve().parents[2]
    args = parse_args()

    render_summary(
        report_json_path=args.report_json,
        coverage_xml_path=args.coverage_xml,
        tests_dir=args.tests_dir,
        repo_root=root,
        output_path=args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
