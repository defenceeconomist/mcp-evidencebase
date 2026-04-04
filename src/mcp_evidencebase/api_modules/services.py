"""Shared service-layer helpers for API routes."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from fastapi import HTTPException, Request

from mcp_evidencebase.api_modules.errors import raise_document_http_error
from mcp_evidencebase.citation_schema import BIBTEX_FIELDS, DOCUMENT_TYPES
from mcp_evidencebase.ingestion import SEARCH_MODES, IngestionService
from mcp_evidencebase.perf import measure


def perform_collection_search(
    *,
    bucket_name: str,
    query: str,
    limit: int,
    mode: str,
    rrf_k: int,
    service: IngestionService,
) -> dict[str, Any]:
    """Execute collection search with shared validation and response shape."""
    normalized_mode = mode.strip().lower()
    if normalized_mode not in SEARCH_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"mode must be one of: {', '.join(SEARCH_MODES)}",
        )

    normalized_bucket_name = bucket_name.strip()
    normalized_query = query.strip()
    try:
        results = service.search_documents(
            bucket_name=normalized_bucket_name,
            query=normalized_query,
            limit=limit,
            mode=normalized_mode,
            rrf_k=rrf_k,
        )
    except Exception as exc:
        raise_document_http_error(exc)

    return {
        "bucket_name": normalized_bucket_name,
        "query": normalized_query,
        "mode": normalized_mode,
        "limit": max(1, min(int(limit), 100)),
        "rrf_k": max(1, int(rrf_k)),
        "results": results,
    }


_DOCUMENT_TYPES: frozenset[str] = frozenset(DOCUMENT_TYPES)
_CITATION_KEY_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9_\-:.]+")
_CITATION_KEY_FALLBACK_TOKEN_PATTERN = re.compile(r"[^A-Za-z0-9]+")


def _normalize_bibtex_entry_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _DOCUMENT_TYPES:
        return normalized
    return "misc"


def _normalize_bibtex_field_value(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", "\n").split()).strip()


def _normalize_citation_key(*, value: Any, fallback_seed: str) -> str:
    normalized = _CITATION_KEY_SANITIZE_PATTERN.sub("", str(value or "").strip())
    if normalized:
        return normalized
    fallback = _CITATION_KEY_FALLBACK_TOKEN_PATTERN.sub("", fallback_seed).lower()
    if fallback:
        return fallback[:40]
    return "citation"


def _uniquify_citation_key(*, preferred_key: str, seen_keys: set[str]) -> str:
    normalized_preferred_key = preferred_key.strip()
    dedupe_key = normalized_preferred_key.casefold()
    if dedupe_key not in seen_keys:
        seen_keys.add(dedupe_key)
        return normalized_preferred_key

    suffix = 2
    while True:
        candidate = f"{normalized_preferred_key}_{suffix}"
        dedupe_candidate = candidate.casefold()
        if dedupe_candidate not in seen_keys:
            seen_keys.add(dedupe_candidate)
            return candidate
        suffix += 1


def _format_bibtex_author_entry(entry: Mapping[str, Any]) -> str:
    first_name = str(
        entry.get("first_name")
        or entry.get("firstName")
        or entry.get("first")
        or entry.get("given")
        or ""
    ).strip()
    last_name = str(
        entry.get("last_name")
        or entry.get("lastName")
        or entry.get("last")
        or entry.get("family")
        or ""
    ).strip()
    suffix = str(entry.get("suffix") or entry.get("suffix_name") or "").strip()

    if last_name and suffix and first_name:
        return f"{last_name}, {suffix}, {first_name}"
    if last_name and first_name:
        return f"{last_name}, {first_name}"
    if last_name and suffix:
        return f"{last_name}, {suffix}"
    if last_name:
        return last_name
    if first_name:
        return first_name
    return ""


def _format_bibtex_author_list(value: Any) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ""
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            return ""
    if not isinstance(value, list):
        return ""

    formatted_entries: list[str] = []
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        formatted = _format_bibtex_author_entry(entry)
        if formatted:
            formatted_entries.append(formatted)
    return " and ".join(formatted_entries)


def _build_bibtex_entry(
    *,
    document_record: Mapping[str, Any],
    seen_citation_keys: set[str],
) -> str:
    document_id = _normalize_bibtex_field_value(
        document_record.get("document_id") or document_record.get("id")
    )
    title = _normalize_bibtex_field_value(
        document_record.get("title")
        or document_record.get("file_path")
        or document_id
        or "document"
    )
    raw_citation_key = _normalize_citation_key(
        value=document_record.get("citation_key"),
        fallback_seed=f"{title}{document_id}",
    )
    citation_key = _uniquify_citation_key(
        preferred_key=raw_citation_key,
        seen_keys=seen_citation_keys,
    )
    entry_type = _normalize_bibtex_entry_type(document_record.get("document_type"))

    source_bibtex_fields = document_record.get("bibtex_fields")
    bibtex_fields: dict[str, str] = {}
    if isinstance(source_bibtex_fields, Mapping):
        for field_name in BIBTEX_FIELDS:
            bibtex_fields[field_name] = _normalize_bibtex_field_value(
                source_bibtex_fields.get(field_name, "")
            )
    else:
        for field_name in BIBTEX_FIELDS:
            bibtex_fields[field_name] = _normalize_bibtex_field_value(
                document_record.get(field_name, "")
            )

    if not bibtex_fields.get("title"):
        bibtex_fields["title"] = title
    if not bibtex_fields.get("author"):
        bibtex_fields["author"] = _normalize_bibtex_field_value(
            _format_bibtex_author_list(document_record.get("authors"))
        )

    field_lines = [
        f"  {field_name} = {{{field_value}}}"
        for field_name in BIBTEX_FIELDS
        if (field_value := bibtex_fields.get(field_name, ""))
    ]
    if not field_lines:
        field_lines = [f"  title = {{{title}}}"]

    body = ",\n".join(field_lines)
    return f"@{entry_type}{{{citation_key},\n{body}\n}}"


def build_collection_bibtex(*, documents: list[dict[str, Any]]) -> tuple[str, int]:
    """Return deterministic BibTeX export payload and entry count for collection documents."""
    seen_citation_keys: set[str] = set()
    entries: list[str] = []

    sorted_documents = sorted(
        (document for document in documents if isinstance(document, Mapping)),
        key=lambda document: (
            _normalize_bibtex_field_value(document.get("citation_key")).casefold(),
            _normalize_bibtex_field_value(document.get("file_path")).casefold(),
            _normalize_bibtex_field_value(
                document.get("document_id") or document.get("id")
            ).casefold(),
        ),
    )
    for document_record in sorted_documents:
        entries.append(
            _build_bibtex_entry(
                document_record=document_record,
                seen_citation_keys=seen_citation_keys,
            )
        )

    if not entries:
        return "", 0
    return "\n\n".join(entries) + "\n", len(entries)


_STOPWORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "is",
        "of",
        "on",
        "or",
        "that",
        "the",
        "to",
        "with",
    }
)

_QUERY_REWRITE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("programme", ("program",)),
    ("program", ("programme",)),
    ("defence", ("defense",)),
    ("defense", ("defence",)),
    ("offsets", ("industrial participation", "countertrade")),
    ("offset", ("industrial participation", "countertrade")),
    ("industrial participation", ("offsets", "industrial offsets")),
    ("fms", ("foreign military sales",)),
    ("foreign military sales", ("fms",)),
)

_COUNTRY_ALIASES: dict[str, tuple[str, ...]] = {
    "australia": ("australia",),
    "canada": ("canada",),
    "france": ("france",),
    "germany": ("germany",),
    "india": ("india",),
    "israel": ("israel",),
    "italy": ("italy",),
    "japan": ("japan",),
    "netherlands": ("netherlands",),
    "norway": ("norway",),
    "poland": ("poland",),
    "saudi arabia": ("saudi arabia",),
    "south korea": ("south korea", "korea"),
    "spain": ("spain",),
    "sweden": ("sweden",),
    "turkiye": ("turkiye", "turkey"),
    "uae": ("uae", "united arab emirates"),
    "uk": ("uk", "u.k.", "united kingdom", "britain"),
    "us": ("us", "u.s.", "usa", "u.s.a.", "united states"),
}

_YEAR_PATTERN = re.compile(r"\b(?:19|20)\d{2}\b")
_YEAR_RANGE_PATTERN = re.compile(
    r"\b(?P<start>(?:19|20)\d{2})\s*(?:-|to|through|until)\s*(?P<end>(?:19|20)\d{2})\b",
    flags=re.IGNORECASE,
)
_QUOTED_PROGRAMME_PATTERN = re.compile(r'"([^"]+)"')
_PROGRAMME_PATTERN = re.compile(
    r"\b([A-Za-z0-9][A-Za-z0-9/&\-\s]{1,90}?"
    r"\s(?:program(?:me)?|initiative|project|scheme|campaign|offsets?))\b",
    flags=re.IGNORECASE,
)

_MINIMAL_GPT_RESPONSE_TOP_LEVEL_FIELDS: tuple[str, ...] = (
    "bucket_name",
    "query",
    "mode",
    "limit",
    "rrf_k",
)

_MINIMAL_GPT_RESULT_FIELDS: tuple[str, ...] = (
    "id",
    "score",
    "document_id",
    "section_id",
    "section_title",
    "source_material_url",
    "resolver_link_url",
    "resolver_url",
    "resolver_reference",
    "page_start",
    "page_end",
)


def _bounded_int(*, value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        resolved = default
    return max(minimum, min(maximum, resolved))


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_query_text(value: str) -> str:
    return " ".join(str(value).split()).strip()


def _tokenize(value: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9][A-Za-z0-9_\-]*", value.lower())


def _replace_term_once(text: str, source: str, replacement: str) -> str:
    pattern = re.compile(rf"\b{re.escape(source)}\b", flags=re.IGNORECASE)
    return pattern.sub(replacement, text, count=1)


def _generate_query_variants(*, query: str, variant_limit: int) -> list[str]:
    normalized_query = _normalize_query_text(query)
    if not normalized_query:
        return []

    target_count = _bounded_int(
        value=variant_limit,
        default=6,
        minimum=3,
        maximum=8,
    )

    candidates: list[str] = [normalized_query]
    for source, replacements in _QUERY_REWRITE_RULES:
        if re.search(rf"\b{re.escape(source)}\b", normalized_query, flags=re.IGNORECASE) is None:
            continue
        for replacement in replacements:
            rewritten = _normalize_query_text(
                _replace_term_once(normalized_query, source, replacement)
            )
            if rewritten:
                candidates.append(rewritten)

    reduced_tokens = [token for token in _tokenize(normalized_query) if token not in _STOPWORDS]
    if reduced_tokens:
        reduced_query = " ".join(reduced_tokens[:10]).strip()
        if reduced_query:
            candidates.append(reduced_query)
    if '"' not in normalized_query:
        candidates.append(f'"{normalized_query}"')

    fallback_suffixes = ("policy", "analysis", "evidence", "framework")
    for suffix in fallback_suffixes:
        candidates.append(f"{normalized_query} {suffix}")

    variants: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized_candidate = _normalize_query_text(candidate)
        if not normalized_candidate:
            continue
        dedupe_key = normalized_candidate.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        variants.append(normalized_candidate)
        if len(variants) >= target_count:
            break

    return variants


def _extract_hard_filters(*, query: str) -> dict[str, Any]:
    normalized_query = _normalize_query_text(query)
    lowered_query = normalized_query.casefold()

    countries: list[str] = []
    for canonical_country, aliases in _COUNTRY_ALIASES.items():
        for alias in aliases:
            if re.search(rf"\b{re.escape(alias.casefold())}\b", lowered_query):
                countries.append(canonical_country)
                break

    years = sorted({int(match.group(0)) for match in _YEAR_PATTERN.finditer(normalized_query)})
    year_ranges: list[dict[str, int]] = []
    for match in _YEAR_RANGE_PATTERN.finditer(normalized_query):
        start_year = int(match.group("start"))
        end_year = int(match.group("end"))
        if end_year < start_year:
            start_year, end_year = end_year, start_year
        year_ranges.append({"start": start_year, "end": end_year})

    programme_names: list[str] = []
    for match in _QUOTED_PROGRAMME_PATTERN.finditer(normalized_query):
        candidate = _normalize_query_text(match.group(1))
        if 3 <= len(candidate) <= 120:
            programme_names.append(candidate)
    for match in _PROGRAMME_PATTERN.finditer(normalized_query):
        candidate = _normalize_query_text(match.group(1))
        if 3 <= len(candidate) <= 120:
            programme_names.append(candidate)

    deduped_programmes: list[str] = []
    seen_programmes: set[str] = set()
    for name in programme_names:
        dedupe_key = name.casefold()
        if dedupe_key in seen_programmes:
            continue
        seen_programmes.add(dedupe_key)
        deduped_programmes.append(name)

    return {
        "countries": countries,
        "years": years,
        "year_ranges": year_ranges,
        "programme_names": deduped_programmes,
    }


def _hard_filter_match_bonus(*, text: str, hard_filters: dict[str, Any]) -> float:
    normalized_text = text.casefold()
    bonus = 0.0

    countries = hard_filters.get("countries", [])
    if isinstance(countries, list):
        matched_countries = 0
        for country in countries:
            aliases = _COUNTRY_ALIASES.get(str(country), ())
            if any(
                re.search(rf"\b{re.escape(alias.casefold())}\b", normalized_text)
                for alias in aliases
            ):
                matched_countries += 1
        if countries:
            bonus += 0.25 * (matched_countries / len(countries))

    years = hard_filters.get("years", [])
    if isinstance(years, list) and years:
        matched_years = sum(1 for year in years if str(year) in normalized_text)
        bonus += 0.2 * (matched_years / len(years))

    programme_names = hard_filters.get("programme_names", [])
    if isinstance(programme_names, list) and programme_names:
        matched_programmes = sum(
            1
            for programme_name in programme_names
            if str(programme_name).casefold() in normalized_text
        )
        bonus += 0.35 * (matched_programmes / len(programme_names))

    return bonus


def _truncate_text(*, value: str, max_chars: int) -> str:
    text = str(value).strip()
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return f"{text[: max_chars - 3].rstrip()}..."


def _has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def _score_section_text(
    *,
    query_variants: list[str],
    hard_filters: dict[str, Any],
    section_title: str,
    section_text: str,
    shortlist_score: float,
    matched_variant_count: int,
) -> float:
    combined_text = f"{section_title}\n{section_text}".strip()
    combined_tokens = set(_tokenize(combined_text))
    query_tokens = set(_tokenize(" ".join(query_variants)))
    token_overlap = 0.0
    if query_tokens and combined_tokens:
        token_overlap = len(query_tokens & combined_tokens) / len(query_tokens)

    normalized_combined = combined_text.casefold()
    variant_phrase_hits = 0
    for variant in query_variants:
        variant_text = _normalize_query_text(variant).casefold()
        if variant_text and variant_text in normalized_combined:
            variant_phrase_hits += 1

    filter_bonus = _hard_filter_match_bonus(text=combined_text, hard_filters=hard_filters)
    return (
        (shortlist_score * 0.55)
        + (token_overlap * 2.0)
        + (0.12 * float(matched_variant_count))
        + (0.08 * float(variant_phrase_hits))
        + filter_bonus
    )


def perform_gpt_collection_search(
    *,
    bucket_name: str,
    query: str,
    limit: int,
    mode: str,
    rrf_k: int,
    service: IngestionService,
    use_staged_retrieval: bool,
    query_variant_limit: int,
    wide_limit_per_variant: int,
    section_shortlist_limit: int,
    max_section_text_chars: int,
) -> dict[str, Any]:
    """Execute GPT search with optional multi-stage retrieval and section-level citations."""
    with measure("perform_gpt_collection_search"):
        if not use_staged_retrieval:
            return perform_collection_search(
                bucket_name=bucket_name,
                query=query,
                limit=limit,
                mode=mode,
                rrf_k=rrf_k,
                service=service,
            )

        normalized_mode = mode.strip().lower()
        if normalized_mode not in SEARCH_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"mode must be one of: {', '.join(SEARCH_MODES)}",
            )

        normalized_bucket_name = bucket_name.strip()
        normalized_query = query.strip()
        resolved_limit = _bounded_int(value=limit, default=10, minimum=1, maximum=100)
        resolved_rrf_k = max(1, int(rrf_k))
        resolved_variant_limit = _bounded_int(
            value=query_variant_limit,
            default=6,
            minimum=3,
            maximum=8,
        )
        resolved_wide_limit = _bounded_int(
            value=wide_limit_per_variant,
            default=75,
            minimum=50,
            maximum=100,
        )
        resolved_shortlist_limit = _bounded_int(
            value=section_shortlist_limit,
            default=20,
            minimum=10,
            maximum=30,
        )
        resolved_max_section_text_chars = _bounded_int(
            value=max_section_text_chars,
            default=2500,
            minimum=250,
            maximum=12000,
        )

        query_variants = _generate_query_variants(
            query=normalized_query,
            variant_limit=resolved_variant_limit,
        )
        hard_filters = _extract_hard_filters(query=normalized_query)

        chunk_hits: dict[str, dict[str, Any]] = {}
        total_wide_hits = 0

        try:
            variant_results_by_query = service.search_document_variants(
                bucket_name=normalized_bucket_name,
                queries=query_variants,
                limit=resolved_wide_limit,
                mode=normalized_mode,
                rrf_k=resolved_rrf_k,
            )
            for variant in query_variants:
                variant_results = variant_results_by_query.get(variant, [])
                total_wide_hits += len(variant_results)
                for rank, item in enumerate(variant_results):
                    if not isinstance(item, dict):
                        continue

                    chunk_id = str(item.get("id", "")).strip()
                    if not chunk_id:
                        fallback_doc = str(item.get("document_id", "")).strip() or "unknown-doc"
                        fallback_idx = int(item.get("chunk_index", rank) or rank)
                        chunk_id = f"{fallback_doc}:{fallback_idx}"

                    item_score = _as_float(item.get("score"))
                    entry = chunk_hits.get(chunk_id)
                    if entry is None:
                        chunk_hits[chunk_id] = {
                            "best_item": dict(item),
                            "max_score": item_score,
                            "scores": [item_score],
                            "matched_variants": [variant],
                            "best_rank": rank,
                        }
                        continue

                    entry_scores = entry.get("scores")
                    if isinstance(entry_scores, list):
                        entry_scores.append(item_score)
                    else:
                        entry["scores"] = [item_score]
                    if item_score > _as_float(entry.get("max_score")):
                        entry["best_item"] = dict(item)
                        entry["max_score"] = item_score
                        entry["best_rank"] = rank
                    matched_variants = entry.get("matched_variants")
                    if isinstance(matched_variants, list) and variant not in matched_variants:
                        matched_variants.append(variant)
        except Exception as exc:
            raise_document_http_error(exc)

        if not chunk_hits:
            return {
                "bucket_name": normalized_bucket_name,
                "query": normalized_query,
                "mode": normalized_mode,
                "limit": resolved_limit,
                "rrf_k": resolved_rrf_k,
                "query_variants": query_variants,
                "hard_filters": hard_filters,
                "stage_stats": {
                    "wide_hits_total": total_wide_hits,
                    "wide_unique_chunks": 0,
                    "section_groups": 0,
                    "shortlisted_sections": 0,
                },
                "citations": [],
                "results": [],
            }

        section_groups: dict[str, dict[str, Any]] = {}
        for chunk_id, chunk_payload in chunk_hits.items():
            best_item_raw = chunk_payload.get("best_item")
            if not isinstance(best_item_raw, dict):
                continue
            best_item = best_item_raw
            document_id = str(best_item.get("document_id", "")).strip()
            if not document_id:
                continue
            section_id = str(best_item.get("section_id", "")).strip() or str(
                best_item.get("parent_section_id", "")
            ).strip()
            if not section_id:
                section_id = f"__chunk__{chunk_id}"

            group_key = f"{document_id}:{section_id}"
            group_entry = section_groups.get(group_key)
            if group_entry is None:
                group_entry = {
                    "document_id": document_id,
                    "section_id": section_id,
                    "chunks": [],
                    "scores": [],
                    "matched_variants": [],
                    "best_item": best_item,
                }
                section_groups[group_key] = group_entry

            group_entry["chunks"].append(
                {
                    "chunk_id": chunk_id,
                    "item": best_item,
                    "score": _as_float(chunk_payload.get("max_score")),
                }
            )
            entry_scores = group_entry.get("scores")
            if isinstance(entry_scores, list):
                entry_scores.append(_as_float(chunk_payload.get("max_score")))
            matched_variants = chunk_payload.get("matched_variants")
            if isinstance(matched_variants, list):
                group_variants = group_entry.get("matched_variants")
                if isinstance(group_variants, list):
                    for variant in matched_variants:
                        if variant not in group_variants:
                            group_variants.append(variant)

        scored_sections: list[dict[str, Any]] = []
        for section_entry in section_groups.values():
            scores_raw = section_entry.get("scores")
            if not isinstance(scores_raw, list) or not scores_raw:
                continue
            sorted_scores = sorted((_as_float(score) for score in scores_raw), reverse=True)
            max_score = sorted_scores[0]
            top3_sum = sum(sorted_scores[:3])
            matched_variants = section_entry.get("matched_variants", [])
            variant_count = len(matched_variants) if isinstance(matched_variants, list) else 0
            best_item = section_entry.get("best_item", {})
            if not isinstance(best_item, dict):
                best_item = {}
            chunk_text = str(best_item.get("text", ""))
            section_title = str(
                best_item.get("section_title", "") or best_item.get("parent_section_title", "")
            )
            shortlist_score = (
                max_score
                + top3_sum
                + (0.1 * float(variant_count))
                + _hard_filter_match_bonus(
                    text=f"{section_title}\n{chunk_text}",
                    hard_filters=hard_filters,
                )
            )
            scored_section = dict(section_entry)
            scored_section["shortlist_score"] = shortlist_score
            scored_sections.append(scored_section)

        scored_sections.sort(
            key=lambda item: _as_float(item.get("shortlist_score")),
            reverse=True,
        )
        shortlisted_sections = scored_sections[:resolved_shortlist_limit]

        prefetched_sections_by_document: dict[str, dict[str, dict[str, Any]]] = {}
        for section_entry in shortlisted_sections:
            document_id = str(section_entry.get("document_id", "")).strip()
            section_id = str(section_entry.get("section_id", "")).strip()
            if (
                not document_id
                or not section_id
                or section_id.startswith("__chunk__")
                or document_id in prefetched_sections_by_document
            ):
                continue
            try:
                prefetched_sections_by_document[document_id] = service.list_document_sections_lookup(
                    bucket_name=normalized_bucket_name,
                    document_id=document_id,
                )
            except Exception:
                prefetched_sections_by_document[document_id] = {}

        reranked_sections: list[dict[str, Any]] = []
        for section_entry in shortlisted_sections:
            document_id = str(section_entry.get("document_id", "")).strip()
            section_id = str(section_entry.get("section_id", "")).strip()
            if not document_id or not section_id:
                continue

            chunks_raw = section_entry.get("chunks")
            chunks: list[dict[str, Any]] = []
            if isinstance(chunks_raw, list):
                chunks = [chunk for chunk in chunks_raw if isinstance(chunk, dict)]
            if not chunks:
                continue

            chunks.sort(key=lambda item: _as_float(item.get("score")), reverse=True)
            best_chunk = chunks[0]
            best_chunk_item_raw = best_chunk.get("item")
            best_chunk_item: dict[str, Any] = (
                dict(best_chunk_item_raw) if isinstance(best_chunk_item_raw, dict) else {}
            )
            shortlist_score = _as_float(section_entry.get("shortlist_score"))

            section_title = str(
                best_chunk_item.get("section_title", "")
                or best_chunk_item.get("parent_section_title", "")
            ).strip()
            section_text = str(
                best_chunk_item.get("parent_section_markdown", "")
                or best_chunk_item.get("parent_section_text", "")
            ).strip()
            section_index = best_chunk_item.get("parent_section_index")

            if not section_id.startswith("__chunk__"):
                resolved_section = prefetched_sections_by_document.get(document_id, {}).get(
                    section_id
                )
                if isinstance(resolved_section, dict):
                    resolved_title = str(resolved_section.get("section_title", "")).strip()
                    resolved_text = str(
                        resolved_section.get("section_markdown", "")
                        or resolved_section.get("section_text", "")
                    ).strip()
                    if resolved_title:
                        section_title = resolved_title
                    if resolved_text:
                        section_text = resolved_text
                    if resolved_section.get("section_index") is not None:
                        section_index = resolved_section.get("section_index")

            truncated_section_text = _truncate_text(
                value=section_text,
                max_chars=resolved_max_section_text_chars,
            )
            matched_variants = section_entry.get("matched_variants")
            matched_variant_count = (
                len(matched_variants) if isinstance(matched_variants, list) else 0
            )
            deep_score = _score_section_text(
                query_variants=query_variants,
                hard_filters=hard_filters,
                section_title=section_title,
                section_text=truncated_section_text,
                shortlist_score=shortlist_score,
                matched_variant_count=matched_variant_count,
            )

            anchor_chunk_ids = [str(chunk.get("chunk_id", "")).strip() for chunk in chunks]
            anchor_chunk_ids = [chunk_id for chunk_id in anchor_chunk_ids if chunk_id]
            anchor_chunk_ids = anchor_chunk_ids[:8]

            if section_title:
                best_chunk_item["section_title"] = section_title
                best_chunk_item["parent_section_title"] = section_title
            if section_id and not section_id.startswith("__chunk__"):
                best_chunk_item["section_id"] = section_id
                best_chunk_item["parent_section_id"] = section_id
            if section_index is not None:
                best_chunk_item["parent_section_index"] = section_index
            if truncated_section_text:
                best_chunk_item["section_text"] = truncated_section_text
                best_chunk_item["parent_section_text"] = truncated_section_text
                best_chunk_item["parent_section_markdown"] = truncated_section_text
            best_chunk_item["chunk_ids_used"] = anchor_chunk_ids
            best_chunk_item["shortlist_score"] = round(shortlist_score, 6)
            best_chunk_item["deep_score"] = round(deep_score, 6)
            best_chunk_item["score"] = round(deep_score, 6)
            if isinstance(matched_variants, list):
                best_chunk_item["query_variants_matched"] = matched_variants

            reranked_sections.append(
                {
                    "document_id": document_id,
                    "section_id": section_id,
                    "score": deep_score,
                    "chunk_ids": anchor_chunk_ids,
                    "result": best_chunk_item,
                }
            )

        reranked_sections.sort(key=lambda item: _as_float(item.get("score")), reverse=True)
        selected_sections = reranked_sections[:resolved_limit]
        results = [section["result"] for section in selected_sections]
        citations = [
            {
                "document_id": str(section.get("document_id", "")).strip(),
                "section_id": str(section.get("section_id", "")).strip(),
                "chunk_ids": section.get("chunk_ids", []),
            }
            for section in selected_sections
        ]

        return {
            "bucket_name": normalized_bucket_name,
            "query": normalized_query,
            "mode": normalized_mode,
            "limit": resolved_limit,
            "rrf_k": resolved_rrf_k,
            "query_variants": query_variants,
            "hard_filters": hard_filters,
            "stage_stats": {
                "wide_hits_total": total_wide_hits,
                "wide_unique_chunks": len(chunk_hits),
                "section_groups": len(section_groups),
                "shortlisted_sections": len(shortlisted_sections),
            },
            "citations": citations,
            "results": results,
        }


def resolve_gpt_search_bucket_name(*, bucket_name: str | None, service: IngestionService) -> str:
    """Resolve bucket name for GPT search requests with single-bucket fallback."""
    normalized_bucket_name = (bucket_name or "").strip()
    if normalized_bucket_name:
        return normalized_bucket_name

    try:
        available_buckets = service.list_buckets()
    except Exception as exc:
        raise_document_http_error(exc)

    if not available_buckets:
        raise HTTPException(status_code=404, detail="No buckets are available to search.")

    if len(available_buckets) == 1:
        return available_buckets[0]

    preview = ", ".join(available_buckets[:10])
    raise HTTPException(
        status_code=400,
        detail=f"bucket_name is required when multiple buckets exist. Available buckets: {preview}",
    )


def normalize_public_base_url(value: str) -> str:
    """Return a normalized absolute base URL without query, fragment, or trailing slash."""
    normalized = value.strip()
    if not normalized:
        return ""
    if "://" not in normalized:
        normalized = f"https://{normalized.lstrip('/')}"
    parsed = urlsplit(normalized)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "https"
    normalized_path = parsed.path.rstrip("/")
    return urlunsplit((scheme, parsed.netloc, normalized_path, "", ""))


def resolve_gpt_links_base_url(request: Request) -> str:
    """Resolve the base URL used to build clickable links in GPT search responses."""
    configured_base_url = normalize_public_base_url(os.getenv("GPT_ACTIONS_LINK_BASE_URL", ""))
    if configured_base_url:
        return configured_base_url
    return normalize_public_base_url(str(request.base_url))


def absolutize_http_url(base_url: str, value: str) -> str:
    """Convert site-relative URLs to absolute URLs against ``base_url``."""
    normalized_value = value.strip()
    if not normalized_value:
        return ""
    parsed = urlsplit(normalized_value)
    if parsed.scheme and parsed.netloc:
        return normalized_value
    if not normalized_value.startswith("/"):
        return normalized_value
    if not base_url:
        return normalized_value
    return f"{base_url.rstrip('/')}{normalized_value}"


def prepare_gpt_search_result(item: dict[str, Any], *, links_base_url: str) -> dict[str, Any]:
    """Normalize GPT search result links for external clients."""
    normalized_result = dict(item)
    source_material_url = absolutize_http_url(
        links_base_url,
        str(item.get("source_material_url", "")),
    )
    resolver_link_url = absolutize_http_url(
        links_base_url,
        str(item.get("resolver_link_url", "")),
    )
    resolver_url = str(item.get("resolver_url", "")).strip()

    if source_material_url:
        normalized_result["source_material_url"] = source_material_url
    if resolver_link_url:
        normalized_result["resolver_link_url"] = resolver_link_url

    if resolver_url.startswith("docs://"):
        normalized_result["resolver_reference"] = resolver_url
        if resolver_link_url:
            normalized_result["resolver_url"] = resolver_link_url
    elif resolver_url:
        normalized_result["resolver_url"] = absolutize_http_url(
            links_base_url,
            resolver_url,
        )
    elif resolver_link_url:
        normalized_result["resolver_url"] = resolver_link_url

    return normalized_result


def prepare_gpt_search_response(
    payload: dict[str, Any], *, links_base_url: str
) -> dict[str, Any]:
    """Return GPT search response payload with normalized link fields."""
    normalized_payload = dict(payload)
    raw_results = payload.get("results", [])
    if not isinstance(raw_results, list):
        return normalized_payload

    normalized_payload["results"] = [
        prepare_gpt_search_result(item, links_base_url=links_base_url)
        if isinstance(item, dict)
        else item
        for item in raw_results
    ]
    return normalized_payload


def prepare_minimal_gpt_search_response(
    payload: dict[str, Any],
    *,
    max_result_text_chars: int,
) -> dict[str, Any]:
    """Return a compact GPT search response shape that minimizes token usage."""
    resolved_text_limit = _bounded_int(
        value=max_result_text_chars,
        default=500,
        minimum=25,
        maximum=2000,
    )
    minimized_payload: dict[str, Any] = {
        key: payload.get(key)
        for key in _MINIMAL_GPT_RESPONSE_TOP_LEVEL_FIELDS
        if key in payload
    }

    citations = payload.get("citations")
    if isinstance(citations, list) and citations:
        minimized_payload["citations"] = citations

    raw_results = payload.get("results", [])
    minimized_results: list[Any] = []
    if isinstance(raw_results, list):
        for item in raw_results:
            if not isinstance(item, dict):
                minimized_results.append(item)
                continue

            minimized_result: dict[str, Any] = {}
            for key in _MINIMAL_GPT_RESULT_FIELDS:
                value = item.get(key)
                if _has_meaningful_value(value):
                    minimized_result[key] = value

            if "resolver_url" not in minimized_result:
                resolver_link_url = str(item.get("resolver_link_url", "")).strip()
                if resolver_link_url:
                    minimized_result["resolver_url"] = resolver_link_url

            text_value = str(item.get("text", "")).strip()
            if not text_value:
                text_value = str(
                    item.get("section_text", "") or item.get("parent_section_text", "")
                ).strip()
            if text_value:
                minimized_result["text"] = _truncate_text(
                    value=text_value,
                    max_chars=resolved_text_limit,
                )

            minimized_results.append(minimized_result)

    minimized_payload["results"] = minimized_results
    return minimized_payload


def build_gpt_openapi_document() -> dict[str, Any]:
    """Return minimal OpenAPI schema for GPT actions."""
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Evidence Base GPT Ping API",
            "version": "1.0.0",
            "description": "Minimal API-key-over-Basic API for ChatGPT custom GPT actions.",
        },
        "servers": [{"url": "https://open.heley.uk/api"}],
        "components": {
            "schemas": {
                "GptSearchRequest": {
                    "type": "object",
                    "required": ["query"],
                    "properties": {
                        "bucket_name": {
                            "type": "string",
                            "description": (
                                "Optional bucket/collection name to search. "
                                "If omitted and exactly one bucket exists, it is "
                                "selected automatically."
                            ),
                        },
                        "query": {
                            "type": "string",
                            "description": "Natural language query text.",
                        },
                        "limit": {
                            "type": "integer",
                            "default": 10,
                            "description": "Maximum number of results to return.",
                        },
                        "mode": {
                            "type": "string",
                            "default": "hybrid",
                            "description": f"Search mode. One of: {', '.join(SEARCH_MODES)}.",
                        },
                        "rrf_k": {
                            "type": "integer",
                            "default": 60,
                            "description": "Reciprocal Rank Fusion parameter for hybrid mode.",
                        },
                        "use_staged_retrieval": {
                            "type": "boolean",
                            "default": True,
                            "description": (
                                "Enable multi-stage retrieval with query variants, "
                                "section shortlist, and section-level citations."
                            ),
                        },
                        "query_variant_limit": {
                            "type": "integer",
                            "default": 6,
                            "description": "Target number of query variants (clamped to 3-8).",
                        },
                        "wide_limit_per_variant": {
                            "type": "integer",
                            "default": 75,
                            "description": (
                                "Top chunks retrieved per query variant "
                                "(clamped to 50-100)."
                            ),
                        },
                        "section_shortlist_limit": {
                            "type": "integer",
                            "default": 20,
                            "description": (
                                "Section groups retained after shortlist scoring "
                                "(clamped to 10-30)."
                            ),
                        },
                        "max_section_text_chars": {
                            "type": "integer",
                            "default": 2500,
                            "description": "Maximum section text characters included per result.",
                        },
                        "minimal_response": {
                            "type": "boolean",
                            "default": True,
                            "description": (
                                "Return compact response fields to reduce token usage. "
                                "Set false for full retrieval diagnostics."
                            ),
                        },
                        "minimal_result_text_chars": {
                            "type": "integer",
                            "default": 500,
                            "description": (
                                "Maximum text characters per result when minimal_response=true "
                                "(clamped to 25-2000)."
                            ),
                        },
                    },
                },
                "GptCitation": {
                    "type": "object",
                    "required": ["document_id", "section_id", "chunk_ids"],
                    "properties": {
                        "document_id": {"type": "string"},
                        "section_id": {"type": "string"},
                        "chunk_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                },
                "GptSearchResponse": {
                    "type": "object",
                    "required": ["bucket_name", "query", "mode", "limit", "rrf_k", "results"],
                    "properties": {
                        "bucket_name": {"type": "string"},
                        "query": {"type": "string"},
                        "mode": {"type": "string"},
                        "limit": {"type": "integer"},
                        "rrf_k": {"type": "integer"},
                        "query_variants": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "hard_filters": {"type": "object", "additionalProperties": True},
                        "stage_stats": {"type": "object", "additionalProperties": True},
                        "citations": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/GptCitation"},
                        },
                        "results": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/GptSearchResult"},
                        },
                    },
                },
                "GptSearchResult": {
                    "type": "object",
                    "required": ["id", "score", "text"],
                    "properties": {
                        "id": {"type": ["string", "integer"]},
                        "score": {"type": "number"},
                        "document_id": {"type": "string"},
                        "bucket_name": {"type": "string"},
                        "file_path": {"type": "string"},
                        "source_material_url": {
                            "type": "string",
                            "description": (
                                "HTTP link to retrieve the source document from this API."
                            ),
                        },
                        "resolver_link_url": {
                            "type": "string",
                            "description": (
                                "HTTP link to open resolver.html with the "
                                "resolved page anchor."
                            ),
                        },
                        "resolver_url": {
                            "type": "string",
                            "description": (
                                "Web-accessible resolver link. Use this for "
                                "clickable links."
                            ),
                        },
                        "resolver_reference": {
                            "type": "string",
                            "description": (
                                "Internal docs:// resolver reference retained "
                                "for compatibility."
                            ),
                        },
                        "page_start": {"type": "integer"},
                        "page_end": {"type": "integer"},
                        "section_id": {"type": "string"},
                        "section_title": {"type": "string"},
                        "parent_section_id": {"type": "string"},
                        "parent_section_index": {"type": "integer"},
                        "parent_section_title": {"type": "string"},
                        "parent_section_text": {"type": "string"},
                        "parent_section_markdown": {"type": "string"},
                        "section_text": {"type": "string"},
                        "chunk_ids_used": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "text": {"type": "string"},
                        "minio_location": {"type": "string"},
                    },
                    "additionalProperties": True,
                },
            },
            "securitySchemes": {
                "BearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "API Key",
                    "description": (
                        "ChatGPT Actions: Authentication type API key, "
                        "Auth Type Bearer."
                    ),
                }
            },
        },
        "paths": {
            "/gpt/ping": {
                "get": {
                    "operationId": "ping",
                    "summary": "Ping endpoint",
                    "description": "Returns a pong response to confirm API connectivity.",
                    "security": [{"BearerAuth": []}],
                    "parameters": [
                        {
                            "name": "message",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string", "default": "ping"},
                            "description": "Optional text to echo back in the response.",
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Successful ping response.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "required": ["status", "reply", "echo", "timestamp_utc"],
                                        "properties": {
                                            "status": {"type": "string"},
                                            "reply": {"type": "string"},
                                            "echo": {"type": "string"},
                                            "timestamp_utc": {
                                                "type": "string",
                                                "format": "date-time",
                                            },
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            },
            "/gpt/search": {
                "post": {
                    "operationId": "searchCollection",
                    "summary": "Search collection",
                    "description": (
                        "Search one collection using semantic, keyword, "
                        "or hybrid retrieval."
                    ),
                    "security": [{"BearerAuth": []}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/GptSearchRequest"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Search results.",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/GptSearchResponse"}
                                }
                            },
                        }
                    },
                }
            },
        },
    }
