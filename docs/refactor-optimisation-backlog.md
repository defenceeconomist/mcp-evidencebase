# MCP Evidence Base Refactor and Optimisation Backlog

Updated: 2026-04-03

This document is the working backlog for refactoring and optimising the MCP Evidence Base codebase. It is intended to be updated as work progresses.

## How To Use This Backlog

- Update the `Status` field for each phase as work moves from `not started` to `in progress` to `complete`.
- Tick task checkboxes as individual items are finished.
- Add links to pull requests, commits, benchmark notes, or follow-up issues under `Evidence` as you go.
- If a phase changes materially, update both the phase summary and the acceptance criteria rather than only editing tasks.

## Current Assessment

The current repo shape suggests three main needs:

1. The developer feedback loop needs hardening before optimisation work is trustworthy.
2. Several modules are still too large and mix orchestration, business logic, and infrastructure details.
3. Performance work should follow boundary cleanup and measurement, not precede it.

Current code hotspots:

- `src/mcp_evidencebase/ingestion_modules/service.py`
- `src/mcp_evidencebase/ingestion_modules/chunking.py`
- `src/mcp_evidencebase/api_modules/services.py`
- `src/mcp_evidencebase/ingestion_modules/repository.py`
- `src/mcp_evidencebase/ingestion_modules/qdrant.py`
- `tests/test_ingestion.py`
- `tests/test_api.py`

## Phase 1. Baseline and Developer Workflow

Status: not started

### Goals

- Make the local development and test path reliable from a clean environment.
- Record baseline measurements for the key runtime and test paths.
- Remove avoidable friction from the feedback loop.

### Tasks

- [ ] Confirm and document the canonical local bootstrap path for backend development.
- [ ] Ensure `pytest` can run in a clean environment without failing on missing optional plugins.
- [ ] Align local setup instructions in `README.md` with CI expectations in `.github/workflows/ci.yml`.
- [ ] Record baseline timings for:
  - [ ] API startup
  - [ ] non-live test suite
  - [ ] representative ingestion flow
  - [ ] representative search flow
- [ ] Capture environment assumptions for local host runs versus Docker-network runs.
- [ ] Decide whether generated docs/test reports should be committed by default or treated as release artifacts.

### Acceptance Criteria

- A clean setup path exists and is documented.
- `pytest` starts successfully without manual workaround flags.
- Baseline measurements are written down and attributable to a specific environment.

### Evidence

- None yet.

## Phase 2. Package Boundaries and Public Surface Cleanup

Status: not started

### Goals

- Reduce ambiguity in package boundaries.
- Remove legacy compatibility patterns that make ownership unclear.
- Make entrypoints thin and predictable.

### Tasks

- [ ] Keep `src/mcp_evidencebase/api.py` as an application entrypoint only.
- [ ] Keep `src/mcp_evidencebase/cli.py` as a CLI entrypoint only.
- [ ] Review and reduce the compatibility facade in `src/mcp_evidencebase/ingestion.py`.
- [ ] Replace wildcard re-exports with explicit public imports where possible.
- [ ] Define clearer domains for:
  - [ ] settings
  - [ ] diagnostics
  - [ ] storage
  - [ ] metadata
  - [ ] search
  - [ ] task orchestration
- [ ] Document the intended module ownership model in a short architecture note or doc section.

### Acceptance Criteria

- Entrypoint modules mostly wire dependencies and expose interfaces.
- Public imports are explicit enough that module ownership is obvious.
- New contributors can tell where to add logic without reading the entire package.

### Evidence

- None yet.

## Phase 3. Ingestion Domain Refactor

Status: not started

### Goals

- Split the ingestion layer by workflow responsibility.
- Reduce coupling between orchestration, persistence, partitioning, and indexing.
- Make ingestion behavior easier to test in smaller units.

### Tasks

- [ ] Break `src/mcp_evidencebase/ingestion_modules/service.py` into smaller workflow-oriented modules.
- [ ] Separate responsibilities for:
  - [ ] partitioning
  - [ ] metadata enrichment
  - [ ] chunk generation
  - [ ] indexing/upsert
  - [ ] rebuild/purge flows
- [ ] Keep `src/mcp_evidencebase/ingestion_modules/repository.py` focused on Redis persistence concerns.
- [ ] Keep `src/mcp_evidencebase/ingestion_modules/qdrant.py` focused on Qdrant/vector concerns.
- [ ] Audit Redis-to-Qdrant coupling and remove unnecessary payload shaping dependencies.
- [ ] Review object lifecycle naming so each stage has a single obvious owner.
- [ ] Preserve existing disabled-dependency behavior and runtime contract checks during the split.

### Acceptance Criteria

- No ingestion orchestration file remains a catch-all for unrelated workflows.
- Redis persistence and Qdrant indexing can be reasoned about independently.
- Existing ingestion behavior remains covered by targeted tests.

### Evidence

- None yet.

## Phase 4. API Service Layer Refactor

Status: not started

### Goals

- Reduce the breadth of `api_modules/services.py`.
- Keep routers thin.
- Make API use cases easier to locate and test.

### Tasks

- [ ] Split `src/mcp_evidencebase/api_modules/services.py` into smaller modules.
- [ ] Separate concerns for:
  - [ ] bibliography generation
  - [ ] collection search
  - [ ] GPT search shaping
  - [ ] URL/base-link resolution
  - [ ] request/response normalization helpers
- [ ] Keep router files focused on HTTP input/output and dependency injection.
- [ ] Review whether shared helpers belong in services, models, or a dedicated utility module.
- [ ] Reduce internal helper sprawl where behavior would be clearer as named use-case modules.

### Acceptance Criteria

- Router modules primarily map HTTP to use-case handlers.
- API business logic is grouped by feature instead of accumulating in one module.
- Search and GPT response logic can be tested without importing unrelated API helpers.

### Evidence

- None yet.

## Phase 5. Performance Optimisation

Status: not started

### Goals

- Improve performance on measured hotspots.
- Avoid speculative tuning.
- Preserve readability while reducing unnecessary work.

### Tasks

- [ ] Profile chunk generation and identify the dominant CPU and allocation costs.
- [ ] Profile representative search requests and identify latency contributors.
- [ ] Review expensive client/model initialisation paths and cache or reuse where safe.
- [ ] Reduce repeated text normalization, payload reshaping, or duplicate parsing in search flows.
- [ ] Audit Redis round-trips during document hydration and section lookups.
- [ ] Batch or coalesce datastore operations where this improves latency without obscuring correctness.
- [ ] Re-measure against the Phase 1 baseline after each meaningful optimisation step.

### Acceptance Criteria

- Performance claims are backed by before/after measurements.
- The most expensive ingestion and search paths are measurably faster.
- Optimisations do not reintroduce broad coupling or hidden state.

### Evidence

- None yet.

## Phase 6. Test Suite Restructure and Coverage Quality

Status: not started

### Goals

- Make tests easier to navigate and cheaper to maintain.
- Align test module boundaries with production code boundaries.
- Add lightweight performance regression coverage where it matters.

### Tasks

- [ ] Split `tests/test_ingestion.py` into smaller domain-aligned test modules.
- [ ] Split `tests/test_api.py` by router or use-case area.
- [ ] Consolidate duplicated fakes and fixtures into shared helpers where that reduces repetition.
- [ ] Keep fixture sharing tight enough to avoid a giant indirect `conftest.py` dump.
- [ ] Add targeted tests for newly extracted modules rather than only preserving legacy broad tests.
- [ ] Add performance smoke checks or benchmark notes for key ingestion/search paths.
- [ ] Review marker usage and ensure the suite is still easy to run selectively.

### Acceptance Criteria

- Large test files are reduced to manageable domain-focused modules.
- Failures are easier to map back to a specific subsystem.
- The suite supports both quick local runs and deeper CI coverage.

### Evidence

- None yet.

## Phase 7. Repository Hygiene and Generated Artifacts

Status: not started

### Goals

- Reduce noise in the worktree.
- Keep source-of-truth artifacts clear.
- Prevent generated files from obscuring meaningful code changes.

### Tasks

- [ ] Decide how `docs/site/` should be managed in normal development.
- [ ] Review whether test reports under `docs/source/_static/tests/` should be committed routinely.
- [ ] Update `.gitignore` or docs build workflows if generated artifacts should not appear in normal edits.
- [ ] Clarify which docs outputs are source, generated, or publication artifacts.
- [ ] Ensure repository status is not routinely polluted by local-only outputs.

### Acceptance Criteria

- Generated artifacts are handled intentionally.
- Routine backend changes do not create large unrelated docs diffs.
- Source and generated documentation responsibilities are explicit.

### Evidence

- None yet.

## Cross-Cutting Constraints

- Maintain the existing runtime contract and dependency-aware startup behavior.
- Do not regress disabled Redis/Qdrant behavior already described in `docs/redis-qdrant-decoupling-remediation-backlog.md`.
- Prefer small, reviewable refactor steps over a single large rewrite.
- Keep CI green throughout; if a temporary break is unavoidable, capture it explicitly in this backlog.

## Suggested PR Sequence

1. Phase 1: fix bootstrap and measurement gaps.
2. Phase 2: tighten package boundaries and public imports.
3. Phase 3: split ingestion workflows.
4. Phase 4: split API service logic.
5. Phase 6: realign tests with the new structure.
6. Phase 5: performance work against stable module boundaries.
7. Phase 7: finalize repository hygiene rules.

## Definition of Done

- No major module remains a broad catch-all without clear ownership.
- The local setup and test path are documented and reproducible.
- Performance improvements are measured and recorded.
- The test suite structure reflects the production architecture.
- Generated outputs are intentionally managed rather than incidental.
