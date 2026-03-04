# Milestone 03: LLM Memory Layer

## Objective
Add a memory pipeline that captures completed task context, distills reusable learnings, and persists them into searchable long-term stores with daily automated consolidation.

## Why This Milestone
Current runs are well-audited but not converted into reusable memory. This milestone introduces a controlled memory system that:
- Preserves raw context or summaries for every actioned task.
- Classifies learnings into `task`, `knowledge`, and `behaviour`.
- Materializes durable artifacts in a dedicated memory repository.
- Supports unattended daily processing with commit + push automation.

## Scope
In scope:
- SQLite capture of per-task transcript/summary memory units.
- Daily scheduled memory digestion job.
- Categorization into `task`, `knowledge`, `behaviour`.
- Artifact generation/update in a separate git repo.
- Safe git commit/push flow from scheduler.

Out of scope (initially):
- Multi-user memory tenancy.
- Vector DB or semantic retrieval service.
- Autonomous deletion/rewriting of historical memory artifacts.

## Target Architecture
1. Runtime stores actioned-task memory candidates in local SQLite.
2. A scheduled `memory_digest` task runs once daily.
3. Digest task loads undigested records, batches by time window, and uses LLM-assisted extraction.
4. Each extracted item receives category, confidence, and destination policy.
5. Writer stage updates files in a dedicated memory repo:
- `task` -> append/create individual markdown docs under `tasks/`.
- `knowledge` -> create/update zettelkasten notes under `zettel/` with cross-links.
- `behaviour` -> update `AGENTS.md` or linked behavior policy files.
6. Digest task commits and pushes if changes exist.
7. SQLite marks records and emitted artifacts as processed, preserving traceability.

## Repository Strategy
Use a separate repo, for example `~/develop/chatting-memory`.

Proposed top-level structure:
- `tasks/` individual task-memory markdown records (append-only bias).
- `zettel/` atomic knowledge notes.
- `behaviour/` durable behavior rules referenced from root `AGENTS.md`.
- `AGENTS.md` active behavior directives for the agent.
- `indexes/` generated indexes (optional), such as tag and link maps.

## Data Model (SQLite)
Add tables (names illustrative):
- `memory_items`
  - `id` (pk), `run_id`, `envelope_id`, `source`, `actor`, `occurred_at`
  - `content_type` (`transcript` | `summary`)
  - `content` (text)
  - `status` (`pending` | `digested` | `failed`)
  - `digest_attempts`, `last_error`, `created_at`, `updated_at`
- `memory_learnings`
  - `id` (pk), `memory_item_id` (fk)
  - `category` (`task` | `knowledge` | `behaviour`)
  - `title`, `body`, `confidence`
  - `objective_ref` (nullable), `tags_json`
  - `target_path`, `target_anchor` (nullable)
  - `created_at`
- `memory_digest_runs`
  - `id` (pk), `started_at`, `finished_at`, `status`
  - `items_seen`, `items_digested`, `learnings_emitted`
  - `commit_sha` (nullable), `error_summary` (nullable)

## Capture Strategy
For each actioned task:
- Prefer storing concise summary when available.
- Store transcript fallback when summary quality is low or unavailable.
- Persist enough metadata (`run_id`, timestamps, actor, source) to support audit and replay.

Summary generation policy:
- Keep summaries short and factual.
- Include decisions, constraints, and durable outcomes.
- Exclude sensitive data where not needed for future behavior.

## Categorization Rules
`task`:
- Learning tied to a specific objective, deliverable, or project thread.
- Output as immutable markdown artifact in `tasks/`.

`knowledge`:
- Reusable, general insight (tooling facts, product research, implementation pattern).
- Output as zettelkasten note with canonical ID and backlinks.
- Allow redrafting existing notes when confidence is high and change is additive.

`behaviour`:
- Stable operating preferences and guardrails (for example Celsius preference).
- Update root `AGENTS.md` or linked policy files under `behaviour/`.
- Require stricter confidence threshold than task/knowledge items.

## Zettelkasten Conventions
Recommended note format:
- Filename: `YYYYMMDDHHMM-slug.md`.
- Frontmatter: `id`, `title`, `tags`, `sources`, `updated_at`.
- Sections: `Context`, `Note`, `Links`.
- Cross-link style: wiki-style (`[[note-id]]`) or markdown links; pick one and enforce.

Linking behavior:
- During digest, search existing zettels by title/tags.
- Link to nearest relevant notes automatically.
- If no good match, create a new atomic note.

## Daily Digest Job
Schedule:
- One run daily (e.g., 02:30 local time).
- Optional catch-up window for missed runs.

Execution flow:
1. Acquire lock to prevent overlapping digests.
2. Read `pending` items (bounded batch size).
3. Extract learnings with structured output schema.
4. Validate category + minimum required fields.
5. Write artifacts to memory repo.
6. Commit changes with digest metadata.
7. Push to remote.
8. Mark items `digested` (or `failed` with reason).

Failure handling:
- Retry transient LLM/git failures with bounded attempts.
- Keep per-item errors in SQLite for manual triage.
- Never mark `digested` before successful file write + commit.

## Git Automation in Memory Repo
Automation requirements:
- Use dedicated branch per digest run (optional) or direct to `main` if low risk.
- Commit message template: `memory-digest: YYYY-MM-DD (N items, M learnings)`.
- Push only when working tree changed.
- Capture pushed `commit_sha` in `memory_digest_runs`.

Recommendation:
- Start with direct commits to `main` for operational simplicity.
- Add PR flow later if digest edits become high-risk.

## Guardrails and Safety
- Add strict schema for LLM extraction output; reject malformed entries.
- Enforce category-specific validators before file writes.
- Add path allowlist so writer cannot escape memory repo.
- Add dry-run mode for validation without write/commit/push.
- Emit audit events for digest start/end/error with trace IDs.

## Implementation Phases
### Phase A: Contracts + Storage (1-2 days)
- Define models for memory item, learning, digest run.
- Add SQLite schema + store methods.
- Add unit tests for persistence and validation.

### Phase B: Capture Integration (1-2 days)
- Hook post-apply flow to enqueue `memory_items` for actioned tasks.
- Add summary/transcript selection logic.
- Add tests in `tests.test_main` and store tests.

### Phase C: Digest Extractor (2-3 days)
- Implement `memory_digest` workflow + scheduled trigger.
- Add LLM structured extraction parser with category constraints.
- Add retry, locking, and status updates.

### Phase D: Artifact Writers (2-3 days)
- Implement task note writer, zettel writer, behavior writer.
- Implement zettel linker and redraft policy.
- Add idempotency checks to avoid duplicate artifacts.

### Phase E: Git Sync + Ops (1-2 days)
- Implement commit/push integration and run metadata capture.
- Add CLI hooks for manual digest run and dry-run.
- Add operator docs and runbook updates.

## Test Plan
- Unit:
  - model validation for category contracts and required fields.
  - SQLite read/write transitions (`pending -> digested/failed`).
  - writer path safety and deterministic filenames.
- Integration:
  - actioned task -> memory item capture -> digest -> artifact files.
  - digest retries and failure recovery.
  - git commit/push success + no-op when no changes.
- Regression:
  - behavior update cannot clobber unrelated `AGENTS.md` content.
  - knowledge redraft preserves links/frontmatter integrity.

## Open Decisions
- Transcript retention limit and redaction policy.
- Exact confidence thresholds per category.
- Whether behavior writes always require human review.
- Branch-vs-direct-commit policy for memory repo.
- Zettel link format (`[[id]]` vs markdown links).

## Initial Acceptance Criteria
- Every actioned task produces a persisted `memory_item`.
- Daily digest processes pending items and emits classified learnings.
- `task`, `knowledge`, and `behaviour` all write to correct destinations.
- Digest run commits and pushes changes in separate memory repo.
- Full run is auditable from source task to generated artifact and commit SHA.
