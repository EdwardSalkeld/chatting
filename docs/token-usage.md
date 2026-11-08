# Model token usage

The worker SQLite database contains `model_token_usage`, one row per distinct
Codex app-server model usage update. New responses are recorded automatically in
the same transaction as their source activity event. This is a usage ledger,
not a bill or the subscription allowance shown by `/usage`.

Rows contain observed time, source activity ID, task/work item/run, attempt,
thread/turn, model and its provenance, context window, input/cache/output/reasoning
counts and the original cumulative counters. Run linkage is filled when the task
finishes; a crash can leave it null. Attempt and model are nullable if the
necessary metadata was not retained. `turn_request` means the model Chatting
requested, `rerouted` means a reported replacement, and `task_envelope` is the
historical selection inferred from the submitted task. No model is guessed from
today's settings. Timestamps are observation times, not measured inference latency.

`input_tokens` includes cached input and cache writes. `output_tokens` includes
reasoning. Do not add those subsets again when computing totals. A missing
optional counter is null, not zero. `cumulative_json` is for provenance and
deduplication: do not sum it. Sum the per-response columns instead.

## Bounded backfill and export

After deployment, replay the last week of retained logs once:

```sh
python3 -P -m app.main_token_usage backfill --db /var/lib/worker/chatting-worker.db --days 7
python3 -P -m app.main_token_usage export --db /var/lib/worker/chatting-worker.db --days 7 > usage.jsonl
```

Backfill defaults to seven days and accepts at most fourteen. It uses small
transactions, snapshots the source high-water mark and is safe to repeat.
Historical replay has separate metadata state so it cannot overwrite a live
attempt's model selection. Existing response rows are retained. A window starting
partway through a task can have unknown metadata. Capture only covers retained
app-server notifications, including responses in attempts that later fail; it
does not reconstruct unreported usage, auxiliary classifiers or legacy backends.
The source activity log remains the audit record. This table stores counts, not
another copy of prompts or model output.

## Personal mail

The personal inbox collector also meters its isolated, subscription-authenticated
`codex exec --json` summaries and digests. These rows have
`usage_source='codex_exec_personal'`. Each row contains the counters reported by
one CLI `turn.completed`, rather than an app-server response snapshot; a call can
report multiple completed turns. `model_source='cli_request'` records the requested
model, not proof of the model ultimately served. The existing export includes
these rows. CLI `thread_id` and `turn_id` are local call IDs and turn ordinals,
not provider thread identifiers. No email body, prompt, answer or CLI diagnostic is copied into usage
activity. CLI sandbox and credential scrubbing remain in effect.

`personal_model_calls` records each invocation, including nonzero exits and
timeouts, with status and `metered_turns`. A failed call can still report usage;
reported counters are retained. Zero metered turns means **unknown usage**, not
free execution. Counters lost on abrupt process death cannot be reconstructed.
Cache replays and messages screened out before Codex do not create calls. Each
retry that invokes Codex gets its own call ID. Historical calls cannot be backfilled
because their CLI counters were discarded. TypeSafe screening remains separate.

```sql
SELECT kind, status, COUNT(*) AS calls,
       SUM(metered_turns = 0) AS calls_without_counters
FROM personal_model_calls
GROUP BY kind, status;
```

Example SQLite query:

```sql
SELECT model, model_source, COUNT(*) AS responses,
       SUM(input_tokens) AS input_tokens,
       SUM(cached_input_tokens) AS cached_input_tokens,
       SUM(output_tokens) AS output_tokens,
       SUM(reasoning_output_tokens) AS reasoning_output_tokens
FROM model_token_usage
WHERE occurred_at >= '2026-11-01T00:00:00Z'
GROUP BY model, model_source;
```

Apply a separately versioned price table to estimate API costs. Preserve each
response's input length for context-size pricing and distinguish unknown cache
write lifetime. Pricing this observed workload as another model is an estimate:
that model can take different numbers of steps and API caching can differ.
