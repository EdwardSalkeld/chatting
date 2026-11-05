# Worker diagnostic maintenance

The activity event log is the stored executor transcript, for both live and
completed runs. Audit events retain the outcome, errors, attempts, model choice
and task links, without another stdout/stderr copy. The full execution result
remains available in memory to recovery logic. App-server request journals are
separate files used for retry safety; these commands do not modify them.

## Scheduled maintenance

Enable built-in maintenance in the handler configuration:

```json
{
  "maintenance_enabled": true,
  "maintenance_cron": "0 4 * * *",
  "maintenance_timezone": "UTC",
  "retention_days": 14
}
```

Maintenance is disabled by default. The remaining values above are the defaults;
retention days must be positive integers. Cron syntax and timezones are validated
at startup when maintenance is enabled. The existing cron connector determines
due times, including timezone handling; no external timer or model is involved.
On restart it schedules from the current time, using the same semantics as other
cron jobs rather than replaying missed days.

At the scheduled time the handler prunes its own database using its state-store
code, then publishes a built-in internal maintenance task with the configured
retention policy. The worker recognizes that task before executor selection and
uses its own configured database, without launching Codex. The handler never
opens the worker database. Paths and policy are configuration; schema knowledge
and all cleanup SQL belong to this repository.

One retention window applies to run/audit/activity diagnostics, acknowledged
outbox entries, and handler delivery/deduplication history. Worker maintenance
strips duplicate audit transcripts, expires old diagnostics, and deletes
acknowledged outbox entries whose last update is older than that window.
Unacknowledged sends, unfinished inbox work and active handler task-ledger
entries are protected regardless of age. The handler's optional legacy
diagnostic tables are handled when present; a fresh Go handler database works too.
Conversation history, workspaces/routes, inbox state and token accounting are
preserved.

Cleanup counts and failures are logged by each process. A failed handler cleanup
is retried on the next ingress poll without stopping normal polling. Worker task
failures use the ordinary supervisor failure reporting; the next scheduled task
can safely repeat maintenance. Broker delivery, task IDs and deduplication use
the normal internal task path.

## Explicit maintenance commands

Run these commands as the database's owning user when a one-off cleanup is useful.
Both commands require an existing worker database and use small transactions with
a 60-second SQLite busy timeout. Each committed batch is restartable; repeated
runs are safe. JSON output reports the number of rows changed per operation.

To remove historical duplicate transcript fields, while retaining all other
audit metadata and the activity transcript:

```sh
python3 -m app.main_db_maintenance strip-audit-transcripts --db /path/to/worker.db
```

To delete run records, audit events and activity events older than a configurable
number of days (default 14):

```sh
python3 -m app.main_db_maintenance prune-worker --db /path/to/worker.db --retention-days 14
```

The cutoff is calculated in UTC when the command starts; rows exactly at the
cutoff remain. Inbox tasks in any state other than `completed` protect their
diagnostics, including pending, attached, active and closing work. Run records
are terminal outcomes; their audit task links are consulted before deletion.
Unfinished inbox state is rechecked within each deletion transaction.

These two explicit commands only modify diagnostics. Scheduled maintenance also
prunes acknowledged outbox history using the same retention window. Conversation
history, work item/routing state, inbox payloads, token usage and dead letters
are preserved, as are external actions such as comments and pull requests.

Deploy the writer change before stripping transcripts. The strip command can be
repeated after supervisors running older code finish, to catch any late audit
copies. The built-in worker maintenance task repeats it before each prune for
this reason.
Use `--batch-size` to adjust the default 100 rows per transaction; it and the
retention period must be positive integers.

Deleting data frees SQLite pages for reuse. It does not automatically shrink the
database file; compaction is a separate operational decision.
