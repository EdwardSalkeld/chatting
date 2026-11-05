# Worker diagnostic maintenance

The activity event log is the stored executor transcript, for both live and
completed runs. Audit events retain the outcome, errors, attempts, model choice
and task links, without another stdout/stderr copy. The full execution result
remains available in memory to recovery logic. App-server request journals are
separate files used for retry safety; these commands do not modify them.

Maintenance is explicit: the app does not start a cleanup scheduler. Run these
commands as the database's owning user, and configure scheduling in the deployment.
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

No conversation history, work item/routing state, inbox payloads, token usage,
dead letters or delivery outbox records are removed by these commands. Those
have separate operational lifetimes. Diagnostic retention does not remove the
conversation or external actions such as comments and pull requests.

Deploy the writer change before stripping transcripts. The strip command can be
repeated after supervisors running older code finish, to catch any late audit
copies. A deployment may run it before each daily prune for this reason.
Use `--batch-size` to adjust the default 100 rows per transaction; it and the
retention period must be positive integers.

Deleting data frees SQLite pages for reuse. It does not automatically shrink the
database file; compaction is a separate operational decision.
