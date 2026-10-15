# Explicit executor termination

Each work item has an `explicit_finish` setting, default **on**. A one-time
migration enables it for all existing items, including those previously off.
After deployment, `/finish off` persists across restarts and disables it for that
work item. New items default on even in databases created with the old off default.

Send `/finish on` in the chat to enable it for the current work item. `/finish`
shows the setting; `/finish off` disables it. Commands do not launch an executor.
Changes apply to subsequent tasks. Other work items remain unchanged, including
other items that happen to share a chat. The activity page displays the setting.

An operator can also use:

```sh
python3 -P -m app.main_work_items --db /var/lib/worker/chatting-worker.db explicit-finish TASK_ID on
```

When enabled, the supervisor creates a private Unix socket for each execution and
passes its task ID and a fresh execution credential in the environment and task
contract. The executor calls `python3 -P -m app.main_finish` when work is complete,
then waits. Reply delivery remains independent; sending a reply does not complete
the task. The supervisor validates the request, writes and fsyncs a completion
record under the workspace's `.chatting-finish/`, then kills the executor process
group (including the waiting command and ordinary child processes). The manager
receives the result through the existing supervisor protocol and completion outbox.

An executor exiting without a request, even with status zero, is restarted after
one second, then two seconds. It receives the original task, current conversation
history and captured prior transcript with instructions to check completed effects
and replies before continuing. Three launches without a request fail the task.
Timeouts remain failures. Credentials and sockets change on every launch; stale
requests cannot finish a replacement executor. Transcripts help avoid duplicate
effects but cannot guarantee exactly-once external operations.

Supervisors still survive manager restarts. Supervisor crashes remain failures;
this change does not add supervisor recovery. Completion records are retained for
inspection, not used to resume crashed supervisors. Existing supervisors drain and
are replaced when their code fingerprint changes, as in phase one.
