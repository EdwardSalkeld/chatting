# Lane executor isolation

Set `isolate_executors: true` in the worker configuration to run Codex as a
distinct Unix user for each work item. The worker must run as root and have
`useradd` available. The workspace root and its ancestors must be traversable
by lane users; the lane directories themselves are mode `0700`. Keep the worker
configuration and SQLite databases mode `0600` and set the service umask to
`0077`.

At first use, the worker creates a `chatlane_*` system account, adopts files in
that lane's existing workspace, and creates private home and temp directories.
It copies the worker's Codex `auth.json` into the lane home on each run, so
credential rotation reaches existing lanes. The worker passes an allowlisted
environment to Codex without the worker configuration path. External repo
context paths are replaced by clone URLs; agents can keep independent clones
inside their lane workspace.

`app.main_reply` detects the lane's Unix reply socket and sends the reply spec
to the worker. The worker checks the connecting UID, active task ID, reply
channel and target, then runs the normal reply path with access to the worker
database and handler endpoint. Attachments sent through this path must live in
the lane workspace. Telegram follow-up claiming and reply recording therefore
continue to use the existing worker state.

This is a filesystem boundary between lane workspaces. Lane processes still
have network access, including local handler APIs, and can read their own copy
of the Codex credential. Separate GitHub credentials or a brokered Git helper
are needed for private repository clones. The worker remains single threaded;
parallel lane claims are a later change.
