package sqlite

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	_ "modernc.org/sqlite"
)

func TestDedupeKeysAreScopedBySource(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	seen, err := store.Seen(ctx, "email", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	if seen {
		t.Fatal("new key was already seen")
	}
	if err := store.MarkSeen(ctx, "email", "message-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSeen(ctx, "email", "message-1"); err != nil {
		t.Fatal(err)
	}

	emailSeen, err := store.Seen(ctx, "email", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	telegramSeen, err := store.Seen(ctx, "telegram", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	if !emailSeen {
		t.Fatal("email key was not seen")
	}
	if telegramSeen {
		t.Fatal("dedupe key leaked across source")
	}
}

func TestRecordTaskRoundTripsAndUsesPythonCompatibleTable(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store := openStoreAt(t, dbPath)
	taskMessage := testTaskMessage(t)

	if err := store.RecordTask(ctx, taskMessage); err != nil {
		t.Fatal(err)
	}

	record, err := store.GetTask(ctx, taskMessage.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if record == nil {
		t.Fatal("task record was not persisted")
	}
	if record.EnvelopeID != taskMessage.Envelope.ID {
		t.Fatalf("EnvelopeID = %q", record.EnvelopeID)
	}
	if record.TraceID != taskMessage.TraceID {
		t.Fatalf("TraceID = %q", record.TraceID)
	}
	if record.TaskMessage.Envelope.ReplyChannel.Target != "alice@example.com" {
		t.Fatalf("reply target = %q", record.TaskMessage.Envelope.ReplyChannel.Target)
	}

	assertTaskLedgerSchemaAndPayload(t, dbPath, taskMessage)
	assertPythonTaskLedgerCanRead(t, dbPath, taskMessage)
}

func TestCompletionBlocksSameEnvelopeAndRemovesOpenTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	taskMessage := testTaskMessage(t)

	if err := store.RecordTask(ctx, taskMessage); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID, taskMessage.TraceID); err != nil {
		t.Fatal(err)
	}

	openTask, err := store.GetTask(ctx, taskMessage.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if openTask != nil {
		t.Fatalf("completed task remained open: %#v", openTask)
	}
	completed, err := store.IsTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("same task/envelope pair was not completed")
	}
	otherEnvelopeCompleted, err := store.IsTaskCompleted(ctx, taskMessage.TaskID, "email:other")
	if err != nil {
		t.Fatal(err)
	}
	if otherEnvelopeCompleted {
		t.Fatal("completion leaked to a different envelope id")
	}
}

func TestRecordTaskClearsPreviousCompletion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	taskMessage := testTaskMessage(t)

	if err := store.MarkTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID, taskMessage.TraceID); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordTask(ctx, taskMessage); err != nil {
		t.Fatal(err)
	}
	completed, err := store.IsTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed {
		t.Fatal("recording a new task did not clear prior completion")
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	return openStoreAt(t, filepath.Join(t.TempDir(), "state.db"))
}

func openStoreAt(t *testing.T, dbPath string) *Store {
	t.Helper()
	store, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return store
}

func testTaskMessage(t *testing.T) contracts.TaskQueueMessage {
	t.Helper()
	name := "message.txt"
	actor := "alice@example.com"
	return contracts.NewTaskQueueMessage(contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "email:1",
		Source:        "email",
		ReceivedAt:    contracts.NewTimestamp(mustTime(t, "2026-03-06T11:00:00Z")),
		Actor:         &actor,
		Content:       "hello from sqlite state",
		Attachments: []contracts.AttachmentRef{{
			URI:  "file:///tmp/message.txt",
			Name: &name,
		}},
		ContextRefs: []string{"repo:/workspace/chatting"},
		PromptContext: &contracts.PromptContext{
			GlobalInstructions:       []string{"Keep replies concise."},
			ReplyChannelInstructions: []string{"Use email formatting."},
		},
		ReplyChannel: contracts.ReplyChannel{
			Type:     "email",
			Target:   "alice@example.com",
			Metadata: map[string]any{"thread_id": "thread-1"},
		},
		DedupeKey: "email:1",
	}, "trace:email:1", mustTime(t, "2026-03-06T11:00:01Z"))
}

func assertTaskLedgerSchemaAndPayload(t *testing.T, dbPath string, taskMessage contracts.TaskQueueMessage) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`PRAGMA table_info(task_ledger)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name+":"+typ)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantColumns := strings.Join([]string{
		"task_id:TEXT",
		"envelope_id:TEXT",
		"trace_id:TEXT",
		"task_payload_json:TEXT",
		"created_at:TEXT",
	}, "|")
	if gotColumns := strings.Join(columns, "|"); gotColumns != wantColumns {
		t.Fatalf("task_ledger columns = %s", gotColumns)
	}

	var taskID, envelopeID, traceID string
	err = db.QueryRow(
		`SELECT task_id, envelope_id, trace_id FROM task_ledger WHERE task_id = ?`,
		taskMessage.TaskID,
	).Scan(&taskID, &envelopeID, &traceID)
	if err != nil {
		t.Fatal(err)
	}
	if taskID != taskMessage.TaskID || envelopeID != taskMessage.Envelope.ID || traceID != taskMessage.TraceID {
		t.Fatalf("raw ledger row = task_id %q envelope_id %q trace_id %q", taskID, envelopeID, traceID)
	}
}

func assertPythonTaskLedgerCanRead(t *testing.T, dbPath string, taskMessage contracts.TaskQueueMessage) {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", "..", "..", ".."))
	script := `
import sys
from app.handler.runtime import TaskLedgerStore

db_path, task_id, envelope_id, trace_id = sys.argv[1:5]
record = TaskLedgerStore(db_path).get_task(task_id)
if record is None:
    raise SystemExit("missing task")
if record.envelope_id != envelope_id:
    raise SystemExit(f"envelope_id={record.envelope_id!r}")
if record.trace_id != trace_id:
    raise SystemExit(f"trace_id={record.trace_id!r}")
if record.task_message.envelope.reply_channel.target != "alice@example.com":
    raise SystemExit("wrong reply target")
`
	command := exec.Command("python3", "-c", script, dbPath, taskMessage.TaskID, taskMessage.Envelope.ID, taskMessage.TraceID)
	command.Dir = repoRoot
	command.Env = append(os.Environ(), "PYTHONPATH="+repoRoot)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("python TaskLedgerStore could not read Go ledger row: %v\n%s", err, output)
	}
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
