package sqlite

import (
	"context"
	"database/sql"
	"net/url"
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

func TestTelegramChatRegistryUpsertsObservedMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	firstSeenAt := mustTime(t, "2026-05-21T06:40:00Z")
	firstMessageAt := mustTime(t, "2026-05-21T06:39:30Z")
	updatedAt := mustTime(t, "2026-05-21T06:45:00Z")
	chatType := "supergroup"
	title := "Build Tests"
	username := "build_tests"

	if err := store.RecordTelegramChat(ctx, TelegramChatObservation{
		ChatID:      "-100123",
		ChatType:    &chatType,
		Title:       &title,
		Username:    &username,
		UpdateID:    1001,
		UpdateKind:  "message",
		MessageDate: &firstMessageAt,
		RetrievedAt: firstSeenAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordTelegramChat(ctx, TelegramChatObservation{
		ChatID:      "-100123",
		UpdateID:    1002,
		UpdateKind:  "my_chat_member",
		RetrievedAt: updatedAt,
	}); err != nil {
		t.Fatal(err)
	}

	records, err := store.ListTelegramChats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %#v", records)
	}
	record := records[0]
	if record.ChatID != "-100123" || derefString(record.ChatType) != "supergroup" || derefString(record.Title) != "Build Tests" || derefString(record.Username) != "build_tests" {
		t.Fatalf("record metadata = %#v", record)
	}
	if !record.FirstSeenAt.Equal(firstSeenAt) {
		t.Fatalf("FirstSeenAt = %s", record.FirstSeenAt)
	}
	if !record.LastRetrievedAt.Equal(updatedAt) {
		t.Fatalf("LastRetrievedAt = %s", record.LastRetrievedAt)
	}
	if record.LastMessageAt == nil || !record.LastMessageAt.Equal(firstMessageAt) {
		t.Fatalf("LastMessageAt = %#v", record.LastMessageAt)
	}
	if record.LastUpdateID != 1002 || record.LastUpdateKind != "my_chat_member" {
		t.Fatalf("last update = %#v", record)
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

func TestDispatchedEventIDCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	dispatched, err := store.HasDispatchedEventID(ctx, "task:email:1", "evt:1")
	if err != nil {
		t.Fatal(err)
	}
	if dispatched {
		t.Fatal("new event id was already dispatched")
	}
	if err := store.MarkDispatchedEventID(ctx, "task:email:1", "evt:1"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatchedEventID(ctx, "task:email:1", "evt:1"); err != nil {
		t.Fatal(err)
	}

	dispatched, err = store.HasDispatchedEventID(ctx, "task:email:1", "evt:1")
	if err != nil {
		t.Fatal(err)
	}
	if !dispatched {
		t.Fatal("event id was not marked dispatched")
	}
	otherTaskDispatched, err := store.HasDispatchedEventID(ctx, "task:email:other", "evt:1")
	if err != nil {
		t.Fatal(err)
	}
	if otherTaskDispatched {
		t.Fatal("event id checkpoint leaked across tasks")
	}
}

func TestStagedEgressEventsAdvanceSequenceWhenMarkedDispatched(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	taskMessage := testTaskMessage(t)
	second := testEgressMessage(t, taskMessage, 1, "evt:task:email:1:1", "second", "message")
	first := testEgressMessage(t, taskMessage, 0, "evt:task:email:1:0", "first", "message")

	if err := store.StageEgressEvent(ctx, second); err != nil {
		t.Fatal(err)
	}
	expected, err := store.ExpectedSequence(ctx, taskMessage.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if expected != 0 {
		t.Fatalf("expected sequence after staging second = %d", expected)
	}
	stagedSecond, err := store.GetStagedEventBySequence(ctx, taskMessage.TaskID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if stagedSecond == nil {
		t.Fatal("second event was not staged")
	}
	if gotBody := derefString(stagedSecond.EgressMessage.Message.Body); gotBody != "second" {
		t.Fatalf("staged second body = %q", gotBody)
	}
	if err := store.StageEgressEvent(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkStagedEventDispatched(ctx, first.TaskID, first.EventID, *first.Sequence); err != nil {
		t.Fatal(err)
	}

	expected, err = store.ExpectedSequence(ctx, taskMessage.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if expected != 1 {
		t.Fatalf("expected sequence after dispatching first = %d", expected)
	}
	stagedFirst, err := store.GetStagedEventBySequence(ctx, taskMessage.TaskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stagedFirst != nil {
		t.Fatalf("first event remained staged: %#v", stagedFirst)
	}
	if err := store.MarkStagedEventDispatched(ctx, second.TaskID, second.EventID, *second.Sequence); err != nil {
		t.Fatal(err)
	}
	expected, err = store.ExpectedSequence(ctx, taskMessage.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if expected != 2 {
		t.Fatalf("expected sequence after dispatching second = %d", expected)
	}
}

func TestCompletionEventsCanBeStagedAndCompletionClearsEgressState(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	taskMessage := testTaskMessage(t)
	completion := testEgressMessage(t, taskMessage, 0, "evt:task:email:1:completion", "done", "completion")

	if err := store.RecordTask(ctx, taskMessage); err != nil {
		t.Fatal(err)
	}
	if err := store.StageEgressEvent(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID, taskMessage.TraceID); err != nil {
		t.Fatal(err)
	}

	staged, err := store.GetStagedEventBySequence(ctx, taskMessage.TaskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if staged != nil {
		t.Fatalf("completion did not clear staged event: %#v", staged)
	}
	expected, err := store.ExpectedSequence(ctx, taskMessage.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if expected != 0 {
		t.Fatalf("completion did not clear sequence state, got %d", expected)
	}
}

func TestTelegramAttachmentLedgerTracksLocalTaskAttachmentsAndCleanup(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	attachmentRoot := t.TempDir()
	attachmentPath := filepath.Join(attachmentRoot, "telegram-1-2-photo.jpg")
	if err := os.WriteFile(attachmentPath, []byte("jpeg-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	name := "photo.jpg"
	taskMessage := contracts.NewTaskQueueMessage(contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "telegram:1",
		Source:        "im",
		ReceivedAt:    contracts.NewTimestamp(mustTime(t, "2026-05-30T12:00:00Z")),
		Content:       "[photo attached]",
		Attachments: []contracts.AttachmentRef{
			{URI: fileURIForTest(attachmentPath), Name: &name},
			{URI: "file:///tmp/outside.jpg", Name: &name},
			{URI: "s3://bucket/photo.jpg", Name: &name},
		},
		ContextRefs: []string{},
		ReplyChannel: contracts.ReplyChannel{
			Type:   "telegram",
			Target: "12345",
		},
		DedupeKey: "telegram:1",
	}, "trace:telegram:1", mustTime(t, "2026-05-30T12:00:01Z"))

	tracked, err := store.RecordTelegramTaskAttachments(ctx, taskMessage, attachmentRoot)
	if err != nil {
		t.Fatal(err)
	}
	if tracked != 1 {
		t.Fatalf("tracked = %d", tracked)
	}
	eligibleAfter := mustTime(t, "2026-05-30T12:10:00Z")
	eligible, err := store.MarkTelegramTaskAttachmentsEligible(ctx, taskMessage.TaskID, eligibleAfter)
	if err != nil {
		t.Fatal(err)
	}
	if eligible != 1 {
		t.Fatalf("eligible = %d", eligible)
	}
	records, err := store.ListTelegramAttachmentRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %#v", records)
	}
	if records[0].AttachmentPath != attachmentPath || records[0].EligibleAfter == nil || !records[0].EligibleAfter.Equal(eligibleAfter) {
		t.Fatalf("record = %#v", records[0])
	}

	result, err := store.CleanupTelegramAttachments(
		ctx,
		attachmentRoot,
		mustTime(t, "2026-05-30T12:11:00Z"),
		mustTime(t, "2026-01-01T00:00:00Z"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 1 || result.MissingCount != 0 || result.FailedCount != 0 || result.ReclaimedBytes != int64(len("jpeg-bytes")) {
		t.Fatalf("cleanup result = %#v", result)
	}
	if _, err := os.Stat(attachmentPath); !os.IsNotExist(err) {
		t.Fatalf("attachment still exists or unexpected stat error: %v", err)
	}
	records, err = store.ListTelegramAttachmentRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if records[0].DeletedAt == nil {
		t.Fatalf("deleted_at not marked: %#v", records[0])
	}
}

func TestTelegramAttachmentCleanupMarksMissingAndRejectsOutsideRoot(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	attachmentRoot := t.TempDir()
	missingPath := filepath.Join(attachmentRoot, "missing.jpg")
	name := "photo.jpg"
	taskMessage := contracts.NewTaskQueueMessage(contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "telegram:missing",
		Source:        "im",
		ReceivedAt:    contracts.NewTimestamp(mustTime(t, "2026-05-30T12:00:00Z")),
		Content:       "[photo attached]",
		Attachments: []contracts.AttachmentRef{{
			URI:  fileURIForTest(missingPath),
			Name: &name,
		}},
		ContextRefs: []string{},
		ReplyChannel: contracts.ReplyChannel{
			Type:   "telegram",
			Target: "12345",
		},
		DedupeKey: "telegram:missing",
	}, "trace:telegram:missing", mustTime(t, "2026-05-30T12:00:01Z"))
	if _, err := store.RecordTelegramTaskAttachments(ctx, taskMessage, attachmentRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTelegramTaskAttachmentsEligible(ctx, taskMessage.TaskID, mustTime(t, "2026-05-30T12:01:00Z")); err != nil {
		t.Fatal(err)
	}
	result, err := store.CleanupTelegramAttachments(
		ctx,
		attachmentRoot,
		mustTime(t, "2026-05-30T12:02:00Z"),
		mustTime(t, "2026-01-01T00:00:00Z"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.MissingCount != 1 || result.DeletedCount != 0 || result.FailedCount != 0 {
		t.Fatalf("cleanup result = %#v", result)
	}

	outsidePath := filepath.Join(t.TempDir(), "outside.jpg")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = store.db.ExecContext(
		ctx,
		`INSERT INTO telegram_attachment_ledger (
			attachment_path,
			attachment_uri,
			task_id,
			envelope_id,
			created_at,
			eligible_after,
			deleted_at,
			cleanup_attempts,
			last_cleanup_error
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		outsidePath,
		fileURIForTest(outsidePath),
		"task:telegram:outside",
		"telegram:outside",
		formatTimestamp(mustTime(t, "2026-05-30T12:00:00Z")),
		formatTimestamp(mustTime(t, "2026-05-30T12:01:00Z")),
		nil,
		0,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err = store.CleanupTelegramAttachments(
		ctx,
		attachmentRoot,
		mustTime(t, "2026-05-30T12:02:00Z"),
		mustTime(t, "2026-01-01T00:00:00Z"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.FailedCount != 1 {
		t.Fatalf("cleanup result = %#v", result)
	}
	records, err := store.ListTelegramAttachmentRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var outsideRecord *TelegramAttachmentRecord
	for index := range records {
		if records[index].AttachmentPath == outsidePath {
			outsideRecord = &records[index]
		}
	}
	if outsideRecord == nil || outsideRecord.CleanupAttempts != 1 || derefString(outsideRecord.LastCleanupError) != "attachment_path_outside_root" {
		t.Fatalf("outside record = %#v", outsideRecord)
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

func testEgressMessage(
	t *testing.T,
	taskMessage contracts.TaskQueueMessage,
	sequence int,
	eventID string,
	body string,
	eventKind string,
) contracts.EgressQueueMessage {
	t.Helper()
	return contracts.EgressQueueMessage{
		SchemaVersion: contracts.SchemaVersion,
		MessageType:   contracts.EgressMessageTypeV2,
		TaskID:        taskMessage.TaskID,
		EnvelopeID:    taskMessage.Envelope.ID,
		TraceID:       taskMessage.TraceID,
		EventID:       eventID,
		EventKind:     eventKind,
		EmittedAt:     contracts.NewTimestamp(mustTime(t, "2026-03-06T12:00:00Z")),
		Message: contracts.OutboundMessage{
			Channel: "email",
			Target:  "alice@example.com",
			Body:    &body,
		},
		Sequence: &sequence,
	}
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func fileURIForTest(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
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
