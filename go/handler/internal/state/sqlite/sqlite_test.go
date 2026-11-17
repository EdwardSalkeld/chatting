package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/testutil"
	_ "modernc.org/sqlite"
)

func TestDedupeKeysAreScopedBySource(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if seen, err := store.Seen(ctx, "email", "message-1"); err != nil || seen {
		t.Fatalf("new key seen=%v err=%v", seen, err)
	}
	for i := 0; i < 2; i++ {
		if err := store.MarkSeen(ctx, "email", "message-1"); err != nil {
			t.Fatal(err)
		}
	}
	emailSeen, err := store.Seen(ctx, "email", "message-1")
	if err != nil || !emailSeen {
		t.Fatalf("email key seen=%v err=%v", emailSeen, err)
	}
	if telegramSeen, err := store.Seen(ctx, "telegram", "message-1"); err != nil || telegramSeen {
		t.Fatal("dedupe key leaked across source")
	}
}

func TestTelegramChatRegistryUpsertsObservedMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	firstSeenAt := testutil.MustTime(t, "2026-05-21T06:40:00Z")
	firstMessageAt := testutil.MustTime(t, "2026-05-21T06:39:30Z")
	updatedAt := testutil.MustTime(t, "2026-05-21T06:45:00Z")
	title := "Build Tests"
	if err := store.RecordTelegramChat(ctx, TelegramChatObservation{
		ChatID: "-100123", Title: &title, UpdateID: 1001, UpdateKind: "message",
		MessageDate: &firstMessageAt, RetrievedAt: firstSeenAt,
	}); err != nil {
		t.Fatal(err)
	}
	// A later update without metadata must not erase what was observed earlier.
	if err := store.RecordTelegramChat(ctx, TelegramChatObservation{
		ChatID: "-100123", UpdateID: 1002, UpdateKind: "my_chat_member", RetrievedAt: updatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	var titleValue, lastMessageAt sql.NullString
	var firstSeen, lastRetrieved, lastUpdateKind string
	var lastUpdateID int64
	if err := store.db.QueryRowContext(ctx,
		`SELECT title, first_seen_at, last_retrieved_at, last_message_at, last_update_id, last_update_kind
		FROM telegram_chat_registry WHERE chat_id = ?`, "-100123",
	).Scan(&titleValue, &firstSeen, &lastRetrieved, &lastMessageAt, &lastUpdateID, &lastUpdateKind); err != nil {
		t.Fatal(err)
	}
	if titleValue.String != title || firstSeen != formatTimestamp(firstSeenAt) || lastRetrieved != formatTimestamp(updatedAt) ||
		lastMessageAt.String != formatTimestamp(firstMessageAt) || lastUpdateID != 1002 || lastUpdateKind != "my_chat_member" {
		t.Fatalf("registry row = %v %s %s %v %d %s", titleValue, firstSeen, lastRetrieved, lastMessageAt, lastUpdateID, lastUpdateKind)
	}
}

func TestOpenDropsRetiredGitHubCheckpointTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE github_assignment_checkpoints (scope_key TEXT PRIMARY KEY, event_id TEXT NOT NULL);
		INSERT INTO github_assignment_checkpoints VALUES ('scope', 'AE_1')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store := openStoreAt(t, dbPath)
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'github_assignment_checkpoints'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("github_assignment_checkpoints still exists: %d %v", count, err)
	}
}

func TestTaskCompletionIsScopedToEnvelopeAndClearedByReopening(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	taskMessage := testTaskMessage(t)
	if err := store.RecordTask(ctx, taskMessage); err != nil {
		t.Fatal(err)
	}
	record, err := store.GetTask(ctx, taskMessage.TaskID)
	if err != nil || record == nil || record.TaskMessage.Envelope.ReplyChannel.Target != "alice@example.com" {
		t.Fatalf("task record = %#v err=%v", record, err)
	}
	if err := store.MarkTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID, taskMessage.TraceID); err != nil {
		t.Fatal(err)
	}
	if openTask, err := store.GetTask(ctx, taskMessage.TaskID); err != nil || openTask != nil {
		t.Fatalf("completed task remained open: %#v %v", openTask, err)
	}
	if completed, err := store.IsTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID); err != nil || !completed {
		t.Fatal("same task/envelope pair was not completed")
	}
	if completed, err := store.IsTaskCompleted(ctx, taskMessage.TaskID, "email:other"); err != nil || completed {
		t.Fatal("completion leaked to a different envelope id")
	}
	if err := store.RecordTask(ctx, taskMessage); err != nil {
		t.Fatal(err)
	}
	if completed, err := store.IsTaskCompleted(ctx, taskMessage.TaskID, taskMessage.Envelope.ID); err != nil || completed {
		t.Fatal("recording a new task did not clear prior completion")
	}
}

func TestDispatchedEventIDCheckpointIsIdempotentAndScopedByTask(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	for i := 0; i < 2; i++ {
		if err := store.MarkDispatchedEventID(ctx, "task:email:1", "evt:1"); err != nil {
			t.Fatal(err)
		}
	}
	if dispatched, err := store.HasDispatchedEventID(ctx, "task:email:1", "evt:1"); err != nil || !dispatched {
		t.Fatal("event id was not marked dispatched")
	}
	if dispatched, err := store.HasDispatchedEventID(ctx, "task:email:other", "evt:1"); err != nil || dispatched {
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

func telegramAttachmentTask(t *testing.T, id string, uris ...string) contracts.TaskQueueMessage {
	t.Helper()
	name := "photo.jpg"
	attachments := make([]contracts.AttachmentRef, 0, len(uris))
	for _, uri := range uris {
		attachments = append(attachments, contracts.AttachmentRef{URI: uri, Name: &name})
	}
	return contracts.NewTaskQueueMessage(contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "telegram:" + id,
		Source:        "im",
		ReceivedAt:    contracts.NewTimestamp(testutil.MustTime(t, "2026-05-30T12:00:00Z")),
		Content:       "[photo attached]",
		Attachments:   attachments,
		ContextRefs:   []string{},
		ReplyChannel:  contracts.ReplyChannel{Type: "telegram", Target: "12345"},
		DedupeKey:     "telegram:" + id,
	}, "trace:telegram:"+id, testutil.MustTime(t, "2026-05-30T12:00:01Z"))
}

func cleanupAttachments(t *testing.T, store *Store, root string, notAfter string) TelegramAttachmentCleanupResult {
	t.Helper()
	result, err := store.CleanupTelegramAttachments(context.Background(), root, testutil.MustTime(t, notAfter), testutil.MustTime(t, "2026-01-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTelegramAttachmentLedgerTracksLocalTaskAttachmentsAndCleanup(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	attachmentRoot := t.TempDir()
	attachmentPath := filepath.Join(attachmentRoot, "telegram-1-2-photo.jpg")
	if err := os.WriteFile(attachmentPath, []byte("jpeg-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	taskMessage := telegramAttachmentTask(t, "1", fileURIForTest(attachmentPath), "file:///tmp/outside.jpg", "s3://bucket/photo.jpg")
	if tracked, err := store.RecordTelegramTaskAttachments(ctx, taskMessage, attachmentRoot); err != nil || tracked != 1 {
		t.Fatalf("tracked = %d err=%v", tracked, err)
	}
	eligibleAfter := testutil.MustTime(t, "2026-05-30T12:10:00Z")
	if eligible, err := store.MarkTelegramTaskAttachmentsEligible(ctx, taskMessage.TaskID, eligibleAfter); err != nil || eligible != 1 {
		t.Fatalf("eligible = %d err=%v", eligible, err)
	}
	records := listTelegramAttachments(t, store)
	if len(records) != 1 || records[0].AttachmentPath != attachmentPath || records[0].EligibleAfter == nil || !records[0].EligibleAfter.Equal(eligibleAfter) {
		t.Fatalf("records = %#v", records)
	}
	result := cleanupAttachments(t, store, attachmentRoot, "2026-05-30T12:11:00Z")
	if result.DeletedCount != 1 || result.MissingCount != 0 || result.FailedCount != 0 || result.ReclaimedBytes != int64(len("jpeg-bytes")) {
		t.Fatalf("cleanup result = %#v", result)
	}
	if _, err := os.Stat(attachmentPath); !os.IsNotExist(err) {
		t.Fatalf("attachment still exists or unexpected stat error: %v", err)
	}
	if records = listTelegramAttachments(t, store); records[0].DeletedAt == nil {
		t.Fatalf("deleted_at not marked: %#v", records[0])
	}
}

func TestTelegramAttachmentCleanupMarksMissingAndRejectsOutsideRoot(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	attachmentRoot := t.TempDir()
	taskMessage := telegramAttachmentTask(t, "missing", fileURIForTest(filepath.Join(attachmentRoot, "missing.jpg")))
	if _, err := store.RecordTelegramTaskAttachments(ctx, taskMessage, attachmentRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTelegramTaskAttachmentsEligible(ctx, taskMessage.TaskID, testutil.MustTime(t, "2026-05-30T12:01:00Z")); err != nil {
		t.Fatal(err)
	}
	if result := cleanupAttachments(t, store, attachmentRoot, "2026-05-30T12:02:00Z"); result.MissingCount != 1 || result.DeletedCount != 0 || result.FailedCount != 0 {
		t.Fatalf("cleanup result = %#v", result)
	}

	outsidePath := filepath.Join(t.TempDir(), "outside.jpg")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx,
		`INSERT INTO telegram_attachment_ledger (attachment_path, attachment_uri, task_id, envelope_id, created_at, eligible_after, cleanup_attempts)
		VALUES (?, ?, 'task:telegram:outside', 'telegram:outside', '2026-05-30T12:00:00Z', '2026-05-30T12:01:00Z', 0)`,
		outsidePath, fileURIForTest(outsidePath),
	); err != nil {
		t.Fatal(err)
	}
	if result := cleanupAttachments(t, store, attachmentRoot, "2026-05-30T12:02:00Z"); result.FailedCount != 1 {
		t.Fatalf("cleanup result = %#v", result)
	}
	if _, err := os.Stat(outsidePath); err != nil {
		t.Fatalf("file outside root was touched: %v", err)
	}
	for _, record := range listTelegramAttachments(t, store) {
		if record.AttachmentPath == outsidePath && (record.CleanupAttempts != 1 || derefString(record.LastCleanupError) != "attachment_path_outside_root") {
			t.Fatalf("outside record = %#v", record)
		}
	}
}

func listTelegramAttachments(t *testing.T, store *Store) []TelegramAttachmentRecord {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), telegramAttachmentSelectColumns+`
		FROM telegram_attachment_ledger
		ORDER BY created_at ASC, attachment_path ASC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	records, err := scanAll(rows, scanTelegramAttachmentRecord)
	if err != nil {
		t.Fatal(err)
	}
	return records
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
		ReceivedAt:    contracts.NewTimestamp(testutil.MustTime(t, "2026-03-06T11:00:00Z")),
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
	}, "trace:email:1", testutil.MustTime(t, "2026-03-06T11:00:01Z"))
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
		EmittedAt:     contracts.NewTimestamp(testutil.MustTime(t, "2026-03-06T12:00:00Z")),
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
