package telegram

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/testutil"
)

func TestPollingFailuresBackOffWithoutExitingAndResetAfterRecovery(t *testing.T) {
	now := time.Now()
	calls := 0
	var observed []bool
	var codes []int
	client := &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("secret upstream URL")
		}
		if calls == 2 {
			r := jsonResponse(`{"ok":false}`)
			r.StatusCode = 503
			return r, nil
		}
		if calls == 3 {
			return jsonResponse(`not json`), nil
		}
		if calls == 4 {
			return jsonResponse(`{"ok":true,"result":[{"update_id":42}]}`), nil
		}
		if req.URL.Query().Get("offset") != "43" {
			t.Fatal("lost update offset")
		}
		return jsonResponse(`{"ok":false,"error_code":429,"parameters":{"retry_after":20}}`), nil
	}}
	c, err := New(Config{BotToken: "token", HTTPClient: client, Now: func() time.Time { return now }, ObservePoll: func(err error, code int) { observed = append(observed, err != nil); codes = append(codes, code) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		if _, err := c.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.pollDelay != delay {
			t.Fatalf("delay = %s, want %s", c.pollDelay, delay)
		}
		before := calls
		if _, err := c.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if calls != before {
			t.Fatal("polled during backoff")
		}
		now = now.Add(delay)
	}
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.pollDelay != 0 {
		t.Fatal("backoff did not reset")
	}
	if _, err := c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.pollDelay != time.Second || !c.nextPollAt.Equal(now.Add(20*time.Second)) {
		t.Fatal("rate limit not honoured")
	}
	if !reflect.DeepEqual(observed, []bool{true, true, true, false, true}) {
		t.Fatalf("poll observations = %v; backoff must preserve failure until a successful API call", observed)
	}
	if !reflect.DeepEqual(codes, []int{0, 503, 0, 0, 429}) {
		t.Fatalf("poll error codes = %v", codes)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestPollingBackoffCapsAtOneMinute(t *testing.T) {
	now := time.Now()
	c, _ := New(Config{BotToken: "token", Now: func() time.Time { return now }, HTTPClient: &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) { return nil, errors.New("offline") }}})
	for i := 0; i < 12; i++ {
		if _, err := c.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.pollDelay > time.Minute {
			t.Fatal("unbounded backoff")
		}
		now = c.nextPollAt
	}
	if c.pollDelay != time.Minute {
		t.Fatal("cap not reached")
	}
}

func TestAttachmentFailureRetriesWithoutLosingUpdates(t *testing.T) {
	for _, stage := range []string{"metadata", "download"} {
		t.Run(stage, func(t *testing.T) {
			now := time.Now()
			attempts, polls := 0, 0
			var offsets []string
			client := &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/getUpdates"):
					polls++
					offsets = append(offsets, req.URL.Query().Get("offset"))
					if polls == 1 {
						return jsonResponse(`{"ok":true,"result":[{"update_id":10,"message":{"message_id":10,"chat":{"id":12345,"type":"private"},"text":"first"}},{"update_id":11,"message":{"message_id":11,"chat":{"id":12345,"type":"private"},"photo":[{"file_id":"photo"}]}},{"update_id":12,"message":{"message_id":12,"chat":{"id":12345,"type":"private"},"text":"last"}}]}`), nil
					}
					return jsonResponse(`{"ok":true,"result":[{"update_id":11,"message":{"message_id":11,"chat":{"id":12345,"type":"private"},"photo":[{"file_id":"photo"}]}},{"update_id":12,"message":{"message_id":12,"chat":{"id":12345,"type":"private"},"text":"last"}}]}`), nil
				case strings.HasSuffix(req.URL.Path, "/getFile"):
					if stage == "metadata" {
						attempts++
						if attempts <= 2 {
							r := jsonResponse(`{"ok":false}`)
							r.StatusCode = 503
							return r, nil
						}
					}
					return jsonResponse(`{"ok":true,"result":{"file_path":"photos/a.jpg"}}`), nil
				case strings.Contains(req.URL.Path, "/file/"):
					if stage == "download" {
						attempts++
						if attempts == 1 {
							r := bytesResponse("upstream error page")
							r.StatusCode = 503
							return r, nil
						}
						if attempts == 2 {
							return nil, errors.New("sensitive upstream details")
						}
					}
					return bytesResponse("image"), nil
				}
				t.Fatal("unexpected request")
				return nil, nil
			}}
			c, err := New(Config{BotToken: "token", HTTPClient: client, Now: func() time.Time { return now }, AttachmentRootDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			first, err := c.Poll(context.Background())
			if err != nil || len(first) != 1 || first[0].ID != "telegram:10" {
				t.Fatalf("first=%v err=%v", first, err)
			}
			if c.nextOffset == nil || *c.nextOffset != 11 {
				t.Fatalf("offset=%v", c.nextOffset)
			}
			if got, err := c.Poll(context.Background()); err != nil || len(got) != 0 || polls != 1 {
				t.Fatalf("backoff got=%v err=%v polls=%d", got, err, polls)
			}
			now = c.nextPollAt
			if got, err := c.Poll(context.Background()); err != nil || len(got) != 0 || c.attachmentDelay != 2*time.Second {
				t.Fatalf("second retry got=%v err=%v delay=%s", got, err, c.attachmentDelay)
			}
			now = c.nextPollAt
			got, err := c.Poll(context.Background())
			if err != nil || len(got) != 2 || got[0].ID != "telegram:11" || len(got[0].Attachments) != 1 || got[1].ID != "telegram:12" {
				t.Fatalf("recovered=%v err=%v", got, err)
			}
			if *c.nextOffset != 13 || c.attachmentDelay != 0 || !reflect.DeepEqual(offsets, []string{"", "11", "11"}) {
				t.Fatalf("offsets=%v next=%v delay=%v", offsets, c.nextOffset, c.attachmentDelay)
			}
		})
	}
}

func TestAttachmentCancellationPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/getUpdates") {
			return jsonResponse(`{"ok":true,"result":[{"update_id":1,"message":{"message_id":1,"chat":{"id":1,"type":"private"},"photo":[{"file_id":"photo"}]}}]}`), nil
		}
		cancel()
		return nil, context.Canceled
	}}
	c, err := New(Config{BotToken: "token", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if c.nextOffset != nil {
		t.Fatal("cancelled update was acknowledged")
	}
}

func TestPollNormalizesAllowedMessageAndObservesChatBeforeAllowlist(t *testing.T) {
	var requestedURL string
	client := &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
		requestedURL = req.URL.String()
		return jsonResponse(`{
			"ok": true,
			"result": [
				{
					"update_id": 1001,
					"message": {
						"message_id": 55,
						"message_thread_id": 7,
						"date": 1779345600,
						"chat": {"id": 12345, "type": "private", "username": "edward"},
						"from": {"id": 7, "username": "sender"},
						"reply_to_message": {"message_id": 41},
						"text": "hello from telegram"
					}
				},
				{
					"update_id": 1002,
					"message": {
						"message_id": 56,
						"date": 1779345660,
						"chat": {"id": 99999, "type": "private", "username": "other"},
						"from": {"id": 8},
						"text": "ignored"
					}
				}
			]
		}`), nil
	}}
	observed := []ChatObservation{}
	connector, err := New(Config{
		BotToken:           "token",
		APIBaseURL:         "https://telegram.example.test",
		PollTimeoutSeconds: 12,
		AllowedChatIDs:     []string{"12345"},
		ContextRefs:        []string{"repo:/workspace/chatting"},
		HTTPClient:         client,
		ObserveChat: func(ctx context.Context, observation ChatObservation) error {
			observed = append(observed, observation)
			return nil
		},
		Now: func() time.Time { return testutil.MustTime(t, "2026-05-21T06:40:00Z") },
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(requestedURL, "/bottoken/getUpdates?") || !strings.Contains(requestedURL, "timeout=12") {
		t.Fatalf("requested URL = %q", requestedURL)
	}
	if len(envelopes) != 1 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	envelope := envelopes[0]
	if envelope.ID != "telegram:1001" || envelope.Source != "im" || envelope.DedupeKey != "telegram:1001" {
		t.Fatalf("envelope identity = %#v", envelope)
	}
	if envelope.Content != "[thread_id=7] hello from telegram" {
		t.Fatalf("content = %q", envelope.Content)
	}
	if envelope.ReplyChannel.Type != "telegram" || envelope.ReplyChannel.Target != "12345" {
		t.Fatalf("reply channel = %#v", envelope.ReplyChannel)
	}
	if envelope.ReplyChannel.Metadata["message_id"] != float64(55) && envelope.ReplyChannel.Metadata["message_id"] != int64(55) {
		t.Fatalf("metadata = %#v", envelope.ReplyChannel.Metadata)
	}
	if envelope.ReplyChannel.Metadata["reply_to_message_id"] != int64(41) {
		t.Fatalf("reply anchor metadata = %#v", envelope.ReplyChannel.Metadata)
	}
	if envelope.ReplyChannel.Metadata["message_thread_id"] != int64(7) {
		t.Fatalf("topic metadata = %#v", envelope.ReplyChannel.Metadata)
	}
	if deref(envelope.Actor) != "7:sender" {
		t.Fatalf("actor = %#v", envelope.Actor)
	}
	if !reflect.DeepEqual(envelope.ContextRefs, []string{"repo:/workspace/chatting"}) {
		t.Fatalf("context refs = %#v", envelope.ContextRefs)
	}
	if len(observed) != 2 {
		t.Fatalf("observed chats = %#v", observed)
	}
	if observed[0].ChatID != "12345" || deref(observed[0].Username) != "edward" || observed[0].UpdateKind != "message" {
		t.Fatalf("first observation = %#v", observed[0])
	}
	if observed[1].ChatID != "99999" {
		t.Fatalf("disallowed chat was not observed before allowlist: %#v", observed)
	}

	requestedURL = ""
	client.do = func(req *http.Request) (*http.Response, error) {
		requestedURL = req.URL.String()
		return jsonResponse(`{"ok": true, "result": []}`), nil
	}
	if _, err := connector.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(requestedURL, "offset=1003") {
		t.Fatalf("second requested URL = %q", requestedURL)
	}
}

func TestPollNormalizesAllowedChannelPostAndMyChatMemberObservation(t *testing.T) {
	envelopes, observed := pollUpdates(t, Config{AllowedChannelIDs: []string{"-100999"}}, `[
		{"update_id": 2001, "channel_post": {"message_id": 9, "date": 1779345600, "chat": {"id": -100999, "type": "channel", "title": "Deploys", "username": "deploys"},
			"sender_chat": {"id": -100999, "type": "channel", "title": "Deploys"}, "text": "release shipped"}},
		{"update_id": 2002, "my_chat_member": {"chat": {"id": -100123, "type": "supergroup", "title": "Test Group"}}}
	]`, nil)

	if len(envelopes) != 1 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	if envelopes[0].ReplyChannel.Target != "-100999" || envelopes[0].Content != "release shipped" {
		t.Fatalf("channel envelope = %#v", envelopes[0])
	}
	if envelopes[0].ReplyChannel.Metadata["chat_title"] != "Deploys" || envelopes[0].ReplyChannel.Metadata["chat_type"] != "channel" {
		t.Fatalf("channel metadata = %#v", envelopes[0].ReplyChannel.Metadata)
	}
	if len(observed) != 2 {
		t.Fatalf("observed = %#v", observed)
	}
	if observed[0].UpdateKind != "channel_post" || observed[0].ChatID != "-100999" {
		t.Fatalf("channel observation = %#v", observed[0])
	}
	if observed[1].UpdateKind != "my_chat_member" || observed[1].ChatID != "-100123" || deref(observed[1].Title) != "Test Group" {
		t.Fatalf("membership observation = %#v", observed[1])
	}
}

func TestPollBuildsInternalNoticesForDisallowedChannelAndGroup(t *testing.T) {
	envelopes, observed := pollUpdates(t, Config{AllowedChatIDs: []string{"12345"}, AllowedChannelIDs: []string{"-100999"}}, `[
		{"update_id": 2101, "channel_post": {"message_id": 19, "date": 1779345600, "chat": {"id": -100777, "type": "channel", "title": "Ops"}, "text": "hello"}},
		{"update_id": 2201, "message": {"message_id": 29, "date": 1779345600, "chat": {"id": -100888, "type": "supergroup", "title": "Ops Group"}, "from": {"id": 7}, "text": "hello group"}}
	]`, nil)
	if len(envelopes) != 2 || len(observed) != 2 {
		t.Fatalf("envelopes = %#v observed = %#v", envelopes, observed)
	}
	for i, want := range []struct{ target, content, notice string }{
		{"-100777", "Not enabled in channel -100777. Add this id to telegram_allowed_channel_ids to enable replies here.", "telegram_channel_not_enabled"},
		{"-100888", "Not enabled in group -100888. Add this id to telegram_allowed_chat_ids to enable replies here.", "telegram_group_not_enabled"},
	} {
		envelope := envelopes[i]
		if envelope.Source != "internal" || envelope.ReplyChannel.Target != want.target || envelope.Content != want.content || envelope.ReplyChannel.Metadata["internal_notice"] != want.notice {
			t.Fatalf("notice %d = %#v", i, envelope)
		}
	}
}

func TestPollDownloadsAttachmentsReadableByWorker(t *testing.T) {
	dir := t.TempDir()
	envelopes, _ := pollUpdates(t, Config{AttachmentRootDir: dir}, `[
		{"update_id": 3001, "message": {"message_id": 77, "date": 1779345600, "chat": {"id": 12345, "type": "private"}, "caption": "leaf spot", "photo": [
			{"file_id": "small", "file_unique_id": "small-unique", "width": 90, "height": 60, "file_size": 10},
			{"file_id": "large", "file_unique_id": "large/unique", "width": 900, "height": 600, "file_size": 100}
		]}},
		{"update_id": 3002, "message": {"message_id": 78, "date": 1779345600, "chat": {"id": 12345, "type": "private"}, "photo": [{"file_id": "only", "width": 800, "height": 600}]}},
		{"update_id": 3003, "message": {"message_id": 79, "date": 1779345600, "chat": {"id": 12345, "type": "private"}, "document": {
			"file_id": "doc", "file_unique_id": "document/unique", "file_name": "quarterly-report.pdf", "mime_type": "application/pdf", "file_size": 14
		}}}
	]`, map[string][2]string{
		"large": {"photos/leaf.jpg", "jpeg-bytes"},
		"only":  {"photos/photo.jpg", "jpeg-bytes"},
		"doc":   {"documents/file_42.dat", "pdf-file-bytes"},
	})
	if len(envelopes) != 3 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	for i, want := range []struct{ content, name, file, body string }{
		{"leaf spot", "leaf.jpg", "telegram-3001-77-large_unique.jpg", "jpeg-bytes"},
		{"[photo attached]", "photo.jpg", "", ""},
		{"[document attached: quarterly-report.pdf]", "quarterly-report.pdf", "telegram-3003-79-document_unique.pdf", "pdf-file-bytes"},
	} {
		envelope := envelopes[i]
		if envelope.Content != want.content || len(envelope.Attachments) != 1 || deref(envelope.Attachments[0].Name) != want.name {
			t.Fatalf("envelope %d = %#v", i, envelope)
		}
		if want.file == "" {
			continue
		}
		if envelope.Attachments[0].URI != "file://"+filepath.Join(dir, want.file) {
			t.Fatalf("attachment URI = %q", envelope.Attachments[0].URI)
		}
		path := filepath.Join(dir, want.file)
		if raw, err := os.ReadFile(path); err != nil || string(raw) != want.body {
			t.Fatalf("downloaded %q, %v", raw, err)
		}
		// The worker runs as a separate OS user, so the attachment must be
		// readable beyond the owner (see extractAttachments).
		if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o044 != 0o044 {
			t.Fatalf("attachment mode = %v, %v; want group/other readable", info, err)
		}
	}
}

func TestPollKeepsOversizedDocumentVisibleWithoutDownloadingIt(t *testing.T) {
	envelopes, _ := pollUpdates(t, Config{}, `[{"update_id": 3004, "message": {"message_id": 80, "date": 1779345600, "chat": {"id": 12345, "type": "private"},
		"document": {"file_id": "too-large", "file_unique_id": "too-large-unique", "file_name": "archive.zip", "file_size": 20971521}}}]`, nil)
	if len(envelopes) != 1 || len(envelopes[0].Attachments) != 0 ||
		envelopes[0].Content != "[document not downloaded: archive.zip exceeds Telegram's 20 MB bot download limit]" {
		t.Fatalf("envelopes = %#v", envelopes)
	}
}

// pollUpdates polls once for the given getUpdates result. files maps a
// file_id to its Telegram file_path and body; any other download fails the test.
func pollUpdates(t *testing.T, config Config, updates string, files map[string][2]string) ([]contracts.TaskEnvelope, []ChatObservation) {
	t.Helper()
	var observed []ChatObservation
	config.BotToken = "token"
	if config.AllowedChatIDs == nil {
		config.AllowedChatIDs = []string{"12345"}
	}
	config.ObserveChat = func(_ context.Context, observation ChatObservation) error {
		observed = append(observed, observation)
		return nil
	}
	config.HTTPClient = &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/getUpdates") {
			return jsonResponse(`{"ok": true, "result": ` + updates + `}`), nil
		}
		if strings.HasSuffix(req.URL.Path, "/getFile") {
			if file, ok := files[req.URL.Query().Get("file_id")]; ok {
				return jsonResponse(`{"ok": true, "result": {"file_path": "` + file[0] + `"}}`), nil
			}
		}
		for _, file := range files {
			if strings.HasSuffix(req.URL.Path, "/file/bottoken/"+file[0]) {
				return bytesResponse(file[1]), nil
			}
		}
		t.Errorf("unexpected request: %s", req.URL)
		return nil, errors.New("unexpected request")
	}}
	connector, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return envelopes, observed
}

type fakeHTTPClient struct {
	do func(*http.Request) (*http.Response, error)
}

func (client *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return client.do(req)
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

func bytesResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
