package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	githubconnector "github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/github"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/imap"
	reminderconnector "github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/reminder"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/schedule"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/telegram"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/dispatch"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/egress"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/reminders"
	handlerruntime "github.com/EdwardSalkeld/chatting/go/handler/internal/runtime"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/schedules"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/workitems"
)

const (
	version = "go-handler-bootstrap"
	// Completed tasks keep their Telegram attachments this long before cleanup.
	telegramAttachmentCleanupGrace = 7 * 24 * time.Hour
)

var ghLookPath = exec.LookPath

type runner interface {
	Run(ctx context.Context) error
}

type runnerFactory func(ctx context.Context, config handlerconfig.Config) (runner, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runWithFactory(ctx, os.Args[1:], os.Stdout, os.Stderr, newRuntimeRunner))
}

func runWithFactory(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, newRunner runnerFactory) int {
	flags := flag.NewFlagSet("chatting-handler", flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "print version and exit")
	configPath := flags.String("config", "", "Path to JSON config file.")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *showVersion {
		fmt.Fprintln(stdout, version)
		return 0
	}

	config, err := handlerconfig.LoadFile(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "config error: %v\n", err)
		return 2
	}

	runner, err := newRunner(ctx, config)
	if err != nil {
		fmt.Fprintf(stderr, "runtime setup error: %v\n", err)
		return 2
	}
	if err := runner.Run(ctx); err != nil {
		fmt.Fprintf(stderr, "runtime error: %v\n", err)
		return 1
	}
	return 0
}

func newRuntimeRunner(ctx context.Context, config handlerconfig.Config) (runner, error) {
	adapter, err := bbmb.NewAdapter(config.BBMBAddress)
	if err != nil {
		return nil, err
	}
	store, err := sqlitestate.Open(ctx, config.DBPath)
	if err != nil {
		return nil, err
	}
	personal, err := newPersonalInbox(config, adapter)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	setupComplete := false
	defer func() {
		if !setupComplete && personal != nil {
			_ = personal.Store.Close()
		}
	}()
	dispatcher, emailSender, err := buildDispatcher(config)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	errorEmailRecipient := resolveErrorEmailRecipient(config)
	egressOptions := []egress.Option{
		egress.WithAllowedChannels(config.AllowedEgressChannels),
		egress.WithCompletionHook(func(ctx context.Context, message contracts.EgressQueueMessage) error {
			if !config.TelegramEnabled {
				return nil
			}
			eligibleAfter := time.Now().UTC().Add(telegramAttachmentCleanupGrace)
			_, err := store.MarkTelegramTaskAttachmentsEligible(ctx, message.TaskID, eligibleAfter)
			return err
		}),
		egress.WithDropHook(func(ctx context.Context, message contracts.EgressQueueMessage, reasonCode string) {
			// The engine already logs a loud line for every error-class drop; this hook
			// is only the operator alert. Correctable attachment validation failures
			// are returned synchronously to the executor and should not produce one
			// email per bad draft. It sends SMTP directly (not via the egress engine)
			// so it cannot recurse into another drop.
			if shouldNotifyEgressDrop(reasonCode) {
				sendEgressDispatchErrorEmail(ctx, emailSender, errorEmailRecipient, message, reasonCode)
			}
		}),
	}
	engine, err := egress.New(
		egress.NewSQLiteState(store),
		dispatcher,
		egressOptions...,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	metricRecorder := metrics.New(time.Time{}, nil)
	connectors := []handlerruntime.Connector{heartbeat.New(nil)}
	scheduleConnector, err := schedule.NewFromSource(schedule.NewStoreSource(store), nil)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	connectors = append(connectors, scheduleConnector)
	reminderConnector, err := reminderconnector.New(
		reminderconnector.NewStoreSource(store),
		[]string{"This task was created as a one-off reminder. Carry out the stored prompt at the requested time and reply through the supplied channel."},
		nil,
		metricRecorder,
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	connectors = append(connectors, reminderConnector)
	if config.IMAPHost != "" {
		password := os.Getenv(config.IMAPPasswordEnv)
		if password == "" {
			_ = store.Close()
			return nil, fmt.Errorf("missing IMAP password env var: %s", config.IMAPPasswordEnv)
		}
		connector, err := imap.New(imap.Config{
			Host:            config.IMAPHost,
			Port:            config.IMAPPort,
			Username:        config.IMAPUsername,
			Password:        password,
			Mailbox:         config.IMAPMailbox,
			SearchCriterion: config.IMAPSearch,
			ContextRefs:     config.ContextRefs,
			UseSSL:          config.IMAPUseSSL,
		})
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		connectors = append(connectors, connector)
	}
	if config.TelegramEnabled {
		token := os.Getenv(config.TelegramBotTokenEnv)
		if token == "" {
			_ = store.Close()
			return nil, fmt.Errorf("missing Telegram bot token env var: %s", config.TelegramBotTokenEnv)
		}
		telegramContextRefs := config.TelegramContextRefs
		if len(telegramContextRefs) == 0 {
			telegramContextRefs = config.ContextRefs
		}
		connector, err := telegram.New(telegram.Config{
			BotToken:           token,
			APIBaseURL:         config.TelegramAPIBaseURL,
			PollTimeoutSeconds: config.TelegramPollTimeoutSeconds,
			AllowedChatIDs:     config.TelegramAllowedChatIDs,
			AllowedChannelIDs:  config.TelegramAllowedChannelIDs,
			ContextRefs:        telegramContextRefs,
			AttachmentRootDir:  config.TelegramAttachmentDir,
			ObserveChat: func(ctx context.Context, observation telegram.ChatObservation) error {
				return store.RecordTelegramChat(ctx, sqlitestate.TelegramChatObservation{
					ChatID:      observation.ChatID,
					ChatType:    observation.ChatType,
					Title:       observation.Title,
					Username:    observation.Username,
					UpdateID:    observation.UpdateID,
					UpdateKind:  observation.UpdateKind,
					MessageDate: observation.MessageDate,
					RetrievedAt: observation.RetrievedAt,
				})
			},
		})
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		connectors = append(connectors, connector)
	}
	if len(config.GitHubRepositories) > 0 {
		assigneeLogin := config.GitHubAssigneeLogin
		checkpointStore := githubCheckpointStore{store: store}
		assignmentConnector, err := githubconnector.NewIssueAssignmentConnector(githubconnector.IssueAssignmentConfig{
			RepositoryPatterns: config.GitHubRepositories,
			AssigneeLogin:      assigneeLogin,
			CheckpointStore:    checkpointStore,
		})
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		reviewConnector, err := githubconnector.NewPullRequestReviewConnector(githubconnector.PullRequestReviewConfig{
			RepositoryPatterns: config.GitHubRepositories,
			AuthorLogin:        assigneeLogin,
			CheckpointStore:    checkpointStore,
		})
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		connectors = append(connectors, assignmentConnector, reviewConnector)
	}
	scheduleService := schedules.NewService(store)
	reminderService := reminders.NewService(store, metricRecorder)
	metricsServer, err := metrics.StartServer(metricRecorder, config.MetricsHost, config.MetricsPort, func(mux *http.ServeMux) {
		schedules.RegisterRoutes(mux, scheduleService)
		schedules.RegisterUIRoutes(mux, scheduleService)
		reminders.RegisterRoutes(mux, reminderService)
		reminders.RegisterUIRoutes(mux, reminderService)
		// This server carries the handler's own pages as well as metrics, and
		// binds loopback. The page shows mail subjects, so that matters.
		if personal != nil {
			personalinbox.RegisterUIRoutes(mux, personal)
		}
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	// A dedicated loopback server for synchronous egress submits from the worker
	// reply script. Kept off the metrics mux on purpose: metrics may bind
	// 0.0.0.0 for scraping, but sending messages to the world must stay
	// loopback-only, like BBMB.
	egressServer, err := egress.StartHTTPServer("127.0.0.1", config.EgressHTTPPort, engine, func(result egress.Result) {
		metricRecorder.RecordEgressResult(result.Status, result.Reason)
	}, func(mux *http.ServeMux) {
		workitems.RegisterRoutes(mux, store)
		if personal != nil {
			personal.RegisterRoutes(mux)
		}
	})
	if err != nil {
		_ = metricsServer.Close()
		_ = store.Close()
		return nil, err
	}
	runner, err := handlerruntime.NewRunner(config, adapter, handlerruntime.WithIngress(handlerIngressState{store: store}, connectors...), handlerruntime.WithMetrics(metricRecorder))
	if err != nil {
		_ = egressServer.Close()
		_ = metricsServer.Close()
		_ = store.Close()
		return nil, err
	}
	closers := []closer{egressServer, metricsServer, store}
	if personal != nil {
		closers = append(closers, personal.Store)
	}
	setupComplete = true
	return &closingRunner{runner: runner, personal: personal, closers: closers}, nil
}

type githubCheckpointStore struct {
	store *sqlitestate.Store
}

type handlerIngressState struct {
	store *sqlitestate.Store
}

var _ handlerruntime.TaskRouter = handlerIngressState{}

func (state handlerIngressState) Seen(ctx context.Context, source string, dedupeKey string) (bool, error) {
	return state.store.Seen(ctx, source, dedupeKey)
}

func (state handlerIngressState) MarkSeen(ctx context.Context, source string, dedupeKey string) error {
	return state.store.MarkSeen(ctx, source, dedupeKey)
}

func (state handlerIngressState) RecordTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) error {
	return state.store.RecordTask(ctx, taskMessage)
}

func (state handlerIngressState) AssignTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) (contracts.TaskQueueMessage, error) {
	return state.store.AssignTask(ctx, taskMessage)
}

func (state handlerIngressState) RecordTelegramTaskAttachments(ctx context.Context, taskMessage contracts.TaskQueueMessage, attachmentRootDir string) (int, error) {
	return state.store.RecordTelegramTaskAttachments(ctx, taskMessage, attachmentRootDir)
}

func (state handlerIngressState) CleanupTelegramAttachmentsForRuntime(ctx context.Context, attachmentRootDir string, notAfter time.Time, maxAgeCutoff time.Time) error {
	return state.store.CleanupTelegramAttachmentsForRuntime(ctx, attachmentRootDir, notAfter, maxAgeCutoff)
}

func (store githubCheckpointStore) GetGitHubCheckpoint(ctx context.Context, scopeKey string) (*githubconnector.AssignmentCheckpoint, error) {
	checkpoint, err := store.store.GetGitHubAssignmentCheckpoint(ctx, scopeKey)
	if err != nil || checkpoint == nil {
		return nil, err
	}
	return &githubconnector.AssignmentCheckpoint{
		EventCreatedAt: checkpoint.EventCreatedAt,
		EventID:        checkpoint.EventID,
	}, nil
}

func (store githubCheckpointStore) SetGitHubCheckpoint(ctx context.Context, scopeKey string, checkpoint githubconnector.AssignmentCheckpoint) error {
	return store.store.SetGitHubAssignmentCheckpoint(ctx, scopeKey, sqlitestate.GitHubAssignmentCheckpoint{
		EventCreatedAt: checkpoint.EventCreatedAt,
		EventID:        checkpoint.EventID,
	})
}

func buildDispatcher(config handlerconfig.Config) (egress.Dispatcher, dispatch.EmailSender, error) {
	dispatcher := dispatch.Dispatcher{}
	if config.SMTPHost != "" {
		password := ""
		if config.SMTPUsername != "" {
			password = os.Getenv(config.SMTPPasswordEnv)
			if password == "" {
				return nil, nil, fmt.Errorf("missing SMTP password env var: %s", config.SMTPPasswordEnv)
			}
		}
		sender, err := dispatch.NewSMTPEmailSender(dispatch.SMTPConfig{
			Host:        config.SMTPHost,
			Port:        config.SMTPPort,
			FromAddress: config.SMTPFrom,
			Username:    config.SMTPUsername,
			Password:    password,
			UseSSL:      config.SMTPUseSSL,
			StartTLS:    config.SMTPStartTLS,
			Timeout:     10 * time.Second,
		})
		if err != nil {
			return nil, nil, err
		}
		dispatcher.EmailSender = sender
	}
	if config.TelegramEnabled {
		token := os.Getenv(config.TelegramBotTokenEnv)
		if token == "" {
			return nil, nil, fmt.Errorf("missing Telegram bot token env var: %s", config.TelegramBotTokenEnv)
		}
		sender, err := dispatch.NewTelegramMessageSender(dispatch.TelegramConfig{
			BotToken:              token,
			APIBaseURL:            config.TelegramAPIBaseURL,
			Timeout:               10 * time.Second,
			AttachmentAllowedDirs: config.EgressAttachmentAllowedDirs,
		})
		if err != nil {
			return nil, nil, err
		}
		dispatcher.TelegramSender = sender
	}
	if _, err := ghLookPath("gh"); err == nil {
		dispatcher.GitHubSender = dispatch.NewGitHubIssueCommentSender(nil)
	}
	return dispatcher, dispatcher.EmailSender, nil
}

func resolveErrorEmailRecipient(config handlerconfig.Config) string {
	for _, candidate := range []string{config.SMTPUsername, config.IMAPUsername} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// shouldNotifyEgressDrop avoids turning an executor-correctable attachment
// validation error into an operator email storm. The synchronous /egress
// response preserves the precise reason for the executor to act on.
func shouldNotifyEgressDrop(reasonCode string) bool {
	return !strings.HasPrefix(strings.TrimSpace(reasonCode), "telegram_attachment_")
}

// sendEgressDispatchErrorEmail notifies the operator that a single egress message was
// dropped (bad payload, unknown task, disallowed channel, or dispatch failure). The
// event has already been dropped so the handler keeps running; this is the visibility
// half of that trade-off.
func sendEgressDispatchErrorEmail(ctx context.Context, sender dispatch.EmailSender, recipient string, message contracts.EgressQueueMessage, reasonCode string) {
	if sender == nil || strings.TrimSpace(recipient) == "" {
		return
	}
	sequence := "None"
	if message.Sequence != nil {
		sequence = strconv.Itoa(*message.Sequence)
	}
	subject := "Chatting handler egress message dropped: " + reasonCode
	body := strings.Join([]string{
		"Message-handler dropped an egress message.",
		"task_id: " + message.TaskID,
		"envelope_id: " + message.EnvelopeID,
		"trace_id: " + message.TraceID,
		"event_id: " + message.EventID,
		"sequence: " + sequence,
		"event_kind: " + message.EventKind,
		"channel: " + message.Message.Channel,
		"target: " + message.Message.Target,
		"reason_code: " + reasonCode,
	}, "\n")
	if err := sender.Send(ctx, recipient, body, &subject); err != nil {
		log.Printf("egress_dispatch_error_email_failed task_id=%s event_id=%s reason=%s recipient=%s err=%v",
			message.TaskID, message.EventID, reasonCode, recipient, err)
	}
}

type closer interface {
	Close() error
}

type closingRunner struct {
	runner   runner
	personal *personalinbox.Service
	closers  []closer
}

func (runner *closingRunner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopped := make(chan struct{})
	if runner.personal != nil {
		go func() { defer close(stopped); runner.personal.Run(ctx) }()
	} else {
		close(stopped)
	}
	defer func() {
		cancel()
		<-stopped
		for _, closer := range runner.closers {
			_ = closer.Close()
		}
	}()
	return runner.runner.Run(ctx)
}
