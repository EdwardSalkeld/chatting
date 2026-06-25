package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/auxiliary"
	githubconnector "github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/github"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/imap"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/schedule"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/telegram"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/dispatch"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/egress"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
	handlerruntime "github.com/EdwardSalkeld/chatting/go/handler/internal/runtime"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const version = "go-handler-bootstrap"

var ghLookPath = exec.LookPath

type runner interface {
	Run(ctx context.Context) error
}

type runnerFactory func(ctx context.Context, config handlerconfig.Config) (runner, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runWithFactory(ctx, os.Args[1:], os.Stdout, os.Stderr, currentEnv(), newRuntimeRunner))
}

func run(args []string, stdout io.Writer, stderr io.Writer, environ map[string]string) int {
	return runWithFactory(context.Background(), args, stdout, stderr, environ, newRuntimeRunner)
}

func runWithFactory(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, environ map[string]string, newRunner runnerFactory) int {
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

	config, err := handlerconfig.LoadFromEnv(*configPath, environ)
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

func currentEnv() map[string]string {
	result := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			result[key] = value
		}
	}
	return result
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
			eligibleAfter := time.Now().UTC().Add(time.Duration(config.TelegramAttachmentCleanupGraceSeconds) * time.Second)
			_, err := store.MarkTelegramTaskAttachmentsEligible(ctx, message.TaskID, eligibleAfter)
			return err
		}),
		egress.WithDispatchFailureHook(func(ctx context.Context, message contracts.EgressQueueMessage, reasonCode string) {
			log.Printf("egress_dispatch_failed task_id=%s event_id=%s channel=%s target=%s reason=%s",
				message.TaskID, message.EventID, message.Message.Channel, message.Message.Target, reasonCode)
			sendEgressDispatchErrorEmail(ctx, emailSender, errorEmailRecipient, message, reasonCode)
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
	connectors := []handlerruntime.Connector{heartbeat.New(nil)}
	if config.ScheduleFile != "" {
		jobs, err := schedule.LoadJobs(config.ScheduleFile)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		connector, err := schedule.New(
			jobs,
			config.GlobalPromptContext,
			config.CronPromptContext,
			nil,
		)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		connectors = append(connectors, connector)
	}
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
			PromptContext: contracts.PromptContext{
				GlobalInstructions:       config.GlobalPromptContext,
				ReplyChannelInstructions: config.EmailPromptContext,
			},
			UseSSL: config.IMAPUseSSL,
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
			PromptContext: contracts.PromptContext{
				GlobalInstructions:       config.GlobalPromptContext,
				ReplyChannelInstructions: config.TelegramPromptContext,
			},
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
		if strings.TrimSpace(assigneeLogin) == "" {
			assigneeLogin, err = githubconnector.FetchAuthenticatedViewerLogin(ctx, nil)
			if err != nil {
				_ = store.Close()
				return nil, fmt.Errorf("github_assignee_login is required when github_repositories is configured unless it can be derived from authenticated gh user")
			}
		}
		checkpointStore := githubCheckpointStore{store: store}
		assignmentConnector, err := githubconnector.NewIssueAssignmentConnector(githubconnector.IssueAssignmentConfig{
			RepositoryPatterns: config.GitHubRepositories,
			AssigneeLogin:      assigneeLogin,
			ContextRefs:        config.GitHubContextRefs,
			CheckpointStore:    checkpointStore,
			MaxIssues:          config.GitHubMaxIssues,
			MaxTimelineEvents:  config.GitHubMaxTimelineEvents,
		})
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		reviewConnector, err := githubconnector.NewPullRequestReviewConnector(githubconnector.PullRequestReviewConfig{
			RepositoryPatterns: config.GitHubRepositories,
			AuthorLogin:        assigneeLogin,
			ContextRefs:        config.GitHubContextRefs,
			CheckpointStore:    checkpointStore,
			MaxPullRequests:    config.GitHubMaxIssues,
			MaxReviews:         config.GitHubMaxTimelineEvents,
		})
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		connectors = append(connectors, assignmentConnector, reviewConnector)
	}
	auxiliaryAdapter := adapter
	if config.AuxiliaryIngressEnabled {
		auxiliaryAddress := config.AuxiliaryIngressBBMBAddress
		if auxiliaryAddress == "" {
			auxiliaryAddress = config.BBMBAddress
		}
		if auxiliaryAddress != config.BBMBAddress {
			auxiliaryAdapter, err = bbmb.NewAdapter(auxiliaryAddress)
			if err != nil {
				_ = store.Close()
				return nil, err
			}
		}
		prompt := contracts.PromptContext{GlobalInstructions: config.GlobalPromptContext}
		for _, queueName := range config.AuxiliaryIngressQueues {
			if err := auxiliaryAdapter.EnsureQueue(ctx, queueName); err != nil {
				_ = store.Close()
				return nil, err
			}
			connector, err := auxiliary.New(
				auxiliaryAdapter,
				queueName,
				config.AuxiliaryIngressContextRefs,
				prompt,
			)
			if err != nil {
				_ = store.Close()
				return nil, err
			}
			connectors = append(connectors, connector)
		}
	}
	metricRecorder := metrics.New(time.Time{}, nil)
	metricsServer, err := metrics.StartServer(metricRecorder, config.MetricsHost, config.MetricsPort)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	runner, err := handlerruntime.NewRunner(config, adapter, egressHandlerFunc(func(ctx context.Context, raw []byte) error {
		result, err := engine.HandleRaw(ctx, raw)
		if err == nil {
			metricRecorder.RecordEgressResult(result.Status, result.Reason)
		}
		return err
	}), handlerruntime.WithIngress(handlerIngressState{store: store}, connectors...), handlerruntime.WithMetrics(metricRecorder))
	if err != nil {
		_ = metricsServer.Close()
		_ = store.Close()
		return nil, err
	}
	return &closingRunner{runner: runner, closers: []closer{metricsServer, store}}, nil
}

type githubCheckpointStore struct {
	store *sqlitestate.Store
}

type handlerIngressState struct {
	store *sqlitestate.Store
}

func (state handlerIngressState) Seen(ctx context.Context, source string, dedupeKey string) (bool, error) {
	return state.store.Seen(ctx, source, dedupeKey)
}

func (state handlerIngressState) MarkSeen(ctx context.Context, source string, dedupeKey string) error {
	return state.store.MarkSeen(ctx, source, dedupeKey)
}

func (state handlerIngressState) RecordTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) error {
	return state.store.RecordTask(ctx, taskMessage)
}

func (state handlerIngressState) RecordTelegramTaskAttachments(ctx context.Context, taskMessage contracts.TaskQueueMessage, attachmentRootDir string) (int, error) {
	return state.store.RecordTelegramTaskAttachments(ctx, taskMessage, attachmentRootDir)
}

func (state handlerIngressState) CleanupTelegramAttachmentsForRuntime(ctx context.Context, attachmentRootDir string, notAfter time.Time, maxAgeCutoff time.Time) error {
	return state.store.CleanupTelegramAttachmentsForRuntime(ctx, attachmentRootDir, notAfter, maxAgeCutoff)
}

func (state handlerIngressState) AppendConversationTurn(ctx context.Context, channel string, target string, role string, content string, runID string) error {
	return state.store.AppendConversationTurn(ctx, channel, target, role, content, runID)
}

func (state handlerIngressState) ListRecentConversationTurns(ctx context.Context, channel string, target string, limit int) ([]handlerruntime.ConversationTurn, error) {
	turns, err := state.store.ListRecentConversationTurns(ctx, channel, target, limit)
	if err != nil {
		return nil, err
	}
	result := make([]handlerruntime.ConversationTurn, 0, len(turns))
	for _, turn := range turns {
		result = append(result, handlerruntime.ConversationTurn{
			Role:    turn.Role,
			Content: turn.Content,
		})
	}
	return result, nil
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
			BotToken:   token,
			APIBaseURL: config.TelegramAPIBaseURL,
			Timeout:    10 * time.Second,
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
	for _, candidate := range []string{config.ErrorEmailTo, config.SMTPUsername, config.IMAPUsername} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// sendEgressDispatchErrorEmail notifies the operator that a single egress event could
// not be delivered. The event has already been dropped so the handler keeps running;
// this is the visibility half of that trade-off.
func sendEgressDispatchErrorEmail(ctx context.Context, sender dispatch.EmailSender, recipient string, message contracts.EgressQueueMessage, reasonCode string) {
	if sender == nil || strings.TrimSpace(recipient) == "" {
		return
	}
	sequence := "None"
	if message.Sequence != nil {
		sequence = strconv.Itoa(*message.Sequence)
	}
	subject := "Chatting handler dispatch error: " + reasonCode
	body := strings.Join([]string{
		"Message-handler egress dispatch failed.",
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

type egressHandlerFunc func(context.Context, []byte) error

func (fn egressHandlerFunc) HandleRaw(ctx context.Context, raw []byte) error {
	return fn(ctx, raw)
}

type unsupportedDispatcher struct{}

func (unsupportedDispatcher) Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error) {
	return dispatch.Dispatcher{}.Dispatch(ctx, message, envelope)
}

type closer interface {
	Close() error
}

type closingRunner struct {
	runner  runner
	closers []closer
}

func (runner *closingRunner) Run(ctx context.Context) error {
	defer func() {
		for _, closer := range runner.closers {
			_ = closer.Close()
		}
	}()
	return runner.runner.Run(ctx)
}
