package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/auxiliary"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/egress"
	handlerruntime "github.com/EdwardSalkeld/chatting/go/handler/internal/runtime"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const version = "go-handler-bootstrap"

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
	engine, err := egress.New(
		egress.NewSQLiteState(store),
		unsupportedDispatcher{},
		egress.WithAllowedChannels(config.AllowedEgressChannels),
	)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	connectors := []handlerruntime.Connector{heartbeat.New(nil)}
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
	runner, err := handlerruntime.NewRunner(config, adapter, egressHandlerFunc(func(ctx context.Context, raw []byte) error {
		_, err := engine.HandleRaw(ctx, raw)
		return err
	}), handlerruntime.WithIngress(store, connectors...))
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return &closingRunner{runner: runner, closer: store}, nil
}

type egressHandlerFunc func(context.Context, []byte) error

func (fn egressHandlerFunc) HandleRaw(ctx context.Context, raw []byte) error {
	return fn(ctx, raw)
}

type unsupportedDispatcher struct{}

func (unsupportedDispatcher) Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error) {
	if heartbeat.IsLogPong(message, envelope) {
		return &message, nil
	}
	return nil, errors.New("dispatch is not implemented in the Go handler yet")
}

type closer interface {
	Close() error
}

type closingRunner struct {
	runner runner
	closer closer
}

func (runner *closingRunner) Run(ctx context.Context) error {
	defer runner.closer.Close()
	return runner.runner.Run(ctx)
}
