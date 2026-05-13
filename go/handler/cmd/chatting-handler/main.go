package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
)

const version = "go-handler-bootstrap"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, currentEnv()))
}

func run(args []string, stdout io.Writer, stderr io.Writer, environ map[string]string) int {
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
	_ = config

	fmt.Fprintln(stderr, "chatting Go handler bootstrap: runtime not implemented yet")
	return 2
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
