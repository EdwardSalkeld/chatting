// Command chatting-platform serves the private unified operator interface.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/platform"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "Path to platform JSON config (defaults to loopback ports).")
	showVersion := flag.Bool("version", false, "Print version and exit.")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	config := platform.DefaultConfig()
	if *configPath != "" {
		file, err := os.Open(*configPath)
		if err != nil {
			log.Fatal(err)
		}
		config, err = platform.DecodeConfig(file)
		_ = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	assets, err := platform.Assets()
	if err != nil {
		log.Fatal(err)
	}
	handler, err := platform.NewHandler(config, assets)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{
		Addr: net.JoinHostPort(config.Host, strconv.Itoa(config.Port)), Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("Platform interface: http://%s", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
