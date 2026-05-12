package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "go-handler-bootstrap"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	fmt.Fprintln(os.Stderr, "chatting Go handler bootstrap: runtime not implemented yet")
	os.Exit(2)
}
