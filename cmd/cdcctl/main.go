package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/BABTUNA/minicdc/internal/config"
	"github.com/BABTUNA/minicdc/internal/verify"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cdcctl verify [--timeout 30s]")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "verify":
		fs := flag.NewFlagSet("verify", flag.ExitOnError)
		timeout := fs.Duration("timeout", 30*time.Second, "how long to wait for source and dest to converge")
		fs.Parse(os.Args[2:])

		if err := verify.Run(context.Background(), config.Load(), *timeout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\nusage: cdcctl verify [--timeout 30s]\n", os.Args[1])
		os.Exit(2)
	}
}
