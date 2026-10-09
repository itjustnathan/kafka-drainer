package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n\n", err)
		fmt.Println("Usage example:")
		fmt.Println("  ./kafka-drainer -brokers 10.0.0.1:9092,10.0.0.2:9092 -group my-group -topics topic_c,topic_d")
		fmt.Println("\nTo see all flags: ./kafka-drainer -help")
		os.Exit(1)
	}

	cfg.PrintSummary()

	// Setup graceful shutdown on SIGINT / SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		fmt.Printf("\n[SYSTEM] Received signal %v. Initiating graceful shutdown and offset flush...\n", sig)
		cancel()
	}()

	var runErr error
	if cfg.Action == "fast-forward" {
		runErr = RunFastForward(ctx, cfg)
	} else {
		runErr = RunDrain(ctx, cfg)
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "\n[FATAL] Execution failed: %v\n", runErr)
		os.Exit(2)
	}

	fmt.Println("[SYSTEM] Kafka drainer exited cleanly.")
}
