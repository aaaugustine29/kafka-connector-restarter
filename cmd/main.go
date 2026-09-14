package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/poll"
)

func main() {
	config := environment.LoadConfig()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	poll.Poll(ctx, config)
}
