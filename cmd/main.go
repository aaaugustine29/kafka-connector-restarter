package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/logging"
	"entropicworks.com/kafka-connector-restarter/internal/poll"
)

func main() {
	config := environment.LoadConfig()
	logging.Configure(config.LoggingConfig.Level)
	slog.Info("starting Kafka connector restarter", "connect_url", config.ConnectConfig.URL, "poll_interval", config.PollingBehavior.Interval)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	poll.Poll(ctx, config)
	slog.Info("Kafka connector restarter stopped")
}
