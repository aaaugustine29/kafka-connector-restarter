package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"entropicworks.com/kafka-connector-restarter/internal/config"
	"entropicworks.com/kafka-connector-restarter/internal/environment"
	"entropicworks.com/kafka-connector-restarter/internal/logging"
	"entropicworks.com/kafka-connector-restarter/internal/poll"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	environmentConfig := environment.LoadConfig()
	configManager := config.NewManager(environmentConfig)
	config, configUpdateChannel := configManager.ConfigurationSnapshot()

	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(config.LoggingConfig.Level)
	logging.Configure(loggingLevel)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-configUpdateChannel:
				config, configUpdateChannel = configManager.ConfigurationSnapshot()
				loggingLevel.Set(config.LoggingConfig.Level)
			}
		}
	}()

	poll.Poll(ctx, configManager)
	slog.Info("Kafka connector restarter stopped")
}
