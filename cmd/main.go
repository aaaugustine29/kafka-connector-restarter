package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/api"
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
	configuration, configUpdateChannel := configManager.ConfigurationSnapshot()

	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(configuration.LoggingConfig.Level)
	logging.Configure(loggingLevel)

	apiComponents := api.APIComponents{
		ConfigManager: configManager,
	}
	server := api.NewServer(apiComponents)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("API serving failed", "err", err)
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-configUpdateChannel:
				configuration, configUpdateChannel = configManager.ConfigurationSnapshot()
				loggingLevel.Set(configuration.LoggingConfig.Level)
			}
		}
	}()

	poll.Poll(ctx, configManager)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("API shutdown failed", "err", err)
		if err := server.Close(); err != nil {
			slog.Error("API close failed", "err", err)
		}
	}

	slog.Info("Kafka connector restarter stopped")
}
