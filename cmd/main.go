package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/api"
	"entropicworks.com/kafka-connector-restarter/internal/config"
	"entropicworks.com/kafka-connector-restarter/internal/logging"
	"entropicworks.com/kafka-connector-restarter/internal/poll"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the base YAML configuration")
	secretPath := flag.String("secret-config", "", "path to an optional Secret YAML overlay")
	flag.Parse()
	if flag.NArg() != 0 {
		slog.Error("unexpected positional arguments; use --config and --secret-config")
		os.Exit(1)
	}
	startupConfig, err := config.LoadFiles(*configPath, *secretPath)
	if err != nil {
		slog.Error("configuration loading failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	configManager := config.NewManager(startupConfig)
	configuration := configManager.GetConfiguration()

	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(configuration.LoggingConfig.Level)
	logging.Configure(loggingLevel)

	apiComponents := api.APIComponents{
		ConfigManager: configManager,
	}
	server := api.NewServer(apiComponents)
	var workers sync.WaitGroup

	workers.Go(func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("API serving failed; polling will continue", "error", err)
		}
	})

	for name, _ := range configuration.ConnectConfigs {
		workers.Go(func() {
			poll.Poll(ctx, configManager, name)
		})
	}

	workers.Go(func() {
		configuration, configUpdateChannel := configManager.ConfigurationSnapshot()
		loggingLevel.Set(configuration.LoggingConfig.Level)

		for {
			select {
			case <-ctx.Done():
				return
			case <-configUpdateChannel:
				configuration, configUpdateChannel = configManager.ConfigurationSnapshot()
				loggingLevel.Set(configuration.LoggingConfig.Level)
			}
		}
	})

	<-ctx.Done()
	slog.Info("Kafka connector restarter stopping")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Warn("API graceful shutdown failed; closing connections", "error", err)
		if err := server.Close(); err != nil {
			slog.Error("API close failed", "error", err)
		}
	}

	workers.Wait()
	slog.Info("Kafka connector restarter stopped")
}
