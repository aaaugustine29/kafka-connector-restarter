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
	"entropicworks.com/kafka-connector-restarter/internal/connectcluster"
	"entropicworks.com/kafka-connector-restarter/internal/logging"
	"entropicworks.com/kafka-connector-restarter/internal/poll"
)

func main() {
	configPath := flag.String("config", "", "path to optional application YAML configuration; omitted uses defaults")
	secretPath := flag.String("secret-config", "", "path to an optional Secret YAML overlay")
	clustersPath := flag.String("connect-clusters", "", "path to optional named Connect cluster configurations; omitted uses localhost:8083")
	clustersSecretPath := flag.String("connect-clusters-secret-config", "", "path to an optional Connect cluster Secret YAML overlay")
	flag.Parse()
	if flag.NArg() != 0 {
		slog.Error("unexpected positional arguments; use configuration file flags")
		os.Exit(1)
	}
	startupConfig, err := config.LoadFiles(*configPath, *secretPath)
	if err != nil {
		slog.Error("configuration loading failed", "error", err)
		os.Exit(1)
	}
	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(startupConfig.LoggingConfig.Level)
	logging.Configure(loggingLevel)

	clusters, err := connectcluster.LoadFiles(*clustersPath, *clustersSecretPath)
	if err != nil {
		slog.Error("Connect cluster configuration loading failed", "error", err)
		os.Exit(1)
	}
	clusterSource := *clustersPath
	if clusterSource == "" {
		clusterSource = "defaults"
	}
	if len(clusters) == 0 {
		slog.Info("no Connect clusters configured; polling disabled", "cluster_config_source", clusterSource)
	} else {
		slog.Info("Connect clusters configured", "cluster_count", len(clusters), "cluster_config_source", clusterSource)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	configManager := config.NewManager(startupConfig)
	configuration := configManager.GetConfiguration()

	apiComponents := api.APIComponents{
		ConfigManager: configManager,
	}
	server := api.NewServer(apiComponents)
	var workers sync.WaitGroup

	workers.Go(func() {
		slog.Info("starting API server", "address", server.Addr, "authentication_enabled", configuration.APIConfig.AuthConfig.Enabled)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("API serving failed; polling will continue", "error", err)
		}
	})

	for name, clusterConfiguration := range clusters {
		poller := poll.NewConnectClusterPoller(name, clusterConfiguration)
		workers.Go(func() {
			poller.Poll(ctx, configManager)
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
