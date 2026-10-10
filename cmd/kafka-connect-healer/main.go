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

	"github.com/Entropic-Works/kafka-connect-healer/internal/api"
	"github.com/Entropic-Works/kafka-connect-healer/internal/config"
	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
	"github.com/Entropic-Works/kafka-connect-healer/internal/logging"
	"github.com/Entropic-Works/kafka-connect-healer/internal/poll"
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
	startupConfiguration, err := config.LoadFiles(*configPath, *secretPath)
	if err != nil {
		slog.Error("configuration loading failed", "error", err)
		os.Exit(1)
	}
	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(startupConfiguration.LoggingConfig.Level)
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

	configurationManager := config.NewManager(startupConfiguration)
	configuration := configurationManager.GetConfiguration()
	clusterManager, err := poll.NewClusterManager(ctx, configurationManager, clusters)
	if err != nil {
		slog.Error("Connect cluster manager startup failed", "error", err)
		os.Exit(1)
	}

	apiComponents := api.Components{
		ConfigurationManager: configurationManager,
		ClusterManager:       clusterManager,
	}
	server := api.NewServer(apiComponents)
	apiLogger := slog.With("component", "api", "address", server.Addr)
	var workers sync.WaitGroup

	workers.Go(func() {
		apiLogger.Info("starting API server", "authentication_enabled", configuration.APIConfig.AuthConfig.Enabled)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			apiLogger.Error("API serving failed; polling will continue", "error", err)
		} else if errors.Is(err, http.ErrServerClosed) {
			apiLogger.Debug("API listener stopped")
		}
	})

	workers.Go(func() {
		configuration, configurationUpdateChannel := configurationManager.ConfigurationSnapshot()
		loggingLevel.Set(configuration.LoggingConfig.Level)

		for {
			select {
			case <-ctx.Done():
				return
			case <-configurationUpdateChannel:
				configuration, configurationUpdateChannel = configurationManager.ConfigurationSnapshot()
				loggingLevel.Set(configuration.LoggingConfig.Level)
			}
		}
	})

	<-ctx.Done()
	slog.Info("Kafka Connect Healer stopping")
	clusterManager.Close()

	const shutdownTimeout = 5 * time.Second
	shutdownStarted := time.Now()
	apiLogger.Info("API graceful shutdown started", "timeout", shutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		apiLogger.Warn("API graceful shutdown failed; closing connections", "duration", time.Since(shutdownStarted), "error", err)
		if err := server.Close(); err != nil {
			apiLogger.Error("API forced shutdown failed", "duration", time.Since(shutdownStarted), "error", err)
		} else {
			apiLogger.Info("API connections forcibly closed", "duration", time.Since(shutdownStarted))
		}
	} else {
		apiLogger.Info("API graceful shutdown completed", "duration", time.Since(shutdownStarted))
	}

	workers.Wait()
	slog.Info("Kafka Connect Healer stopped")
}
