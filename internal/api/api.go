package api

import (
	"log/slog"
	"net/http"
	"time"

	configAPI "entropicworks.com/kafka-connector-restarter/internal/api/config"
	"entropicworks.com/kafka-connector-restarter/internal/api/home"
	"entropicworks.com/kafka-connector-restarter/internal/config"
)

type APIComponents struct {
	ConfigManager *config.Manager
}

func NewServer(components APIComponents) *http.Server {
	configHandler := configAPI.NewConfigHandler(components.ConfigManager)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", home.HomeHandler)
	mux.HandleFunc("GET /config", configHandler.GetConfig)
	mux.HandleFunc("PATCH /config", configHandler.PatchConfig)
	return &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
	}
}
