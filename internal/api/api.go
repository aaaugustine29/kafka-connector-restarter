package api

import (
	"net/http"

	configAPI "entropicworks.com/kafka-connector-restarter/internal/api/config"
	"entropicworks.com/kafka-connector-restarter/internal/api/home"
	"entropicworks.com/kafka-connector-restarter/internal/config"
)

type APIEngine struct {
	mux *http.ServeMux
}

type APIComponents struct {
	ConfigManager *config.Manager
}

func (api *APIEngine) StartAPI(components APIComponents) error {
	configHandler := configAPI.NewConfigHandler(components.ConfigManager)

	api.mux = http.NewServeMux()
	api.mux.HandleFunc("GET /{$}", home.HomeHandler)
	api.mux.HandleFunc("GET /config", configHandler.GetConfig)
	server := &http.Server{
		Addr:    ":8080",
		Handler: api.mux,
	}

	return server.ListenAndServe()
}
