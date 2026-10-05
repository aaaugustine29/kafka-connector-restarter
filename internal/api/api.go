package api

import (
	"net/http"

	configApi "entropicworks.com/kafka-connector-restarter/internal/api/config"
	"entropicworks.com/kafka-connector-restarter/internal/api/home"
	"entropicworks.com/kafka-connector-restarter/internal/config"
)

type ApiEngine struct {
	mux *http.ServeMux
}

type ApiComponents struct {
	ConfigManager *config.Manager
}

func (api *ApiEngine) StartApi(components ApiComponents) error {
	configHandler := configApi.NewConfigHandler(components.ConfigManager)

	api.mux = http.NewServeMux()
	api.mux.HandleFunc("GET /{$}", home.HomeHandler)
	api.mux.HandleFunc("GET /config", configHandler.GetConfig)
	server := &http.Server{
		Addr:    ":8080",
		Handler: api.mux,
	}

	return server.ListenAndServe()
}
