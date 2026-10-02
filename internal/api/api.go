package api

import (
	"net/http"

	"entropicworks.com/kafka-connector-restarter/internal/api/handlers"
)

type ApiEngine struct {
	mux *http.ServeMux
}

func (api *ApiEngine) StartApi() error {
	api.mux = http.NewServeMux()
	api.mux.HandleFunc("GET /{$}", handlers.HomeHandler)
	server := &http.Server{
		Addr:    ":8080",
		Handler: api.mux,
	}

	return server.ListenAndServe()
}
