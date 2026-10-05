package config

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

type ConfigHandler struct {
	manager *config.Manager
}

func NewConfigHandler(manager *config.Manager) *ConfigHandler {
	return &ConfigHandler{manager: manager}
}

func (handler *ConfigHandler) GetConfig(
	w http.ResponseWriter,
	r *http.Request,
) {
	configuration := handler.manager.GetConfiguration()

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(configuration); err != nil {
		slog.Error("failed to write config API response", "error", err)
		return
	}
}
