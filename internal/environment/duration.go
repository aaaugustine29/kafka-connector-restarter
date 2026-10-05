package environment

import (
	"log/slog"
	"os"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

func loadDuration(setting string, defaultValue config.Duration) config.Duration {
	value := os.Getenv(setting)
	if value == "" {
		slog.Debug("duration setting is not configured; using default", "setting", setting, "value", defaultValue)
		return defaultValue
	}

	duration, err := config.ParseDuration(value)
	if err != nil || duration <= 0 {
		slog.Warn("invalid duration setting; using default", "setting", setting, "value", value, "default", defaultValue)
		return defaultValue
	}

	slog.Debug("duration setting configured", "setting", setting, "value", duration)
	return duration
}
