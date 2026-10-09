package config

import (
	"fmt"
	"strings"
)

// ValidateConfiguration checks the same invariants for startup and runtime updates.
func ValidateConfiguration(config ApplicationConfiguration) error {
	pollingConfig := config.PollingBehavior
	if pollingConfig.Interval <= 0 {
		return fmt.Errorf("pollingBehavior.interval: polling interval must be greater than zero")
	}

	backoffConfig := pollingConfig.Backoff
	if backoffConfig.BaseDelay <= 0 {
		return fmt.Errorf("pollingBehavior.backoff.baseDelay: backoff base delay must be greater than zero")
	}
	if backoffConfig.MaxDelay < backoffConfig.BaseDelay {
		return fmt.Errorf("pollingBehavior.backoff.maxDelay: backoff maximum delay must be at least the base delay")
	}

	if config.CommunicationConfig.RequestTimeout <= 0 {
		return fmt.Errorf("communicationConfig.requestTimeout: HTTP request timeout must be greater than zero")
	}

	apiAuth := config.APIConfig.AuthConfig
	if apiAuth.Enabled {
		if strings.TrimSpace(apiAuth.Username) == "" || strings.Contains(apiAuth.Username, ":") {
			return fmt.Errorf("apiConfig.authConfig.username: API Basic Auth requires a nonblank username without a colon")
		}
		if apiAuth.Password == "" {
			return fmt.Errorf("apiConfig.authConfig.password: API Basic Auth requires a password")
		}
	}

	return nil
}
