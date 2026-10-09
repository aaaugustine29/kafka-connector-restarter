package config

import (
	"fmt"
	"strings"
	"sync"
)

type Manager struct {
	mu           sync.RWMutex
	config       ApplicationConfiguration
	changeSignal chan struct{}
}

func NewManager(config ApplicationConfiguration) *Manager {
	return &Manager{
		config:       config,
		changeSignal: make(chan struct{}),
	}
}

func (m *Manager) GetConfiguration() ApplicationConfiguration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config
}

func (m *Manager) ConfigurationSnapshot() (ApplicationConfiguration, <-chan struct{}) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config, m.changeSignal
}

func (m *Manager) UpdateConfiguration(changeConfig func(*ApplicationConfiguration) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := m.config
	if err := changeConfig(&next); err != nil {
		return err
	}
	if next.APIConfig != m.config.APIConfig {
		return fmt.Errorf("apiConfig: API authentication is startup-only; update the YAML configuration and restart")
	}
	if err := ValidateConfiguration(next); err != nil {
		return err
	}
	if next == m.config {
		return nil
	}
	m.config = next
	close(m.changeSignal)
	m.changeSignal = make(chan struct{})
	return nil
}

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
