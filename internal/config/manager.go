package config

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type Manager struct {
	mu           sync.RWMutex
	config       Configuration
	changeSignal chan struct{}
}

func NewManager(config Configuration) *Manager {
	return &Manager{
		config:       config,
		changeSignal: make(chan struct{}),
	}
}

func (m *Manager) GetConfiguration() Configuration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config
}

func (m *Manager) ConfigurationSnapshot() (Configuration, <-chan struct{}) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config, m.changeSignal
}

func (m *Manager) UpdateConfiguration(changeConfig func(*Configuration) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := m.config
	if err := changeConfig(&next); err != nil {
		return err
	}
	if err := validateConfiguration(next); err != nil {
		return err
	}
	m.config = next
	close(m.changeSignal)
	m.changeSignal = make(chan struct{})
	return nil
}

func validateConfiguration(config Configuration) error {
	pollingConfig := config.PollingBehavior
	if pollingConfig.Interval <= 0 {
		return fmt.Errorf("polling interval must be greater than zero")
	}

	backoffConfig := pollingConfig.Backoff
	if backoffConfig.BaseDelay <= 0 {
		return fmt.Errorf("backoff base delay must be greater than zero")
	}
	if backoffConfig.MaxDelay < backoffConfig.BaseDelay {
		return fmt.Errorf("backoff maximum delay must be at least the base delay")
	}

	if config.CommunicationConfig.RequestTimeout <= 0 {
		return fmt.Errorf("HTTP request timeout must be greater than zero")
	}

	connectConfig := config.ConnectConfig
	if strings.TrimSpace(connectConfig.Host) == "" {
		return fmt.Errorf("Connect host must not be empty")
	}
	port, err := strconv.Atoi(connectConfig.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("Connect port must be between 1 and 65535")
	}

	if connectConfig.AuthConfig.Enabled {
		if !connectConfig.HTTPS {
			return fmt.Errorf("Basic Auth requires HTTPS")
		}
		if strings.TrimSpace(connectConfig.AuthConfig.Username) == "" ||
			connectConfig.AuthConfig.Password == "" {
			return fmt.Errorf("Basic Auth requires a username and password")
		}
	}

	return nil
}
