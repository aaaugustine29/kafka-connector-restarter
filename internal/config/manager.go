package config

import (
	"fmt"
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
