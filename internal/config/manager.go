package config

import "sync"

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
	m.config = next
	close(m.changeSignal)
	m.changeSignal = make(chan struct{})
	return nil
}
