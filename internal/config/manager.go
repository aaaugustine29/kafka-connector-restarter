package config

import "sync"

type Manager struct {
	mu      sync.RWMutex
	config  Configuration
	changed chan struct{}
}

func NewManager(config Configuration) *Manager {
	return &Manager{
		config:  config,
		changed: make(chan struct{}),
	}
}

func (m *Manager) Get() Configuration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config
}

func (m *Manager) Snapshot() (Configuration, <-chan struct{}) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config, m.changed
}

func (m *Manager) Update(changeConfig func(*Configuration) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := m.config
	if err := changeConfig(&next); err != nil {
		return err
	}
	m.config = next
	close(m.changed)
	m.changed = make(chan struct{})
	return nil
}
