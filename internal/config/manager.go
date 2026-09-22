package config

import "sync"

type Manager struct {
	mu     sync.RWMutex
	config Configuration
}

func NewManager(config Configuration) *Manager {
	return &Manager{config: config}
}

func (m *Manager) Get() Configuration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.config
}

func (m *Manager) Update(update func(*Configuration) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := m.config
	if err := update(&next); err != nil {
		return err
	}

	m.config = next
	return nil
}
