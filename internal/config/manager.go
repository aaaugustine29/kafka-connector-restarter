package config

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"unicode"
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
func ValidateConfiguration(config Configuration) error {
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

	connectConfig := config.ConnectConfig
	if strings.TrimSpace(connectConfig.Host) == "" {
		return fmt.Errorf("connectConfig.host: Connect host must not be empty")
	}
	if !validConnectHost(connectConfig.Host) {
		return fmt.Errorf("connectConfig.host: Connect host must be a hostname or unbracketed IP address without a scheme, port, path, or whitespace")
	}
	port, err := strconv.Atoi(connectConfig.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("connectConfig.port: Connect port must be between 1 and 65535")
	}

	if connectConfig.AuthConfig.Enabled {
		if !connectConfig.HTTPS {
			return fmt.Errorf("connectConfig.https: Basic Auth requires HTTPS")
		}
		if strings.TrimSpace(connectConfig.AuthConfig.Username) == "" {
			return fmt.Errorf("connectConfig.authConfig.username: Basic Auth requires a username and password")
		}
		if connectConfig.AuthConfig.Password == "" {
			return fmt.Errorf("connectConfig.authConfig.password: Basic Auth requires a username and password")
		}
	}

	return nil
}

func validConnectHost(host string) bool {
	if strings.ContainsAny(host, "/\\?#@[]") || strings.ContainsFunc(host, func(character rune) bool {
		return unicode.IsSpace(character) || unicode.IsControl(character)
	}) {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}
	if len(strings.TrimSuffix(host, ".")) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
				(character >= '0' && character <= '9') || character == '-') {
				return false
			}
		}
	}
	return true
}
