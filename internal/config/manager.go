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
	if next.APIConfig != m.config.APIConfig {
		return fmt.Errorf("apiConfig: API authentication is startup-only; update the YAML configuration and restart")
	}
	if err := ValidateConfiguration(next); err != nil {
		return err
	}
	if ConfigurationsAreEqual(next, m.config) {
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

	apiAuth := config.APIConfig.AuthConfig
	if apiAuth.Enabled {
		if strings.TrimSpace(apiAuth.Username) == "" || strings.Contains(apiAuth.Username, ":") {
			return fmt.Errorf("apiConfig.authConfig.username: API Basic Auth requires a nonblank username without a colon")
		}
		if apiAuth.Password == "" {
			return fmt.Errorf("apiConfig.authConfig.password: API Basic Auth requires a password")
		}
	}

	connectConfigs := config.ConnectConfigs
	for name, config := range connectConfigs {
		if strings.TrimSpace(config.Host) == "" {
			return fmt.Errorf("For connect", name, "connectConfig.host: Connect host must not be empty")
		}
		if !validConnectHost(config.Host) {
			return fmt.Errorf("For connect", name, "connectConfig.host: Connect host must be a hostname or unbracketed IP address without a scheme, port, path, or whitespace")
		}
		port, err := strconv.Atoi(config.Port)
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("For connect", name, "connectConfig.port: Connect port must be between 1 and 65535")
		}

		if config.AuthConfig.Enabled {
			if !config.HTTPS {
				return fmt.Errorf("For connect", name, "connectConfig.https: Basic Auth requires HTTPS")
			}
			if strings.TrimSpace(config.AuthConfig.Username) == "" {
				return fmt.Errorf("For connect", name, "connectConfig.authConfig.username: Basic Auth requires a username and password")
			}
			if config.AuthConfig.Password == "" {
				return fmt.Errorf("For connect", name, "connectConfig.authConfig.password: Basic Auth requires a username and password")
			}
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
