package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManagerUpdateCommitsSingleValue(t *testing.T) {
	manager := NewManager(DefaultConfiguration())

	err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = Duration(2 * time.Second)
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}
	if got := manager.GetConfiguration().CommunicationConfig.RequestTimeout; got != Duration(2*time.Second) {
		t.Fatalf("request timeout = %v, want %v", got, 2*time.Second)
	}
}

func TestManagerUpdateRejectsChangeAtomically(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	before, changes := manager.ConfigurationSnapshot()

	err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = Duration(time.Second)
		return errors.New("reject update")
	})
	if err == nil {
		t.Fatal("UpdateConfiguration() error = nil, want error")
	}
	if got := manager.GetConfiguration(); got != before {
		t.Fatalf("configuration after rejected update = %#v, want %#v", got, before)
	}
	select {
	case <-changes:
		t.Fatal("rejected update sent a change notification")
	default:
	}
}

func TestManagerUpdateBroadcastsLatestConfig(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	_, firstListener := manager.ConfigurationSnapshot()
	_, secondListener := manager.ConfigurationSnapshot()

	if err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = Duration(time.Second)
		return nil
	}); err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}

	for name, changes := range map[string]<-chan struct{}{
		"first listener":  firstListener,
		"second listener": secondListener,
	} {
		select {
		case _, open := <-changes:
			if open {
				t.Fatalf("%s received a value instead of a closed channel", name)
			}
		default:
			t.Fatalf("%s did not observe the update", name)
		}
	}

	current, nextChanges := manager.ConfigurationSnapshot()
	if got := current.CommunicationConfig.RequestTimeout; got != Duration(time.Second) {
		t.Fatalf("request timeout after notification = %v, want %v", got, time.Second)
	}
	select {
	case <-nextChanges:
		t.Fatal("new change channel is already closed")
	default:
	}

	if err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = Duration(2 * time.Second)
		return nil
	}); err != nil {
		t.Fatalf("second UpdateConfiguration() error = %v", err)
	}
	select {
	case <-nextChanges:
	default:
		t.Fatal("second update did not close the new change channel")
	}
	if got, _ := manager.ConfigurationSnapshot(); got.CommunicationConfig.RequestTimeout != Duration(2*time.Second) {
		t.Fatalf("request timeout after second notification = %v, want %v", got.CommunicationConfig.RequestTimeout, 2*time.Second)
	}
}

func TestManagerUpdateRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		change    func(*Configuration)
		wantError string
	}{
		{
			name:      "zero polling interval",
			change:    func(config *Configuration) { config.PollingBehavior.Interval = 0 },
			wantError: "polling interval",
		},
		{
			name:      "zero backoff base delay",
			change:    func(config *Configuration) { config.PollingBehavior.Backoff.BaseDelay = 0 },
			wantError: "backoff base delay",
		},
		{
			name: "backoff maximum below base",
			change: func(config *Configuration) {
				config.PollingBehavior.Backoff.MaxDelay = config.PollingBehavior.Backoff.BaseDelay - Duration(time.Millisecond)
			},
			wantError: "backoff maximum delay",
		},
		{
			name:      "zero request timeout",
			change:    func(config *Configuration) { config.CommunicationConfig.RequestTimeout = 0 },
			wantError: "HTTP request timeout",
		},
		{
			name:      "empty host",
			change:    func(config *Configuration) { config.ConnectConfig.Host = " " },
			wantError: "Connect host",
		},
		{
			name:      "invalid port",
			change:    func(config *Configuration) { config.ConnectConfig.Port = "not-a-port" },
			wantError: "Connect port",
		},
		{
			name:      "port out of range",
			change:    func(config *Configuration) { config.ConnectConfig.Port = "65536" },
			wantError: "Connect port",
		},
		{
			name: "basic auth without HTTPS",
			change: func(config *Configuration) {
				config.ConnectConfig.AuthConfig.Enabled = true
			},
			wantError: "Basic Auth requires HTTPS",
		},
		{
			name: "basic auth without username",
			change: func(config *Configuration) {
				config.ConnectConfig.HTTPS = true
				config.ConnectConfig.AuthConfig.Enabled = true
				config.ConnectConfig.AuthConfig.Password = "password"
			},
			wantError: "Basic Auth requires a username and password",
		},
		{
			name: "basic auth without password",
			change: func(config *Configuration) {
				config.ConnectConfig.HTTPS = true
				config.ConnectConfig.AuthConfig.Enabled = true
				config.ConnectConfig.AuthConfig.Username = "user"
			},
			wantError: "Basic Auth requires a username and password",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(DefaultConfiguration())
			before, changes := manager.ConfigurationSnapshot()

			err := manager.UpdateConfiguration(func(config *Configuration) error {
				test.change(config)
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("UpdateConfiguration() error = %v, want %q", err, test.wantError)
			}
			if got := manager.GetConfiguration(); got != before {
				t.Fatalf("configuration after invalid update = %#v, want %#v", got, before)
			}
			select {
			case <-changes:
				t.Fatal("invalid update sent a change notification")
			default:
			}
		})
	}
}

func TestManagerUpdateAcceptsValidBasicAuth(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	err := manager.UpdateConfiguration(func(config *Configuration) error {
		config.ConnectConfig.HTTPS = true
		config.ConnectConfig.AuthConfig = AuthConfiguration{
			Enabled:  true,
			Username: "user",
			Password: "password",
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}
	if !manager.GetConfiguration().ConnectConfig.AuthConfig.Enabled {
		t.Fatal("valid Basic Auth configuration was not committed")
	}
}
