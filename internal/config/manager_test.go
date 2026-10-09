package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManagerUpdateCommitsSingleValue(t *testing.T) {
	manager := NewManager(DefaultConfiguration())

	err := manager.UpdateConfiguration(func(configuration *ApplicationConfiguration) error {
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

func TestValidateAPIAuthentication(t *testing.T) {
	for _, test := range []struct {
		name string
		auth AuthConfiguration
		want string
	}{
		{name: "disabled"},
		{name: "valid", auth: AuthConfiguration{Enabled: true, Username: "api-user", Password: "api-secret"}},
		{name: "missing username", auth: AuthConfiguration{Enabled: true, Password: "api-secret"}, want: "apiConfig.authConfig.username:"},
		{name: "blank username", auth: AuthConfiguration{Enabled: true, Username: " ", Password: "api-secret"}, want: "apiConfig.authConfig.username:"},
		{name: "colon in username", auth: AuthConfiguration{Enabled: true, Username: "api:user", Password: "api-secret"}, want: "apiConfig.authConfig.username:"},
		{name: "missing password", auth: AuthConfiguration{Enabled: true, Username: "api-user"}, want: "apiConfig.authConfig.password:"},
	} {
		t.Run(test.name, func(t *testing.T) {
			configuration := DefaultConfiguration()
			configuration.APIConfig.AuthConfig = test.auth
			err := ValidateConfiguration(configuration)
			if test.want == "" {
				if err != nil {
					t.Fatalf("valid API authentication rejected: %v", err)
				}
			} else if err == nil || !strings.HasPrefix(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
			if err != nil && strings.Contains(err.Error(), "api-secret") {
				t.Fatal("validation error exposed API credentials")
			}
		})
	}
}

func TestManagerRejectsEnablingAPIAuthenticationAtRuntime(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	before, changes := manager.ConfigurationSnapshot()
	err := manager.UpdateConfiguration(func(c *ApplicationConfiguration) error {
		c.APIConfig.AuthConfig = AuthConfiguration{Enabled: true, Username: "user", Password: "secret"}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "startup-only") {
		t.Fatalf("error = %v, want startup-only error", err)
	}
	if manager.GetConfiguration() != before {
		t.Fatal("API auth update changed configuration")
	}
	select {
	case <-changes:
		t.Fatal("rejected API auth update notified workers")
	default:
	}
}

func TestManagerUpdateRejectsChangeAtomically(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	before, changes := manager.ConfigurationSnapshot()

	err := manager.UpdateConfiguration(func(configuration *ApplicationConfiguration) error {
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

	if err := manager.UpdateConfiguration(func(configuration *ApplicationConfiguration) error {
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

	if err := manager.UpdateConfiguration(func(configuration *ApplicationConfiguration) error {
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
		change    func(*ApplicationConfiguration)
		wantError string
	}{
		{
			name:      "zero polling interval",
			change:    func(config *ApplicationConfiguration) { config.PollingBehavior.Interval = 0 },
			wantError: "polling interval",
		},
		{
			name:      "zero backoff base delay",
			change:    func(config *ApplicationConfiguration) { config.PollingBehavior.Backoff.BaseDelay = 0 },
			wantError: "backoff base delay",
		},
		{
			name: "backoff maximum below base",
			change: func(config *ApplicationConfiguration) {
				config.PollingBehavior.Backoff.MaxDelay = config.PollingBehavior.Backoff.BaseDelay - Duration(time.Millisecond)
			},
			wantError: "backoff maximum delay",
		},
		{
			name:      "zero request timeout",
			change:    func(config *ApplicationConfiguration) { config.CommunicationConfig.RequestTimeout = 0 },
			wantError: "HTTP request timeout",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(DefaultConfiguration())
			before, changes := manager.ConfigurationSnapshot()

			err := manager.UpdateConfiguration(func(config *ApplicationConfiguration) error {
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
