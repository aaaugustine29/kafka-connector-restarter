package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFilesDefaultsAndPartialOverlay(t *testing.T) {
	base := writeConfigFile(t, `
connectConfig:
  host: connect.kafka.svc
  https: true
  authConfig:
    enabled: true
    username: base-user
pollingBehavior:
  interval: 1m30s
  restartFailedTasks: false
  backoff:
    baseDelay: 30s
loggingConfig:
  level: DEBUG
`)
	overlay := writeConfigFile(t, `
connectConfig:
  authConfig:
    username: secret-user
    password: "test-password"
pollingBehavior:
  interval: 500ms
  backoff:
    enabled: false
    exponential: false
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	want := DefaultConfiguration()
	want.ConnectConfig.Host = "connect.kafka.svc"
	want.ConnectConfig.HTTPS = true
	want.ConnectConfig.AuthConfig = AuthConfiguration{Enabled: true, Username: "secret-user", Password: "test-password"}
	want.PollingBehavior.Interval = Duration(500 * time.Millisecond)
	want.PollingBehavior.RestartFailedTasks = false
	want.PollingBehavior.Backoff.BaseDelay = Duration(30 * time.Second)
	want.PollingBehavior.Backoff.Enabled = false
	want.PollingBehavior.Backoff.Exponential = false
	want.LoggingConfig.Level = slog.LevelDebug
	if got != want {
		t.Fatal("merged configuration did not preserve defaults, base values, and explicit overlay values")
	}
}

func TestLoadFilesWithoutOverlay(t *testing.T) {
	for _, contents := range []string{"{}", "pollingBehavior:\n  backoff: {}\n"} {
		got, err := LoadFiles(writeConfigFile(t, contents), "")
		if err != nil {
			t.Fatalf("LoadFiles() error = %v", err)
		}
		if got != DefaultConfiguration() {
			t.Fatal("omitted fields did not retain defaults")
		}
	}
}

func TestLoadFilesOverlayCanExplicitlyClearValues(t *testing.T) {
	base := writeConfigFile(t, `
connectConfig:
  https: true
  authConfig:
    enabled: true
    username: base-user
    password: base-password
`)
	overlay := writeConfigFile(t, `
connectConfig:
  https: false
  authConfig:
    enabled: false
    username: ""
    password: ""
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConnectConfig != DefaultConnectAPIConfiguration() {
		t.Fatal("explicit false and empty strings did not clear base values")
	}
}

func TestLoadFilesRejectsInvalidYAMLInEitherFile(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"empty file", ""},
		{"comments only", "# no settings"},
		{"scalar root", "hello"},
		{"sequence root", "[]"},
		{"malformed syntax", "connectConfig: ["},
		{"multiple documents", "{}\n---\n{}"},
		{"trailing empty document", "{}\n---"},
		{"unknown top level", "pollingBehavour: {}"},
		{"unknown nested field", "pollingBehavior:\n  backoff:\n    maxDely: 1m"},
		{"duplicate top level", "connectConfig: {}\nconnectConfig: {}"},
		{"duplicate nested field", "connectConfig:\n  host: first\n  host: second"},
		{"null root", "null"},
		{"null section", "connectConfig: null"},
		{"null scalar", "pollingBehavior:\n  interval: null"},
		{"empty scalar", "pollingBehavior:\n  restartFailedTasks:"},
		{"numeric duration", "pollingBehavior:\n  interval: 1000"},
		{"unitless duration", "pollingBehavior:\n  interval: \"1000\""},
		{"overflow duration", "pollingBehavior:\n  interval: 9223372036854775808ns"},
		{"invalid duration", "communicationConfig:\n  requestTimeout: someday"},
		{"invalid boolean", "pollingBehavior:\n  restartFailedTasks: perhaps"},
		{"invalid log level", "loggingConfig:\n  level: VERBOSE"},
		{"numeric log level", "loggingConfig:\n  level: 4"},
		{"alias", "connectConfig:\n  host: &host connect\n  port: *host"},
		{"merge key", "connectConfig:\n  <<: {host: connect}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := writeConfigFile(t, test.data)
			valid := writeConfigFile(t, "{}")
			for _, paths := range [][2]string{{invalid, ""}, {valid, invalid}} {
				got, err := LoadFiles(paths[0], paths[1])
				if err == nil {
					t.Fatal("LoadFiles() accepted invalid YAML")
				}
				if got != (Configuration{}) {
					t.Fatal("LoadFiles() returned partial configuration on failure")
				}
			}
		})
	}
}

func TestLoadFilesValidatesMergedConfiguration(t *testing.T) {
	for _, test := range []struct {
		data string
		want string
	}{
		{"pollingBehavior:\n  interval: 0s", "polling interval"},
		{"pollingBehavior:\n  interval: -1s", "polling interval"},
		{"pollingBehavior:\n  backoff:\n    baseDelay: 0s", "backoff base delay"},
		{"pollingBehavior:\n  backoff:\n    maxDelay: 1s", "backoff maximum delay"},
		{"communicationConfig:\n  requestTimeout: 0s", "HTTP request timeout"},
		{"connectConfig:\n  host: \" \"", "Connect host"},
		{"connectConfig:\n  host: http://localhost", "Connect host"},
		{"connectConfig:\n  port: \"65536\"", "Connect port"},
		{"connectConfig:\n  authConfig:\n    enabled: true", "Basic Auth requires HTTPS"},
		{"connectConfig:\n  https: true\n  authConfig:\n    enabled: true", "Basic Auth requires a username and password"},
	} {
		got, err := LoadFiles(writeConfigFile(t, "{}"), writeConfigFile(t, test.data))
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("LoadFiles() error = %v, want %q", err, test.want)
		}
		if got != (Configuration{}) {
			t.Fatal("LoadFiles() returned invalid configuration")
		}
	}
}

func TestLoadFilesRequiresEverySpecifiedFile(t *testing.T) {
	valid := writeConfigFile(t, "{}")
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for _, paths := range [][2]string{{"", ""}, {missing, ""}, {valid, missing}, {t.TempDir(), ""}} {
		if _, err := LoadFiles(paths[0], paths[1]); err == nil {
			t.Fatal("LoadFiles() accepted a missing or unreadable file")
		}
	}
}

func TestLoadFilesErrorsDoNotExposeCredentials(t *testing.T) {
	const sensitive = "credential-that-must-not-be-logged"
	for _, data := range []string{
		"connectConfig:\n  authConfig:\n    password: [" + sensitive + "]",
		"loggingConfig:\n  level: " + sensitive,
		"pollingBehavior:\n  interval: " + sensitive,
		"connectConfig:\n  authConfig:\n    password: first\n    password: " + sensitive,
	} {
		invalid := writeConfigFile(t, data)
		for _, paths := range [][2]string{{invalid, ""}, {writeConfigFile(t, "{}"), invalid}} {
			_, err := LoadFiles(paths[0], paths[1])
			if err == nil || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("expected a safe configuration error, got %v", err)
			}
		}
	}
}

func TestConfigurationYAMLRoundTrip(t *testing.T) {
	want := DefaultConfiguration()
	want.PollingBehavior.RestartFailedTasks = false
	want.LoggingConfig.Level = slog.LevelWarn
	data, err := yaml.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadFiles(writeConfigFile(t, string(data)), "")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("YAML round trip changed configuration")
	}
}
