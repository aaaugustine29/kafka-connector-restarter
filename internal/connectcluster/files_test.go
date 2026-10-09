package connectcluster

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clusters.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFilesWithoutBaseUsesDefaultCluster(t *testing.T) {
	got, err := LoadFiles("", "")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]ConnectClusterAPIConfiguration{"default": DefaultConfiguration()}
	if !maps.Equal(got, want) {
		t.Fatalf("clusters = %#v, want %#v", got, want)
	}
}

func TestLoadFilesOverlayWithoutBase(t *testing.T) {
	overlay := writeConfigFile(t, "default:\n  host: connect.local\nstaging: {}")
	got, err := LoadFiles("", overlay)
	if err != nil {
		t.Fatal(err)
	}
	defaultCluster := DefaultConfiguration()
	defaultCluster.Host = "connect.local"
	want := map[string]ConnectClusterAPIConfiguration{
		"default": defaultCluster,
		"staging": DefaultConfiguration(),
	}
	if !maps.Equal(got, want) {
		t.Fatalf("clusters = %#v, want %#v", got, want)
	}
}

func TestLoadFilesDefaultsAndPartialOverlay(t *testing.T) {
	base := writeConfigFile(t, `
production:
  host: connect.production.svc
  port: "8443"
  https: true
  authConfig:
    enabled: true
    username: base-user
development: {}
`)
	overlay := writeConfigFile(t, `
production:
  authConfig:
    username: secret-user
    password: secret-password
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	want := map[string]ConnectClusterAPIConfiguration{
		"production": {
			Host: "connect.production.svc", Port: "8443", HTTPS: true,
			AuthConfig: AuthConfiguration{Enabled: true, Username: "secret-user", Password: "secret-password"},
		},
		"development": DefaultConfiguration(),
	}
	if !maps.Equal(got, want) {
		t.Fatal("cluster overlay lost endpoint settings, credentials, or per-cluster defaults")
	}
	if _, err := LoadFiles(base, ""); err == nil || !strings.Contains(err.Error(), `cluster "production": authConfig.password:`) {
		t.Fatalf("missing credentials error = %v", err)
	}
}

func TestLoadFilesClusterMembershipAndEmptyMappings(t *testing.T) {
	got, err := LoadFiles(writeConfigFile(t, "production:\n  host: connect.production\n"), writeConfigFile(t, "production: {}\nstaging: {}"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]ConnectClusterAPIConfiguration{"production": DefaultConfiguration(), "staging": DefaultConfiguration()}
	production := want["production"]
	production.Host = "connect.production"
	want["production"] = production
	if !maps.Equal(got, want) {
		t.Fatal("loader added an implicit cluster or lost existing values")
	}
	empty, err := LoadFiles(writeConfigFile(t, "{}"), "")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty mapping = %v, error = %v, want no clusters", empty, err)
	}
}

func TestLoadFilesOverlayCanExplicitlyClearValues(t *testing.T) {
	base := writeConfigFile(t, "production:\n  https: true\n  authConfig:\n    enabled: true\n    username: user\n    password: password")
	overlay := writeConfigFile(t, "production:\n  https: false\n  authConfig:\n    enabled: false\n    username: \"\"\n    password: \"\"")
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if got["production"] != DefaultConfiguration() {
		t.Fatal("explicit false and empty strings did not clear existing values")
	}
}

func TestLoadFilesRejectsInvalidYAMLInEitherFile(t *testing.T) {
	for _, contents := range []string{
		"", "# comment", "null", "[]", "production: [", "{}\n---\n{}", "{}\n---",
		"production: null", "production: []", "production: scalar",
		"production: {}\nproduction: {}",
		"production:\n  host: first\n  host: second",
		"production:\n  authConfig:\n    password: first\n    password: second",
		"production:\n  host: null", "production:\n  https:",
		"production:\n  typo: true", "production:\n  authConfig:\n    typo: true",
		"production:\n  https: perhaps",
		"production:\n  host: &host connect\n  port: *host",
		"production:\n  <<: {host: connect}", "1: {}",
	} {
		t.Run(contents, func(t *testing.T) {
			invalid := writeConfigFile(t, contents)
			for _, paths := range [][2]string{{invalid, ""}, {writeConfigFile(t, "production: {}"), invalid}} {
				got, err := LoadFiles(paths[0], paths[1])
				if err == nil || got != nil {
					t.Fatal("invalid file was accepted or returned partial clusters")
				}
			}
		})
	}
}

func TestLoadFilesErrorsDoNotExposeCredentials(t *testing.T) {
	const sensitive = "credential-that-must-not-be-logged"
	for _, contents := range []string{
		"production:\n  authConfig:\n    password: [" + sensitive + "]",
		"production:\n  authConfig:\n    password: first\n    password: " + sensitive,
		"production:\n  https: " + sensitive,
		"production:\n  host: " + sensitive + "/invalid",
	} {
		invalid := writeConfigFile(t, contents)
		for _, paths := range [][2]string{{invalid, ""}, {writeConfigFile(t, "production: {}"), invalid}} {
			got, err := LoadFiles(paths[0], paths[1])
			if got != nil || err == nil || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("expected a safe configuration error, got %v", err)
			}
		}
	}
}

func TestLoadFilesRequiresEverySpecifiedFile(t *testing.T) {
	valid := writeConfigFile(t, "production: {}")
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for _, paths := range [][2]string{{"", missing}, {missing, ""}, {valid, missing}, {t.TempDir(), ""}} {
		if got, err := LoadFiles(paths[0], paths[1]); err == nil || got != nil {
			t.Fatal("missing or unreadable file was accepted")
		}
	}
}
