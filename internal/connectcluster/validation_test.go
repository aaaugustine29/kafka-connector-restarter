package connectcluster

import (
	"strings"
	"testing"
)

func TestValidateConnectHost(t *testing.T) {
	for _, host := range []string{
		"localhost", "kafka-connect", "connect.kafka.svc.cluster.local", "connect.example.test.",
		"CONNECT.example.test", "127.0.0.1", "2001:db8::1", "::1", "fe80::1%eth0",
	} {
		t.Run("valid/"+host, func(t *testing.T) {
			configuration := DefaultConfiguration()
			configuration.Host = host
			if err := ValidateConfiguration(configuration); err != nil {
				t.Fatalf("valid host rejected: %v", err)
			}
		})
	}
	for _, host := range []string{
		"", " ", "http://localhost", "https://connect.example.test", "localhost:8083", "connect/path", "connect\\path",
		"connect?query", "connect#fragment", "user@connect", "[::1]", "[::1]:8083", "2001:db8::invalid",
		" connect", "connect ", "con nect", "connect\t", "connect\n", "con\x00nect", "con\u00a0nect",
		"connect%2Fpath", ".", "connect..example", "-connect", "connect-", "con_nect", "connect..",
		strings.Repeat("a", 64) + ".example", strings.Repeat("a.", 128) + "test",
	} {
		t.Run("invalid/"+host, func(t *testing.T) {
			configuration := DefaultConfiguration()
			configuration.Host = host
			if err := ValidateConfiguration(configuration); err == nil || !strings.HasPrefix(err.Error(), "host:") {
				t.Fatalf("host validation error = %v", err)
			}
		})
	}
}

func TestValidatePortAndAuthentication(t *testing.T) {
	for _, test := range []struct {
		configuration ConnectClusterAPIConfiguration
		want          string
	}{
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "65536"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "not-a-port"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "0"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "+8083"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "-8083"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: " 8083"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083 "}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "８０８３"}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: ""}, "port:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "0008083"}, ""},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "1"}, ""},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "65535"}, ""},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083", AuthConfig: AuthConfiguration{Enabled: true}}, "https:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083", HTTPS: true, AuthConfig: AuthConfiguration{Enabled: true, Password: "secret"}}, "authConfig.username:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083", HTTPS: true, AuthConfig: AuthConfiguration{Enabled: true, Username: "user"}}, "authConfig.password:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083", HTTPS: true, AuthConfig: AuthConfiguration{Enabled: true, Username: "team:user", Password: "secret"}}, "authConfig.username:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083", HTTPS: true, AuthConfig: AuthConfiguration{Enabled: true, Username: " ", Password: "secret"}}, "authConfig.username:"},
		{ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083", HTTPS: true, AuthConfig: AuthConfiguration{Enabled: true, Username: "user", Password: "secret"}}, ""},
	} {
		err := ValidateConfiguration(test.configuration)
		if test.want == "" {
			if err != nil {
				t.Fatalf("valid cluster rejected: %v", err)
			}
		} else if err == nil || !strings.HasPrefix(err.Error(), test.want) {
			t.Fatalf("validation error = %v, want %q", err, test.want)
		}
	}
}
