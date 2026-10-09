package requests

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/connectcluster"
)

func TestNewConnectAPIBuildsBaseURL(t *testing.T) {
	tests := []struct {
		name          string
		configuration connectcluster.ConnectClusterAPIConfiguration
		wantURL       string
	}{
		{
			name:          "HTTP",
			configuration: connectcluster.ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083"},
			wantURL:       "http://localhost:8083",
		},
		{
			name:          "HTTPS",
			configuration: connectcluster.ConnectClusterAPIConfiguration{Host: "connect.example.test", Port: "8443", HTTPS: true},
			wantURL:       "https://connect.example.test:8443",
		},
		{
			name:          "IPv6",
			configuration: connectcluster.ConnectClusterAPIConfiguration{Host: "2001:db8::1", Port: "8083"},
			wantURL:       "http://[2001:db8::1]:8083",
		},
		{
			name:          "scoped IPv6",
			configuration: connectcluster.ConnectClusterAPIConfiguration{Host: "fe80::1%eth0", Port: "8083"},
			wantURL:       "http://[fe80::1%25eth0]:8083",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := NewConnectAPI(&http.Client{}, test.configuration)
			if connect.BaseURL != test.wantURL {
				t.Fatalf("BaseURL = %q, want %q", connect.BaseURL, test.wantURL)
			}
		})
	}
}

func TestConnectAPIDoesNotFollowRedirects(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(method+"/"+strconv.Itoa(code), func(t *testing.T) {
				var targetRequests atomic.Int32
				target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					targetRequests.Add(1)
					w.WriteHeader(http.StatusOK)
				}))
				defer target.Close()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Location", target.URL)
					w.WriteHeader(code)
					io.WriteString(w, "original redirect response")
				}))
				defer server.Close()
				serverURL, err := url.Parse(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				client := server.Client()
				client.Timeout = 2 * time.Second
				originalError := errors.New("original redirect policy")
				client.CheckRedirect = func(*http.Request, []*http.Request) error { return originalError }
				connect := NewConnectAPI(client, connectcluster.ConnectClusterAPIConfiguration{
					Host: serverURL.Hostname(), Port: serverURL.Port(),
				})
				if connect.HTTPClient.Timeout != client.Timeout || connect.HTTPClient.Transport != client.Transport {
					t.Fatal("Connect client did not preserve the timeout and transport")
				}
				if err := client.CheckRedirect(nil, nil); !errors.Is(err, originalError) {
					t.Fatal("NewConnectAPI changed the caller's redirect policy")
				}
				request, err := connect.NewRequest(context.Background(), method, connect.BaseURL)
				if err != nil {
					t.Fatal(err)
				}
				response, err := connect.HTTPClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != code || string(body) != "original redirect response" {
					t.Fatalf("response = (%d, %q), want original redirect response", response.StatusCode, body)
				}
				if targetRequests.Load() != 0 {
					t.Fatal("Connect client followed the redirect")
				}
			})
		}
	}
}

func TestConnectAPINewRequestBasicAuth(t *testing.T) {
	tests := []struct {
		name     string
		auth     connectcluster.AuthConfiguration
		expected bool
	}{
		{
			name:     "disabled authentication omits credentials",
			expected: false,
		},
		{
			name: "enabled authentication adds credentials",
			auth: connectcluster.AuthConfiguration{
				Enabled:  true,
				Username: "connect-user",
				Password: "connect-password",
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connect := ConnectAPI{Auth: test.auth}
			request, err := connect.NewRequest(context.Background(), http.MethodGet, "https://connect.example.test/connectors")
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}

			username, password, ok := request.BasicAuth()
			if ok != test.expected {
				t.Fatalf("request.BasicAuth() found credentials = %t, want %t", ok, test.expected)
			}
			if test.expected && (username != test.auth.Username || password != test.auth.Password) {
				t.Fatalf("request.BasicAuth() = (%q, %q), want (%q, %q)", username, password, test.auth.Username, test.auth.Password)
			}
		})
	}
}
