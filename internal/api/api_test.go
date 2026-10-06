package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"entropicworks.com/kafka-connector-restarter/internal/config"
)

func startTestServer(t *testing.T, server *http.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
		select {
		case err := <-serveResult:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Serve() error = %v, want http.ErrServerClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after closing the server")
		}
	})
	return "http://" + listener.Addr().String()
}

func TestNewServerRoutes(t *testing.T) {
	configuration := config.DefaultConfiguration()
	configuration.PollingBehavior.Interval = config.Duration(250 * time.Millisecond)
	server := NewServer(APIComponents{ConfigManager: config.NewManager(configuration)})
	baseURL := startTestServer(t, server)
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()

	for _, test := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/", http.StatusOK},
		{http.MethodGet, "/config", http.StatusOK},
		{http.MethodGet, "/missing", http.StatusNotFound},
		{http.MethodPost, "/config", http.StatusMethodNotAllowed},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			request, err := http.NewRequest(test.method, baseURL+test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.status)
			}
			if test.status != http.StatusOK {
				return
			}
			if got := response.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", got)
			}
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !json.Valid(body) {
				t.Fatalf("invalid JSON response: %s", body)
			}
			if test.path == "/config" {
				var got config.Configuration
				if err := json.Unmarshal(body, &got); err != nil {
					t.Fatal(err)
				}
				if got.PollingBehavior.Interval != configuration.PollingBehavior.Interval {
					t.Fatalf("interval = %v, want %v", got.PollingBehavior.Interval, configuration.PollingBehavior.Interval)
				}
			}
		})
	}
}

func TestServerShutdownWaitsForActiveRequest(t *testing.T) {
	server := NewServer(APIComponents{ConfigManager: config.NewManager(config.DefaultConfiguration())})
	handler := server.Handler
	requestStarted := make(chan struct{})
	handlerCtx, releaseRequest := context.WithCancel(context.Background())
	defer releaseRequest()
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-handlerCtx.Done():
			handler.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	})
	shutdownStarted := make(chan struct{})
	server.RegisterOnShutdown(func() { close(shutdownStarted) })
	baseURL := startTestServer(t, server)
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	type requestResult struct {
		status int
		body   []byte
		err    error
	}
	requestDone := make(chan requestResult, 1)
	go func() {
		response, err := client.Get(baseURL + "/config")
		if err != nil {
			requestDone <- requestResult{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		requestDone <- requestResult{status: response.StatusCode, body: body, err: err}
	}()
	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the handler")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(shutdownCtx) }()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not start")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before the active request finished: %v", err)
	default:
	}

	releaseRequest()
	select {
	case result := <-requestDone:
		if result.err != nil {
			t.Fatalf("active request failed during shutdown: %v", result.err)
		}
		if result.status != http.StatusOK || !json.Valid(result.body) {
			t.Fatalf("active request returned status %d, body %q", result.status, result.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("active request did not finish")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after releasing the request")
	}
}
