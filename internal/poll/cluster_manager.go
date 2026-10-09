package poll

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"entropicworks.com/kafka-connector-restarter/internal/config"
	"entropicworks.com/kafka-connector-restarter/internal/connectcluster"
)

var (
	ErrClusterNotFound      = errors.New("Connect cluster not found")
	ErrClusterManagerClosed = errors.New("Connect cluster manager is shutting down")
)

type clusterWorker struct {
	configuration connectcluster.ConnectClusterAPIConfiguration
	cancel        context.CancelFunc
	done          chan struct{}
}

// ClusterManager owns cluster definitions and their polling goroutines.
type ClusterManager struct {
	changeMu      sync.Mutex // Serialize lifecycle changes, including waiting for stops.
	mu            sync.RWMutex
	ctx           context.Context // Application lifetime, never an API request context.
	configManager *config.Manager
	clusters      map[string]*clusterWorker
	closed        bool
}

func NewClusterManager(ctx context.Context, configManager *config.Manager, clusters map[string]connectcluster.ConnectClusterAPIConfiguration) (*ClusterManager, error) {
	if ctx.Err() != nil {
		return nil, ErrClusterManagerClosed
	}
	// Validate every initial definition before starting any goroutine.
	for name, configuration := range clusters {
		if err := validateCluster(name, configuration); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", name, err)
		}
	}
	manager := &ClusterManager{
		ctx: ctx, configManager: configManager,
		clusters: make(map[string]*clusterWorker, len(clusters)),
	}
	for name, configuration := range clusters {
		manager.clusters[name] = manager.startCluster(name, configuration)
	}
	return manager, nil
}

func (manager *ClusterManager) GetClusters() map[string]connectcluster.ConnectClusterAPIConfiguration {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	clusters := make(map[string]connectcluster.ConnectClusterAPIConfiguration, len(manager.clusters))
	for name, worker := range manager.clusters {
		clusters[name] = worker.configuration
	}
	return clusters
}

// PutCluster replaces one complete definition. Identical definitions are a no-op.
func (manager *ClusterManager) PutCluster(name string, configuration connectcluster.ConnectClusterAPIConfiguration) (bool, error) {
	if err := validateCluster(name, configuration); err != nil {
		return false, err
	}
	manager.changeMu.Lock()
	defer manager.changeMu.Unlock()
	if manager.closed || manager.ctx.Err() != nil {
		return false, ErrClusterManagerClosed
	}
	manager.mu.RLock()
	previous, exists := manager.clusters[name]
	manager.mu.RUnlock()
	if exists {
		if previous.configuration == configuration {
			return false, nil
		}
		previous.cancel()
		<-previous.done
	}
	if manager.ctx.Err() != nil {
		return false, ErrClusterManagerClosed
	}
	manager.mu.Lock()
	manager.clusters[name] = manager.startCluster(name, configuration)
	manager.mu.Unlock()
	connectcluster.WarnDuplicateEndpoints(manager.GetClusters())
	slog.Info("Connect cluster configured", "connect_cluster", name, "created", !exists)
	return !exists, nil
}

func (manager *ClusterManager) DeleteCluster(name string) error {
	manager.changeMu.Lock()
	defer manager.changeMu.Unlock()
	if manager.closed || manager.ctx.Err() != nil {
		return ErrClusterManagerClosed
	}
	manager.mu.RLock()
	worker, exists := manager.clusters[name]
	manager.mu.RUnlock()
	if !exists {
		return ErrClusterNotFound
	}
	worker.cancel()
	<-worker.done
	manager.mu.Lock()
	delete(manager.clusters, name)
	manager.mu.Unlock()
	slog.Info("Connect cluster removed", "connect_cluster", name)
	return nil
}

// Close rejects new changes, cancels all pollers, and waits for them to exit.
func (manager *ClusterManager) Close() {
	manager.changeMu.Lock()
	defer manager.changeMu.Unlock()
	if manager.closed {
		return
	}
	manager.closed = true
	manager.mu.Lock()
	clusters := manager.clusters
	manager.clusters = make(map[string]*clusterWorker)
	manager.mu.Unlock()
	for _, worker := range clusters {
		worker.cancel()
	}
	for _, worker := range clusters {
		<-worker.done
	}
}

func (manager *ClusterManager) startCluster(name string, configuration connectcluster.ConnectClusterAPIConfiguration) *clusterWorker {
	ctx, cancel := context.WithCancel(manager.ctx)
	worker := &clusterWorker{configuration: configuration, cancel: cancel, done: make(chan struct{})}
	poller := NewConnectClusterPoller(name, configuration)
	go func() {
		defer close(worker.done)
		defer cancel()
		poller.Poll(ctx, manager.configManager)
	}()
	return worker
}

func validateCluster(name string, configuration connectcluster.ConnectClusterAPIConfiguration) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("cluster name must not be empty")
	}
	return connectcluster.ValidateConfiguration(configuration)
}
