package config

import (
	"errors"
	"testing"
	"time"
)

func TestManagerUpdateCommitsSingleValue(t *testing.T) {
	manager := NewManager(DefaultConfiguration())

	err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = 2 * time.Second
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateConfiguration() error = %v", err)
	}
	if got := manager.GetConfiguration().CommunicationConfig.RequestTimeout; got != 2*time.Second {
		t.Fatalf("request timeout = %v, want %v", got, 2*time.Second)
	}
}

func TestManagerUpdateRejectsChangeAtomically(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	before, changes := manager.ConfigurationSnapshot()

	err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = time.Second
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
		configuration.CommunicationConfig.RequestTimeout = time.Second
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
	if got := current.CommunicationConfig.RequestTimeout; got != time.Second {
		t.Fatalf("request timeout after notification = %v, want %v", got, time.Second)
	}
	select {
	case <-nextChanges:
		t.Fatal("new change channel is already closed")
	default:
	}

	if err := manager.UpdateConfiguration(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = 2 * time.Second
		return nil
	}); err != nil {
		t.Fatalf("second UpdateConfiguration() error = %v", err)
	}
	select {
	case <-nextChanges:
	default:
		t.Fatal("second update did not close the new change channel")
	}
	if got, _ := manager.ConfigurationSnapshot(); got.CommunicationConfig.RequestTimeout != 2*time.Second {
		t.Fatalf("request timeout after second notification = %v, want %v", got.CommunicationConfig.RequestTimeout, 2*time.Second)
	}
}
