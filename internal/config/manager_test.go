package config

import (
	"errors"
	"testing"
	"time"
)

func TestManagerUpdateCommitsSingleValue(t *testing.T) {
	manager := NewManager(DefaultConfiguration())

	err := manager.Update(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = 2 * time.Second
		return nil
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if got := manager.Get().CommunicationConfig.RequestTimeout; got != 2*time.Second {
		t.Fatalf("request timeout = %v, want %v", got, 2*time.Second)
	}
}

func TestManagerUpdateRejectsChangeAtomically(t *testing.T) {
	manager := NewManager(DefaultConfiguration())
	before := manager.Get()

	err := manager.Update(func(configuration *Configuration) error {
		configuration.CommunicationConfig.RequestTimeout = time.Second
		return errors.New("reject update")
	})
	if err == nil {
		t.Fatal("Update() error = nil, want error")
	}
	if got := manager.Get(); got != before {
		t.Fatalf("configuration after rejected update = %#v, want %#v", got, before)
	}
}
