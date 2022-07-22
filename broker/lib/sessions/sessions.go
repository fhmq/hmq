package sessions

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

var (
	ErrSessionsProviderNotFound = errors.New("Session: Session provider not found")
	ErrKeyNotAvailable          = errors.New("Session: not item found for key.")

	providers = make(map[string]func() SessionsProvider)
)

type SessionsProvider interface {
	New(id string) (*Session, error)
	Get(id string) (*Session, error)
	Del(id string)
	Save(id string) error
	Count() int
	Close() error
}

// Register makes a session provider available by the provided name.
// If a Register is called twice with the same name or if the driver is nil,
// it panics.
func Register(name string, providerFn func() SessionsProvider) {
	if providerFn == nil {
		panic("session: Register provider is nil")
	}

	if _, dup := providers[name]; dup {
		panic("session: Register called twice for provider " + name)
	}

	providers[name] = providerFn
}

func Unregister(name string) {
	delete(providers, name)
}

type Manager struct {
	p SessionsProvider
}

func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: p()}, nil
}

func (this *Manager) New(id string) (*Session, error) {
	if id == "" {
		id = this.sessionId()
	}
	return this.p.New(id)
}

func (this *Manager) Get(id string) (*Session, error) {
	return this.p.Get(id)
}

func (this *Manager) Del(id string) {
	this.p.Del(id)
}

func (this *Manager) Save(id string) error {
	return this.p.Save(id)
}

func (this *Manager) Count() int {
	return this.p.Count()
}

func (this *Manager) Close() error {
	return this.p.Close()
}

func (manager *Manager) sessionId() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(b)
}
