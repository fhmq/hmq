package sessions

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	ErrSessionsProviderNotFound = errors.New("Session: Session provider not found")
	ErrKeyNotAvailable          = errors.New("Session: not item found for key.")

	providers   = make(map[string]func() SessionsProvider)
	providersMu sync.RWMutex
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
func Register(name string, provider SessionsProvider) {
	if provider == nil {
		panic("session: Register provide is nil")
	}

	registerFactory(name, func() SessionsProvider { return provider })
}

func registerFactory(name string, factory func() SessionsProvider) {
	providersMu.Lock()
	defer providersMu.Unlock()
	if _, dup := providers[name]; dup {
		panic("session: Register called twice for provider " + name)
	}

	providers[name] = factory
}

func Unregister(name string) {
	providersMu.Lock()
	defer providersMu.Unlock()
	delete(providers, name)
}

type Manager struct {
	p SessionsProvider
}

func NewManager(providerName string) (*Manager, error) {
	providersMu.RLock()
	factory, ok := providers[providerName]
	providersMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: factory()}, nil
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
