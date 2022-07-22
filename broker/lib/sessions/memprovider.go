package sessions

import (
	"fmt"
	"sync"
)

var _ SessionsProvider = (*memProvider)(nil)

func init() {
	Register("mem", func() SessionsProvider { return NewMemProvider() })
}

type memProvider struct {
	st map[string]*Session
	mu sync.RWMutex
}

func NewMemProvider() *memProvider {
	return &memProvider{
		st: make(map[string]*Session),
	}
}

func (this *memProvider) New(id string) (*Session, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.st[id] = &Session{id: id}
	return this.st[id], nil
}

func (this *memProvider) Get(id string) (*Session, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	sess, ok := this.st[id]
	if !ok {
		return nil, fmt.Errorf("store/Get: No session found for key %s", id)
	}

	return sess, nil
}

func (this *memProvider) Del(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	delete(this.st, id)
}

func (this *memProvider) Save(id string) error {
	return nil
}

func (this *memProvider) Count() int {
	return len(this.st)
}

func (this *memProvider) Close() error {
	this.st = make(map[string]*Session)
	return nil
}
