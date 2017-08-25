package broker

import "sync"

type ClientMap interface {
	Set(key string, val *client)
	Get(key string) (*client, bool)
	Items() map[string]*client
	Exist(key string) bool
	Update(key string, val *client) (*client, bool)
	Count() int
	Remove(key string)
}

type clientMap struct {
	items map[string]*client
	mu    sync.RWMutex
}

func NewClientMap() ClientMap {
	smap := &clientMap{
		items: make(map[string]*client),
	}
	return smap
}

func (s *clientMap) Set(key string, val *client) {
	s.mu.Lock()
	s.items[key] = val
	s.mu.Unlock()
}

func (s *clientMap) Get(key string) (*client, bool) {
	s.mu.RLock()
	val, ok := s.items[key]
	s.mu.RUnlock()
	return val, ok
}

func (s *clientMap) Exist(key string) bool {
	s.mu.RLock()
	_, ok := s.items[key]
	s.mu.RUnlock()
	return ok
}

func (s *clientMap) Update(key string, val *client) (*client, bool) {
	s.mu.Lock()
	old, ok := s.items[key]
	s.items[key] = val
	s.mu.Unlock()
	return old, ok
}

func (s *clientMap) Count() int {
	s.mu.RLock()
	len := len(s.items)
	s.mu.RUnlock()
	return len
}

func (s *clientMap) Remove(key string) {
	s.mu.Lock()
	delete(s.items, key)
	s.mu.Unlock()
}

func (s *clientMap) Items() map[string]*client {
	s.mu.RLock()
	items := s.items
	s.mu.RUnlock()
	return items
}
