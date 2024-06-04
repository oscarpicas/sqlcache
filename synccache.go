package sqlcache

import (
	"context"
	"github.com/oscarpicas/sqlcache/cache"
	"sync"
	"time"
)

type syncCache struct {
	wrapped cache.Cacher
	x       map[string]*sync.RWMutex
	m       *sync.RWMutex
}

func NewSyncCache(wrapped cache.Cacher) cache.Cacher {
	return &syncCache{wrapped: wrapped, x: make(map[string]*sync.RWMutex), m: &sync.RWMutex{}}
}

func (s syncCache) Get(ctx context.Context, key string) (*cache.Item, bool, error) {
	s.m.RLock()
	if _, ok := s.x[key]; !ok {
		s.m.RUnlock()
		s.m.Lock()
		if _, ok := s.x[key]; !ok {
			s.x[key] = &sync.RWMutex{}
		}
		s.m.Unlock()
	} else {
		s.m.RUnlock()
	}

	s.x[key].RLock()
	defer s.x[key].RUnlock()
	return s.wrapped.Get(ctx, key)
}

func (s syncCache) Set(ctx context.Context, key string, item *cache.Item, ttl time.Duration) error {
	s.m.RLock()
	if _, ok := s.x[key]; !ok {
		s.m.RUnlock()
		s.m.Lock()
		if _, ok := s.x[key]; !ok {
			s.x[key] = &sync.RWMutex{}
		}
		s.m.Unlock()
	} else {
		s.m.RUnlock()
	}

	s.x[key].Lock()
	defer s.x[key].Unlock()
	return s.wrapped.Set(ctx, key, item, ttl)
}

func (s syncCache) Invalidate(ctx context.Context, key string) error {
	s.m.RLock()
	if _, ok := s.x[key]; !ok {
		s.m.RUnlock()
		s.m.Lock()
		if _, ok := s.x[key]; !ok {
			s.x[key] = &sync.RWMutex{}
		}
		s.m.Unlock()
	} else {
		s.m.RUnlock()
	}

	s.x[key].Lock()
	defer s.x[key].Unlock()
	return s.wrapped.Invalidate(ctx, key)
}
