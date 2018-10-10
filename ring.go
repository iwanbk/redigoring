package redigoring

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/lafikl/consistent"
)

var (
	// ErrEmptyPools returned when there is no available pool
	// needed for the respective operation
	ErrEmptyPools = errors.New("empty pools")

	// ErrPoolNotFound returned when there is no pool with the given key
	ErrPoolNotFound = errors.New("pool not found")
)

// Ring represent the redigo ring
type Ring struct {
	pools               map[string]*redis.Pool
	sickPools           map[string]*redis.Pool
	ring                *consistent.Consistent
	mux                 sync.RWMutex
	healthCheckInterval time.Duration
}

// New creates new redigo ring object
func New(ctx context.Context, pools map[string]*redis.Pool, opts ...Option) (*Ring, error) {
	if len(pools) == 0 {
		return nil, ErrEmptyPools
	}

	// init the ring
	ring := consistent.New()
	for name := range pools {
		ring.Add(name)
	}

	r := &Ring{
		pools:               pools,
		sickPools:           make(map[string]*redis.Pool),
		ring:                ring,
		healthCheckInterval: DefaultHealtCheckInterval,
	}

	for _, opt := range opts {
		opt(r)
	}

	go r.run(ctx)

	return r, nil
}

func (r *Ring) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.healthCheckInterval):
			r.healtCheck()
		}
	}
}

func (r *Ring) healtCheck() {
	var wg sync.WaitGroup

	// health check
	r.mux.RLock()
	for name, pool := range r.pools {
		wg.Add(1)
		go func(name string, pool *redis.Pool) {
			defer wg.Done()

			conn := pool.Get()
			defer conn.Close()

			// put to sick pools if PING failed
			ret, err := redis.String(conn.Do("PING"))
			if err != nil || ret != "PONG" {
				r.Remove(name)
			}

		}(name, pool)
	}
	r.mux.RUnlock()
	wg.Wait()

	// sick check
	r.mux.RLock()
	for name, pool := range r.sickPools {
		wg.Add(1)
		go func(name string, pool *redis.Pool) {
			defer wg.Done()

			conn := pool.Get()
			defer conn.Close()

			// put back to healty pools if PING succeed
			ret, err := redis.String(conn.Do("PING"))
			if err == nil || ret == "PONG" {
				r.Add(name, pool)
			}
		}(name, pool)
	}
	r.mux.RUnlock()
	wg.Wait()
}

// Remove removes a pool with given name from the ring
func (r *Ring) Remove(name string) (*redis.Pool, error) {
	r.mux.Lock()
	defer r.mux.Unlock()

	pool, ok := r.pools[name]
	if !ok {
		return nil, ErrPoolNotFound
	}

	r.ring.Remove(name)

	delete(r.pools, name)
	r.sickPools[name] = pool
	return pool, nil
}

// Add adds a pool with the given name to the ring
func (r *Ring) Add(name string, pool *redis.Pool) {
	r.mux.Lock()
	defer r.mux.Unlock()

	_, ok := r.sickPools[name]
	if ok {
		delete(r.sickPools, name)
	}

	r.ring.Add(name)

	r.pools[name] = pool
}

// GetPool returns pool for given key.
// Except you want to know the location of your key, you can use `GetConn` directly.
func (r *Ring) GetPool(key string) (string, *redis.Pool, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()

	name, err := r.ring.Get(key)
	if err != nil {
		return "", nil, err
	}

	pool, ok := r.pools[name]
	if !ok {
		return "", nil, ErrPoolNotFound
	}
	return name, pool, nil
}

// GetConn get connection for the given key
func (r *Ring) GetConn(key string) (redis.Conn, error) {
	_, pool, err := r.GetPool(key)
	if err != nil {
		return nil, err
	}

	return pool.Get(), nil
}
