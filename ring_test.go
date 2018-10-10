package redigoring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

// Tests that we could get back they key we previously set
func TestSetGet(t *testing.T) {
	const (
		serverPrefix = "redisserver_"
		numServers   = 3
		numKeys      = 5000
		keyPrefix    = "key_"
		val          = "val"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create pools
	pools, cleanup := createPools(t, numServers, serverPrefix)
	defer cleanup()

	// create ring
	ring, err := New(ctx, pools)
	require.NoError(t, err)

	genKey := func(idx int) string {
		return fmt.Sprintf("%s%d", keyPrefix, idx)
	}

	// set keys
	for i := 0; i < numKeys; i++ {
		key := genKey(i)
		// get conn
		conn, err := ring.GetConn(key)
		require.NoError(t, err)

		// set
		ret, err := redis.String(conn.Do("SET", key, val))
		require.NoError(t, err)
		require.Equal(t, "OK", ret)

		conn.Close()
	}

	// get it back
	for i := 0; i < numKeys; i++ {
		key := genKey(i)

		// get conn
		conn, err := ring.GetConn(key)
		require.NoError(t, err)

		// get & check
		ret, err := redis.String(conn.Do("GET", key))
		require.NoError(t, err)
		require.Equal(t, val, ret)

		conn.Close()
	}

}

// Tests that we have good enough key distribution
// If even disribution is X%, then each server can
// only have up to X+5% of the keys
func TestDistribution(t *testing.T) {
	const (
		serverPrefix = "redisserver_"
		numServers   = 4
		numKeys      = 1000000
		keyPrefix    = "key_"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create pools
	pools, cleanup := createPools(t, numServers, serverPrefix)
	defer cleanup()

	// create ring
	ring, err := New(ctx, pools)
	require.NoError(t, err)

	// distribution map : server_name -> num_keys
	dist := make(map[string]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		name, _, err := ring.GetPool(key)
		require.NoError(t, err)

		val, _ := dist[name]
		dist[name] = val + 1
	}

	// check
	require.Len(t, dist, numServers)
	maxKeys := maxKeysPerServer(numServers, numKeys)
	t.Logf("numServers:%v, numKeys:%v, maxKeys:%v", numServers, numKeys, maxKeys)
	for name, count := range dist {
		require.Contains(t, name, serverPrefix)
		require.Truef(t, float64(count) <= maxKeys, "max:%v, count:%v", maxKeys, count)
		t.Logf("%v -> %v", name, count)
	}
}

// max keys allowed per server
// max 5% of it's average
func maxKeysPerServer(numServers, numKeys int) float64 {
	avg := float64(1) / float64(numServers)
	return float64((avg + 0.05) * float64(numKeys))
}

func TestAddRemove(t *testing.T) {
	const (
		serverPrefix = "redisserver_"
		numServers   = 4
		key          = "key1"
		msHealtheck  = 100 // healthCheckInterval in millisecond
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create pools
	pools, cleanup := createPools(t, numServers, serverPrefix)
	defer cleanup()

	// create ring
	ring, err := New(ctx, pools, WithHealthCheckInterval(msHealtheck*time.Millisecond))
	require.NoError(t, err)

	// get name of a pool for the key
	name1, _, err := ring.GetPool(key)
	require.NoError(t, err)

	// remove that pool
	_, err = ring.Remove(name1)
	require.NoError(t, err)

	// the key should go to other pool
	name2, pool, err := ring.GetPool(key)
	require.NoError(t, err)
	require.NotEqual(t, name1, name2)

	// add again
	ring.Add(name1, pool)

	// the key should go back to pool1
	name3, pool, err := ring.GetPool(key)
	require.NoError(t, err)
	require.Equal(t, name1, name3)

}

// TestHealthCheck tests that we have a proper health checking
// - get name of a pool for the given key
// - kill that pool
// - redo the first action, it should return different pool
func TestHealthCheck(t *testing.T) {
	const (
		serverPrefix = "redisserver_"
		numServers   = 4
		key          = "key1"
		msHealtheck  = 100 // healthCheckInterval in millisecond
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create pools
	pools, cleanup := createPools(t, numServers, serverPrefix)
	defer cleanup()

	// create ring
	ring, err := New(ctx, pools, WithHealthCheckInterval(msHealtheck*time.Millisecond))
	require.NoError(t, err)

	// get name of a pool for the key
	name, pool, err := ring.GetPool(key)
	require.NoError(t, err)

	// close the pool, and wait for the healthcheck to work
	pool.Close()
	time.Sleep(msHealtheck * 10 * time.Millisecond)

	// make sure we got different pool
	name2, _, err := ring.GetPool(key)
	require.NoError(t, err)
	require.NotEqual(t, name, name2)
}

// Test that sick server could be added back to ring
// when it become healty
// - get name of a pool for the given key
// - close the server (simulate down server)
// - redo the first action, it should return different pool
// - restart the server
// - redo the first action, it should return same pool as the first one
func testSick(t *testing.T) {
	const (
		count       = 4
		key         = "key1"
		msHealtheck = 100 // healthCheckInterval in millisecond
	)
	var (
		servers = make(map[string]*miniredis.Miniredis)
		pools   = make(map[string]*redis.Pool)
		waitDur = msHealtheck * 10 * time.Millisecond
	)

	// create servers & pools
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("server_redis_%d", i)

		srv, err := miniredis.Run()
		require.NoError(t, err)
		defer srv.Close()

		servers[name] = srv

		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", srv.Addr())
			},
		}
		pools[name] = pool
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create ring
	ring, err := New(ctx, pools, WithHealthCheckInterval(msHealtheck*time.Millisecond))
	require.NoError(t, err)

	// get name of a pool for the key
	name1, _, err := ring.GetPool(key)
	require.NoError(t, err)

	// kill the server
	servers[name1].Close()

	time.Sleep(waitDur) // wait for the sick-check to run

	// get again the name
	name2, _, err := ring.GetPool(key)
	require.NoError(t, err)
	require.NotEqual(t, name1, name2)

	// restart the server
	err = servers[name1].Restart()
	require.NoError(t, err)

	// get the key back, should return name1
	name3, _, err := ring.GetPool(key)
	require.NoError(t, err)
	require.Equal(t, name1, name3)
	require.NotEmpty(t, name3)

}

func createPools(t *testing.T, count int, prefix string) (map[string]*redis.Pool, func()) {
	var (
		servers []*miniredis.Miniredis
		pools   = make(map[string]*redis.Pool)
	)

	for i := 0; i < count; i++ {
		srv, err := miniredis.Run()
		require.NoError(t, err)

		servers = append(servers, srv)

		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", srv.Addr())
			},
		}

		name := fmt.Sprintf("%s%d", prefix, i)
		pools[name] = pool
	}

	return pools, func() {
		for _, srv := range servers {
			srv.Close()
		}
	}
}
