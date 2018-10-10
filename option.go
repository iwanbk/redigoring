package redigoring

import (
	"time"
)

var (
	// DefaultHealtCheckInterval is the default value for the health check interval
	DefaultHealtCheckInterval = 3 * time.Second
)

// Option represents optional option of the redigoring
type Option func(*Ring)

// WithHealthCheckInterval sets the healt check interval
// to the given duration
func WithHealthCheckInterval(dur time.Duration) Option {
	return func(r *Ring) {
		r.healthCheckInterval = dur
	}
}
