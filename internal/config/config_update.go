package config

import "sync"

// The swap mutex is registered process-wide rather than stored on the
// Config struct: UpdateConfig replaces the struct in place on every call,
// and a struct field that service goroutines read at runtime (to obtain
// the lock itself) would be a data race against that replacement. One
// daemon owns one Config, so a process-wide registration matches the
// product shape; each test registers its own mutex the same way.
var (
	registeredUpdateMuMu  sync.RWMutex
	registeredUpdateMuRef *sync.RWMutex
)

// SetUpdateMu registers the mutex UpdateConfig takes around the in-place
// struct swap, so the owning service's goroutines can read config fields
// under the same mutex's read lock. A nil registration (the default)
// leaves the swap unlocked. Call it once, before any goroutine starts
// reading the config.
func SetUpdateMu(mu *sync.RWMutex) {
	registeredUpdateMuMu.Lock()
	registeredUpdateMuRef = mu
	registeredUpdateMuMu.Unlock()
}

// UpdateMu returns the registered swap mutex (or nil when none was
// registered). Services that share the config but own a different lock -
// the transfer manager and the web handlers - read config fields under
// this mutex's read lock so their reads cannot interleave with the
// in-place struct swap.
func UpdateMu() *sync.RWMutex {
	registeredUpdateMuMu.RLock()
	defer registeredUpdateMuMu.RUnlock()
	return registeredUpdateMuRef
}

func (c *Config) UpdateConfig(_newConfig Config) {
	// An update body that omits Arrs decodes as a nil slice, which the API
	// would then serve back as "Arrs": null; normalize to an empty slice so
	// the array field always serializes as an array.
	if _newConfig.Arrs == nil {
		_newConfig.Arrs = []ArrConfig{}
	}

	// The swap lock is resolved through the process-wide registry, not a
	// struct field: the in-place swap below rewrites the whole struct, and
	// a field the service goroutines read at runtime to obtain the lock
	// itself would race that rewrite (the detector flags the equal-value
	// rewrite too).
	mu := UpdateMu()

	var oldConfig, newConfig Config
	if mu != nil {
		mu.Lock()
		// The private fields are carried over while the swap lock is held:
		// they are read from the live struct, and an unlocked read would
		// race a concurrent swap.
		_newConfig.appCallback = c.appCallback
		_newConfig.altConfigLocation = c.altConfigLocation
		oldConfig = *c
		*c = _newConfig
		// Copy the swapped-in config under the lock too: handing the
		// callback a copy taken outside the lock would race the next swap.
		newConfig = *c
		mu.Unlock()
	} else {
		_newConfig.appCallback = c.appCallback
		_newConfig.altConfigLocation = c.altConfigLocation
		oldConfig = *c
		*c = _newConfig
		newConfig = *c
	}

	// The callback runs with the swap already visible but without the lock:
	// the services' callbacks take the same mutex for their own state, and
	// an RWMutex is not reentrant.
	c.appCallback(oldConfig, newConfig)

	// Persist under the same read lock: Save marshals every field of *c,
	// and a concurrent swap must not interleave with the field-wise read or
	// a mixed config.yaml (new value for one field, old for another) is
	// written to disk.
	if mu != nil {
		mu.RLock()
		c.Save()
		mu.RUnlock()
	} else {
		c.Save()
	}
}
