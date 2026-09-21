package config

import "sync"

// SetUpdateMu registers the mutex UpdateConfig takes around the in-place
// struct swap, so the owning service's goroutines can read config fields
// under the same mutex's read lock. A nil registration (the default) leaves
// the swap unlocked.
func (c *Config) SetUpdateMu(mu *sync.RWMutex) {
	c.updateMu = mu
}

func (c *Config) UpdateConfig(_newConfig Config) {
	// An update body that omits Arrs decodes as a nil slice, which the API
	// would then serve back as "Arrs": null; normalize to an empty slice so
	// the array field always serializes as an array.
	if _newConfig.Arrs == nil {
		_newConfig.Arrs = []ArrConfig{}
	}

	//move private fields over
	_newConfig.appCallback = c.appCallback
	_newConfig.altConfigLocation = c.altConfigLocation
	// Each decoded request config carries no lock registration; carry the
	// owning service's mutex over so the swap stays locked across updates.
	_newConfig.updateMu = c.updateMu

	var oldConfig Config
	if c.updateMu != nil {
		c.updateMu.Lock()
		oldConfig = *c
		*c = _newConfig
		c.updateMu.Unlock()
	} else {
		oldConfig = *c
		*c = _newConfig
	}

	// The callback runs with the swap already visible but without the lock:
	// the services' callbacks take the same mutex for their own state, and
	// an RWMutex is not reentrant.
	c.appCallback(oldConfig, *c)
	c.Save()
}
