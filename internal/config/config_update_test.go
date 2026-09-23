package config

import (
	"os"
	"path"
	"sync"
	"testing"

	"gopkg.in/yaml.v2"
)

// TestUpdateConfigConcurrentSavesPersistCompleteConfigs is the regression
// test for finding C-17: two concurrent web saves call UpdateConfig on the
// same Config; the private-field carry-over reads, the swapped-in copy
// handed to the callback, and the field-wise YAML marshal in Save must all
// run under the registered swap lock, or a swap interleaved with the
// marshal persists a config that mixes fields of the two updates. Run with
// -race; the pre-fix tree reports data races on the carry-over reads and
// the marshal.
func TestUpdateConfigConcurrentSavesPersistCompleteConfigs(t *testing.T) {
	loc := t.TempDir()
	cfg, err := LoadOrCreateConfig(loc, func(oldConfig, newConfig Config) {})
	if err != nil {
		t.Fatalf("LoadOrCreateConfig: %v", err)
	}
	var mu sync.RWMutex
	SetUpdateMu(&mu)

	a := cfg
	a.BlackholeDirectory = "blackhole-a"
	a.Arrs = []ArrConfig{{Name: "alpha"}}
	b := cfg
	b.BlackholeDirectory = "blackhole-b"
	b.Arrs = []ArrConfig{{Name: "beta"}}

	const iterations = 50
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			(&cfg).UpdateConfig(a)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			(&cfg).UpdateConfig(b)
		}
	}()
	close(start)
	wg.Wait()

	data, err := os.ReadFile(path.Join(loc, "config.yaml"))
	if err != nil {
		t.Fatalf("read persisted config: %v", err)
	}
	var persisted Config
	if err := yaml.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("persisted config does not parse: %v", err)
	}

	// The persisted file must be one complete update, never a field-wise
	// mixture of the two.
	isA := persisted.BlackholeDirectory == "blackhole-a" &&
		len(persisted.Arrs) == 1 && persisted.Arrs[0].Name == "alpha"
	isB := persisted.BlackholeDirectory == "blackhole-b" &&
		len(persisted.Arrs) == 1 && persisted.Arrs[0].Name == "beta"
	if !isA && !isB {
		t.Fatalf("persisted config is a torn mixture of the two updates: BlackholeDirectory=%q Arrs=%+v", persisted.BlackholeDirectory, persisted.Arrs)
	}
}
