package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ensingerphilipp/premiumizearr-nova/internal/config"
	"github.com/ensingerphilipp/premiumizearr-nova/pkg/stringqueue"
)

// TestBlackholeHandlerDerivesArrFromPath is the regression test for
// finding R1-12: the Arr column must be derived with filepath (not path),
// because fsnotify event names carry the platform separator - on Windows
// that is a backslash, which path.Base/path.Dir do not split. filepath
// normalizes both separator styles on both platforms. This test pins the
// Linux behavior (a file directly under the blackhole root has an empty
// Arr, a file inside an Arr subfolder carries the subfolder name); the
// Windows backslash behavior follows from the same filepath calls but is
// not executable here because CI runs on Linux only.
func TestBlackholeHandlerDerivesArrFromPath(t *testing.T) {
	bh := t.TempDir()
	sonarrDir := filepath.Join(bh, "sonarr")
	if err := os.MkdirAll(sonarrDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dw := NewDirectoryWatcherService()
	dw.Queue = stringqueue.NewStringQueue()
	dw.Queue.Add(filepath.Join(bh, "root-file.magnet"))
	dw.Queue.Add(filepath.Join(sonarrDir, "episode.magnet"))

	s := &WebServerService{
		directoryWatcherService: &dw,
		config:                  &config.Config{BlackholeDirectory: bh},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/blackhole", nil)
	rec := httptest.NewRecorder()
	s.BlackholeHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("BlackholeHandler status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	var resp BlackholeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Status == "Not Initialized" {
		t.Fatal("BlackholeHandler reported Not Initialized for an initialized watcher")
	}
	if len(resp.BlackholeFiles) != 2 {
		t.Fatalf("BlackholeFiles = %v, want 2 entries", resp.BlackholeFiles)
	}

	byName := map[string]BlackholeFile{}
	for _, f := range resp.BlackholeFiles {
		byName[f.Name] = f
	}
	root, ok := byName["root-file.magnet"]
	if !ok {
		t.Fatalf("root file missing from response: %v", resp.BlackholeFiles)
	}
	if root.Arr != "" {
		t.Fatalf("root file Arr = %q, want empty", root.Arr)
	}
	sub, ok := byName["episode.magnet"]
	if !ok {
		t.Fatalf("subfolder file missing from response: %v", resp.BlackholeFiles)
	}
	if sub.Arr != "sonarr" {
		t.Fatalf("subfolder file Arr = %q, want sonarr", sub.Arr)
	}
}

// TestBlackholeHandlerNotInitialized guards the nil-watcher path: the
// handler reports the uninitialized state instead of panicking.
func TestBlackholeHandlerNotInitialized(t *testing.T) {
	s := &WebServerService{config: &config.Config{}}

	req := httptest.NewRequest(http.MethodGet, "/api/blackhole", nil)
	rec := httptest.NewRecorder()
	s.BlackholeHandler(rec, req)

	var resp BlackholeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Status != "Not Initialized" {
		t.Fatalf("Status = %q, want Not Initialized", resp.Status)
	}
}
