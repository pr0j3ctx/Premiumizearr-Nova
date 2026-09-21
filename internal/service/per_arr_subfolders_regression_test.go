package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ensingerphilipp/premiumizearr-nova/internal/config"
	"github.com/ensingerphilipp/premiumizearr-nova/internal/directory_watcher"
	"github.com/ensingerphilipp/premiumizearr-nova/pkg/premiumizeme"
	"github.com/ensingerphilipp/premiumizearr-nova/pkg/stringqueue"
)

// pmeCall records one request the test pme stub received.
type pmeCall struct {
	Method string
	Path   string
	Query  map[string]string
}

// pmeStub serves the premiumize.me endpoints the service code under test
// uses, records every call, and keeps a mutable folder table so
// CreateFolder behaves like the real API (the new folder appears in the
// parent's listing). failAll makes every endpoint return 500.
type pmeStub struct {
	server          *httptest.Server
	mu              sync.Mutex
	calls           []pmeCall
	table           map[string][]premiumizeme.Item
	failAll         bool
	nextID          int
	transferTargets []string
	createdFolders  []string
}

func newPmeStub(t *testing.T, failAll bool) *pmeStub {
	t.Helper()
	s := &pmeStub{
		table:   map[string][]premiumizeme.Item{"": {{ID: "main-id", Name: "arrDownloads", Type: "folder"}}},
		failAll: failAll,
		nextID:  1,
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.server.Close)
	return s
}

func (s *pmeStub) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	call := pmeCall{Method: r.Method, Path: r.URL.Path, Query: map[string]string{}}
	for k, v := range r.URL.Query() {
		call.Query[k] = v[0]
	}
	s.calls = append(s.calls, call)

	if s.failAll {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"status":"error","message":"pme down"}`))
		return
	}

	switch {
	case r.URL.Path == "/api/account/info":
		_, _ = w.Write([]byte(`{"status":"success","limit_used":0.25,"booster_points":0}`))
	case r.URL.Path == "/api/folder/list":
		items := s.table[call.Query["id"]]
		writeJSON(w, map[string]any{"status": "success", "content": items})
	case r.URL.Path == "/api/folder/create":
		id := fmt.Sprintf("created-%d", s.nextID)
		s.nextID++
		s.table[call.Query["parent_id"]] = append(s.table[call.Query["parent_id"]], premiumizeme.Item{ID: id, Name: call.Query["name"], Type: "folder"})
		s.createdFolders = append(s.createdFolders, call.Query["name"])
		writeJSON(w, map[string]any{"status": "success", "id": id})
	case r.URL.Path == "/api/transfer/create":
		_ = r.ParseMultipartForm(1 << 20)
		s.transferTargets = append(s.transferTargets, r.FormValue("folder_id"))
		_, _ = w.Write([]byte(`{"status":"success","id":"transfer-1"}`))
	case r.URL.Path == "/api/folder/paste":
		_, _ = w.Write([]byte(`{"status":"success"}`))
	case r.URL.Path == "/api/folder/delete":
		_, _ = w.Write([]byte(`{"status":"success"}`))
	case r.URL.Path == "/api/item/details":
		writeJSON(w, map[string]any{"status": "success", "type": "file", "link": "http://example.invalid/f"})
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	_ = json.NewEncoder(w).Encode(v)
}

func (s *pmeStub) anyCall(pred func(pmeCall) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if pred(c) {
			return true
		}
	}
	return false
}

// assertNoCallWithin fails (immediately) as soon as a matching call is
// observed, and keeps watching until timeout: an async goroutine that
// triggers the forbidden call after the first poll must still be caught.
func assertNoCallWithin(t *testing.T, s *pmeStub, timeout time.Duration, what string, pred func(pmeCall) bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.anyCall(pred) {
			t.Fatalf("%s: %v", what, s.calls)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newPmeTestClient(t *testing.T, s *pmeStub) *premiumizeme.Premiumizeme {
	t.Helper()
	client := premiumizeme.NewPremiumizemeClient("test-key")
	client.APIBaseURL = s.server.URL + "/api/"
	client.HTTPClient = s.server.Client()
	return &client
}

// newArrSubfolderTestService builds a directory watcher service pointed at a
// stub pme, the way Start() would leave it after the downloads folder was
// resolved: downloadsFolderID set, no watcher (watch mode off).
func newArrSubfolderTestService(t *testing.T, stub *pmeStub, blackhole string, enabled bool, arrs []config.ArrConfig) *DirectoryWatcherService {
	t.Helper()
	dw := NewDirectoryWatcherService()
	dw.Init(newPmeTestClient(t, stub), &config.Config{
		BlackholeDirectory:  blackhole,
		TransferDirectory:   "arrDownloads",
		EnableArrSubfolders: enabled,
		Arrs:                arrs,
	})
	dw.Queue = stringqueue.NewStringQueue()
	dw.downloadsFolderID = "main-id"
	return &dw
}

// TestUpdateConfigRaceAgainstWatcherReaders is the regression test for
// findings R1-3, R1-21 and R1-24: UpdateConfig's in-place struct swap must
// be protected against the service's goroutines (fsnotify readers, the
// upload processor, the config callback) by the one registered mutex, and
// every new read/write site of the shared config and service state must take
// it. Run with -race; the pre-fix tree reports data races on the Arvs slice
// header, BlackholeDirectory string header, the arrFolders map, and
// downloadsFolderID.
func TestUpdateConfigRaceAgainstWatcherReaders(t *testing.T) {
	bh1 := t.TempDir()
	bh2 := t.TempDir()
	sonarrDir1 := filepath.Join(bh1, "sonarr")
	if err := os.MkdirAll(sonarrDir1, 0o755); err != nil {
		t.Fatal(err)
	}
	fileA := filepath.Join(bh1, "root.magnet")
	fileB := filepath.Join(sonarrDir1, "episode.magnet")
	if err := os.WriteFile(fileA, []byte("magnet:?xt=urn:btih:a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fileB, []byte("magnet:?xt=urn:btih:b"), 0o600); err != nil {
		t.Fatal(err)
	}

	sonarr := config.ArrConfig{Name: "sonarr", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}
	radarr := config.ArrConfig{Name: "radarr", URL: "http://127.0.0.1:7878", APIKey: "k", Type: config.Radarr}

	dw := NewDirectoryWatcherService()
	// The callback is installed through the product load path so the
	// unexported appCallback/altConfigLocation fields are set the way
	// cmd/premiumizearrd sets them.
	cfg, err := config.LoadOrCreateConfig(t.TempDir(), func(oldConfig, newConfig config.Config) {
		dw.ConfigUpdatedCallback(oldConfig, newConfig)
	})
	if err != nil {
		t.Fatalf("LoadOrCreateConfig: %v", err)
	}
	cfgArrs := []config.ArrConfig{sonarr}
	cfg.BlackholeDirectory = bh1
	cfg.DownloadsDirectory = t.TempDir()
	cfg.TransferDirectory = "arrDownloads"
	cfg.EnableArrSubfolders = true
	cfg.Arrs = cfgArrs

	stub := newPmeStub(t, false)
	dw.Init(newPmeTestClient(t, stub), &cfg)
	dw.Queue = stringqueue.NewStringQueue()
	dw.downloadsFolderID = "main-id"

	start := make(chan struct{})
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Web-save shape: whole-struct swap through UpdateConfig, which runs the
	// config callback (map snapshot loop + resolveArrFolders) afterwards.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			newCfg := config.Config{
				PremiumizemeAPIKey:                      "test-key",
				BlackholeDirectory:                      bh1,
				DownloadsDirectory:                      cfg.DownloadsDirectory,
				TransferDirectory:                       "arrDownloads",
				PollBlackholeIntervalMinutes:            10,
				SimultaneousDownloads:                   5,
				DownloadSpeedLimit:                      100,
				ArrHistoryUpdateIntervalSeconds:         20,
				ErroredTransferDeleteGracePeriodSeconds: 300,
			}
			// Vary slice length, toggle and blackhole target on every
			// swap so the torn-header windows are exercised, like a user
			// editing the config in the web UI.
			if i%2 == 0 {
				newCfg.EnableArrSubfolders = true
				newCfg.Arrs = []config.ArrConfig{sonarr, radarr}
			} else {
				newCfg.EnableArrSubfolders = false
				newCfg.Arrs = []config.ArrConfig{sonarr}
				newCfg.BlackholeDirectory = bh2
			}
			cfg.UpdateConfig(newCfg)
		}
	}()

	// processUploads goroutine shape: the upload processor resolves and
	// processes files, rewrites downloadsFolderID on transfer-directory
	// changes, and writes resolved slugs into the map.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				dw.processUpload(fileA)
			} else {
				dw.processUpload(fileB)
			}
			// Re-create whatever processUpload consumed so the loop keeps
			// exercising the same read sites.
			if err := os.WriteFile(fileA, []byte("magnet:?xt=urn:btih:a"), 0o600); err != nil {
				t.Error(err)
				return
			}
			if err := os.MkdirAll(sonarrDir1, 0o755); err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(fileB, []byte("magnet:?xt=urn:btih:b"), 0o600); err != nil {
				t.Error(err)
				return
			}
			if i%10 == 0 {
				dw.resolveSingleArrFolder("sonarr", "main-id")
			}
			if i%15 == 0 {
				dw.setTransferDirectory("arrDownloads")
			}
		}
	}()

	// fsnotify reader shape: the event loop's checkFile and the poller's
	// scanArrFolders read the config on every iteration.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for {
			select {
			case <-stop:
				return
			default:
			}
			dw.checkFile(fileA)
			dw.checkFile(sonarrDir1)
			dw.isConfiguredArrSlug("sonarr")
			dw.scanArrFolders()
		}
	}()

	close(start)
	time.Sleep(2500 * time.Millisecond)
	close(stop)
	wg.Wait()
	// Let callback-spawned directory scans drain before the temp dirs go.
	time.Sleep(200 * time.Millisecond)
}

// TestProcessUploadSubfolderGates is the regression test for finding R1-2:
// processUpload must route only files inside the CURRENT blackhole whose
// parent basename is a configured Arr slug into per-Arr resolution.
// Everything else keeps the pre-feature main-folder destination, and no pme
// folder is created for an arbitrary name.
func TestProcessUploadSubfolderGates(t *testing.T) {
	sonarr := config.ArrConfig{Name: "sonarr", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}

	t.Run("feature off keeps main folder destination", func(t *testing.T) {
		stub := newPmeStub(t, false)
		bh := t.TempDir()
		sub := filepath.Join(bh, "sonarr")
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(sub, "x.magnet")
		if err := os.WriteFile(file, []byte("magnet:?xt=urn:btih:x"), 0o600); err != nil {
			t.Fatal(err)
		}

		dw := newArrSubfolderTestService(t, stub, bh, false, nil)
		dw.processUpload(file)

		if got := stub.transferTargets; len(got) != 1 || got[0] != "main-id" {
			t.Fatalf("transfer targets = %v, want [main-id]", got)
		}
		if len(stub.createdFolders) != 0 {
			t.Fatalf("pme folders created = %v, want none", stub.createdFolders)
		}
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Fatalf("source file still exists after successful transfer: %v", err)
		}
	})

	t.Run("feature on unconfigured subfolder keeps main folder destination", func(t *testing.T) {
		stub := newPmeStub(t, false)
		bh := t.TempDir()
		sub := filepath.Join(bh, "not-an-arr")
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(sub, "y.magnet")
		if err := os.WriteFile(file, []byte("magnet:?xt=urn:btih:y"), 0o600); err != nil {
			t.Fatal(err)
		}

		dw := newArrSubfolderTestService(t, stub, bh, true, []config.ArrConfig{sonarr})
		dw.processUpload(file)

		if got := stub.transferTargets; len(got) != 1 || got[0] != "main-id" {
			t.Fatalf("transfer targets = %v, want [main-id]", got)
		}
		if len(stub.createdFolders) != 0 {
			t.Fatalf("pme folders created = %v, want none for an unconfigured name", stub.createdFolders)
		}
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Fatalf("source file still exists after successful transfer: %v", err)
		}
	})

	t.Run("old blackhole leftover goes to main folder", func(t *testing.T) {
		stub := newPmeStub(t, false)
		oldBh := t.TempDir()
		oldSub := filepath.Join(oldBh, "sonarr")
		if err := os.MkdirAll(oldSub, 0o755); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(oldSub, "z.magnet")
		if err := os.WriteFile(file, []byte("magnet:?xt=urn:btih:z"), 0o600); err != nil {
			t.Fatal(err)
		}

		dw := newArrSubfolderTestService(t, stub, t.TempDir(), true, []config.ArrConfig{sonarr})
		dw.processUpload(file)

		if got := stub.transferTargets; len(got) != 1 || got[0] != "main-id" {
			t.Fatalf("transfer targets = %v, want [main-id] for a previous blackhole location", got)
		}
		if len(stub.createdFolders) != 0 {
			t.Fatalf("pme folders created = %v, want none for a parent outside the blackhole", stub.createdFolders)
		}
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Fatalf("source file still exists after successful transfer: %v", err)
		}
	})

	t.Run("configured slug still resolves its subfolder", func(t *testing.T) {
		stub := newPmeStub(t, false)
		bh := t.TempDir()
		sub := filepath.Join(bh, "sonarr")
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(sub, "w.nzb")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}

		dw := newArrSubfolderTestService(t, stub, bh, true, []config.ArrConfig{sonarr})
		dw.processUpload(file)

		if got := stub.transferTargets; len(got) != 1 || got[0] == "" || got[0] == "main-id" {
			t.Fatalf("transfer targets = %v, want the resolved Arr subfolder ID", got)
		}
		if len(stub.createdFolders) != 1 || stub.createdFolders[0] != "sonarr" {
			t.Fatalf("pme folders created = %v, want [sonarr]", stub.createdFolders)
		}
		dw.mu.RLock()
		id, ok := dw.arrFolders["sonarr"]
		dw.mu.RUnlock()
		if !ok || id != stub.transferTargets[0] {
			t.Fatalf("arrFolders[sonarr] = %q, %v; want the ID the transfer used (%q)", id, ok, stub.transferTargets[0])
		}
	})
}

// TestResolveArrFoldersCarriesOverOnPmeFailure is the regression test for
// finding R1-4: a transient premiumize.me failure during re-resolution must
// not discard a previously resolved subfolder ID, and the local subfolder
// must still be created, watched and scanned so the upload can retry once
// premiumize.me recovers.
func TestResolveArrFoldersCarriesOverOnPmeFailure(t *testing.T) {
	sonarr := config.ArrConfig{Name: "sonarr", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}

	stub := newPmeStub(t, true)
	bh := t.TempDir()
	dw := newArrSubfolderTestService(t, stub, bh, true, []config.ArrConfig{sonarr})
	dw.mu.Lock()
	dw.arrFolders = map[string]string{"sonarr": "existing-id"}
	dw.mu.Unlock()

	dw.resolveArrFolders()

	dw.mu.RLock()
	id, present := dw.arrFolders["sonarr"]
	dw.mu.RUnlock()
	if !present || id != "existing-id" {
		t.Fatalf("arrFolders[sonarr] = %q, present=%v; the previously resolved ID must survive a pme failure", id, present)
	}
	if _, err := os.Stat(filepath.Join(bh, "sonarr")); err != nil {
		t.Fatalf("blackhole subfolder not created during the pme outage: %v", err)
	}
}

// TestResolveArrFoldersNoPanicOnUninitializedWatcher is the regression test
// for finding R1-22: a watcher whose Watch() failed leaves Watcher nil, and
// every AddWatchPath/RemoveWatchPath/UpdatePath call site must degrade to a
// logged no-op instead of nil-dereferencing the fsnotify Watcher.
func TestResolveArrFoldersNoPanicOnUninitializedWatcher(t *testing.T) {
	sonarr := config.ArrConfig{Name: "sonarr", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}

	stub := newPmeStub(t, false)
	bh := t.TempDir()
	dw := newArrSubfolderTestService(t, stub, bh, true, []config.ArrConfig{sonarr})
	// Exactly the state a failed Watch() leaves behind.
	dw.watchDirectory = &directory_watcher.WatchDirectory{}

	dw.resolveArrFolders()

	dw.mu.RLock()
	id, ok := dw.arrFolders["sonarr"]
	dw.mu.RUnlock()
	if !ok || id == "" {
		t.Fatalf("arrFolders[sonarr] = %q, %v; want the resolved ID despite the unavailable watcher", id, ok)
	}
	if _, err := os.Stat(filepath.Join(bh, "sonarr")); err != nil {
		t.Fatalf("blackhole subfolder not created: %v", err)
	}

	// The guards themselves: the zero-value watcher reports "not
	// initialized" instead of panicking; Stop degrades to a no-op.
	wd := &directory_watcher.WatchDirectory{}
	for name, fn := range map[string]func() error{
		"AddWatchPath":    func() error { return wd.AddWatchPath("/does/not/matter") },
		"RemoveWatchPath": func() error { return wd.RemoveWatchPath("/does/not/matter") },
		"UpdatePath":      func() error { return wd.UpdatePath("/does/not/matter") },
	} {
		if err := fn(); err == nil {
			t.Errorf("%s on an uninitialized watcher: err = nil, want an error", name)
		}
	}
	if err := wd.Stop(); err != nil {
		t.Errorf("Stop on an uninitialized watcher: err = %v, want nil", err)
	}
}

func newManagerTestService(t *testing.T, stub *pmeStub, cfg *config.Config, arrFolders map[string]string) *TransferManagerService {
	t.Helper()
	m := TransferManagerService{}.New()
	m.premiumizemeClient = newPmeTestClient(t, stub)
	m.config = cfg
	m.downloadsFolderID = "main-id"
	m.arrFoldersMutex.Lock()
	m.arrFolders = arrFolders
	m.arrFoldersMutex.Unlock()
	return &m
}

// TestManagerRenamedArrStaysExcludedFromRootScan is the regression test for
// finding R1-5: an Arr that is renamed away (or that fails re-resolution)
// must stay tracked in arrFolders with an empty ID, so its pme folder keeps
// being excluded from the root scan's download-and-delete path for the
// lifetime of the process; empty IDs are skipped by the per-slug pass.
func TestManagerRenamedArrStaysExcludedFromRootScan(t *testing.T) {
	renamed := config.ArrConfig{Name: "sonarr-2", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}

	stub := newPmeStub(t, false)
	stub.mu.Lock()
	// The pme root still holds the folder of the Arr under its old name.
	stub.table["main-id"] = []premiumizeme.Item{{ID: "old-sonarr-id", Name: "sonarr", Type: "folder"}}
	stub.mu.Unlock()

	m := newManagerTestService(t, stub, &config.Config{
		EnableArrSubfolders:   true,
		Arrs:                  []config.ArrConfig{renamed},
		DownloadsDirectory:    t.TempDir(),
		TransferDirectory:     "arrDownloads",
		SimultaneousDownloads: 5,
		DownloadSpeedLimit:    100,
	}, map[string]string{"sonarr": "old-sonarr-id"})

	m.resolveArrFolders()

	m.arrFoldersMutex.Lock()
	oldID, oldPresent := m.arrFolders["sonarr"]
	newID, newPresent := m.arrFolders["sonarr-2"]
	m.arrFoldersMutex.Unlock()
	if !oldPresent || oldID != "" {
		t.Fatalf("arrFolders[sonarr] = %q, present=%v; a renamed-away Arr must stay tracked with an empty ID", oldID, oldPresent)
	}
	if !newPresent || newID == "" {
		t.Fatalf("arrFolders[sonarr-2] = %q, present=%v; want the newly resolved ID", newID, newPresent)
	}

	m.TaskCheckPremiumizeDownloadsFolder()

	// The old folder must not be handed to HandleFinishedItem (listing,
	// child downloads, or deletion) by either the root scan or the
	// per-slug pass.
	assertNoCallWithin(t, stub, time.Second, "renamed-away Arr folder was handed to the download path", func(c pmeCall) bool {
		return c.Query["id"] == "old-sonarr-id" && (c.Path == "/api/folder/list" || c.Path == "/api/folder/delete")
	})
}

// TestCheckFolderSkipsOnlyFolderItemsNamedLikeSlugs is the regression test
// for finding R1-6: the root-scan exclusion applies to folders only - a
// completed FILE named like a configured Arr slug must be processed, not
// skipped on every poll forever.
func TestCheckFolderSkipsOnlyFolderItemsNamedLikeSlugs(t *testing.T) {
	stub := newPmeStub(t, false)
	stub.mu.Lock()
	stub.table["main-id"] = []premiumizeme.Item{
		{ID: "arr-folder-id", Name: "sonarr", Type: "folder"},
		{ID: "sonarr-file-id", Name: "sonarr", Type: "file"},
		{ID: "other-file-id", Name: "other", Type: "file"},
	}
	stub.mu.Unlock()

	m := newManagerTestService(t, stub, &config.Config{
		EnableArrSubfolders:   true,
		Arrs:                  []config.ArrConfig{{Name: "sonarr", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}},
		DownloadsDirectory:    t.TempDir(),
		TransferDirectory:     "arrDownloads",
		SimultaneousDownloads: 5,
		DownloadSpeedLimit:    100,
	}, map[string]string{"sonarr": "arr-folder-id"})

	if !m.checkFolder("main-id", m.config.DownloadsDirectory, map[string]bool{"sonarr": true}) {
		t.Fatalf("checkFolder returned false, want true (cap not reached)")
	}

	// The file named like the slug must be processed: its single-file
	// wrapper folder is created in the main downloads folder.
	if !stub.anyCall(func(c pmeCall) bool {
		return c.Path == "/api/folder/create" && c.Query["name"] == "sonarr.folder" && c.Query["parent_id"] == "main-id"
	}) {
		t.Fatalf("file item named like a configured slug was not processed: calls = %v", stub.calls)
	}

	// The folder container must stay excluded from the root scan's
	// download-and-delete path.
	assertNoCallWithin(t, stub, time.Second, "Arr container folder was handed to the root scan", func(c pmeCall) bool {
		return c.Query["id"] == "arr-folder-id" && (c.Path == "/api/folder/list" || c.Path == "/api/folder/delete")
	})
}

// waitForQueuedPath pops the queue until want is observed or the deadline
// passes; other queued paths are discarded as the loop runs.
func waitForQueuedPath(t *testing.T, q *stringqueue.StringQueue, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for {
			isFile, p := q.PopTopOfQueue()
			if !isFile {
				break
			}
			if p == want {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("path %s was never queued within %s; queue: %v", want, timeout, q.GetQueue())
}

// TestCheckFileReestablishesWatchOnRecreatedSubfolder is the regression
// test for finding R1-20: a configured Arr subfolder that lost its watch
// (pme outage at resolution, delete+recreate, failed AddWatchPath) must be
// re-watched and rescanned when it (re)appears in the blackhole, so files
// dropped into it are queued in default watch mode instead of being
// silently ignored forever.
func TestCheckFileReestablishesWatchOnRecreatedSubfolder(t *testing.T) {
	sonarr := config.ArrConfig{Name: "sonarr", URL: "http://127.0.0.1:8989", APIKey: "k", Type: config.Sonarr}
	bh := t.TempDir()
	sonarrDir := filepath.Join(bh, "sonarr")
	if err := os.MkdirAll(sonarrDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dw := NewDirectoryWatcherService()
	dw.config = &config.Config{
		BlackholeDirectory:  bh,
		EnableArrSubfolders: true,
		Arrs:                []config.ArrConfig{sonarr},
	}
	dw.Queue = stringqueue.NewStringQueue()
	dw.watchDirectory = directory_watcher.NewDirectoryWatcher(bh, true, dw.checkFile, dw.addFileToQueue)
	if err := dw.watchDirectory.Watch(); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer dw.watchDirectory.Stop()
	if err := dw.watchDirectory.AddWatchPath(sonarrDir); err != nil {
		t.Fatalf("AddWatchPath: %v", err)
	}

	// Control: the harness must deliver a file created in the watched root.
	control := filepath.Join(bh, "control.magnet")
	if err := os.WriteFile(control, []byte("magnet:?xt=urn:btih:control"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForQueuedPath(t, dw.Queue, control, 5*time.Second)

	// A file in the still-watched subfolder is queued (pre-existing path).
	inside := filepath.Join(sonarrDir, "inside.torrent")
	if err := os.WriteFile(inside, []byte("bt"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForQueuedPath(t, dw.Queue, inside, 5*time.Second)

	// Delete and recreate the subfolder: its watch dies with the deleted
	// directory and must be re-established by the Create event.
	if err := os.RemoveAll(sonarrDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sonarrDir, 0o755); err != nil {
		t.Fatal(err)
	}
	episode := filepath.Join(sonarrDir, "episode.torrent")
	if err := os.WriteFile(episode, []byte("bt"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForQueuedPath(t, dw.Queue, episode, 5*time.Second)
}
