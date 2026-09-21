package service

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ensingerphilipp/premiumizearr-nova/internal/config"
	"github.com/ensingerphilipp/premiumizearr-nova/internal/directory_watcher"
	"github.com/ensingerphilipp/premiumizearr-nova/internal/utils"
	"github.com/ensingerphilipp/premiumizearr-nova/pkg/premiumizeme"
	"github.com/ensingerphilipp/premiumizearr-nova/pkg/stringqueue"
	log "github.com/sirupsen/logrus"
)

type DirectoryWatcherService struct {
	mu                 sync.RWMutex
	premiumizemeClient *premiumizeme.Premiumizeme
	config             *config.Config
	Queue              *stringqueue.StringQueue
	status             string
	quotaBlocked       bool
	quotaCheckFailed   bool
	downloadsFolderID  string
	watchDirectory     *directory_watcher.WatchDirectory
	arrFolders         map[string]string // Arr slug -> pme subfolder ID
}

const (
	ERROR_LIMIT_REACHED    = "Limit of transfers reached!"
	ERROR_ALREADY_UPLOADED = "You already added this job."
)

func NewDirectoryWatcherService() DirectoryWatcherService {
	return DirectoryWatcherService{
		premiumizemeClient: nil,
		config:             nil,
		Queue:              nil,
		status:             "",
		downloadsFolderID:  "",
	}
}

func (dw *DirectoryWatcherService) Init(premiumizemeClient *premiumizeme.Premiumizeme, config *config.Config) {
	dw.premiumizemeClient = premiumizemeClient
	dw.config = config
	// Register this service's lock as the config swap lock: UpdateConfig
	// takes it around the in-place struct replacement, and this service's
	// goroutines read config fields under the matching read lock.
	dw.config.SetUpdateMu(&dw.mu)
}

func (dw *DirectoryWatcherService) ConfigUpdatedCallback(currentConfig config.Config, newConfig config.Config) {
	blackholeChanged := currentConfig.BlackholeDirectory != newConfig.BlackholeDirectory
	transferChanged := currentConfig.TransferDirectory != newConfig.TransferDirectory
	arrsChanged := !reflect.DeepEqual(currentConfig.Arrs, newConfig.Arrs)
	toggleChanged := currentConfig.EnableArrSubfolders != newConfig.EnableArrSubfolders

	if blackholeChanged {
		log.Info("Blackhole directory changed, restarting directory watcher...")
		log.Info("Running initial directory scan...")
		dw.mu.RLock()
		blackholeDir := dw.config.BlackholeDirectory
		dw.mu.RUnlock()
		go dw.directoryScan(blackholeDir)

		if dw.watchDirectory != nil {
			dw.watchDirectory.UpdatePath(newConfig.BlackholeDirectory)

			// Snapshot the keys under the lock: the map must not be
			// iterated while resolveSingleArrFolder writes to it.
			dw.mu.RLock()
			slugs := make([]string, 0, len(dw.arrFolders))
			for slug := range dw.arrFolders {
				slugs = append(slugs, slug)
			}
			dw.mu.RUnlock()
			for _, slug := range slugs {
				dw.watchDirectory.RemoveWatchPath(filepath.Join(currentConfig.BlackholeDirectory, slug))
			}
		}
	}

	if transferChanged {
		log.Info("TransferDirectory directory changed, changing directory watcher...")
		dw.setTransferDirectory(newConfig.TransferDirectory)
	}

	if currentConfig.PollBlackholeDirectory != newConfig.PollBlackholeDirectory {
		log.Info("Poll blackhole directory changed, restarting directory watcher...")
	}

	if blackholeChanged || transferChanged || arrsChanged || toggleChanged {
		dw.resolveArrFolders()
	}
}

func (dw *DirectoryWatcherService) GetStatus() string {
	dw.mu.RLock()
	defer dw.mu.RUnlock()
	return dw.status
}

// Start: This is the entrypoint for the directory watcher
func (dw *DirectoryWatcherService) Start() {
	log.Info("Starting directory watcher...")

	log.Info("Creating Queue...")
	dw.Queue = stringqueue.NewStringQueue()

	dw.mu.RLock()
	transferDir := dw.config.TransferDirectory
	blackholeDir := dw.config.BlackholeDirectory
	poll := dw.config.PollBlackholeDirectory
	dw.mu.RUnlock()

	newID := utils.GetDownloadsFolderIDFromPremiumizeme(dw.premiumizemeClient, transferDir)
	dw.mu.Lock()
	dw.downloadsFolderID = newID
	dw.mu.Unlock()

	log.Info("Starting uploads processor...")
	go dw.processUploads()

	log.Info("Running initial directory scan...")
	go dw.directoryScan(blackholeDir)

	if dw.watchDirectory != nil {
		log.Info("Stopping directory watcher...")
		err := dw.watchDirectory.Stop()
		if err != nil {
			log.Errorf("Error stopping directory watcher: %s", err)
		}
	}

	if poll {
		log.Info("Starting directory poller...")
		go func() {
			for {
				dw.mu.RLock()
				polling := dw.config.PollBlackholeDirectory
				interval := dw.config.PollBlackholeIntervalMinutes
				dw.mu.RUnlock()
				if !polling {
					log.Info("Directory poller stopped")
					break
				}
				time.Sleep(time.Duration(interval) * time.Minute)
				dw.mu.RLock()
				directory := dw.config.BlackholeDirectory
				dw.mu.RUnlock()
				log.Infof("Running directory scan of %s", directory)
				dw.directoryScan(directory)
				dw.scanArrFolders()
				log.Infof("Scan complete, next scan in %d minutes", interval)
			}
		}()
	} else {
		log.Info("Starting directory watcher...")
		dw.watchDirectory = directory_watcher.NewDirectoryWatcher(blackholeDir,
			true,
			dw.checkFile,
			dw.addFileToQueue,
		)
		if err := dw.watchDirectory.Watch(); err != nil {
			// A watcher that cannot be created degrades to the one-time
			// startup scan instead of crashing the daemon: nil the handle so
			// the AddWatchPath/RemoveWatchPath/UpdatePath call sites stay
			// no-ops.
			log.Errorf("Error starting directory watcher: %s", err)
			dw.watchDirectory = nil
		}
	}

	dw.resolveArrFolders()
}

// clearArrFolders stops watching all known Arr subfolders and forgets their
// pme IDs, without deleting anything on disk or on pme.
func (dw *DirectoryWatcherService) clearArrFolders(blackholeDir string) {
	dw.mu.Lock()
	oldFolders := dw.arrFolders
	dw.arrFolders = nil
	dw.mu.Unlock()

	if dw.watchDirectory != nil {
		for slug := range oldFolders {
			dw.watchDirectory.RemoveWatchPath(filepath.Join(blackholeDir, slug))
		}
	}
}

// resolveArrFolders ensures each configured Arr has a local blackhole subfolder
// and a matching pme subfolder, and watches/scans it for existing files.
func (dw *DirectoryWatcherService) resolveArrFolders() {
	// Snapshot everything under the read lock before any network/FS work:
	// UpdateConfig swaps the whole config struct on the web goroutine and
	// an RWMutex is not reentrant, so the loop below must not read
	// dw.config again.
	dw.mu.RLock()
	enabled := dw.config.EnableArrSubfolders
	blackholeDir := dw.config.BlackholeDirectory
	arrs := make([]config.ArrConfig, len(dw.config.Arrs))
	copy(arrs, dw.config.Arrs)
	mainFolderID := dw.downloadsFolderID
	oldFolders := make(map[string]string, len(dw.arrFolders))
	for slug, folderID := range dw.arrFolders {
		oldFolders[slug] = folderID
	}
	dw.mu.RUnlock()

	if !enabled {
		dw.clearArrFolders(blackholeDir)
		return
	}

	newFolders := make(map[string]string, len(arrs))

	for _, arr := range arrs {
		local := filepath.Join(blackholeDir, arr.Name)
		id, err := utils.GetOrCreateSubfolderID(dw.premiumizemeClient, mainFolderID, arr.Name)
		if err != nil {
			log.Errorf("Cannot resolve premiumize.me subfolder for Arr %s: %s", arr.Name, err)
			// A transient premiumize.me failure must not discard a
			// previously resolved subfolder ID: carry the mapping forward so
			// queued uploads keep their target folder.
			if prev, ok := oldFolders[arr.Name]; ok {
				newFolders[arr.Name] = prev
			}
			// The local subfolder is still created, watched and scanned:
			// the watch stays in place so an upload into it can retry the
			// resolution once premiumize.me recovers.
			if err := os.MkdirAll(local, os.ModePerm); err != nil {
				log.Errorf("Cannot create blackhole subfolder for Arr %s: %s", arr.Name, err)
			} else {
				dw.watchArrFolder(local)
				dw.directoryScan(local)
			}
			continue
		}

		if err := os.MkdirAll(local, os.ModePerm); err != nil {
			log.Errorf("Cannot create blackhole subfolder for Arr %s: %s", arr.Name, err)
			// Drop the entry: a slug whose local subfolder could not be
			// created is not fully resolved.
			continue
		}
		newFolders[arr.Name] = id
		dw.watchArrFolder(local)
		dw.directoryScan(local)
	}

	dw.mu.Lock()
	dw.arrFolders = newFolders
	dw.mu.Unlock()

	configuredSlugs := make(map[string]bool, len(arrs))
	for _, arr := range arrs {
		configuredSlugs[arr.Name] = true
	}

	for slug := range oldFolders {
		if configuredSlugs[slug] {
			continue
		}
		if dw.watchDirectory != nil {
			dw.watchDirectory.RemoveWatchPath(filepath.Join(blackholeDir, slug))
		}
		log.Infof("Arr %s no longer configured, local/premiumize subfolder is kept but no longer watched", slug)
	}
}

// watchArrFolder registers the watch on a local Arr subfolder when the
// watcher is available; a failed registration is logged, not fatal.
func (dw *DirectoryWatcherService) watchArrFolder(local string) {
	if dw.watchDirectory == nil {
		return
	}
	if err := dw.watchDirectory.AddWatchPath(local); err != nil {
		log.Errorf("Cannot watch blackhole subfolder %s: %s", local, err)
	}
}

func (dw *DirectoryWatcherService) scanArrFolders() {
	dw.mu.RLock()
	enabled := dw.config.EnableArrSubfolders
	blackholeDir := dw.config.BlackholeDirectory
	arrs := make([]config.ArrConfig, len(dw.config.Arrs))
	copy(arrs, dw.config.Arrs)
	dw.mu.RUnlock()

	if !enabled {
		return
	}

	for _, arr := range arrs {
		local := filepath.Join(blackholeDir, arr.Name)
		if _, err := os.Stat(local); err != nil {
			continue
		}
		dw.directoryScan(local)
	}
}

func (dw *DirectoryWatcherService) directoryScan(p string) {
	log.Trace("Running directory scan")
	files, err := ioutil.ReadDir(p)
	if err != nil {
		log.Errorf("Error with directory scan %+v", err)
		return
	}

	for _, file := range files {
		filePath := path.Join(p, file.Name())
		if dw.checkFile(filePath) == 1 {
			dw.addFileToQueue(filePath)
		}
	}
}

func (dw *DirectoryWatcherService) checkFile(path string) int {
	log.Tracef("Checking file %s", path)

	fi, err := os.Stat(path)
	if err != nil {
		log.Errorf("Error checking file %s", path)
		return 0
	}

	if fi.IsDir() {
		if dw.isConfiguredArrSlug(filepath.Base(path)) {
			log.Tracef("Directory %s is a configured Arr subfolder, handled separately", path)
			// (Re)establish the subfolder's own watch - fsnotify re-Add of an
			// already-watched path is idempotent - and rescan it, so a
			// subfolder that lost its watch (pme outage at resolution,
			// delete+recreate, failed AddWatchPath) self-heals in watch mode
			// and files that landed while it was down get queued.
			dw.watchArrFolder(path)
			go dw.directoryScan(path)
			return 0
		}
		log.Errorf("Directory created in blackhole %s ignoring (Warning premiumizearrd does not look in subfolders!)", path)
		return 2
	}

	ext := filepath.Ext(path)
	if ext == ".nzb" || ext == ".magnet" || ext == ".torrent" {
		return 1
	} else {
		return 0
	}
}

func (dw *DirectoryWatcherService) isConfiguredArrSlug(slug string) bool {
	dw.mu.RLock()
	defer dw.mu.RUnlock()
	if !dw.config.EnableArrSubfolders {
		return false
	}
	for _, arr := range dw.config.Arrs {
		if arr.Name == slug {
			return true
		}
	}
	return false
}

func (dw *DirectoryWatcherService) addFileToQueue(path string) {
	if !dw.Queue.AddIfAbsent(path) {
		return
	}
	log.Infof("File created in blackhole %s added to Queue. Queue length %d", path, dw.Queue.Len())
}

// resolveSingleArrFolder resolves (creating if needed) the pme subfolder for
// one Arr slug on demand, using the caller's snapshot of the main folder ID.
func (dw *DirectoryWatcherService) resolveSingleArrFolder(slug string, mainFolderID string) (string, bool) {
	dw.mu.RLock()
	enabled := dw.config.EnableArrSubfolders
	dw.mu.RUnlock()
	if !enabled {
		return "", false
	}

	id, err := utils.GetOrCreateSubfolderID(dw.premiumizemeClient, mainFolderID, slug)
	if err != nil {
		log.Errorf("Cannot resolve premiumize.me subfolder for Arr %s: %s", slug, err)
		return "", false
	}

	dw.mu.Lock()
	if dw.arrFolders == nil {
		dw.arrFolders = map[string]string{}
	}
	dw.arrFolders[slug] = id
	dw.mu.Unlock()

	return id, true
}

func resolveTargetFolderID(filePath, blackholeDir, mainFolderID string, arrFolders map[string]string) (folderID string, ok bool, slug string) {
	dir := filepath.Clean(filepath.Dir(filePath))
	cleanBlackhole := filepath.Clean(blackholeDir)
	if dir == cleanBlackhole {
		return mainFolderID, true, ""
	}
	// A file whose parent is not the current blackhole and not a direct
	// subfolder of it (a leftover of a previous blackhole location) goes to
	// the main downloads folder: reporting a slug for it would make
	// processUpload create a pme folder named after a directory the user
	// never defined.
	if !strings.HasPrefix(dir, cleanBlackhole+string(filepath.Separator)) {
		return mainFolderID, mainFolderID != "", ""
	}

	slug = filepath.Base(dir)
	id, found := arrFolders[slug]
	return id, found, slug
}

func (dw *DirectoryWatcherService) processUploads() {
	for {
		processed := dw.processUploadCycle()
		if processed == 0 {
			if dw.Queue.Len() == 0 {
				log.Trace("No files in queue, sleeping for 10 seconds")
			} else {
				log.Trace("Blackhole submissions are paused, checking quota again in 10 seconds")
			}
			time.Sleep(time.Second * time.Duration(10))
		} else {
			time.Sleep(2 * time.Second)
		}
	}
}

// processUploadCycle checks the account once and processes the files that were
// queued at the start of the cycle. Files added during processing wait for the
// next cycle and its fresh quota check.
func (dw *DirectoryWatcherService) processUploadCycle() int {
	queuedFiles := dw.Queue.Len()
	if queuedFiles == 0 || !dw.submissionsAllowed() {
		return 0
	}

	processed := 0
	for range queuedFiles {
		isQueueFile, filePath := dw.Queue.PopTopOfQueue()
		if !isQueueFile {
			break
		}
		if filePath == "" {
			log.Error("Received an empty path from the blackhole queue")
			continue
		}

		if processed > 0 {
			time.Sleep(2 * time.Second)
		}
		processed++
		if dw.processUpload(filePath) {
			// The account check and transfer submission are separate requests, so
			// the limit can be reached between them. Put the file back in the
			// queue and stop this batch so watcher mode retries it after backoff.
			dw.Queue.Add(filePath)
			return 0
		}
	}

	return processed
}

func (dw *DirectoryWatcherService) submissionsAllowed() bool {
	accountInfo, err := dw.premiumizemeClient.GetAccountInfo()
	if err != nil {
		dw.mu.Lock()
		alreadyFailed := dw.quotaCheckFailed
		dw.quotaCheckFailed = true
		dw.status = "Premiumize fair-use quota unknown; continuing submissions"
		// Preserve the last known quota state across failed lookups so a
		// transient error does not duplicate pause logs or hide recovery.
		dw.mu.Unlock()
		if !alreadyFailed {
			log.Warnf("Could not check Premiumize fair-use quota; continuing with existing submission behavior: %s", err)
		}
		return true
	}

	exhausted := accountInfo.QuotaExhausted()
	dw.mu.Lock()
	dw.quotaCheckFailed = false
	wasBlocked := dw.quotaBlocked
	dw.quotaBlocked = exhausted
	if exhausted {
		dw.status = "Paused: Premiumize fair-use quota exhausted"
	} else {
		dw.status = "Okay"
	}
	dw.mu.Unlock()

	if exhausted && !wasBlocked {
		log.Warn("Premiumize fair-use quota is exhausted and no booster points are available; new blackhole submissions are paused and files will remain untouched")
	} else if !exhausted && wasBlocked {
		log.Info("Premiumize fair-use quota is available; resuming blackhole submissions")
	}

	return !exhausted
}

// processUpload returns true when the source should be queued for a later retry.
func (dw *DirectoryWatcherService) processUpload(filePath string) bool {
	log.Debugf("Processing %s", filePath)
	dw.mu.RLock()
	folderID, ok, slug := resolveTargetFolderID(filePath, dw.config.BlackholeDirectory, dw.downloadsFolderID, dw.arrFolders)
	mainFolderID := dw.downloadsFolderID
	dw.mu.RUnlock()
	if !ok && slug != "" {
		if dw.isConfiguredArrSlug(slug) {
			folderID, ok = dw.resolveSingleArrFolder(slug, mainFolderID)
		} else {
			// A file in a subfolder that is not a configured Arr (feature
			// off, or a name no Arr uses) keeps the pre-feature destination
			// instead of having a pme folder created for the arbitrary name.
			folderID, ok = mainFolderID, mainFolderID != ""
		}
	}
	if !ok {
		log.Errorf("No resolved target folder for %s, skipping upload", filePath)
		return false
	}

	err := dw.premiumizemeClient.CreateTransfer(filePath, folderID)
	if err != nil {
		switch err.Error() {
		case ERROR_LIMIT_REACHED:
			dw.mu.Lock()
			dw.status = "Limit of transfers reached!"
			dw.mu.Unlock()
			log.Trace("Transfer limit reached; the source file remains in the blackhole directory")
			return true
		case ERROR_ALREADY_UPLOADED:
			log.Trace("File already uploaded, removing from disk")
			if err := os.Remove(filePath); err != nil {
				log.Errorf("Could not delete %s: %+v", filePath, err)
			}
		default:
			log.Errorf("Error creating transfer: %s", err)
		}
		return false
	}

	dw.mu.Lock()
	dw.status = "Okay"
	dw.mu.Unlock()
	if err := os.Remove(filePath); err != nil {
		log.Errorf("Could not delete %s: %+v", filePath, err)
		return false
	}
	log.Infof("Removed %s from blackhole queue. Queue size: %d", filePath, dw.Queue.Len())
	return false
}

func (dw *DirectoryWatcherService) setTransferDirectory(newDir string) {
	newID := utils.GetDownloadsFolderIDFromPremiumizeme(dw.premiumizemeClient, newDir)

	dw.mu.Lock()
	defer dw.mu.Unlock()

	dw.downloadsFolderID = newID
}
