package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// stateFileVersion identifies the multi-object checkpoint format written by stateStore.
const stateFileVersion = 2

// stateFlushDelay bounds how often download progress is persisted. Losing at most this much progress
// on a hard kill only means re-fetching a few parts; writing on every part would rewrite the file
// hundreds of times per second when many files are in flight.
const stateFlushDelay = time.Second

// UploadCheckpoint is the resumable state of one multipart upload. Completed parts are not stored:
// S3 (ListParts) is the source of truth, so the checkpoint only has to survive with the UploadID.
type UploadCheckpoint struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	UploadID string `json:"upload_id"`
	PartSize int64  `json:"part_size"`
	FileSize int64  `json:"file_size"`
	// SourceModTime (UnixNano) detects a source file modified between pause and resume, which would
	// otherwise stitch old and new content into one object. 0 = unknown (legacy state file).
	SourceModTime int64 `json:"source_mtime_ns,omitempty"`
}

// DownloadCheckpoint is the resumable state of one ranged download into a ".s5part" file.
type DownloadCheckpoint struct {
	Bucket   string         `json:"bucket"`
	Key      string         `json:"key"`
	PartPath string         `json:"part_path"`
	PartSize int64          `json:"part_size"`
	FileSize int64          `json:"file_size"`
	ETag     string         `json:"etag,omitempty"`
	Parts    map[int64]bool `json:"parts"`
}

// CompletedObject records an object fully transferred by this job, so a resumed run skips it even in
// overwrite mode (AzCopy job-plan semantics): Version is the source file's mtime (uploads) or the
// object's ETag (downloads).
type CompletedObject struct {
	Size    int64  `json:"size"`
	Version string `json:"version"`
}

type stateFileData struct {
	Version   int                            `json:"version"`
	Uploads   map[string]*UploadCheckpoint   `json:"uploads"`
	Downloads map[string]*DownloadCheckpoint `json:"downloads"`
	Completed map[string]*CompletedObject    `json:"completed,omitempty"`
}

// stateStore holds the checkpoints of every object a process transfers, keyed per object, in one
// file. A single store per path (shared by all workers) serialises updates, so parallel transfers
// never overwrite each other's checkpoints.
type stateStore struct {
	path    string
	mu      sync.Mutex // guards data, dirty, timer
	data    stateFileData
	dirty   bool
	timer   *time.Timer
	writeMu sync.Mutex // serialises disk writes
}

var (
	stateStoresMu sync.Mutex
	stateStores   = map[string]*stateStore{}
)

func uploadStateKey(bucket, key string) string { return "s3://" + bucket + "/" + key }

func downloadStateKey(bucket, key, dst string) string {
	return "s3://" + bucket + "/" + key + " -> " + dst
}

// openStateStore returns the process-wide store for path, loading it on first use.
func openStateStore(path string) *stateStore {
	stateStoresMu.Lock()
	defer stateStoresMu.Unlock()
	if st, ok := stateStores[path]; ok {
		return st
	}
	st := &stateStore{path: path}
	st.data = loadStateFile(path)
	stateStores[path] = st
	return st
}

func emptyStateData() stateFileData {
	return stateFileData{
		Version:   stateFileVersion,
		Uploads:   map[string]*UploadCheckpoint{},
		Downloads: map[string]*DownloadCheckpoint{},
		Completed: map[string]*CompletedObject{},
	}
}

// loadStateFile reads a v2 store, or imports a legacy single-object state file.
func loadStateFile(path string) stateFileData {
	data := emptyStateData()
	raw, err := os.ReadFile(path)
	if err != nil {
		return data
	}
	var probe map[string]json.RawMessage
	if json.Unmarshal(raw, &probe) != nil {
		return data
	}
	if _, ok := probe["version"]; ok {
		var v2 stateFileData
		if json.Unmarshal(raw, &v2) == nil {
			if v2.Uploads != nil {
				data.Uploads = v2.Uploads
			}
			if v2.Downloads != nil {
				data.Downloads = v2.Downloads
			}
			if v2.Completed != nil {
				data.Completed = v2.Completed
			}
		}
		return data
	}
	// Legacy format: one object per file (written by earlier builds).
	if _, ok := probe["part_path"]; ok {
		var d DownloadCheckpoint
		if json.Unmarshal(raw, &d) == nil && d.Bucket != "" && d.Key != "" {
			if d.Parts == nil {
				d.Parts = map[int64]bool{}
			}
			dst := d.PartPath[:len(d.PartPath)-len(filepath.Ext(d.PartPath))]
			data.Downloads[downloadStateKey(d.Bucket, d.Key, dst)] = &d
		}
		return data
	}
	var u UploadCheckpoint
	if json.Unmarshal(raw, &u) == nil && u.Bucket != "" && u.Key != "" && u.UploadID != "" {
		data.Uploads[uploadStateKey(u.Bucket, u.Key)] = &u
	}
	return data
}

func (st *stateStore) getUpload(key string) (UploadCheckpoint, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	cp, ok := st.data.Uploads[key]
	if !ok {
		return UploadCheckpoint{}, false
	}
	return *cp, true
}

// putUpload records an upload and persists it immediately: losing an UploadID would orphan the
// multipart upload (and its stored parts) on the server.
func (st *stateStore) putUpload(key string, cp UploadCheckpoint) error {
	st.mu.Lock()
	st.data.Uploads[key] = &cp
	st.mu.Unlock()
	return st.flush()
}

func (st *stateStore) deleteUpload(key string) error {
	st.mu.Lock()
	delete(st.data.Uploads, key)
	st.mu.Unlock()
	return st.flush()
}

func (st *stateStore) getDownload(key string) (DownloadCheckpoint, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	cp, ok := st.data.Downloads[key]
	if !ok {
		return DownloadCheckpoint{}, false
	}
	c := *cp
	c.Parts = make(map[int64]bool, len(cp.Parts))
	for k, v := range cp.Parts {
		c.Parts[k] = v
	}
	return c, true
}

func (st *stateStore) putDownload(key string, cp DownloadCheckpoint) error {
	if cp.Parts == nil {
		cp.Parts = map[int64]bool{}
	}
	st.mu.Lock()
	st.data.Downloads[key] = &cp
	st.mu.Unlock()
	return st.flush()
}

// markDownloadPart records a completed part; the write is coalesced (see stateFlushDelay).
func (st *stateStore) markDownloadPart(key string, part int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	cp, ok := st.data.Downloads[key]
	if !ok {
		return
	}
	cp.Parts[part] = true
	st.dirty = true
	if st.timer == nil {
		st.timer = time.AfterFunc(stateFlushDelay, func() { _ = st.flush() })
	}
}

func (st *stateStore) deleteDownload(key string) error {
	st.mu.Lock()
	delete(st.data.Downloads, key)
	st.mu.Unlock()
	return st.flush()
}

// snapshot returns copies of all checkpoints (used by abort).
func (st *stateStore) snapshot() ([]UploadCheckpoint, []DownloadCheckpoint) {
	st.mu.Lock()
	defer st.mu.Unlock()
	ups := make([]UploadCheckpoint, 0, len(st.data.Uploads))
	for _, u := range st.data.Uploads {
		ups = append(ups, *u)
	}
	downs := make([]DownloadCheckpoint, 0, len(st.data.Downloads))
	for _, d := range st.data.Downloads {
		downs = append(downs, *d)
	}
	return ups, downs
}

// flush writes the store atomically (temp file + fsync + rename) and removes the file once no
// checkpoints remain.
func (st *stateStore) flush() error {
	st.mu.Lock()
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
	st.dirty = false
	empty := len(st.data.Uploads) == 0 && len(st.data.Downloads) == 0 && len(st.data.Completed) == 0
	var payload []byte
	if !empty {
		var err error
		payload, err = json.Marshal(st.data)
		if err != nil {
			st.mu.Unlock()
			return err
		}
	}
	st.mu.Unlock()

	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	if empty {
		_ = os.Remove(st.path + ".tmp")
		if err := os.Remove(st.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return writeFileAtomic(st.path, payload)
}

func writeFileAtomic(path string, payload []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write state file: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		return fmt.Errorf("write state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close state file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}
	return nil
}

func (st *stateStore) isCompleted(key string, size int64, version string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	c, ok := st.data.Completed[key]
	return ok && c.Size == size && c.Version == version && version != ""
}

// markCompleted records a finished object; the write is coalesced like download progress.
func (st *stateStore) markCompleted(key string, size int64, version string) {
	if version == "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.data.Completed[key] = &CompletedObject{Size: size, Version: version}
	st.dirty = true
	if st.timer == nil {
		st.timer = time.AfterFunc(stateFlushDelay, func() { _ = st.flush() })
	}
}

func (st *stateStore) clearCompleted() error {
	st.mu.Lock()
	st.data.Completed = map[string]*CompletedObject{}
	st.mu.Unlock()
	return st.flush()
}

// UploadCompletedKey / DownloadCompletedKey identify an object within a job's completion records.
func UploadCompletedKey(src, dst string) string   { return "up:" + src + " -> " + dst }
func DownloadCompletedKey(src, dst string) string { return "down:" + src + " -> " + dst }

// StateHasCompleted reports whether any completion record exists for key (cheap pre-check before a HEAD).
func StateHasCompleted(path, key string) bool {
	if path == "" {
		return false
	}
	st := openStateStore(path)
	st.mu.Lock()
	defer st.mu.Unlock()
	_, ok := st.data.Completed[key]
	return ok
}

// StateCompleted reports whether the job's state file records the object as already transferred with
// the same size and version (a resumed run then skips it).
func StateCompleted(path, key string, size int64, version string) bool {
	if path == "" {
		return false
	}
	return openStateStore(path).isCompleted(key, size, version)
}

// MarkStateCompleted records a transferred object in the job's state file.
func MarkStateCompleted(path, key string, size int64, version string) {
	if path != "" {
		openStateStore(path).markCompleted(key, size, version)
	}
}

// ClearStateCompleted drops the completion records once the whole command has succeeded (the state file
// is removed when nothing else remains in it).
func ClearStateCompleted(path string) {
	if path != "" {
		_ = openStateStore(path).clearCompleted()
	}
}

// FlushStateStores persists pending checkpoint updates of every open store (called on exit).
func FlushStateStores() {
	stateStoresMu.Lock()
	stores := make([]*stateStore, 0, len(stateStores))
	for _, st := range stateStores {
		stores = append(stores, st)
	}
	stateStoresMu.Unlock()
	for _, st := range stores {
		st.mu.Lock()
		dirty := st.dirty
		st.mu.Unlock()
		if dirty {
			_ = st.flush()
		}
	}
}
