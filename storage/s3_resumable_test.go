package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func resetStateStores() {
	stateStoresMu.Lock()
	stateStores = map[string]*stateStore{}
	stateStoresMu.Unlock()
}

func TestStateStoreKeepsCheckpointsPerObject(t *testing.T) {
	resetStateStores()
	path := filepath.Join(t.TempDir(), "job_state.json")
	st := openStateStore(path)

	// Many objects checkpointed concurrently must all survive (the old format kept only the last).
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := uploadStateKey("bkt", fmt.Sprintf("dir/file%02d.bin", i))
			if err := st.putUpload(key, UploadCheckpoint{Bucket: "bkt", Key: key, UploadID: fmt.Sprintf("u%d", i),
				PartSize: 64 << 20, FileSize: 1 << 30}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	resetStateStores() // simulate a new process
	reloaded := openStateStore(path)
	ups, _ := reloaded.snapshot()
	if len(ups) != 50 {
		t.Fatalf("checkpoints after reload = %d, want 50", len(ups))
	}
	if cp, ok := reloaded.getUpload(uploadStateKey("bkt", "dir/file07.bin")); !ok || cp.UploadID != "u7" {
		t.Fatalf("file07 checkpoint = %+v (found=%v)", cp, ok)
	}
}

func TestStateStoreRemovesFileWhenEmpty(t *testing.T) {
	resetStateStores()
	path := filepath.Join(t.TempDir(), "job_state.json")
	st := openStateStore(path)
	key := uploadStateKey("bkt", "a.bin")
	if err := st.putUpload(key, UploadCheckpoint{Bucket: "bkt", Key: "a.bin", UploadID: "u"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not written: %v", err)
	}
	if err := st.deleteUpload(key); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("state file should be removed once empty, stat err = %v", err)
	}
}

func TestStateStoreCoalescesDownloadProgress(t *testing.T) {
	resetStateStores()
	path := filepath.Join(t.TempDir(), "job_state.json")
	st := openStateStore(path)
	key := downloadStateKey("bkt", "big.bin", "/data/big.bin")
	if err := st.putDownload(key, DownloadCheckpoint{Bucket: "bkt", Key: "big.bin", PartPath: "/data/big.bin.s5part",
		PartSize: 8, FileSize: 80}); err != nil {
		t.Fatal(err)
	}
	for p := int64(1); p <= 5; p++ {
		st.markDownloadPart(key, p)
	}
	FlushStateStores()

	resetStateStores()
	cp, ok := openStateStore(path).getDownload(key)
	if !ok || len(cp.Parts) != 5 {
		t.Fatalf("download parts after flush = %v (found=%v), want 5", cp.Parts, ok)
	}
}

func TestStateStoreDelayedFlushWritesProgress(t *testing.T) {
	resetStateStores()
	path := filepath.Join(t.TempDir(), "job_state.json")
	st := openStateStore(path)
	key := downloadStateKey("bkt", "big.bin", "/data/big.bin")
	_ = st.putDownload(key, DownloadCheckpoint{Bucket: "bkt", Key: "big.bin", PartPath: "/data/big.bin.s5part",
		PartSize: 8, FileSize: 80})
	st.markDownloadPart(key, 3)
	time.Sleep(stateFlushDelay + 500*time.Millisecond)

	raw, _ := os.ReadFile(path)
	var data stateFileData
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	if !data.Downloads[key].Parts[3] {
		t.Fatalf("part 3 not persisted by the delayed flush: %s", raw)
	}
}

func TestLoadLegacyUploadState(t *testing.T) {
	resetStateStores()
	path := filepath.Join(t.TempDir(), "legacy.json")
	legacy := ResumableState{Bucket: "bkt", Key: "big.tar", UploadID: "legacy-upload", PartSize: 16 << 20,
		FileSize: 100 << 20, TotalParts: 7, Parts: map[int64]string{1: `"e1"`}}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	cp, ok := openStateStore(path).getUpload(uploadStateKey("bkt", "big.tar"))
	if !ok || cp.UploadID != "legacy-upload" || cp.PartSize != 16<<20 || cp.SourceModTime != 0 {
		t.Fatalf("legacy upload import = %+v (found=%v)", cp, ok)
	}
}

func TestLoadLegacyDownloadState(t *testing.T) {
	resetStateStores()
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.json")
	dst := filepath.Join(dir, "out.bin")
	legacy := ResumableDownloadState{Bucket: "bkt", Key: "out.bin", PartPath: dst + ".s5part", PartSize: 8,
		FileSize: 80, TotalParts: 10, Parts: map[int64]bool{1: true, 2: true}}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	cp, ok := openStateStore(path).getDownload(downloadStateKey("bkt", "out.bin", dst))
	if !ok || len(cp.Parts) != 2 {
		t.Fatalf("legacy download import = %+v (found=%v)", cp, ok)
	}
}

func TestPartLayout(t *testing.T) {
	if got := partLength(3, 10, 25); got != 5 {
		t.Fatalf("last part length = %d, want 5", got)
	}
	if got := partLength(1, 10, 25); got != 10 {
		t.Fatalf("first part length = %d, want 10", got)
	}
	// A 5 TiB object must fit within the part limit.
	size := int64(5) << 40
	if parts := (size + minPartSize(size) - 1) / minPartSize(size); parts > maxMultipartParts {
		t.Fatalf("parts = %d exceeds %d", parts, maxMultipartParts)
	}
}

func TestTokenBucketRate(t *testing.T) {
	b := newTokenBucket(4 << 20) // 4 MiB/s
	start := time.Now()
	sent := 0
	for sent < 6<<20 { // 6 MiB -> ~1.4 s after the initial burst
		b.wait(rateLimitChunk)
		sent += rateLimitChunk
	}
	elapsed := time.Since(start)
	if elapsed < 1200*time.Millisecond || elapsed > 2500*time.Millisecond {
		t.Fatalf("6 MiB at 4 MiB/s took %v, want ~1.4s", elapsed)
	}
}
