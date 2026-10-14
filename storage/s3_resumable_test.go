package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestResumableStateSerialization(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "test_job_state.json")

	initial := ResumableState{
		Bucket:     "my-test-bucket",
		Key:        "large_file.tar.gz",
		UploadID:   "upload-id-xyz-12345",
		PartSize:   16 * 1024 * 1024,
		FileSize:   100 * 1024 * 1024,
		TotalParts: 7,
		Parts: map[int64]string{
			1: "\"etag-part-1\"",
			2: "\"etag-part-2\"",
			3: "\"etag-part-3\"",
		},
	}

	saveStateFile(stateFile, initial)

	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("Failed to read state file: %v", err)
	}

	var loaded ResumableState
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("Failed to unmarshal state: %v", err)
	}

	if loaded.Bucket != initial.Bucket {
		t.Errorf("Bucket mismatch: got %v want %v", loaded.Bucket, initial.Bucket)
	}
	if loaded.Key != initial.Key {
		t.Errorf("Key mismatch: got %v want %v", loaded.Key, initial.Key)
	}
	if loaded.UploadID != initial.UploadID {
		t.Errorf("UploadID mismatch: got %v want %v", loaded.UploadID, initial.UploadID)
	}
	if len(loaded.Parts) != 3 {
		t.Errorf("Parts count mismatch: got %d want 3", len(loaded.Parts))
	}
	if loaded.Parts[2] != "\"etag-part-2\"" {
		t.Errorf("Part 2 etag mismatch: got %v want %v", loaded.Parts[2], "\"etag-part-2\"")
	}
}

