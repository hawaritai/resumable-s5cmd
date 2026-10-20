package e2e

import (
	stdjson "encoding/json"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

type checkFirstPlan struct {
	SendFiles int64 `json:"send_files"`
	SendBytes int64 `json:"send_bytes"`
	SkipFiles int64 `json:"skip_files"`
	SkipBytes int64 `json:"skip_bytes"`
}

type checkFirstProgress struct {
	Operation    string          `json:"operation"`
	Bytes        int64           `json:"bytes"`
	TotalBytes   int64           `json:"total_bytes"`
	Files        int64           `json:"files"`
	TotalFiles   int64           `json:"total_files"`
	Transferred  int64           `json:"transferred_bytes"`
	SkippedFiles int64           `json:"skipped_files"`
	ListingDone  bool            `json:"listing_done"`
	Phase        string          `json:"phase"`
	CheckedFiles int64           `json:"checked_files"`
	Plan         *checkFirstPlan `json:"plan"`
	Final        bool            `json:"final"`
}

// finalProgress returns the last progress line; every line with a plan must carry the same plan.
func finalProgress(t *testing.T, stdout string) checkFirstProgress {
	t.Helper()
	var last checkFirstProgress
	var plan *checkFirstPlan
	for _, ln := range strings.Split(stdout, "\n") {
		if !strings.HasPrefix(ln, `{"operation":"progress"`) {
			continue
		}
		var p checkFirstProgress
		assert.NilError(t, stdjson.Unmarshal([]byte(ln), &p))
		if p.Plan != nil {
			if plan != nil {
				assert.DeepEqual(t, *plan, *p.Plan) // the plan never changes once reported
			}
			plan = p.Plan
		}
		last = p
	}
	assert.Assert(t, last.Final, "no final progress line in %q", stdout)
	return last
}

// cp --check-first -n -s dir s3://bucket/: only new and changed files are sent; the plan says so up front.
func TestCopyCheckFirstUploadSendsOnlyWhatIsNeeded(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)

	putFile(t, s3client, bucket, "dir/same.txt", "AAAA")     // same size as the local file: skipped
	putFile(t, s3client, bucket, "dir/changed.txt", "old")   // different size: sent
	putFile(t, s3client, bucket, "other/unrelated.txt", "x") // outside the target prefix

	workdir := fs.NewDir(t, t.Name(), fs.WithDir("dir",
		fs.WithFile("same.txt", "BBBB"),
		fs.WithFile("changed.txt", "new content"),
		fs.WithFile("new.txt", "brand new"),
	))
	defer workdir.Remove()

	cmd := s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "dir", "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))
	result.Assert(t, icmd.Success)

	p := finalProgress(t, result.Stdout())
	assert.Assert(t, p.Plan != nil)
	assert.DeepEqual(t, *p.Plan, checkFirstPlan{SendFiles: 2, SendBytes: 20, SkipFiles: 1, SkipBytes: 4})
	assert.Equal(t, p.TotalFiles, int64(3))
	assert.Equal(t, p.Files, int64(3))
	assert.Equal(t, p.Transferred, int64(20))
	assert.Equal(t, p.Phase, "transferring")

	assert.Assert(t, ensureS3Object(s3client, bucket, "dir/same.txt", "AAAA")) // not overwritten
	assert.Assert(t, ensureS3Object(s3client, bucket, "dir/changed.txt", "new content"))
	assert.Assert(t, ensureS3Object(s3client, bucket, "dir/new.txt", "brand new"))
}

// Without -n / -s / -u everything is sent; the plan still gives exact totals before the transfer.
func TestCopyCheckFirstUploadOverwriteMode(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "dir/same.txt", "AAAA")

	workdir := fs.NewDir(t, t.Name(), fs.WithDir("dir", fs.WithFile("same.txt", "BBBB"), fs.WithFile("b.txt", "bb")))
	defer workdir.Remove()

	result := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "dir", "s3://"+bucket+"/"),
		withWorkingDir(workdir))
	result.Assert(t, icmd.Success)
	p := finalProgress(t, result.Stdout())
	assert.DeepEqual(t, *p.Plan, checkFirstPlan{SendFiles: 2, SendBytes: 6})
	assert.Assert(t, ensureS3Object(s3client, bucket, "dir/same.txt", "BBBB"))
}

// cp --check-first -n -s 's3://bucket/dir/*' out/: files already on this PC are not downloaded again.
func TestCopyCheckFirstDownloadSendsOnlyWhatIsNeeded(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "dir/same.txt", "AAAA")
	putFile(t, s3client, bucket, "dir/changed.txt", "new content")
	putFile(t, s3client, bucket, "dir/sub/new.txt", "brand new")

	workdir := fs.NewDir(t, t.Name(), fs.WithDir("out",
		fs.WithFile("same.txt", "BBBB"),
		fs.WithFile("changed.txt", "old"),
	))
	defer workdir.Remove()

	cmd := s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "s3://"+bucket+"/dir/*", "out/")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))
	result.Assert(t, icmd.Success)

	p := finalProgress(t, result.Stdout())
	assert.DeepEqual(t, *p.Plan, checkFirstPlan{SendFiles: 2, SendBytes: 20, SkipFiles: 1, SkipBytes: 4})
	assert.Equal(t, p.Transferred, int64(20))

	expected := fs.Expected(t, fs.WithDir("out",
		fs.WithFile("same.txt", "BBBB"), // not overwritten
		fs.WithFile("changed.txt", "new content"),
		fs.WithDir("sub", fs.WithFile("new.txt", "brand new")),
	))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// Many files across sub-prefixes: the parallel destination listing finds every existing object.
func TestCopyCheckFirstUploadManyFiles(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)

	var files []fs.PathOp
	for d := 0; d < 5; d++ {
		var sub []fs.PathOp
		for f := 0; f < 20; f++ {
			name := string(rune('a'+d)) + "/" + strings.Repeat("x", f+1) + ".bin"
			sub = append(sub, fs.WithFile(strings.Repeat("x", f+1)+".bin", strings.Repeat("z", f+1)))
			if f%2 == 0 { // half of them are already in the bucket
				putFile(t, s3client, bucket, "data/"+name, strings.Repeat("q", f+1))
			}
		}
		files = append(files, fs.WithDir(string(rune('a'+d)), sub...))
	}
	workdir := fs.NewDir(t, t.Name(), fs.WithDir("data", files...))
	defer workdir.Remove()

	result := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "data", "s3://"+bucket+"/"),
		withWorkingDir(workdir))
	result.Assert(t, icmd.Success)
	p := finalProgress(t, result.Stdout())
	assert.Equal(t, p.Plan.SkipFiles, int64(50))
	assert.Equal(t, p.Plan.SendFiles, int64(50))
	assert.Equal(t, p.Files, int64(100))
}
