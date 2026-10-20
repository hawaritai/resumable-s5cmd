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

// cp --check-first -n -s file s3://bucket/prefix/: a single file, already there with the same size, is skipped.
func TestCopyCheckFirstSingleFileUpload(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "p/one.txt", "1111")

	workdir := fs.NewDir(t, t.Name(), fs.WithFile("one.txt", "2222"), fs.WithFile("two.txt", "22"))
	defer workdir.Remove()

	for _, tc := range []struct {
		file string
		plan checkFirstPlan
	}{
		{"one.txt", checkFirstPlan{SkipFiles: 1, SkipBytes: 4}},
		{"two.txt", checkFirstPlan{SendFiles: 1, SendBytes: 2}},
	} {
		result := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", tc.file, "s3://"+bucket+"/p/"),
			withWorkingDir(workdir))
		result.Assert(t, icmd.Success)
		assert.DeepEqual(t, *finalProgress(t, result.Stdout()).Plan, tc.plan)
	}
	assert.Assert(t, ensureS3Object(s3client, bucket, "p/one.txt", "1111"))
	assert.Assert(t, ensureS3Object(s3client, bucket, "p/two.txt", "22"))
}

// cp --check-first -n -s s3://bucket/key dir/: a single object, already on this PC, is not downloaded again.
func TestCopyCheckFirstSingleObjectDownload(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "p/one.txt", "1111")
	putFile(t, s3client, bucket, "p/two.txt", "22")

	workdir := fs.NewDir(t, t.Name(), fs.WithDir("out", fs.WithFile("one.txt", "AAAA")))
	defer workdir.Remove()

	for _, tc := range []struct {
		key  string
		plan checkFirstPlan
	}{
		{"p/one.txt", checkFirstPlan{SkipFiles: 1, SkipBytes: 4}},
		{"p/two.txt", checkFirstPlan{SendFiles: 1, SendBytes: 2}},
	} {
		result := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "s3://"+bucket+"/"+tc.key, "out/"),
			withWorkingDir(workdir))
		result.Assert(t, icmd.Success)
		assert.DeepEqual(t, *finalProgress(t, result.Stdout()).Plan, tc.plan)
	}
	expected := fs.Expected(t, fs.WithDir("out", fs.WithFile("one.txt", "AAAA"), fs.WithFile("two.txt", "22")))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// A folder download into a folder that already holds part of it, in nested sub-folders: one walk finds them.
func TestCopyCheckFirstDownloadNestedExisting(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	for _, k := range []string{"d/a/1.bin", "d/a/b/2.bin", "d/a/b/c/3.bin", "d/x/4.bin"} {
		putFile(t, s3client, bucket, k, "data")
	}
	workdir := fs.NewDir(t, t.Name(), fs.WithDir("out",
		fs.WithDir("a", fs.WithFile("1.bin", "DATA"), fs.WithDir("b", fs.WithDir("c", fs.WithFile("3.bin", "DATA")))),
		fs.WithFile("unrelated.txt", "zzz"),
	))
	defer workdir.Remove()

	result := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "s3://"+bucket+"/d/*", "out/"),
		withWorkingDir(workdir))
	result.Assert(t, icmd.Success)
	assert.DeepEqual(t, *finalProgress(t, result.Stdout()).Plan, checkFirstPlan{SendFiles: 2, SendBytes: 8, SkipFiles: 2, SkipBytes: 8})
	expected := fs.Expected(t, fs.WithDir("out",
		fs.WithDir("a", fs.WithFile("1.bin", "DATA"), fs.WithDir("b", fs.WithFile("2.bin", "data"),
			fs.WithDir("c", fs.WithFile("3.bin", "DATA")))),
		fs.WithDir("x", fs.WithFile("4.bin", "data")),
		fs.WithFile("unrelated.txt", "zzz"),
	))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

// Re-running a finished copy sends nothing: everything is in the plan as already there.
func TestCopyCheckFirstRerunSendsNothing(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	workdir := fs.NewDir(t, t.Name(), fs.WithDir("dir", fs.WithFile("a.txt", "aa"), fs.WithDir("s", fs.WithFile("b.txt", "bbb"))))
	defer workdir.Remove()

	up := func() checkFirstProgress {
		r := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "dir", "s3://"+bucket+"/"), withWorkingDir(workdir))
		r.Assert(t, icmd.Success)
		return finalProgress(t, r.Stdout())
	}
	assert.DeepEqual(t, *up().Plan, checkFirstPlan{SendFiles: 2, SendBytes: 5})
	second := up()
	assert.DeepEqual(t, *second.Plan, checkFirstPlan{SkipFiles: 2, SkipBytes: 5})
	assert.Equal(t, second.Transferred, int64(0))

	down := func() checkFirstProgress {
		r := icmd.RunCmd(s5cmd("cp", "--check-first", "--progress-json", "-n", "-s", "s3://"+bucket+"/dir/*", "back/"), withWorkingDir(workdir))
		r.Assert(t, icmd.Success)
		return finalProgress(t, r.Stdout())
	}
	assert.DeepEqual(t, *down().Plan, checkFirstPlan{SendFiles: 2, SendBytes: 5})
	assert.DeepEqual(t, *down().Plan, checkFirstPlan{SkipFiles: 2, SkipBytes: 5})
}

// The app always runs with a checkpoint (--state-file): check-first and resume work together.
func TestCopyCheckFirstWithStateFile(t *testing.T) {
	t.Parallel()
	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t)
	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "dir/old.txt", "OLD!")
	workdir := fs.NewDir(t, t.Name(),
		fs.WithDir("dir", fs.WithFile("old.txt", "old!"), fs.WithFile("new.txt", "new file")),
	)
	defer workdir.Remove()
	state := workdir.Join("job_state.json")

	run := func(args ...string) checkFirstProgress {
		base := []string{"cp", "--check-first", "--progress-json", "--state-file", state, "-n", "-s"}
		r := icmd.RunCmd(s5cmd(append(base, args...)...), withWorkingDir(workdir))
		r.Assert(t, icmd.Success)
		return finalProgress(t, r.Stdout())
	}
	assert.DeepEqual(t, *run("dir", "s3://"+bucket+"/").Plan, checkFirstPlan{SendFiles: 1, SendBytes: 8, SkipFiles: 1, SkipBytes: 4})
	assert.Assert(t, ensureS3Object(s3client, bucket, "dir/new.txt", "new file"))
	p := run("s3://"+bucket+"/dir/*", "back/")
	assert.DeepEqual(t, *p.Plan, checkFirstPlan{SendFiles: 2, SendBytes: 12})
	assert.Equal(t, p.Files, int64(2))
}
