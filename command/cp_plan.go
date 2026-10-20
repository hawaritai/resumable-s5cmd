package command

// Check-first copy planning (cp --check-first; the model of rclone --check-first and aws s3 sync):
//
//  1. list every source object (phase "listing"),
//  2. compare them with the destination in bulk (phase "checking"): an upload lists the destination prefix
//     once (1,000 keys per request, sub-prefixes in parallel) instead of one HEAD per file; a folder
//     download walks the destination folder once (one directory read per folder), using the sizes and dates
//     the source listing already returned,
//  3. report the exact plan (files and bytes to send, files and bytes already there) and transfer only what
//     has to be sent (phase "transferring"), so totals and percentages are exact from the first byte.
//
// The decision per object is the same as without --check-first (overrideDecision). If the destination can't
// be listed (e.g. a key allowed to write but not to list) or holds far more objects than are being copied,
// planning falls back to the per-object check inside each transfer task.

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/peak/s5cmd/v2/progressbar"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

// Listing the destination stops (and the per-object check is used instead) once it holds this many more
// objects than are being copied: then a HEAD per object is cheaper than reading the whole prefix.
const planListSlack = 10_000

const planListWorkers = 16

// planCopy splits the listed source objects into those to transfer and those already at the destination.
// ok=false: the destination could not be compared in bulk; every object is returned and the transfer tasks
// check the destination themselves.
func (c Copy) planCopy(ctx context.Context, objs []*storage.Object, isBatch bool) (send []*storage.Object, ok bool) {
	progressbar.SetPhase(c.progressbar, "checking")
	if !c.noClobber && !c.ifSizeDiffer && !c.ifSourceNewer {
		progressbar.AddChecked(c.progressbar, int64(len(objs)))
		return objs, true // overwrite mode: everything is sent, nothing to compare
	}
	if len(objs) == 0 {
		return objs, true
	}
	var existing func(i int) (*storage.Object, bool)
	switch {
	case c.src.IsRemote() && !c.dst.IsRemote(): // download
		existing = c.localDestinations(ctx, objs, isBatch)
	case !c.src.IsRemote() && c.dst.IsRemote(): // upload
		existing = c.remoteDestinations(ctx, objs, isBatch)
	}
	if existing == nil {
		return objs, false
	}
	for i, obj := range objs {
		dstObj, known := existing(i)
		progressbar.AddChecked(c.progressbar, 1)
		if !known {
			send = append(send, obj) // couldn't tell: the task checks this one itself
			continue
		}
		if err := c.overrideDecision(obj, dstObj); err != nil {
			// Already at the destination: completed without a transfer.
			progressbar.AddSkippedObject(c.progressbar, obj.Size)
			continue
		}
		send = append(send, obj)
	}
	return send, true
}

// remoteDestinations lists the destination prefix that all upload targets share, in parallel by sub-prefix.
func (c Copy) remoteDestinations(ctx context.Context, objs []*storage.Object, isBatch bool) func(int) (*storage.Object, bool) {
	keys := make([]string, len(objs))
	for i, obj := range objs {
		keys[i] = prepareRemoteDestination(obj.URL, c.dst, c.flatten, isBatch).Path
	}
	prefix := commonPrefix(keys)
	if i := strings.IndexAny(prefix, "*?["); i >= 0 {
		prefix = prefix[:i] // a wider listing is still correct; wildcard characters can't be escaped
	}

	client, err := storage.NewRemoteClient(ctx, c.dst, c.storageOpts)
	if err != nil {
		return nil
	}
	listed, ok := listPrefixParallel(ctx, client, c.dst.Bucket, prefix, int64(len(objs))+planListSlack, c.progressbar)
	if !ok {
		return nil
	}
	return func(i int) (*storage.Object, bool) {
		return listed[keys[i]], true
	}
}

// localDestinations finds which download targets already exist. A folder download walks the destination folder
// once (one directory read per folder, with sizes and dates, also fast on a network share) instead of one stat
// per file; a single object, or a destination far larger than the copy, is checked per file in parallel.
func (c Copy) localDestinations(ctx context.Context, objs []*storage.Object, isBatch bool) func(int) (*storage.Object, bool) {
	if isBatch && !c.flatten {
		if existing, ok := walkLocal(c.dst.Absolute(), int64(len(objs))+planListSlack, c.progressbar); ok {
			return func(i int) (*storage.Object, bool) {
				// The batch target of prepareLocalDestination: <destination>/<path relative to the source prefix>.
				target := c.dst.Join(objs[i].URL.Relative()).Absolute()
				return existing[localKey(target)], true
			}
		}
	}
	return c.statDestinationsParallel(ctx, objs, isBatch)
}

// statDestinationsParallel resolves and stats each download target, planListWorkers at a time.
func (c Copy) statDestinationsParallel(ctx context.Context, objs []*storage.Object, isBatch bool) func(int) (*storage.Object, bool) {
	type result struct {
		obj   *storage.Object
		known bool
	}
	results := make([]result, len(objs))
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < planListWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				dsturl, err := prepareLocalDestination(ctx, objs[i].URL, c.dst, c.flatten, isBatch, c.storageOpts)
				if err != nil {
					continue // unknown: the transfer task checks it
				}
				fi, err := os.Stat(dsturl.Absolute())
				switch {
				case err != nil && errors.Is(err, os.ErrNotExist):
					results[i] = result{nil, true}
				case err != nil || fi.IsDir():
					// unknown: the transfer task checks it
				default:
					mod := fi.ModTime()
					results[i] = result{&storage.Object{URL: dsturl, Size: fi.Size(), ModTime: &mod}, true}
				}
			}
		}()
	}
	for i := range objs {
		work <- i
	}
	close(work)
	wg.Wait()
	return func(i int) (*storage.Object, bool) { return results[i].obj, results[i].known }
}

// walkLocal returns every regular file under root by localKey. A missing root is an empty destination.
// ok=false when the walk fails or finds more than limit files.
func walkLocal(root string, limit int64, pb progressbar.ProgressBar) (map[string]*storage.Object, bool) {
	out := make(map[string]*storage.Object)
	root = filepath.FromSlash(root)
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return out, true
	}
	errTooMany := errors.New("too many files")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info() // from the directory listing on Windows: no extra request per file
		if err != nil {
			return err
		}
		mod := info.ModTime()
		out[localKey(path)] = &storage.Object{Size: info.Size(), ModTime: &mod}
		progressbar.AddDestListed(pb, 1)
		if int64(len(out)) > limit {
			return errTooMany
		}
		return nil
	})
	return out, err == nil
}

// localKey compares local paths the way the file system does: separators and "." / ".." normalised, and
// without letter case on Windows (NTFS and SMB shares are case-insensitive).
func localKey(path string) string {
	p := filepath.Clean(filepath.FromSlash(path))
	if runtime.GOOS == "windows" {
		p = strings.ToLower(p)
	}
	return p
}

// listPrefixParallel returns every object under bucket/prefix by key. The first level is listed with a
// delimiter; each sub-prefix found is then listed recursively by its own worker. ok=false when listing failed
// or more than limit objects exist.
func listPrefixParallel(
	ctx context.Context, client *storage.S3, bucket, prefix string, limit int64, pb progressbar.ProgressBar,
) (map[string]*storage.Object, bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu     sync.Mutex
		out    = make(map[string]*storage.Object)
		failed bool
		count  int64
	)
	add := func(obj *storage.Object) bool {
		mu.Lock()
		defer mu.Unlock()
		if failed {
			return false
		}
		if obj.Err != nil {
			if errors.Is(obj.Err, storage.ErrNoObjectFound) {
				return true
			}
			failed = true
			cancel()
			return false
		}
		if obj.Type.IsDir() {
			return true
		}
		out[obj.URL.Path] = obj
		count++
		progressbar.AddDestListed(pb, 1)
		if count > limit {
			failed = true
			cancel()
			return false
		}
		return true
	}

	// The prefix has no wildcard characters, so this lists one level (delimiter "/"): objects directly under
	// it, and its sub-prefixes, which the workers then list recursively.
	top, err := url.New("s3://" + bucket + "/" + prefix)
	if err != nil {
		return nil, false
	}
	var subs []string
	ch := client.List(ctx, top, false)
	for obj := range ch {
		if obj.Err == nil && obj.Type.IsDir() {
			subs = append(subs, obj.URL.Path)
			continue
		}
		if !add(obj) {
			drain(ch)
			break
		}
	}

	work := make(chan string)
	var wg sync.WaitGroup
	for w := 0; w < planListWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sub := range work {
				p := sub
				if i := strings.IndexAny(p, "*?["); i >= 0 {
					p = p[:i]
				}
				u, err := url.New("s3://" + bucket + "/" + p + "*")
				if err != nil {
					mu.Lock()
					failed = true
					mu.Unlock()
					cancel()
					continue
				}
				ch := client.List(ctx, u, false)
				for obj := range ch {
					if !add(obj) {
						drain(ch)
						break
					}
				}
			}
		}()
	}
	for _, sub := range subs {
		mu.Lock()
		stop := failed
		mu.Unlock()
		if stop {
			break
		}
		work <- sub
	}
	close(work)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return out, !failed
}

// drain reads a listing to its end, so its producer goroutine can finish after a cancel.
func drain(ch <-chan *storage.Object) {
	for range ch {
	}
}

func commonPrefix(keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	p := keys[0]
	for _, k := range keys[1:] {
		n := len(p)
		if len(k) < n {
			n = len(k)
		}
		i := 0
		for i < n && p[i] == k[i] {
			i++
		}
		p = p[:i]
		if p == "" {
			break
		}
	}
	return p
}
