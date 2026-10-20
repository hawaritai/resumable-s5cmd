package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/peak/s5cmd/v2/storage/url"
)

// S3 limits a multipart upload to 10,000 parts; stay slightly below so rounding never exceeds it.
const maxMultipartParts = 9900

// ResumableState is the legacy (single-object) upload state format, still accepted when reading
// state files written by earlier builds.
type ResumableState struct {
	Bucket     string           `json:"bucket"`
	Key        string           `json:"key"`
	UploadID   string           `json:"upload_id"`
	PartSize   int64            `json:"part_size"`
	FileSize   int64            `json:"file_size"`
	TotalParts int              `json:"total_parts"`
	Parts      map[int64]string `json:"parts"` // PartNumber -> ETag
}

type fileProvider interface {
	File() *os.File
}

type progressProvider interface {
	AddCompletedBytes(n int64)
}

// Optional progress accounting (see progressbar.Accounting); used when the reporter supports it.
type resumedProvider interface{ AddResumedBytes(n int64) }
type inflightProvider interface{ AddInflightBytes(n int64) }

func reportResumed(p interface{}, n int64) {
	if r, ok := p.(resumedProvider); ok {
		r.AddResumedBytes(n)
	} else if c, ok := p.(progressProvider); ok {
		c.AddCompletedBytes(n)
	}
}

func inflightFunc(p interface{}) func(int64) {
	if r, ok := p.(inflightProvider); ok {
		return r.AddInflightBytes
	}
	return func(int64) {}
}

// countingPartReader reports bytes of an upload part as they are read for sending. The SDK reads the
// body once to sign it (payload hash) and again on every retry, rewinding with Seek to the start; each
// rewind rolls back what was counted, so only the final send pass is reported.
type countingPartReader struct {
	*io.SectionReader
	report  func(int64)
	counted int64
}

func (r *countingPartReader) Read(p []byte) (int, error) {
	n, err := r.SectionReader.Read(p)
	if n > 0 {
		r.counted += int64(n)
		r.report(int64(n))
	}
	return n, err
}

func (r *countingPartReader) Seek(offset int64, whence int) (int64, error) {
	pos, err := r.SectionReader.Seek(offset, whence)
	if err == nil && pos == 0 && r.counted > 0 {
		r.report(-r.counted)
		r.counted = 0
	}
	return pos, err
}

// settle removes this part's in-flight bytes (it then counts as completed, or failed).
func (r *countingPartReader) settle() {
	if r.counted > 0 {
		r.report(-r.counted)
		r.counted = 0
	}
}

// countingWriter reports downloaded bytes of a range as they are written to disk.
type countingWriter struct {
	w       io.Writer
	report  func(int64)
	counted int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.counted += int64(n)
		c.report(int64(n))
	}
	return n, err
}

func (c *countingWriter) settle() {
	if c.counted > 0 {
		c.report(-c.counted)
		c.counted = 0
	}
}

// copyBufPool provides large buffers for streaming ranged downloads to disk.
var copyBufPool = sync.Pool{New: func() any { b := make([]byte, transportIOBufferSize); return &b }}

func emitEvent(msg map[string]interface{}) {
	if b, err := json.Marshal(msg); err == nil {
		fmt.Println(string(b))
	}
}

// minPartSize returns the smallest part size that keeps fileSize within the multipart part limit.
func minPartSize(fileSize int64) int64 {
	return int64(math.Ceil(float64(fileSize) / maxMultipartParts))
}

func partLength(part, partSize, fileSize int64) int64 {
	offset := (part - 1) * partSize
	if offset+partSize > fileSize {
		return fileSize - offset
	}
	return partSize
}

func isAWSCode(err error, codes ...string) bool {
	var aerr awserr.Error
	if errors.As(err, &aerr) {
		for _, c := range codes {
			if aerr.Code() == c {
				return true
			}
		}
	}
	return false
}

// ResumablePut uploads a local file as a multipart upload whose progress survives pause, crash and
// network loss. Each object has its own checkpoint in the shared state file; completed parts are
// recovered from S3 (ListParts) on resume.
func (s *S3) ResumablePut(
	ctx context.Context,
	reader io.Reader,
	to *url.URL,
	metadata Metadata,
	concurrency int,
	partSize int64,
) error {
	var file *os.File
	if fp, ok := reader.(fileProvider); ok {
		file = fp.File()
	} else if f, ok := reader.(*os.File); ok {
		file = f
	}

	contentType := metadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3manager.UploadInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		Metadata:     make(map[string]*string),
		RequestPayer: s.RequestPayer(),
	}
	if metadata.StorageClass != "" {
		input.StorageClass = aws.String(metadata.StorageClass)
	}
	if metadata.ACL != "" {
		input.ACL = aws.String(metadata.ACL)
	}
	if metadata.CacheControl != "" {
		input.CacheControl = aws.String(metadata.CacheControl)
	}
	if metadata.Expires != "" {
		if t, err := time.Parse(time.RFC3339, metadata.Expires); err == nil {
			input.Expires = aws.Time(t)
		}
	}
	if metadata.EncryptionMethod != "" {
		input.ServerSideEncryption = aws.String(metadata.EncryptionMethod)
		if metadata.EncryptionKeyID != "" {
			input.SSEKMSKeyId = aws.String(metadata.EncryptionKeyID)
		}
	}
	if metadata.ContentEncoding != "" {
		input.ContentEncoding = aws.String(metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		input.ContentDisposition = aws.String(metadata.ContentDisposition)
	}
	if len(metadata.UserDefined) != 0 {
		for k, v := range metadata.UserDefined {
			input.Metadata[k] = aws.String(v)
		}
	}

	standardUpload := func() error {
		_, err := s.uploader.UploadWithContext(ctx, input, func(u *s3manager.Uploader) {
			u.PartSize = partSize
			u.Concurrency = concurrency
		})
		return err
	}

	// Not a local file (e.g. stdin) or no state file: nothing to resume.
	if file == nil || s.stateFile == "" {
		return standardUpload()
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	sourceModTime := stat.ModTime().UnixNano()

	// Single-request upload for small files: nothing worth checkpointing.
	if fileSize <= partSize {
		return standardUpload()
	}
	if min := minPartSize(fileSize); partSize < min {
		partSize = min
	}
	if concurrency <= 0 {
		concurrency = 4
	}

	store := openStateStore(s.stateFile)
	stateKey := uploadStateKey(to.Bucket, to.Path)
	parts := map[int64]string{}
	uploadID := ""

	// 1. Resume a recorded upload of the same, unmodified file.
	if cp, ok := store.getUpload(stateKey); ok && cp.UploadID != "" {
		sameFile := cp.FileSize == fileSize && (cp.SourceModTime == 0 || cp.SourceModTime == sourceModTime)
		if !sameFile {
			// Parts of a different file version must never be stitched into this object.
			s.abortUpload(ctx, cp.Bucket, cp.Key, cp.UploadID)
			_ = store.deleteUpload(stateKey)
		} else {
			listed, lerr := s.listUploadedParts(ctx, to, cp.UploadID, cp.PartSize, fileSize)
			switch {
			case lerr == nil:
				uploadID = cp.UploadID
				partSize = cp.PartSize // part numbers map to offsets of the original part size
				parts = listed
			case isAWSCode(lerr, "NoSuchUpload"):
				_ = store.deleteUpload(stateKey) // expired or aborted on the server: start over
			default:
				return fmt.Errorf("cannot resume multipart upload: %w", lerr)
			}
		}
	}

	// 2. Start a new multipart upload and checkpoint its id before sending any data.
	if uploadID == "" {
		createInput := &s3.CreateMultipartUploadInput{
			Bucket:               input.Bucket,
			Key:                  input.Key,
			ContentType:          input.ContentType,
			Metadata:             input.Metadata,
			RequestPayer:         input.RequestPayer,
			StorageClass:         input.StorageClass,
			ACL:                  input.ACL,
			CacheControl:         input.CacheControl,
			Expires:              input.Expires,
			ServerSideEncryption: input.ServerSideEncryption,
			SSEKMSKeyId:          input.SSEKMSKeyId,
			ContentEncoding:      input.ContentEncoding,
			ContentDisposition:   input.ContentDisposition,
		}
		createResp, err := s.api.CreateMultipartUploadWithContext(ctx, createInput)
		if err != nil {
			return fmt.Errorf("failed to initiate multipart upload: %w", err)
		}
		uploadID = aws.StringValue(createResp.UploadId)
		if err := store.putUpload(stateKey, UploadCheckpoint{
			Bucket: to.Bucket, Key: to.Path, UploadID: uploadID,
			PartSize: partSize, FileSize: fileSize, SourceModTime: sourceModTime,
		}); err != nil {
			s.abortUpload(ctx, to.Bucket, to.Path, uploadID)
			return fmt.Errorf("cannot record upload checkpoint: %w", err)
		}
	}

	totalParts := int64(math.Ceil(float64(fileSize) / float64(partSize)))
	var completedBytes int64
	for p := range parts {
		completedBytes += partLength(p, partSize, fileSize)
	}
	if completedBytes > 0 {
		reportResumed(reader, completedBytes)
	}
	inflight := inflightFunc(reader)
	emitEvent(map[string]interface{}{
		"operation":       "multipart_init",
		"upload_id":       uploadID,
		"bucket":          to.Bucket,
		"key":             to.Path,
		"total_bytes":     fileSize,
		"total_parts":     totalParts,
		"completed_parts": len(parts),
		"resumed_bytes":   completedBytes,
	})

	// 3. Upload the missing parts with a bounded worker pool.
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}

	for p := int64(1); p <= totalParts; p++ {
		if _, done := parts[p]; done {
			continue
		}
		if ctx.Err() != nil || failed() {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(part int64) {
			defer func() {
				<-sem
				wg.Done()
			}()
			if ctx.Err() != nil {
				return
			}
			length := partLength(part, partSize, fileSize)
			// Stream the part from the file: no part-sized buffer (memory stays flat for any number of
			// files in flight). The SectionReader is seekable, so signing and SDK retries re-read it;
			// bytes are reported as in flight while they are sent.
			body := &countingPartReader{SectionReader: io.NewSectionReader(file, (part-1)*partSize, length), report: inflight}
			resp, err := s.api.UploadPartWithContext(ctx, &s3.UploadPartInput{
				Bucket:       aws.String(to.Bucket),
				Key:          aws.String(to.Path),
				UploadId:     aws.String(uploadID),
				PartNumber:   aws.Int64(part),
				Body:         body,
				RequestPayer: s.RequestPayer(),
			})
			body.settle()
			if err != nil {
				fail(fmt.Errorf("failed to upload part %d: %w", part, err))
				return
			}
			mu.Lock()
			parts[part] = aws.StringValue(resp.ETag)
			mu.Unlock()

			if prog, ok := reader.(progressProvider); ok {
				prog.AddCompletedBytes(length)
			}
			emitEvent(map[string]interface{}{
				"operation":   "multipart_part",
				"upload_id":   uploadID,
				"part_number": part,
				"total_parts": totalParts,
				"part_bytes":  length,
			})
		}(p)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// 4. Complete and drop the checkpoint.
	if err := s.completeMultipart(ctx, to, uploadID, parts); err != nil {
		return err
	}
	_ = store.deleteUpload(stateKey)
	emitEvent(map[string]interface{}{
		"operation": "multipart_complete",
		"upload_id": uploadID,
		"bucket":    to.Bucket,
		"key":       to.Path,
		"success":   true,
	})
	return nil
}

// listUploadedParts returns the parts already stored for uploadID (all pages), keeping only parts
// whose size matches the expected layout so a corrupt or foreign part is re-sent.
func (s *S3) listUploadedParts(ctx context.Context, to *url.URL, uploadID string, partSize, fileSize int64) (map[int64]string, error) {
	parts := map[int64]string{}
	err := s.api.ListPartsPagesWithContext(ctx, &s3.ListPartsInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		UploadId:     aws.String(uploadID),
		RequestPayer: s.RequestPayer(),
	}, func(page *s3.ListPartsOutput, _ bool) bool {
		for _, p := range page.Parts {
			if p.PartNumber == nil || p.ETag == nil || p.Size == nil {
				continue
			}
			n := *p.PartNumber
			if n < 1 || (n-1)*partSize >= fileSize || *p.Size != partLength(n, partSize, fileSize) {
				continue
			}
			parts[n] = *p.ETag
		}
		return true
	})
	return parts, err
}

func (s *S3) completeMultipart(ctx context.Context, to *url.URL, uploadID string, parts map[int64]string) error {
	completed := make([]*s3.CompletedPart, 0, len(parts))
	for n, etag := range parts {
		completed = append(completed, &s3.CompletedPart{PartNumber: aws.Int64(n), ETag: aws.String(etag)})
	}
	sort.Slice(completed, func(i, j int) bool { return *completed[i].PartNumber < *completed[j].PartNumber })

	_, err := s.api.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(to.Bucket),
		Key:             aws.String(to.Path),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completed},
		RequestPayer:    s.RequestPayer(),
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	return nil
}

// abortUpload discards a multipart upload and its stored parts (best effort; a bucket lifecycle rule
// "AbortIncompleteMultipartUpload" is the server-side safety net).
func (s *S3) abortUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := s.api.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		UploadId:     aws.String(uploadID),
		RequestPayer: s.RequestPayer(),
	})
	if err != nil && isAWSCode(err, "NoSuchUpload") {
		return nil
	}
	return err
}

// ResumableDownloadState is the legacy (single-object) download state format.
type ResumableDownloadState struct {
	Bucket     string         `json:"bucket"`
	Key        string         `json:"key"`
	PartPath   string         `json:"part_path"`
	PartSize   int64          `json:"part_size"`
	FileSize   int64          `json:"file_size"`
	TotalParts int            `json:"total_parts"`
	Parts      map[int64]bool `json:"parts"`
}

func copyAndRemoveFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	_ = in.Close()
	_ = out.Close()
	_ = os.Remove(src)
	return nil
}

func finalizeDownload(partPath, dstFilePath string) error {
	_ = os.Remove(dstFilePath)
	if err := os.Rename(partPath, dstFilePath); err != nil {
		return copyAndRemoveFile(partPath, dstFilePath)
	}
	return nil
}

// ResumableGet downloads an object with parallel ranged GETs straight into "<dst>.s5part", then
// renames it into place. Progress is checkpointed per object; every range request is conditional
// on the object's ETag, so an object replaced mid-download is detected instead of mixed.
func (s *S3) ResumableGet(
	ctx context.Context,
	from *url.URL,
	dstFilePath string,
	concurrency int,
	partSize int64,
	prog progressProvider,
) (int64, error) {
	if s.dryRun {
		return 0, nil
	}

	head, err := s.api.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.RequestPayer(),
	})
	if err != nil {
		return 0, fmt.Errorf("head object failed (%s): %w", from.Absolute(), err)
	}
	fileSize := aws.Int64Value(head.ContentLength)
	etag := aws.StringValue(head.ETag)
	if err := os.MkdirAll(filepath.Dir(dstFilePath), 0o755); err != nil {
		return 0, err
	}
	if fileSize == 0 {
		return 0, os.WriteFile(dstFilePath, []byte{}, 0o644)
	}

	if partSize <= 0 {
		partSize = 16 * 1024 * 1024
	}
	if min := minPartSize(fileSize); partSize < min {
		partSize = min
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	partPath := dstFilePath + ".s5part"

	var store *stateStore
	stateKey := downloadStateKey(from.Bucket, from.Path, dstFilePath)
	cp := DownloadCheckpoint{
		Bucket: from.Bucket, Key: from.Path, PartPath: partPath,
		PartSize: partSize, FileSize: fileSize, ETag: etag, Parts: map[int64]bool{},
	}

	// 1. Resume only if the same object version and the partial file are intact.
	if s.stateFile != "" {
		store = openStateStore(s.stateFile)
		if old, ok := store.getDownload(stateKey); ok {
			fi, statErr := os.Stat(partPath)
			sameObject := old.FileSize == fileSize && (old.ETag == "" || etag == "" || old.ETag == etag)
			if sameObject && statErr == nil && fi.Size() == fileSize && old.PartSize > 0 {
				cp.PartSize = old.PartSize
				cp.Parts = old.Parts
			}
		}
		if err := store.putDownload(stateKey, cp); err != nil {
			return 0, fmt.Errorf("cannot record download checkpoint: %w", err)
		}
	}
	partSize = cp.PartSize
	totalParts := int64(math.Ceil(float64(fileSize) / float64(partSize)))

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return 0, fmt.Errorf("cannot open download partial file (%s): %w", partPath, err)
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil && fi.Size() != fileSize {
		if err := f.Truncate(fileSize); err != nil {
			return 0, fmt.Errorf("cannot allocate %s: %w", partPath, err)
		}
	}

	var completedBytes int64
	for p := range cp.Parts {
		completedBytes += partLength(p, partSize, fileSize)
	}
	if completedBytes > 0 && prog != nil {
		reportResumed(prog, completedBytes)
	}
	inflight := inflightFunc(prog)
	emitEvent(map[string]interface{}{
		"operation":       "multipart_init",
		"direction":       "download",
		"bucket":          from.Bucket,
		"key":             from.Path,
		"total_bytes":     fileSize,
		"total_parts":     totalParts,
		"completed_parts": len(cp.Parts),
		"resumed_bytes":   completedBytes,
	})

	// 2. Fetch the missing ranges with a bounded worker pool.
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}

	for p := int64(1); p <= totalParts; p++ {
		if cp.Parts[p] {
			continue
		}
		if ctx.Err() != nil || failed() {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(part int64) {
			defer func() {
				<-sem
				wg.Done()
			}()
			if ctx.Err() != nil {
				return
			}
			offset := (part - 1) * partSize
			length := partLength(part, partSize, fileSize)
			getInput := &s3.GetObjectInput{
				Bucket:       aws.String(from.Bucket),
				Key:          aws.String(from.Path),
				Range:        aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
				RequestPayer: s.RequestPayer(),
			}
			if etag != "" {
				getInput.IfMatch = aws.String(etag)
			}
			resp, err := s.api.GetObjectWithContext(ctx, getInput)
			if err != nil {
				if isAWSCode(err, "PreconditionFailed") {
					err = fmt.Errorf("object %s changed during download; it will be fetched again from the start", from.Absolute())
					if store != nil {
						_ = store.deleteDownload(stateKey)
					}
				}
				fail(fmt.Errorf("failed to download part %d: %w", part, err))
				return
			}
			defer resp.Body.Close()

			// Stream straight to the file at the part's offset (no part-sized buffer).
			bufp := copyBufPool.Get().(*[]byte)
			dst := &countingWriter{w: io.NewOffsetWriter(f, offset), report: inflight}
			n, err := io.CopyBuffer(dst, io.LimitReader(resp.Body, length), *bufp)
			copyBufPool.Put(bufp)
			dst.settle()
			if err != nil {
				fail(fmt.Errorf("failed to download part %d: %w", part, err))
				return
			}
			if n != length {
				fail(fmt.Errorf("part %d length mismatch: expected %d, got %d", part, length, n))
				return
			}
			if store != nil {
				store.markDownloadPart(stateKey, part)
			}
			if prog != nil {
				prog.AddCompletedBytes(length)
			}
			emitEvent(map[string]interface{}{
				"operation":   "multipart_part",
				"direction":   "download",
				"part_number": part,
				"total_parts": totalParts,
				"part_bytes":  length,
			})
		}(p)
	}
	wg.Wait()

	if firstErr != nil {
		return 0, firstErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// 3. Make the data durable, then atomically move it into place and drop the checkpoint.
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("cannot flush %s: %w", partPath, err)
	}
	_ = f.Close()
	if err := finalizeDownload(partPath, dstFilePath); err != nil {
		return 0, err
	}
	if store != nil {
		_ = store.deleteDownload(stateKey)
		store.markCompleted(DownloadCompletedKey(from.Absolute(), dstFilePath), fileSize, etag)
	}
	emitEvent(map[string]interface{}{
		"operation": "multipart_complete",
		"direction": "download",
		"bucket":    from.Bucket,
		"key":       from.Path,
		"success":   true,
	})
	return fileSize, nil
}

// AbortStateFile discards everything a state file records: multipart uploads are aborted on the
// server (freeing their stored parts) and partial download files are deleted. Used on cancel.
func AbortStateFile(ctx context.Context, path string, opts Options) (aborted, removed int, err error) {
	store := openStateStore(path)
	uploads, downloads := store.snapshot()
	var errs []error
	for _, u := range uploads {
		target, uerr := url.New("s3://" + u.Bucket + "/" + u.Key)
		if uerr != nil {
			errs = append(errs, uerr)
			continue
		}
		client, cerr := NewRemoteClient(ctx, target, opts)
		if cerr != nil {
			errs = append(errs, cerr)
			continue
		}
		if aerr := client.abortUpload(ctx, u.Bucket, u.Key, u.UploadID); aerr != nil {
			errs = append(errs, fmt.Errorf("abort %s: %w", uploadStateKey(u.Bucket, u.Key), aerr))
			continue
		}
		_ = store.deleteUpload(uploadStateKey(u.Bucket, u.Key))
		aborted++
	}
	for _, d := range downloads {
		if rerr := os.Remove(d.PartPath); rerr == nil || os.IsNotExist(rerr) {
			removed++
		}
		dst := d.PartPath[:len(d.PartPath)-len(filepath.Ext(d.PartPath))]
		_ = store.deleteDownload(downloadStateKey(d.Bucket, d.Key, dst))
	}
	if len(errs) == 0 {
		_ = store.clearCompleted()
	}
	return aborted, removed, errors.Join(errs...)
}
