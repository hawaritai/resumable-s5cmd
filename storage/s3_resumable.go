package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/peak/s5cmd/v2/storage/url"
)

// ResumableState records multipart upload metadata for pause & resume.
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

func saveStateFile(filename string, state ResumableState) {
	if filename == "" {
		return
	}
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	tmp := filename + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err == nil {
		_ = os.Rename(tmp, filename)
	}
}

// ResumablePut executes high-speed parallel multipart uploads with byte/chunk state persistence.
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
		m := make(map[string]*string)
		for k, v := range metadata.UserDefined {
			m[k] = aws.String(v)
		}
		input.Metadata = m
	}

	// Fallback to standard uploader if reader is not a file or state file not specified
	if file == nil || s.stateFile == "" {
		uploaderOptsFn := func(u *s3manager.Uploader) {
			u.PartSize = partSize
			u.Concurrency = concurrency
		}
		_, err := s.uploader.UploadWithContext(ctx, input, uploaderOptsFn)
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	// For small files (<= partSize), use standard upload
	if fileSize <= partSize {
		uploaderOptsFn := func(u *s3manager.Uploader) {
			u.PartSize = partSize
			u.Concurrency = concurrency
		}
		_, err := s.uploader.UploadWithContext(ctx, input, uploaderOptsFn)
		return err
	}

	state := ResumableState{
		Bucket:   to.Bucket,
		Key:      to.Path,
		PartSize: partSize,
		FileSize: fileSize,
		Parts:    make(map[int64]string),
	}

	// 1. Check existing state file and query S3 ListParts
	if rawBytes, err := os.ReadFile(s.stateFile); err == nil {
		var existing ResumableState
		if err := json.Unmarshal(rawBytes, &existing); err == nil {
			if existing.Bucket == to.Bucket && existing.Key == to.Path && existing.FileSize == fileSize && existing.UploadID != "" {
				listInput := &s3.ListPartsInput{
					Bucket:       aws.String(to.Bucket),
					Key:          aws.String(to.Path),
					UploadId:     aws.String(existing.UploadID),
					RequestPayer: s.RequestPayer(),
				}
				partsOutput, err := s.api.ListPartsWithContext(ctx, listInput)
				if err == nil {
					state.UploadID = existing.UploadID
					for _, p := range partsOutput.Parts {
						if p.PartNumber != nil && p.ETag != nil {
							state.Parts[*p.PartNumber] = *p.ETag
						}
					}
				}
			}
		}
	}

	// 2. Initiate new multipart upload if needed
	if state.UploadID == "" {
		createInput := &s3.CreateMultipartUploadInput{
			Bucket:       aws.String(to.Bucket),
			Key:          aws.String(to.Path),
			ContentType:  aws.String(contentType),
			Metadata:     input.Metadata,
			RequestPayer: s.RequestPayer(),
		}
		if metadata.StorageClass != "" {
			createInput.StorageClass = aws.String(metadata.StorageClass)
		}
		if metadata.ACL != "" {
			createInput.ACL = aws.String(metadata.ACL)
		}
		if metadata.CacheControl != "" {
			createInput.CacheControl = aws.String(metadata.CacheControl)
		}
		if metadata.Expires != "" {
			if t, err := time.Parse(time.RFC3339, metadata.Expires); err == nil {
				createInput.Expires = aws.Time(t)
			}
		}
		if metadata.EncryptionMethod != "" {
			createInput.ServerSideEncryption = aws.String(metadata.EncryptionMethod)
			if metadata.EncryptionKeyID != "" {
				createInput.SSEKMSKeyId = aws.String(metadata.EncryptionKeyID)
			}
		}

		createResp, err := s.api.CreateMultipartUploadWithContext(ctx, createInput)
		if err != nil {
			return fmt.Errorf("failed to initiate multipart upload: %w", err)
		}
		state.UploadID = *createResp.UploadId
		saveStateFile(s.stateFile, state)
	}

	totalParts := int(math.Ceil(float64(fileSize) / float64(partSize)))
	state.TotalParts = totalParts

	// Calculate already completed bytes
	var completedBytes int64
	for pNum := range state.Parts {
		if int(pNum) == totalParts {
			rem := fileSize - (pNum-1)*partSize
			completedBytes += rem
		} else {
			completedBytes += partSize
		}
	}

	if completedBytes > 0 {
		if prog, ok := reader.(progressProvider); ok {
			prog.AddCompletedBytes(completedBytes)
		}
	}

	// Emit JSON telemetry event
	initMsg := map[string]interface{}{
		"operation":       "multipart_init",
		"upload_id":       state.UploadID,
		"bucket":          to.Bucket,
		"key":             to.Path,
		"total_bytes":     fileSize,
		"total_parts":     totalParts,
		"completed_parts": len(state.Parts),
		"resumed_bytes":   completedBytes,
	}
	if b, err := json.Marshal(initMsg); err == nil {
		fmt.Println(string(b))
	}

	// 3. Complete early if all parts already finished
	if len(state.Parts) == totalParts {
		return s.completeMultipart(ctx, to, state)
	}

	// 4. Build list of missing parts
	type partTask struct {
		number int64
		offset int64
		length int64
	}

	var tasks []partTask
	for p := int64(1); p <= int64(totalParts); p++ {
		if _, ok := state.Parts[p]; ok {
			continue // Skip already uploaded chunk!
		}
		offset := (p - 1) * partSize
		length := partSize
		if offset+length > fileSize {
			length = fileSize - offset
		}
		tasks = append(tasks, partTask{
			number: p,
			offset: offset,
			length: length,
		})
	}

	if concurrency <= 0 {
		concurrency = 4
	}

	// 5. Execute parallel worker pool
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
	)

	for _, t := range tasks {
		if ctx.Err() != nil {
			break
		}
		mu.Lock()
		if firstErr != nil {
			mu.Unlock()
			break
		}
		mu.Unlock()

		sem <- struct{}{}
		wg.Add(1)

		go func(task partTask) {
			defer func() {
				<-sem
				wg.Done()
			}()

			if ctx.Err() != nil {
				return
			}

			buf := make([]byte, task.length)
			_, err := file.ReadAt(buf, task.offset)
			if err != nil && err != io.EOF {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("read error at offset %d: %w", task.offset, err)
				}
				mu.Unlock()
				return
			}

			partInput := &s3.UploadPartInput{
				Bucket:       aws.String(to.Bucket),
				Key:          aws.String(to.Path),
				UploadId:     aws.String(state.UploadID),
				PartNumber:   aws.Int64(task.number),
				Body:         bytes.NewReader(buf),
				RequestPayer: s.RequestPayer(),
			}

			uploadResp, err := s.api.UploadPartWithContext(ctx, partInput)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to upload part %d: %w", task.number, err)
				}
				mu.Unlock()
				return
			}

			etag := *uploadResp.ETag
			mu.Lock()
			state.Parts[task.number] = etag
			saveStateFile(s.stateFile, state)
			mu.Unlock()

			if prog, ok := reader.(progressProvider); ok {
				prog.AddCompletedBytes(task.length)
			}

			partMsg := map[string]interface{}{
				"operation":   "multipart_part",
				"upload_id":   state.UploadID,
				"part_number": task.number,
				"total_parts": totalParts,
				"part_bytes":  task.length,
			}
			if b, err := json.Marshal(partMsg); err == nil {
				fmt.Println(string(b))
			}
		}(t)
	}

	wg.Wait()

	if firstErr != nil {
		saveStateFile(s.stateFile, state)
		return firstErr
	}
	if ctx.Err() != nil {
		saveStateFile(s.stateFile, state)
		return ctx.Err()
	}

	// 6. Complete multipart upload
	return s.completeMultipart(ctx, to, state)
}

func (s *S3) completeMultipart(ctx context.Context, to *url.URL, state ResumableState) error {
	var completedParts []*s3.CompletedPart
	for pNum, etag := range state.Parts {
		completedParts = append(completedParts, &s3.CompletedPart{
			PartNumber: aws.Int64(pNum),
			ETag:       aws.String(etag),
		})
	}
	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(to.Bucket),
		Key:             aws.String(to.Path),
		UploadId:        aws.String(state.UploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
		RequestPayer:    s.RequestPayer(),
	}

	_, err := s.api.CompleteMultipartUploadWithContext(ctx, completeInput)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	if s.stateFile != "" {
		_ = os.Remove(s.stateFile)
		_ = os.Remove(s.stateFile + ".tmp")
	}

	completeMsg := map[string]interface{}{
		"operation": "multipart_complete",
		"upload_id": state.UploadID,
		"bucket":    to.Bucket,
		"key":       to.Path,
		"success":   true,
	}
	if b, err := json.Marshal(completeMsg); err == nil {
		fmt.Println(string(b))
	}

	return nil
}

// ResumableDownloadState records download chunk state for pause & resume.
type ResumableDownloadState struct {
	Bucket     string         `json:"bucket"`
	Key        string         `json:"key"`
	PartPath   string         `json:"part_path"`
	PartSize   int64          `json:"part_size"`
	FileSize   int64          `json:"file_size"`
	TotalParts int            `json:"total_parts"`
	Parts      map[int64]bool `json:"parts"` // PartNumber -> true (completed)
}

func saveDownloadStateFile(filename string, state ResumableDownloadState) {
	if filename == "" {
		return
	}
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	tmp := filename + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err == nil {
		_ = os.Rename(tmp, filename)
	}
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
	_ = in.Close()
	_ = out.Close()
	_ = os.Remove(src)
	return nil
}

// ResumableGet executes high-speed parallel range downloads with byte/chunk state persistence.
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

	headInput := &s3.HeadObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.RequestPayer(),
	}
	head, err := s.api.HeadObjectWithContext(ctx, headInput)
	if err != nil {
		return 0, fmt.Errorf("head object failed (%s): %w", from.Absolute(), err)
	}
	fileSize := aws.Int64Value(head.ContentLength)
	if fileSize == 0 {
		_ = os.MkdirAll(filepath.Dir(dstFilePath), 0o755)
		if err := os.WriteFile(dstFilePath, []byte{}, 0o644); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if partSize <= 0 {
		partSize = 16 * 1024 * 1024
	}
	minPart := int64(math.Ceil(float64(fileSize) / 9900.0))
	if partSize < minPart {
		partSize = minPart
	}

	totalParts := int(math.Ceil(float64(fileSize) / float64(partSize)))
	partPath := dstFilePath + ".s5part"
	_ = os.MkdirAll(filepath.Dir(dstFilePath), 0o755)

	state := ResumableDownloadState{
		Bucket:     from.Bucket,
		Key:        from.Path,
		PartPath:   partPath,
		PartSize:   partSize,
		FileSize:   fileSize,
		TotalParts: totalParts,
		Parts:      make(map[int64]bool),
	}

	// 1. Check existing state file and verify partial file
	if s.stateFile != "" {
		if rawBytes, err := os.ReadFile(s.stateFile); err == nil {
			var existing ResumableDownloadState
			if err := json.Unmarshal(rawBytes, &existing); err == nil {
				if existing.Bucket == from.Bucket && existing.Key == from.Path && existing.FileSize == fileSize {
					if fi, err := os.Stat(partPath); err == nil && fi.Size() == fileSize {
						state.Parts = existing.Parts
					}
				}
			}
		}
	}

	// 2. Open or create partial file pre-allocated to fileSize
	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return 0, fmt.Errorf("cannot open download partial file (%s): %w", partPath, err)
	}
	defer f.Close()

	if fi, err := f.Stat(); err == nil && fi.Size() != fileSize {
		_ = f.Truncate(fileSize)
	}

	// 3. Calculate already completed bytes
	var completedBytes int64
	for pNum := range state.Parts {
		if int(pNum) == totalParts {
			rem := fileSize - (pNum-1)*partSize
			completedBytes += rem
		} else {
			completedBytes += partSize
		}
	}

	if completedBytes > 0 && prog != nil {
		prog.AddCompletedBytes(completedBytes)
	}

	// Emit JSON telemetry event
	initMsg := map[string]interface{}{
		"operation":       "multipart_init",
		"direction":       "download",
		"bucket":          from.Bucket,
		"key":             from.Path,
		"total_bytes":     fileSize,
		"total_parts":     totalParts,
		"completed_parts": len(state.Parts),
		"resumed_bytes":   completedBytes,
	}
	if b, err := json.Marshal(initMsg); err == nil {
		fmt.Println(string(b))
	}

	// 4. Complete early if all parts already downloaded
	if len(state.Parts) == totalParts {
		_ = f.Close()
		_ = os.Remove(dstFilePath)
		if err := os.Rename(partPath, dstFilePath); err != nil {
			if cpErr := copyAndRemoveFile(partPath, dstFilePath); cpErr != nil {
				return 0, cpErr
			}
		}
		if s.stateFile != "" {
			_ = os.Remove(s.stateFile)
			_ = os.Remove(s.stateFile + ".tmp")
		}
		completeMsg := map[string]interface{}{
			"operation": "multipart_complete",
			"direction": "download",
			"bucket":    from.Bucket,
			"key":       from.Path,
			"success":   true,
		}
		if b, err := json.Marshal(completeMsg); err == nil {
			fmt.Println(string(b))
		}
		return fileSize, nil
	}

	// 5. Build list of missing parts
	type dlTask struct {
		number int64
		offset int64
		length int64
	}
	var tasks []dlTask
	for p := int64(1); p <= int64(totalParts); p++ {
		if state.Parts[p] {
			continue
		}
		offset := (p - 1) * partSize
		length := partSize
		if offset+length > fileSize {
			length = fileSize - offset
		}
		tasks = append(tasks, dlTask{
			number: p,
			offset: offset,
			length: length,
		})
	}

	if concurrency <= 0 {
		concurrency = 4
	}

	// 6. Execute parallel worker pool
	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
	)

	for _, t := range tasks {
		if ctx.Err() != nil {
			break
		}
		mu.Lock()
		if firstErr != nil {
			mu.Unlock()
			break
		}
		mu.Unlock()

		sem <- struct{}{}
		wg.Add(1)

		go func(task dlTask) {
			defer func() {
				<-sem
				wg.Done()
			}()

			if ctx.Err() != nil {
				return
			}

			rangeHeader := fmt.Sprintf("bytes=%d-%d", task.offset, task.offset+task.length-1)
			getInput := &s3.GetObjectInput{
				Bucket:       aws.String(from.Bucket),
				Key:          aws.String(from.Path),
				Range:        aws.String(rangeHeader),
				RequestPayer: s.RequestPayer(),
			}

			getResp, err := s.api.GetObjectWithContext(ctx, getInput)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to download part %d: %w", task.number, err)
				}
				mu.Unlock()
				return
			}
			defer getResp.Body.Close()

			buf, err := io.ReadAll(getResp.Body)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to read body for part %d: %w", task.number, err)
				}
				mu.Unlock()
				return
			}

			if int64(len(buf)) != task.length {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("part %d length mismatch: expected %d, got %d", task.number, task.length, len(buf))
				}
				mu.Unlock()
				return
			}

			_, err = f.WriteAt(buf, task.offset)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("write error at offset %d: %w", task.offset, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			state.Parts[task.number] = true
			if s.stateFile != "" {
				saveDownloadStateFile(s.stateFile, state)
			}
			mu.Unlock()

			if prog != nil {
				prog.AddCompletedBytes(task.length)
			}

			partMsg := map[string]interface{}{
				"operation":   "multipart_part",
				"direction":   "download",
				"part_number": task.number,
				"total_parts": totalParts,
				"part_bytes":  task.length,
			}
			if b, err := json.Marshal(partMsg); err == nil {
				fmt.Println(string(b))
			}
		}(t)
	}

	wg.Wait()

	if firstErr != nil {
		if s.stateFile != "" {
			saveDownloadStateFile(s.stateFile, state)
		}
		return 0, firstErr
	}
	if ctx.Err() != nil {
		if s.stateFile != "" {
			saveDownloadStateFile(s.stateFile, state)
		}
		return 0, ctx.Err()
	}

	// 7. Atomic finalize and rename
	_ = f.Close()
	_ = os.Remove(dstFilePath)
	if err := os.Rename(partPath, dstFilePath); err != nil {
		if cpErr := copyAndRemoveFile(partPath, dstFilePath); cpErr != nil {
			return 0, cpErr
		}
	}

	if s.stateFile != "" {
		_ = os.Remove(s.stateFile)
		_ = os.Remove(s.stateFile + ".tmp")
	}

	completeMsg := map[string]interface{}{
		"operation": "multipart_complete",
		"direction": "download",
		"bucket":    from.Bucket,
		"key":       from.Path,
		"success":   true,
	}
	if b, err := json.Marshal(completeMsg); err == nil {
		fmt.Println(string(b))
	}

	return fileSize, nil
}
