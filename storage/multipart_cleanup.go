package storage

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// IncompleteUpload is a multipart upload that was started but neither completed nor aborted. Its parts
// occupy (and are billed as) bucket storage until it is aborted.
type IncompleteUpload struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

// ListIncompleteUploads calls fn for every incomplete multipart upload under prefix (all pages).
// Returning false from fn stops the listing.
func (s *S3) ListIncompleteUploads(ctx context.Context, bucket, prefix string, fn func(IncompleteUpload) bool) error {
	input := &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket), RequestPayer: s.RequestPayer()}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}
	return s.api.ListMultipartUploadsPagesWithContext(ctx, input, func(page *s3.ListMultipartUploadsOutput, _ bool) bool {
		for _, u := range page.Uploads {
			if u.Key == nil || u.UploadId == nil {
				continue
			}
			if !fn(IncompleteUpload{Key: *u.Key, UploadID: *u.UploadId, Initiated: aws.TimeValue(u.Initiated)}) {
				return false
			}
		}
		return true
	})
}

// AbortIncompleteUpload aborts one multipart upload; an upload that no longer exists is not an error.
func (s *S3) AbortIncompleteUpload(ctx context.Context, bucket, key, uploadID string) error {
	return s.abortUpload(ctx, bucket, key, uploadID)
}
