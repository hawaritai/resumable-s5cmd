package command

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

// NewCleanupUploadsCommand aborts stale incomplete multipart uploads (the model of `rclone cleanup` and of
// an S3 AbortIncompleteMultipartUpload lifecycle rule), but only with object-level permissions and while
// keeping the uploads of transfers that may still resume.
func NewCleanupUploadsCommand() *cli.Command {
	return &cli.Command{
		Name:      "cleanup-uploads",
		HelpName:  "cleanup-uploads",
		Usage:     "abort incomplete multipart uploads older than a given age",
		ArgsUsage: "s3://bucket[/prefix]",
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:  "older-than",
				Value: 7 * 24 * time.Hour,
				Usage: "only abort uploads started longer ago than this (e.g. 24h, 168h)",
			},
			&cli.TimestampFlag{
				Name:   "started-after",
				Layout: time.RFC3339,
				Usage:  "only abort uploads started after this time (RFC3339), e.g. those of one cancelled run",
			},
			&cli.StringFlag{
				Name:  "keep-upload-ids",
				Usage: "file with upload ids (one per line) that must never be aborted (resumable transfers)",
			},
		},
		Action: func(c *cli.Context) error {
			if c.Args().Len() != 1 {
				return cli.Exit("expected one argument: s3://bucket[/prefix]", 1)
			}
			target, err := url.New(c.Args().First())
			if err != nil || !target.IsRemote() || target.Bucket == "" {
				return cli.Exit(fmt.Sprintf("invalid target %q: expected s3://bucket[/prefix]", c.Args().First()), 1)
			}
			keep, err := readKeepIDs(c.String("keep-upload-ids"))
			if err != nil {
				return cli.Exit(err.Error(), 1)
			}
			client, err := storage.NewRemoteClient(c.Context, target, NewStorageOpts(c))
			if err != nil {
				return cli.Exit(err.Error(), 1)
			}
			cutoff := time.Now().Add(-c.Duration("older-than"))
			var startedAfter time.Time
			if ts := c.Timestamp("started-after"); ts != nil {
				startedAfter = *ts
			}
			dryRun := c.Bool("dry-run")

			var found, aborted, kept, recent, failed int64
			var wg sync.WaitGroup
			sem := make(chan struct{}, 16)
			listErr := client.ListIncompleteUploads(c.Context, target.Bucket, target.Path, func(u storage.IncompleteUpload) bool {
				atomic.AddInt64(&found, 1)
				switch {
				case keep[u.UploadID]:
					atomic.AddInt64(&kept, 1)
					return true
				case !u.Initiated.IsZero() && u.Initiated.After(cutoff):
					atomic.AddInt64(&recent, 1)
					return true
				case !startedAfter.IsZero() && !u.Initiated.IsZero() && u.Initiated.Before(startedAfter):
					atomic.AddInt64(&recent, 1) // outside the requested window: not ours to touch
					return true
				}
				if dryRun {
					printCleanupEvent(u, "would_abort", nil)
					return true
				}
				sem <- struct{}{}
				wg.Add(1)
				go func(u storage.IncompleteUpload) {
					defer func() { <-sem; wg.Done() }()
					aerr := client.AbortIncompleteUpload(c.Context, target.Bucket, u.Key, u.UploadID)
					if aerr != nil {
						atomic.AddInt64(&failed, 1)
					} else {
						atomic.AddInt64(&aborted, 1)
					}
					printCleanupEvent(u, "aborted", aerr)
				}(u)
				return c.Context.Err() == nil
			})
			wg.Wait()

			summary := map[string]interface{}{
				"operation": "cleanup-uploads-summary", "bucket": target.Bucket, "prefix": target.Path,
				"found": found, "aborted": aborted, "kept": kept, "recent": recent, "failed": failed,
				"dry_run": dryRun, "success": listErr == nil && failed == 0,
			}
			if listErr != nil {
				summary["error"] = listErr.Error()
			}
			if b, mErr := json.Marshal(summary); mErr == nil {
				fmt.Println(string(b))
			}
			if listErr != nil || failed > 0 {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func readKeepIDs(path string) (map[string]bool, error) {
	keep := map[string]bool{}
	if path == "" {
		return keep, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read --keep-upload-ids: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if id := strings.TrimSpace(sc.Text()); id != "" {
			keep[id] = true
		}
	}
	return keep, sc.Err()
}

func printCleanupEvent(u storage.IncompleteUpload, action string, err error) {
	msg := map[string]interface{}{
		"operation": "cleanup-uploads", "key": u.Key, "upload_id": u.UploadID,
		"initiated": u.Initiated.UTC().Format(time.RFC3339), "action": action, "success": err == nil,
	}
	if err != nil {
		msg["error"] = err.Error()
	}
	if b, mErr := json.Marshal(msg); mErr == nil {
		fmt.Println(string(b))
	}
}
