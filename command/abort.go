package command

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/storage"
)

// NewAbortCommand discards a cancelled transfer: it aborts the multipart uploads recorded in a
// --state-file (so their already-stored parts stop occupying, and being billed for, bucket space)
// and deletes partial download files. Equivalent to "aws s3api abort-multipart-upload" per object.
func NewAbortCommand() *cli.Command {
	return &cli.Command{
		Name:     "abort",
		HelpName: "abort",
		Usage:    "abort the multipart uploads and remove the partial downloads recorded in a state file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "state-file",
				Usage:    "state file written by 'cp --state-file'",
				Required: true,
			},
		},
		Action: func(c *cli.Context) error {
			path := c.String("state-file")
			if _, err := os.Stat(path); os.IsNotExist(err) {
				printAbortResult(0, 0, nil)
				return nil
			}
			aborted, removed, err := storage.AbortStateFile(c.Context, path, NewStorageOpts(c))
			printAbortResult(aborted, removed, err)
			if err != nil {
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func printAbortResult(aborted, removed int, err error) {
	msg := map[string]interface{}{
		"operation":         "abort",
		"aborted_uploads":   aborted,
		"removed_downloads": removed,
		"success":           err == nil,
	}
	if err != nil {
		msg["error"] = err.Error()
	}
	if b, mErr := json.Marshal(msg); mErr == nil {
		fmt.Println(string(b))
	}
}
