package command

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// maxCredentialLineBytes bounds each credential line read from stdin. STS session tokens can be a few
// KB; anything far beyond that is not a credential and is rejected instead of buffered.
const maxCredentialLineBytes = 16 * 1024

// credentialStdinVars are read from stdin, one value per line, in this order. A blank line unsets the
// variable, so a missing session token cannot be inherited from the parent environment.
var credentialStdinVars = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}

// loadCredentialsFromStdin reads the access key, secret key and (optional) session token from r and
// exports them into this process's environment, where the AWS SDK's default chain picks them up.
//
// This keeps secrets off the command line (visible in process listings) and out of script files on
// remote hosts: the caller streams them over an already-encrypted channel (e.g. SSH).
//
// Input is read one byte at a time, so nothing past the last credential line is consumed and stdin
// remains usable afterwards (e.g. by "run").
func loadCredentialsFromStdin(r io.Reader) error {
	for i, name := range credentialStdinVars {
		line, err := readCredentialLine(r)
		if err != nil {
			return fmt.Errorf("credentials-stdin: reading %s: %w", name, err)
		}
		if i < 2 && line == "" {
			return fmt.Errorf("credentials-stdin: %s is empty", name)
		}
		if line == "" {
			_ = os.Unsetenv(name)
			continue
		}
		if err := os.Setenv(name, line); err != nil {
			return fmt.Errorf("credentials-stdin: setting %s: %w", name, err)
		}
	}
	return nil
}

// readCredentialLine reads up to the next '\n' (or EOF), dropping a trailing '\r'. A missing final
// line (EOF) is returned as an empty value so the optional session token may be omitted entirely.
func readCredentialLine(r io.Reader) (string, error) {
	var sb strings.Builder
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		if n == 1 {
			if buf[0] == '\n' {
				break
			}
			if sb.Len() >= maxCredentialLineBytes {
				return "", fmt.Errorf("line exceeds %d bytes", maxCredentialLineBytes)
			}
			sb.WriteByte(buf[0])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return strings.TrimRight(sb.String(), "\r"), nil
}
