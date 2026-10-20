package command

import (
	"os"
	"strings"
	"testing"
)

func TestLoadCredentialsFromStdin(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		wantAK    string
		wantSK    string
		wantToken string
		tokenSet  bool
	}{
		{name: "key secret token", input: "AK\nSK\nTOK\n", wantAK: "AK", wantSK: "SK", wantToken: "TOK", tokenSet: true},
		{name: "crlf line endings", input: "AK\r\nSK\r\nTOK\r\n", wantAK: "AK", wantSK: "SK", wantToken: "TOK", tokenSet: true},
		{name: "blank token unsets", input: "AK\nSK\n\n", wantAK: "AK", wantSK: "SK"},
		{name: "token omitted at eof", input: "AK\nSK\n", wantAK: "AK", wantSK: "SK"},
		{name: "no trailing newline", input: "AK\nSK", wantAK: "AK", wantSK: "SK"},
		{name: "shell metacharacters kept verbatim", input: "AK\n!\"^&%|<>'$(x)\n\n", wantAK: "AK", wantSK: "!\"^&%|<>'$(x)"},
		{name: "empty access key", input: "\nSK\n", wantErr: true},
		{name: "empty secret key", input: "AK\n\n", wantErr: true},
		{name: "empty input", input: "", wantErr: true},
		{name: "oversized line", input: strings.Repeat("x", maxCredentialLineBytes+1) + "\n", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A stale token from the parent environment must never survive.
			t.Setenv("AWS_SESSION_TOKEN", "stale")
			t.Setenv("AWS_ACCESS_KEY_ID", "")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "")

			err := loadCredentialsFromStdin(strings.NewReader(tc.input))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := os.Getenv("AWS_ACCESS_KEY_ID"); got != tc.wantAK {
				t.Errorf("AWS_ACCESS_KEY_ID = %q, want %q", got, tc.wantAK)
			}
			if got := os.Getenv("AWS_SECRET_ACCESS_KEY"); got != tc.wantSK {
				t.Errorf("AWS_SECRET_ACCESS_KEY = %q, want %q", got, tc.wantSK)
			}
			got, ok := os.LookupEnv("AWS_SESSION_TOKEN")
			if ok != tc.tokenSet || got != tc.wantToken {
				t.Errorf("AWS_SESSION_TOKEN = %q (set=%v), want %q (set=%v)", got, ok, tc.wantToken, tc.tokenSet)
			}
		})
	}
}

func TestLoadCredentialsFromStdinLeavesRemainingInput(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	r := strings.NewReader("AK\nSK\n\nls s3://bucket\n")
	if err := loadCredentialsFromStdin(r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rest := make([]byte, 64)
	n, _ := r.Read(rest)
	if got := string(rest[:n]); got != "ls s3://bucket\n" {
		t.Errorf("remaining stdin = %q, want %q", got, "ls s3://bucket\n")
	}
}
