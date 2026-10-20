//go:build windows

package url

import "testing"

func TestJoinKeepsUNCPrefix(t *testing.T) {
	for _, tc := range []struct{ base, add, want string }{
		{`\\nas01\share\dl\`, "file.bin", "//nas01/share/dl/file.bin"},
		{`//localhost/C$/Users/x/`, "a/b.txt", "//localhost/C$/Users/x/a/b.txt"},
		{`C:\data\`, "f.txt", "C:/data/f.txt"},
	} {
		u, err := New(tc.base)
		if err != nil {
			t.Fatal(err)
		}
		if got := u.Join(tc.add).Path; got != tc.want {
			t.Errorf("Join(%q, %q) = %q, want %q", tc.base, tc.add, got, tc.want)
		}
	}
}
