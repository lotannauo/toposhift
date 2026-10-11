//go:build darwin

package datadir

import (
	"syscall"
	"testing"
)

func TestClassifyFS(t *testing.T) {
	tests := []struct {
		fstype, want string
	}{
		{"nfs", "nfs"},
		{"smbfs", "smbfs"},
		{"apfs", ""},
		{"hfs", ""},
		{"nfsx", ""},
		{"", ""},
	}
	for _, tt := range tests {
		var st syscall.Statfs_t
		for i := 0; i < len(tt.fstype); i++ {
			st.Fstypename[i] = int8(tt.fstype[i])
		}
		if got := classifyFS(&st); got != tt.want {
			t.Errorf("classifyFS(%q) = %q, want %q", tt.fstype, got, tt.want)
		}
	}
}

func TestNetworkFSOnALocalDirectory(t *testing.T) {
	kind, err := networkFS(t.TempDir())
	if err != nil || kind != "" {
		t.Errorf("networkFS(temp dir) = %q, %v; want a local filesystem", kind, err)
	}
	if _, err := networkFS("/does/not/exist"); err == nil {
		t.Error("networkFS of a missing path returned no error")
	}
}
