//go:build linux

package datadir

import (
	"reflect"
	"syscall"
	"testing"
)

// setType stores a 32-bit f_type in the field whatever its width and sign on
// this architecture.
func setType(st *syscall.Statfs_t, v uint32) {
	f := reflect.ValueOf(&st.Type).Elem()
	switch f.Kind() {
	case reflect.Int32:
		f.SetInt(int64(int32(v)))
	case reflect.Int64:
		f.SetInt(int64(v))
	default:
		f.SetUint(uint64(v))
	}
}

func TestClassifyFS(t *testing.T) {
	tests := []struct {
		fType uint32
		want  string
	}{
		{0x6969, "nfs"},
		{0x517B, "smb"},
		{0xFF534D42, "cifs"},
		{0xFE534D42, "smb2"},
		{0xEF53, ""},     // ext4
		{0x58465342, ""}, // xfs
		{0, ""},
	}
	for _, tt := range tests {
		var st syscall.Statfs_t
		setType(&st, tt.fType)
		if got := classifyFS(&st); got != tt.want {
			t.Errorf("classifyFS(%#x) = %q, want %q", tt.fType, got, tt.want)
		}
	}
}

func TestNetworkFSOnALocalDirectory(t *testing.T) {
	if _, err := networkFS("/does/not/exist"); err == nil {
		t.Error("networkFS of a missing path returned no error")
	}
}
