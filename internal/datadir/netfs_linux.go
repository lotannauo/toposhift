//go:build linux

package datadir

import "syscall"

// The f_type values of statfs(2) for network filesystems.
const (
	magicNFS  = 0x6969
	magicSMB  = 0x517B
	magicCIFS = 0xFF534D42
	magicSMB2 = 0xFE534D42
	nameNFS   = "nfs"
	nameSMB   = "smb"
	nameCIFS  = "cifs"
	nameSMB2  = "smb2"
)

// networkFS names the network filesystem path is on, or returns "".
func networkFS(path string) (string, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return "", err
	}
	return classifyFS(&st), nil
}

// classifyFS names the network filesystem a Statfs result describes. The type
// is compared as 32 bits, because the field is signed and 32 bits wide on some
// architectures, and the CIFS and SMB2 values do not fit a signed 32.
func classifyFS(st *syscall.Statfs_t) string {
	switch uint32(st.Type) {
	case magicNFS:
		return nameNFS
	case magicSMB:
		return nameSMB
	case magicCIFS:
		return nameCIFS
	case magicSMB2:
		return nameSMB2
	}
	return ""
}
