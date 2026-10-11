//go:build darwin

package datadir

import "syscall"

// networkFS names the network filesystem path is on, or returns "".
func networkFS(path string) (string, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return "", err
	}
	return classifyFS(&st), nil
}

// classifyFS names the network filesystem a Statfs result describes.
func classifyFS(st *syscall.Statfs_t) string {
	name := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		name = append(name, byte(c))
	}
	switch s := string(name); s {
	case "nfs", "smbfs":
		return s
	}
	return ""
}
