package rockskv

import "syscall"

// cpuModel is the processor's name, as the system gives it.
func cpuModel() string {
	if s, err := syscall.Sysctl("machdep.cpu.brand_string"); err == nil && s != "" {
		return s
	}
	return "unknown"
}
