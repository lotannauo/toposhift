//go:build !darwin

package rockskv

import (
	"os"
	"strings"
)

// cpuModel is the processor's name, from /proc/cpuinfo ("model name", or for an
// ARM processor its implementer and part), or "unknown".
func cpuModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	var impl, part string
	for line := range strings.SplitSeq(string(b), "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(name) {
		case "model name":
			return strings.TrimSpace(value)
		case "CPU implementer":
			if impl == "" {
				impl = strings.TrimSpace(value)
			}
		case "CPU part":
			if part == "" {
				part = strings.TrimSpace(value)
			}
		}
	}
	if impl != "" || part != "" {
		return "implementer " + impl + " part " + part
	}
	return "unknown"
}
