// Package scripts holds the files that make the Dolt server the baseline runs against, so
// that a program can say which server a record is of without being told: the settings
// (server.yaml), the global configuration that switches usage reporting off, and the
// script that starts the image. The files are what is embedded, so a binary names the very
// bytes it was built with, and the registered image is read from the script, the one place
// it is written.
package scripts

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"regexp"
)

// ServerYAML is the server's settings, the one source of every setting.
//
//go:embed server.yaml
var ServerYAML string

// GlobalJSON is the container's global configuration (usage reporting off).
//
//go:embed global.json
var GlobalJSON string

// ServerScript is the script that starts the server.
//
//go:embed dolt-server.sh
var ServerScript string

var (
	imageLine   = regexp.MustCompile(`(?m)^image='([^'\n]+)'$`)
	versionLine = regexp.MustCompile(`(?m)^version='([^'\n]+)'$`)
)

// Image is the image the script starts, by tag and by the digest of its multi-architecture
// index, as the script writes it: the registered server.
func Image() (string, error) { return one(imageLine, "image") }

// Version is the version of Dolt the script expects the server to say it is.
func Version() (string, error) { return one(versionLine, "version") }

func one(re *regexp.Regexp, what string) (string, error) {
	m := re.FindAllStringSubmatch(ServerScript, -1)
	if len(m) != 1 {
		return "", fmt.Errorf("scripts: dolt-server.sh has %d lines that set the %s, want one", len(m), what)
	}
	return m[0][1], nil
}

// SHA256 is the digest of a file's text, as hex.
func SHA256(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}
