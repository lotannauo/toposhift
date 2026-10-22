package xtnode

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// The environment variables of the server script that change the node's configuration. A
// node started with either is not the registered one.
const (
	EnvJavaOptions = "TOPOSHIFT_XTDB_JAVA_OPTS"
	EnvConfig      = "TOPOSHIFT_XTDB_CONFIG"
)

// Overrides returns the names of the configuration overrides that are set, in a fixed
// order, given a lookup of the environment.
func Overrides(lookup func(string) (string, bool)) []string {
	var out []string
	for _, name := range []string{EnvConfig, EnvJavaOptions} {
		if v, ok := lookup(name); ok && v != "" {
			out = append(out, name)
		}
	}
	return out
}

// Pinned is what the server script and the configuration file register: the image, the
// JVM options and the hash of the node's configuration.
type Pinned struct {
	// Image is the image reference the script pulls, by the digest of its index.
	Image string
	// JavaOptions are the JVM options the script starts the node with when no override is
	// given.
	JavaOptions string
	// ConfigSHA256 is the SHA-256 of the configuration file beside the script.
	ConfigSHA256 string
}

var (
	imageLine = regexp.MustCompile(`(?m)^image='([^']+)'\s*$`)
	javaLine  = regexp.MustCompile(`(?m)^registered_jvm_options='([^']+)'\s*$`)
)

// ReadPinned reads the registered image and options from the server script, and the hash of
// the configuration file (xtdb.yaml) in the script's directory. The image must name a
// digest: a tag alone is not a pin.
func ReadPinned(script string) (Pinned, error) {
	b, err := os.ReadFile(script)
	if err != nil {
		return Pinned{}, err
	}
	var p Pinned
	if m := imageLine.FindSubmatch(b); m != nil {
		p.Image = string(m[1])
	}
	if m := javaLine.FindSubmatch(b); m != nil {
		p.JavaOptions = string(m[1])
	}
	if p.Image == "" || p.JavaOptions == "" {
		return Pinned{}, fmt.Errorf("xtnode: %s has no image or no registered JVM options line", script)
	}
	if !strings.Contains(p.Image, "@sha256:") {
		return Pinned{}, fmt.Errorf("xtnode: the image %s of %s is not pinned by a digest", p.Image, script)
	}
	cfg, err := os.ReadFile(filepath.Join(filepath.Dir(script), "xtdb.yaml"))
	if err != nil {
		return Pinned{}, err
	}
	sum := sha256.Sum256(cfg)
	p.ConfigSHA256 = hex.EncodeToString(sum[:])
	return p, nil
}

// Container is what a running container says about the node in it.
type Container struct {
	Name string
	// ImageRef is the reference the container was created from; ImageID the image's id.
	ImageRef, ImageID string
	// JavaOptions is the container's JDK_JAVA_OPTIONS, XTDBVersion and GitSHA the image's own
	// variables.
	JavaOptions, XTDBVersion, GitSHA string
	// ConfigFile and DataDir are the host paths mounted at /config/xtdb.yaml and
	// /var/lib/xtdb.
	ConfigFile, DataDir string
	// ConfigSHA256 is the hash of ConfigFile as it is now, if it could be read.
	ConfigSHA256 string
	Running      bool
}

// ParseInspect reads the JSON `docker inspect` prints for one container.
func ParseInspect(b []byte) (Container, error) {
	var all []struct {
		Name   string
		Image  string
		State  struct{ Running bool }
		Config struct {
			Image string
			Env   []string
		}
		Mounts []struct{ Source, Destination string }
	}
	if err := json.Unmarshal(b, &all); err != nil {
		return Container{}, fmt.Errorf("xtnode: docker inspect: %w", err)
	}
	if len(all) != 1 {
		return Container{}, fmt.Errorf("xtnode: docker inspect described %d containers, not one", len(all))
	}
	d := all[0]
	c := Container{Name: strings.TrimPrefix(d.Name, "/"), ImageRef: d.Config.Image, ImageID: d.Image, Running: d.State.Running}
	for _, e := range d.Config.Env {
		k, v, _ := strings.Cut(e, "=")
		switch k {
		case "JDK_JAVA_OPTIONS":
			c.JavaOptions = v
		case "XTDB_VERSION":
			c.XTDBVersion = v
		case "GIT_SHA":
			c.GitSHA = v
		}
	}
	for _, m := range d.Mounts {
		switch m.Destination {
		case "/config/xtdb.yaml":
			c.ConfigFile = m.Source
		case "/var/lib/xtdb":
			c.DataDir = m.Source
		}
	}
	return c, nil
}

// Docker runs the docker command with arguments and returns its standard output.
type Docker func(ctx context.Context, args ...string) ([]byte, error)

// RunDocker is [Docker] by the docker command on the path.
func RunDocker(ctx context.Context, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}

// Inspect describes the container with the name, including the hash of the configuration
// file it mounts.
func Inspect(ctx context.Context, run Docker, name string) (Container, error) {
	b, err := run(ctx, "inspect", name)
	if err != nil {
		return Container{}, fmt.Errorf("xtnode: %w", err)
	}
	c, err := ParseInspect(b)
	if err != nil {
		return Container{}, err
	}
	if c.ConfigFile != "" {
		if cfg, err := os.ReadFile(c.ConfigFile); err == nil {
			sum := sha256.Sum256(cfg)
			c.ConfigSHA256 = hex.EncodeToString(sum[:])
		}
	}
	return c, nil
}

// NotRegistered says in what ways a node differs from the registered configuration: the
// overrides that are set, and, if the container is known, an image, JVM options or
// configuration that is not the pinned one. An empty result is a registered node. A
// container of nil is a node whose process was not inspected, which is not registered either.
func NotRegistered(p Pinned, c *Container, overrides []string) []string {
	var out []string
	for _, o := range overrides {
		out = append(out, "the override "+o+" is set")
	}
	if c == nil {
		return append(out, "the node's container was not inspected")
	}
	if !c.Running {
		out = append(out, "the container is not running")
	}
	if c.ImageRef != p.Image {
		out = append(out, fmt.Sprintf("the container's image is %s, not %s", c.ImageRef, p.Image))
	}
	if c.JavaOptions != p.JavaOptions {
		out = append(out, fmt.Sprintf("the container's JVM options are %q, not %q", c.JavaOptions, p.JavaOptions))
	}
	if c.ConfigSHA256 != p.ConfigSHA256 {
		out = append(out, "the container's configuration file is not xtdb.yaml (hash "+c.ConfigSHA256+")")
	}
	return out
}

// Dir is the size of a directory the node writes to.
type Dir struct {
	Path string `json:"path"`
	// Bytes and Files are the regular files under it. ListingDigest is the SHA-256 of
	// their sorted relative names and sizes, one per line, so that two directories that
	// hold the same files at the same sizes have the same digest.
	Bytes         int64  `json:"bytes"`
	Files         int    `json:"files"`
	ListingDigest string `json:"listing_digest"`
}

// MeasureDir sums the regular files under dir. A file that goes while it is being listed is
// an error: the directory is being written to.
func MeasureDir(dir string) (Dir, error) {
	d := Dir{Path: dir}
	var lines []string
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		d.Bytes += info.Size()
		d.Files++
		lines = append(lines, fmt.Sprintf("%s %d\n", filepath.ToSlash(rel), info.Size()))
		return nil
	})
	if err != nil {
		return Dir{}, fmt.Errorf("xtnode: size of %s: %w", dir, err)
	}
	slices.Sort(lines)
	h := sha256.New()
	for _, l := range lines {
		h.Write([]byte(l))
	}
	d.ListingDigest = hex.EncodeToString(h.Sum(nil))
	return d, nil
}

// ParseCPUInfo names the processor in the text of /proc/cpuinfo: the "model name" of the
// first processor where there is one (x86), and otherwise the implementer, architecture,
// variant and part it reports (arm). A text that names none is "unknown", not an empty
// name.
func ParseCPUInfo(text string) string {
	sc := bufio.NewScanner(strings.NewReader(text))
	fields := map[string]string{}
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			if strings.TrimSpace(sc.Text()) == "" && len(fields) > 0 {
				break // the first processor ends at the blank line
			}
			continue
		}
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if _, seen := fields[name]; !seen {
			fields[name] = value
		}
	}
	if m := fields["model name"]; m != "" {
		return m
	}
	var parts []string
	for _, k := range []string{"CPU implementer", "CPU architecture", "CPU variant", "CPU part"} {
		if v := fields[k]; v != "" {
			parts = append(parts, k+" "+v)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, ", ")
	}
	return "unknown"
}

// CPUModel names the processor of this machine from /proc/cpuinfo, or "unknown" where the
// machine has none.
func CPUModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	return ParseCPUInfo(string(b))
}
