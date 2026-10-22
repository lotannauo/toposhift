package vmrun

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
)

// Env is where a run was made and what it was made against: recorded in every output.
type Env struct {
	// Image is the image reference the registered script starts, tag and index digest
	// ("victoriametrics/victoria-metrics:v1.153.0@sha256:..."); ImageChecked says the
	// container named was inspected and was started from it.
	Image        string `json:",omitempty"`
	ImageChecked bool
	// Version is the server's own version ("v1.153.0"), checked against the image's tag.
	Version string `json:",omitempty"`
	// FlagsSHA256 is the SHA-256 of flags.conf; FlagsChecked says the server's flags were
	// read and are exactly the file's.
	FlagsSHA256  string
	FlagsChecked bool
	// CPU, OS, Arch and Go are the machine and toolchain the binary runs on; Revision and
	// Modified are the commit the binary was built at and whether the tree was modified
	// (empty and false in a build made outside a checkout).
	CPU, OS, Arch, Go string
	Revision          string `json:",omitempty"`
	Modified          bool
}

// EnvOptions say where the run is made and what to check.
type EnvOptions struct {
	// FlagsPath is flags.conf (required).
	FlagsPath string
	// ScriptPath is scripts/vm-server.sh, whose image line is the registered image. With
	// Container set it is required.
	ScriptPath string
	// Container is the name of the server's container: it is inspected, and refused unless
	// it was started from the registered image.
	Container string
	// Docker returns the image reference a container was started from; nil runs
	// `docker inspect`.
	Docker func(ctx context.Context, name string) (string, error)
}

// ErrImageDiffers marks a server container started from another image than the registered one.
var ErrImageDiffers = errors.New("vmrun: the container was not started from the registered image")

// ErrVersionDiffers marks a server whose version is not the registered image's tag.
var ErrVersionDiffers = errors.New("vmrun: the server's version is not the registered image's tag")

var imageLine = regexp.MustCompile(`(?m)^image='([^']+)'\s*$`)

// ParseImage reads the image reference out of vm-server.sh: its `image='...'` line.
func ParseImage(script []byte) (string, error) {
	m := imageLine.FindAllSubmatch(script, -1)
	if len(m) != 1 {
		return "", fmt.Errorf("vmrun: expected one image='...' line in the server script, found %d", len(m))
	}
	img := string(m[0][1])
	if ImageTag(img) == "" || !strings.Contains(img, "@sha256:") {
		return "", fmt.Errorf("vmrun: the image %q is not a tag and a digest", img)
	}
	return img, nil
}

// ImageTag returns the tag of an image reference with a tag and a digest.
func ImageTag(img string) string {
	name, _, _ := strings.Cut(img, "@")
	if i := strings.LastIndexByte(name, ':'); i >= 0 && !strings.Contains(name[i+1:], "/") {
		return name[i+1:]
	}
	return ""
}

// ServerVersion reads the version the server reports as its short version, such as v1.153.0.
func ServerVersion(m vmengine.Metrics) (string, error) {
	for k := range m {
		if strings.HasPrefix(k, "vm_app_version{") {
			if _, rest, ok := strings.Cut(k, `short_version="`); ok {
				v, _, _ := strings.Cut(rest, `"`)
				return v, nil
			}
		}
	}
	return "", errors.New("vmrun: the server does not report its version (vm_app_version)")
}

func dockerImage(ctx context.Context, name string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.Config.Image}}", name).Output()
	if err != nil {
		return "", fmt.Errorf("vmrun: docker inspect %s: %w", name, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// CollectEnv records where the run is made. With a client it also checks the server: its
// flags must be exactly those of flags.conf, and its version the registered image's tag;
// and with a container name, the container must have been started from the registered
// image. Any of these failing is an error and the Env returned holds what was learned.
func CollectEnv(ctx context.Context, c *vmengine.Client, o EnvOptions) (Env, error) {
	e := Env{OS: runtime.GOOS, Arch: runtime.GOARCH, Go: runtime.Version(), CPU: cpuModel(ctx)}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				e.Revision = s.Value
			case "vcs.modified":
				e.Modified = s.Value == "true"
			}
		}
	}
	if o.FlagsPath == "" {
		return e, errors.New("vmrun: no flags.conf")
	}
	flags, sum, err := vmengine.LoadFlags(o.FlagsPath)
	if err != nil {
		return e, err
	}
	e.FlagsSHA256 = sum
	if o.ScriptPath != "" {
		b, err := os.ReadFile(o.ScriptPath)
		if err != nil {
			return e, err
		}
		if e.Image, err = ParseImage(b); err != nil {
			return e, err
		}
	}
	if c == nil {
		return e, nil
	}
	if err := vmengine.CheckFlags(ctx, c, flags); err != nil {
		return e, err
	}
	e.FlagsChecked = true
	m, err := c.Metrics(ctx)
	if err != nil {
		return e, err
	}
	if e.Version, err = ServerVersion(m); err != nil {
		return e, err
	}
	if e.Image != "" {
		if want := ImageTag(e.Image); e.Version != want {
			return e, fmt.Errorf("%w: the server is %s and the image is tagged %s", ErrVersionDiffers, e.Version, want)
		}
	}
	if o.Container != "" {
		if e.Image == "" {
			return e, errors.New("vmrun: a container can be checked only against the server script's image (give its path)")
		}
		inspect := o.Docker
		if inspect == nil {
			inspect = dockerImage
		}
		got, err := inspect(ctx, o.Container)
		if err != nil {
			return e, err
		}
		if got != e.Image {
			return e, fmt.Errorf("%w: container %s runs %s, the registered image is %s", ErrImageDiffers, o.Container, got, e.Image)
		}
		e.ImageChecked = true
	}
	return e, nil
}

// cpuModel is the CPU's model name, as far as the machine says it ("unknown" otherwise).
func cpuModel(ctx context.Context) string {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.CommandContext(ctx, "sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return s
			}
		}
	case "linux":
		if out, err := exec.CommandContext(ctx, "lscpu").Output(); err == nil {
			sc := bufio.NewScanner(strings.NewReader(string(out)))
			for sc.Scan() {
				if name, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(name) == "Model name" {
					return strings.TrimSpace(v)
				}
			}
		}
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			sc := bufio.NewScanner(strings.NewReader(string(b)))
			for sc.Scan() {
				if name, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(name) == "model name" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	return "unknown"
}
