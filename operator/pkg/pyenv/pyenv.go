// SPDX-License-Identifier: Apache-2.0

// Package pyenv completes the Python of an image the agent did not build: a
// provider's image that already carries CUDA and torch, say. It never
// reinstalls what the image has. A venv on the image's interpreter sees the
// image's packages (--system-site-packages), and uv installs only what is
// missing, constrained to the versions the image already has, so torch is
// neither upgraded nor downloaded again.
//
// Nothing here needs root, a package manager or a shell: uv is one static
// binary, pinned through SHA256SUMS and upstream.lock (FetchUV).
package pyenv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-logr/logr"
)

// EnvToolchains names the toolchains a container prepares, as JSON:
// [{"name":"python","version":"3.12","packages":["requests"]}].
const EnvToolchains = "ZENTHROS_TOOLCHAINS"

// Toolchain is one entry of EnvToolchains, the shape of a pool's
// runtime.toolchains.
type Toolchain struct {
	Name     string   `json:"name"`
	Version  string   `json:"version,omitempty"`
	Packages []string `json:"packages,omitempty"`
}

// Parse reads EnvToolchains. Only python is completed in a container: the
// other toolchains need an installer, which a container does not run.
func Parse(raw string) ([]Toolchain, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out []Toolchain
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("%s: %w", EnvToolchains, err)
	}
	for _, tc := range out {
		if tc.Name != "python" {
			return nil, fmt.Errorf("%s: toolchain %q cannot be added to a container; only python is completed with uv, the rest belongs in the image", EnvToolchains, tc.Name)
		}
	}
	if len(out) > 1 {
		return nil, fmt.Errorf("%s: %d python toolchains; a container has one", EnvToolchains, len(out))
	}
	return out, nil
}

// Env is what a job needs to run in the prepared environment.
type Env struct {
	// PathPrefix goes in front of the job's PATH: python3 is the venv's.
	PathPrefix []string
	// Vars are added to the job's environment (VIRTUAL_ENV).
	Vars map[string]string
}

// Config is where and how Prepare works.
type Config struct {
	// Root holds the venv, the constraints and uv's cache.
	Root string
	// Python is the image's interpreter; "python3" on PATH when empty.
	Python string
	// UV is the uv binary (FetchUV).
	UV string
	// Run runs a program and returns its combined output; os/exec when nil.
	Run func(ctx context.Context, env []string, name string, args ...string) ([]byte, error)
	// LookPath finds a program; exec.LookPath when nil.
	LookPath func(string) (string, error)
	Logger   logr.Logger
}

// freezeScript lists the image's distributions as name==version, the
// constraints that keep uv from touching them. importlib.metadata needs no
// pip, which a slim or distroless image may lack.
const freezeScript = `import importlib.metadata as m
seen = set()
for d in m.distributions():
    name = d.metadata["Name"]
    if not name:
        continue
    key = name.lower().replace("_", "-")
    if key in seen:
        continue
    seen.add(key)
    print(f"{name}=={d.version}")
`

// Prepare makes the venv for tc. A venv already made for the same toolchain
// and interpreter is kept: a restarted container does not install again.
func Prepare(ctx context.Context, cfg Config, tc Toolchain) (Env, error) {
	run, look := cfg.Run, cfg.LookPath
	if run == nil {
		run = runExec
	}
	if look == nil {
		look = exec.LookPath
	}
	if cfg.Root == "" || cfg.UV == "" {
		return Env{}, errors.New("pyenv: Root and UV are required")
	}
	name := cfg.Python
	if name == "" {
		name = "python3"
	}
	system, err := look(name)
	if err != nil {
		return Env{}, fmt.Errorf("the image has no %s to complete: %w", name, err)
	}
	venv := filepath.Join(cfg.Root, "venv")
	env := Env{
		PathPrefix: []string{filepath.Join(venv, "bin")},
		Vars:       map[string]string{"VIRTUAL_ENV": venv},
	}
	spec := specHash(tc, system)
	specFile := filepath.Join(cfg.Root, "spec")
	if done, _ := os.ReadFile(specFile); string(done) == spec && exists(filepath.Join(venv, "bin", "python")) {
		cfg.Logger.Info("python environment already prepared", "venv", venv)
		return env, nil
	}

	uvEnv := append(os.Environ(),
		"UV_CACHE_DIR="+envOr("UV_CACHE_DIR", filepath.Join(cfg.Root, "cache")),
		"UV_PYTHON_INSTALL_DIR="+filepath.Join(cfg.Root, "pythons"),
		"UV_NO_PROGRESS=1",
		"UV_NO_CONFIG=1",
	)
	out, err := run(ctx, nil, system, "-c", "import sys; print('%d.%d.%d' % sys.version_info[:3])")
	if err != nil {
		return Env{}, fmt.Errorf("%s: %w: %s", system, err, out)
	}
	imageVersion := strings.TrimSpace(string(out))
	interpreter := system
	onImage := tc.Version == "" || versionMatches(imageVersion, tc.Version)
	if !onImage {
		// Another version than the image's: uv brings its own interpreter,
		// and the image's packages are not under it.
		interpreter = tc.Version
		cfg.Logger.Info("the image's python is another version; uv installs one", "image", imageVersion, "want", tc.Version)
	}

	if err := os.RemoveAll(venv); err != nil {
		return Env{}, err
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return Env{}, err
	}
	_ = os.Remove(specFile)
	if out, err := run(ctx, uvEnv, cfg.UV, "venv", "--system-site-packages", "--python", interpreter, venv); err != nil {
		return Env{}, fmt.Errorf("uv venv: %w: %s", err, out)
	}
	if len(tc.Packages) > 0 {
		install := tc.Packages
		if onImage {
			if install, err = missingFromImage(ctx, run, uvEnv, cfg, system, tc.Packages); err != nil {
				return Env{}, err
			}
		}
		if len(install) > 0 {
			args := []string{"pip", "install", "--python", filepath.Join(venv, "bin", "python")}
			if onImage {
				// Resolved against the image already: exactly these, and
				// nothing the image has.
				args = append(args, "--no-deps")
			}
			out, err := run(ctx, uvEnv, cfg.UV, append(args, install...)...)
			if err != nil {
				return Env{}, fmt.Errorf("uv pip install: %w: %s", err, out)
			}
		}
		cfg.Logger.Info("python packages added", "requested", tc.Packages, "installed", install)
	}
	if err := os.WriteFile(specFile, []byte(spec), 0o644); err != nil {
		return Env{}, err
	}
	return env, nil
}

// missingFromImage resolves packages against the image's own interpreter
// and returns, pinned, only the distributions the image lacks. uv does not
// count a --system-site-packages venv's system packages as installed: asked
// to install into the venv, it installs numpy again even where the image has
// it (seen in the container e2e), and for torch that is gigabytes. A dry run
// against the image writes nothing, so it may look at an externally managed
// interpreter; what the image has is a constraint, so nothing it has changes.
func missingFromImage(ctx context.Context, run func(context.Context, []string, string, ...string) ([]byte, error), uvEnv []string, cfg Config, system string, packages []string) ([]string, error) {
	frozen, err := run(ctx, nil, system, "-c", freezeScript)
	if err != nil {
		return nil, fmt.Errorf("list the image's packages: %w: %s", err, frozen)
	}
	constraints := filepath.Join(cfg.Root, "constraints.txt")
	if err := os.WriteFile(constraints, frozen, 0o644); err != nil {
		return nil, err
	}
	args := append([]string{"pip", "install", "--dry-run", "--python", system, "--break-system-packages",
		"--constraint", constraints}, packages...)
	out, err := run(ctx, uvEnv, cfg.UV, args...)
	if err != nil {
		return nil, fmt.Errorf("resolve %s against the image's python: %w: %s", strings.Join(packages, " "), err, out)
	}
	var missing []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "+ "):
			missing = append(missing, strings.TrimSpace(strings.TrimPrefix(line, "+ ")))
		case strings.HasPrefix(line, "- "):
			return nil, fmt.Errorf("adding %s would change the image's %s; the image's packages are kept as they are", strings.Join(packages, " "), strings.TrimPrefix(line, "- "))
		}
	}
	return missing, nil
}

// versionMatches reports whether the image's x.y.z is the wanted x, x.y or
// x.y.z.
func versionMatches(have, want string) bool {
	want = strings.TrimPrefix(strings.TrimSpace(want), "python")
	return have == want || strings.HasPrefix(have, want+".")
}

func specHash(tc Toolchain, interpreter string) string {
	packages := slices.Clone(tc.Packages)
	slices.Sort(packages)
	raw, _ := json.Marshal(struct {
		Version, Interpreter string
		Packages             []string
	}{tc.Version, interpreter, packages})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func runExec(ctx context.Context, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if env != nil {
		cmd.Env = env
	}
	return cmd.CombinedOutput()
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
