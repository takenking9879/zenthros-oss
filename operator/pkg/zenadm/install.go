// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Pin the packages the bench AMI actually ran (1.102.3). A later bump is a
// one-line change; we do not call apt-get update to discover "latest".
const tailscaleRelease = "1.102.3"

const (
	artifactDeb = "deb"
	artifactTGZ = "tgz"
)

type artifact struct {
	Name string
	URL  string
	Kind string
}

type installPlan struct {
	missing []artifact
}

// installEnv is the seam tests inject. Zero value uses real LookPath / HTTP / dpkg.
type installEnv struct {
	lookPath      func(string) bool
	dpkgQuery     func(string) bool
	dpkgAvailable func() bool
	readOSRelease func() (map[string]string, error)
	goarch        string
	download      func(ctx context.Context, url, dest string) error
	installDebs   func(ctx context.Context, files []string) error
	installTGZ    func(ctx context.Context, tgz string) error
	osRelease     map[string]string
	which         func(string) string
	hashFile      func(string) (string, error)
	placeFile     func(src, dest string) error
}

// pinnedBinary is an agent/zenadm artifact verified by SHA-256. Presence on
// PATH is not enough: the installed file must match the pin.
type pinnedBinary struct {
	Name   string
	URL    string
	SHA256 string
	Dest   string
}

func (e *installEnv) ensureTailscale(ctx context.Context) error {
	if e.lookup("tailscale") {
		return nil
	}
	plan, err := e.planArtifacts()
	if err != nil {
		return err
	}
	if len(plan.missing) == 0 {
		return nil
	}
	dir, err := os.MkdirTemp("", "zenthros-tailscale-")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer os.RemoveAll(dir)

	type result struct {
		art  artifact
		path string
		err  error
	}
	ch := make(chan result, len(plan.missing))
	var wg sync.WaitGroup
	for i, art := range plan.missing {
		wg.Add(1)
		go func(i int, art artifact) {
			defer wg.Done()
			dest := filepath.Join(dir, fmt.Sprintf("%d-%s", i, filepath.Base(art.URL)))
			err := e.fetch(ctx, art.URL, dest)
			ch <- result{art: art, path: dest, err: err}
		}(i, art)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	var debs []string
	var tgz string
	for r := range ch {
		if r.err != nil {
			return fmt.Errorf("download %s: %w", r.art.URL, r.err)
		}
		switch r.art.Kind {
		case artifactDeb:
			debs = append(debs, r.path)
		case artifactTGZ:
			tgz = r.path
		}
	}
	if tgz != "" {
		if err := e.unpackTGZ(ctx, tgz); err != nil {
			return err
		}
	}
	if len(debs) > 0 {
		if err := e.dpkgI(ctx, debs); err != nil {
			return err
		}
	}
	if !e.lookup("tailscale") {
		return fmt.Errorf("tailscale not on PATH after install")
	}
	return nil
}

func (e *installEnv) planArtifacts() (installPlan, error) {
	if e.lookup("tailscale") {
		return installPlan{}, nil
	}
	arch := e.arch()
	osRel, err := e.osrelease()
	if err != nil {
		osRel = map[string]string{}
	}
	id := strings.ToLower(osRel["ID"])
	codename := osRel["VERSION_CODENAME"]
	if codename == "" {
		codename = osRel["UBUNTU_CODENAME"]
	}

	useDeb := e.hasDpkg() && (id == "ubuntu" || id == "debian" || id == "linuxmint" || id == "pop" || id == "raspbian")
	if useDeb && e.iptablesPresent() {
		var missing []artifact
		if !e.lookup("tailscale") {
			missing = append(missing, artifact{
				Name: "tailscale",
				Kind: artifactDeb,
				URL:  fmt.Sprintf("https://pkgs.tailscale.com/stable/%s/pool/tailscale_%s_%s.deb", debDistro(id), tailscaleRelease, arch),
			})
		}
		if !e.pkgInstalled("tailscale-archive-keyring") {
			missing = append(missing, artifact{
				Name: "tailscale-archive-keyring",
				Kind: artifactDeb,
				URL:  fmt.Sprintf("https://pkgs.tailscale.com/stable/%s/pool/tailscale-archive-keyring_1.35.181_all.deb", debDistro(id)),
			})
		}
		return installPlan{missing: missing}, nil
	}
	// No apt, no dpkg, or missing iptables (cannot satisfy Depends without apt-get
	// update): static tarball. No apt-get update on any path.
	return installPlan{missing: []artifact{{
		Name: "tailscale",
		Kind: artifactTGZ,
		URL:  fmt.Sprintf("https://pkgs.tailscale.com/stable/tailscale_%s_%s.tgz", tailscaleRelease, arch),
	}}}, nil
}

func debDistro(id string) string {
	switch id {
	case "debian", "raspbian":
		return "debian"
	default:
		return "ubuntu"
	}
}

func (e *installEnv) lookup(name string) bool {
	if e.lookPath != nil {
		return e.lookPath(name)
	}
	_, err := exec.LookPath(name)
	return err == nil
}

func (e *installEnv) hasDpkg() bool {
	if e.dpkgAvailable != nil {
		return e.dpkgAvailable()
	}
	_, err := exec.LookPath("dpkg")
	return err == nil
}

func (e *installEnv) pkgInstalled(name string) bool {
	if e.dpkgQuery != nil {
		return e.dpkgQuery(name)
	}
	if !e.hasDpkg() {
		return false
	}
	cmd := exec.Command("dpkg-query", "-W", "-f=${Status}", name)
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "install ok installed")
}

func (e *installEnv) iptablesPresent() bool {
	return e.lookup("iptables") || e.pkgInstalled("iptables")
}

func (e *installEnv) osrelease() (map[string]string, error) {
	if e.readOSRelease != nil {
		return e.readOSRelease()
	}
	data, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[k] = strings.Trim(v, `"`)
	}
	return out, nil
}

func (e *installEnv) arch() string {
	if e.goarch != "" {
		return e.goarch
	}
	return runtime.GOARCH
}

func (e *installEnv) fetch(ctx context.Context, url, dest string) error {
	if e.download != nil {
		return e.download(ctx, url, dest)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func (e *installEnv) dpkgI(ctx context.Context, files []string) error {
	if e.installDebs != nil {
		return e.installDebs(ctx, files)
	}
	args := append([]string{"dpkg", "-i"}, files...)
	return runCmd(ctx, "sudo", args...)
}

func (e *installEnv) unpackTGZ(ctx context.Context, tgz string) error {
	if e.installTGZ != nil {
		return e.installTGZ(ctx, tgz)
	}
	f, err := os.Open(tgz)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var tailscaleBin, tailscaledBin, unit string
	tmp, err := os.MkdirTemp("", "zenthros-ts-extract-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		base := filepath.Base(hdr.Name)
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		dest := filepath.Join(tmp, base)
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return err
		}
		out.Close()
		switch base {
		case "tailscale":
			tailscaleBin = dest
		case "tailscaled":
			tailscaledBin = dest
		case "tailscaled.service":
			unit = dest
		}
	}
	if tailscaleBin == "" || tailscaledBin == "" {
		return fmt.Errorf("tarball %s missing tailscale/tailscaled", tgz)
	}
	if err := runCmd(ctx, "sudo", "cp", tailscaleBin, tailscaledBin, "/usr/local/bin/"); err != nil {
		return fmt.Errorf("install tailscale binaries: %w", err)
	}
	if err := runCmd(ctx, "sudo", "chmod", "0755", "/usr/local/bin/tailscale", "/usr/local/bin/tailscaled"); err != nil {
		return err
	}
	if unit != "" {
		if err := runCmd(ctx, "sudo", "cp", unit, "/etc/systemd/system/tailscaled.service"); err != nil {
			return err
		}
		_ = runCmd(ctx, "sudo", "systemctl", "daemon-reload")
		if err := runCmd(ctx, "sudo", "systemctl", "enable", "--now", "tailscaled"); err != nil {
			return fmt.Errorf("start tailscaled: %w", err)
		}
	}
	return nil
}

func ensureTailscaleInstalled(ctx context.Context) error {
	var env installEnv
	return env.ensureTailscale(ctx)
}

func (e *installEnv) resolvePath(name string) string {
	if e.which != nil {
		return e.which(name)
	}
	path, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	return path
}

func (e *installEnv) fileHash(path string) (string, error) {
	if e.hashFile != nil {
		return e.hashFile(path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func (e *installEnv) shaMatches(path, want string) bool {
	if want == "" {
		return false
	}
	got, err := e.fileHash(path)
	if err != nil {
		return false
	}
	return strings.EqualFold(got, want)
}

func (e *installEnv) pinnedSHAMatches(bin pinnedBinary) bool {
	path := e.resolvePath(bin.Name)
	if path == "" && bin.Dest != "" {
		path = bin.Dest
	}
	if path == "" {
		return false
	}
	return e.shaMatches(path, bin.SHA256)
}

func (e *installEnv) ensurePinned(ctx context.Context, bin pinnedBinary) error {
	if bin.SHA256 == "" {
		return fmt.Errorf("%s sha256 is required", bin.Name)
	}
	if e.pinnedSHAMatches(bin) {
		return nil
	}
	dest := bin.Dest
	if dest == "" {
		dest = filepath.Join("/usr/local/bin", bin.Name)
	}
	tmp := dest + ".download"
	if err := e.fetch(ctx, bin.URL, tmp); err != nil {
		return fmt.Errorf("download %s: %w", bin.Name, err)
	}
	if !e.shaMatches(tmp, bin.SHA256) {
		return fmt.Errorf("%s sha256 mismatch", bin.Name)
	}
	if e.placeFile != nil {
		return e.placeFile(tmp, dest)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("install %s: %w", bin.Name, err)
	}
	return os.Chmod(dest, 0755)
}

// EnsurePinnedBinary downloads name from url unless the already-installed
// binary's SHA-256 matches sha256hex.
func EnsurePinnedBinary(ctx context.Context, name, url, sha256hex string) error {
	return EnsurePinnedBinaryDest(ctx, name, url, sha256hex, "")
}

// EnsurePinnedBinaryDest is EnsurePinnedBinary with an explicit install path.
func EnsurePinnedBinaryDest(ctx context.Context, name, url, sha256hex, dest string) error {
	var env installEnv
	return env.ensurePinned(ctx, pinnedBinary{Name: name, URL: url, SHA256: sha256hex, Dest: dest})
}
