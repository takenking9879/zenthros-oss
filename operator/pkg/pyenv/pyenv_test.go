// SPDX-License-Identifier: Apache-2.0

package pyenv

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
)

func TestParseCompletesOnlyPython(t *testing.T) {
	got, err := Parse(`[{"name":"python","version":"3.12","packages":["requests"]}]`)
	if err != nil || len(got) != 1 || got[0].Packages[0] != "requests" {
		t.Fatalf("Parse = %+v, %v", got, err)
	}
	if got, err := Parse(""); err != nil || got != nil {
		t.Fatalf("empty = %+v, %v", got, err)
	}
	_, err = Parse(`[{"name":"node","version":"24"}]`)
	if err == nil || !strings.Contains(err.Error(), `toolchain "node" cannot be added to a container`) {
		t.Fatalf("node: %v", err)
	}
}

// fakePython records every program Prepare runs. The venv call makes the
// venv's interpreter, as uv would.
type fakePython struct {
	calls   [][]string
	failPip bool
	version string
	// plan is uv's dry run against the image: what it would add.
	plan string
}

func (f *fakePython) run(_ context.Context, _ []string, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	switch {
	case len(args) == 2 && strings.Contains(args[1], "version_info"):
		return []byte(f.version + "\n"), nil
	case len(args) == 2 && strings.Contains(args[1], "importlib.metadata"):
		return []byte("numpy==1.26.4\ntorch==2.8.0\n"), nil
	case len(args) > 0 && args[0] == "venv":
		venv := args[len(args)-1]
		if err := os.MkdirAll(filepath.Join(venv, "bin"), 0o755); err != nil {
			return nil, err
		}
		return nil, os.WriteFile(filepath.Join(venv, "bin", "python"), nil, 0o755)
	case len(args) > 0 && args[0] == "pip" && f.failPip:
		return []byte("No solution found"), errors.New("exit status 1")
	case slices.Contains(args, "--dry-run"):
		return []byte("Resolved 7 packages in 540ms\nWould install 2 packages\n" + f.plan), nil
	}
	return []byte("Installed 1 package"), nil
}

func (f *fakePython) uvCalls() [][]string {
	var out [][]string
	for _, c := range f.calls {
		if c[0] == "/opt/uv" {
			out = append(out, c[1:])
		}
	}
	return out
}

func testConfig(t *testing.T, f *fakePython) Config {
	return Config{
		Root:     t.TempDir(),
		UV:       "/opt/uv",
		Run:      f.run,
		LookPath: func(string) (string, error) { return "/usr/bin/python3", nil },
		Logger:   logr.Discard(),
	}
}

// The venv sees the image's packages, and uv adds only what the image
// lacks: the request is resolved against the image's interpreter, with what
// the image has as constraints, and only the distributions that resolution
// adds go into the venv, pinned and without their dependencies. uv asked to
// install into the venv directly reinstalls numpy the image already has
// (the container e2e); for torch that is gigabytes.
func TestPrepareCompletesTheImagesPython(t *testing.T) {
	f := &fakePython{version: "3.12.3", plan: " + requests==2.32.3\n + urllib3==2.2.3\n"}
	cfg := testConfig(t, f)
	env, err := Prepare(context.Background(), cfg, Toolchain{Name: "python", Version: "3.12", Packages: []string{"requests", "numpy"}})
	if err != nil {
		t.Fatal(err)
	}
	venv := filepath.Join(cfg.Root, "venv")
	constraints := filepath.Join(cfg.Root, "constraints.txt")
	want := [][]string{
		{"venv", "--system-site-packages", "--python", "/usr/bin/python3", venv},
		{"pip", "install", "--dry-run", "--python", "/usr/bin/python3", "--break-system-packages", "--constraint", constraints, "requests", "numpy"},
		{"pip", "install", "--python", filepath.Join(venv, "bin", "python"), "--no-deps", "requests==2.32.3", "urllib3==2.2.3"},
	}
	if got := f.uvCalls(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("uv calls\n got %q\nwant %q", got, want)
	}
	if frozen, _ := os.ReadFile(constraints); string(frozen) != "numpy==1.26.4\ntorch==2.8.0\n" {
		t.Fatalf("constraints = %q", frozen)
	}
	if env.Vars["VIRTUAL_ENV"] != venv || len(env.PathPrefix) != 1 || env.PathPrefix[0] != filepath.Join(venv, "bin") {
		t.Fatalf("env = %+v", env)
	}
}

// A restarted container keeps the venv it made; a changed toolchain makes a
// new one.
func TestPrepareKeepsAPreparedVenv(t *testing.T) {
	f := &fakePython{version: "3.12.3"}
	cfg := testConfig(t, f)
	tc := Toolchain{Name: "python", Packages: []string{"requests"}}
	if _, err := Prepare(context.Background(), cfg, tc); err != nil {
		t.Fatal(err)
	}
	f.calls = nil
	if _, err := Prepare(context.Background(), cfg, tc); err != nil {
		t.Fatal(err)
	}
	if len(f.uvCalls()) != 0 {
		t.Fatalf("a prepared venv was made again: %q", f.uvCalls())
	}
	tc.Packages = append(tc.Packages, "rich")
	if _, err := Prepare(context.Background(), cfg, tc); err != nil {
		t.Fatal(err)
	}
	if len(f.uvCalls()) == 0 {
		t.Fatalf("a changed toolchain reused the venv: %q", f.uvCalls())
	}
}

// Everything requested already in the image: the venv is made, nothing is
// installed. A plan that would remove or replace an image package is
// refused, naming it.
func TestPrepareInstallsNothingTheImageHas(t *testing.T) {
	f := &fakePython{version: "3.12.3"}
	cfg := testConfig(t, f)
	if _, err := Prepare(context.Background(), cfg, Toolchain{Name: "python", Packages: []string{"numpy", "torch"}}); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.uvCalls() {
		if call[0] == "pip" && !slices.Contains(call, "--dry-run") {
			t.Fatalf("installed what the image has: %q", call)
		}
	}

	f = &fakePython{version: "3.12.3", plan: " - numpy==1.26.4\n + numpy==2.1.3\n"}
	cfg = testConfig(t, f)
	_, err := Prepare(context.Background(), cfg, Toolchain{Name: "python", Packages: []string{"numpy>=2"}})
	if err == nil || !strings.Contains(err.Error(), "would change the image's numpy==1.26.4") {
		t.Fatalf("err = %v", err)
	}
}

// A failed install leaves no record of a prepared venv, so the next start
// tries again instead of running jobs without their packages.
func TestPrepareRecordsOnlyAFinishedInstall(t *testing.T) {
	f := &fakePython{version: "3.12.3", failPip: true}
	cfg := testConfig(t, f)
	tc := Toolchain{Name: "python", Packages: []string{"numpy>=2"}}
	_, err := Prepare(context.Background(), cfg, tc)
	if err == nil || !strings.Contains(err.Error(), "No solution found") {
		t.Fatalf("err = %v, want uv's own words", err)
	}
	if exists(filepath.Join(cfg.Root, "spec")) {
		t.Fatal("a failed install was recorded as prepared")
	}
	f.failPip, f.calls = false, nil
	if _, err := Prepare(context.Background(), cfg, tc); err != nil {
		t.Fatal(err)
	}
	if !exists(filepath.Join(cfg.Root, "spec")) {
		t.Fatalf("the retry did not finish: %q", f.uvCalls())
	}
}

// Another version than the image's is uv's own interpreter; the image's
// packages are not under it, so they constrain nothing.
func TestPrepareOtherVersionUsesUVsPython(t *testing.T) {
	f := &fakePython{version: "3.12.3"}
	cfg := testConfig(t, f)
	if _, err := Prepare(context.Background(), cfg, Toolchain{Name: "python", Version: "3.13", Packages: []string{"requests"}}); err != nil {
		t.Fatal(err)
	}
	calls := f.uvCalls()
	if len(calls) != 2 || calls[0][3] != "3.13" || slices.Contains(calls[1], "--dry-run") || slices.Contains(calls[1], "--no-deps") {
		t.Fatalf("uv calls = %q", calls)
	}
}

func TestVersionMatches(t *testing.T) {
	for _, c := range []struct {
		have, want string
		ok         bool
	}{
		{"3.12.3", "3.12", true}, {"3.12.3", "3", true}, {"3.12.3", "3.12.3", true},
		{"3.12.3", "python3.12", true}, {"3.12.3", "3.1", false}, {"3.12.3", "3.13", false},
	} {
		if got := versionMatches(c.have, c.want); got != c.ok {
			t.Errorf("versionMatches(%q, %q) = %v", c.have, c.want, got)
		}
	}
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func uvArchive(t *testing.T, binary []byte) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, f := range []struct {
		name string
		body []byte
	}{{"uv-x86_64-unknown-linux-gnu/uvx", []byte("uvx")}, {"uv-x86_64-unknown-linux-gnu/uv", binary}} {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o755, Size: int64(len(f.body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		_, _ = tw.Write(f.body)
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

// uv is trusted only through the chain the installer uses: the pinned
// SHA256SUMS, the upstream.lock it lists, the archive the lock pins. A break
// anywhere leaves no uv behind.
func TestFetchUVVerifiesEveryLink(t *testing.T) {
	archive := uvArchive(t, []byte("#!uv"))
	const uvURL = "https://example.test/uv-x86_64-unknown-linux-gnu.tar.gz"
	lock := []byte("# key arch sha256 url\nuv@0.12.19 arm64 " + strings.Repeat("0", 64) + " https://example.test/arm\nuv@0.12.19 amd64 " + sha(archive) + " " + uvURL + "\n")
	sums := []byte(sha(lock) + "  upstream.lock\n" + strings.Repeat("1", 64) + "  install-agent.sh\n")
	files := func() map[string][]byte {
		return map[string][]byte{"https://base.test/SHA256SUMS": sums, "https://base.test/upstream.lock": lock, uvURL: archive}
	}
	fetch := func(t *testing.T, served map[string][]byte, pin string) (string, string, error) {
		dir := t.TempDir()
		got, err := FetchUV(context.Background(), Fetch{
			Base: "https://base.test/", SumsSHA256: pin, Arch: "amd64", Dir: dir,
			Get: func(_ context.Context, url string) ([]byte, error) {
				body, ok := served[url]
				if !ok {
					return nil, fmt.Errorf("404 %s", url)
				}
				return body, nil
			},
		})
		return got, dir, err
	}

	got, dir, err := fetch(t, files(), sha(sums))
	if err != nil {
		t.Fatal(err)
	}
	if body, _ := os.ReadFile(got); string(body) != "#!uv" || got != filepath.Join(dir, "uv") {
		t.Fatalf("uv at %s = %q", got, body)
	}
	if info, _ := os.Stat(got); info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("uv is not executable: %v", info.Mode())
	}

	for name, tamper := range map[string]func(map[string][]byte) string{
		"SHA256SUMS": func(m map[string][]byte) string {
			m["https://base.test/SHA256SUMS"] = append(sums, '\n')
			return sha(sums)
		},
		"upstream.lock": func(m map[string][]byte) string {
			m["https://base.test/upstream.lock"] = append(lock, '\n')
			return sha(sums)
		},
		"archive": func(m map[string][]byte) string { m[uvURL] = uvArchive(t, []byte("evil")); return sha(sums) },
	} {
		served := files()
		pin := tamper(served)
		_, dir, err := fetch(t, served, pin)
		if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("tampered %s: err = %v", name, err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Fatalf("tampered %s left %v behind", name, entries)
		}
	}
}

// Per-artifact URLs (ZENTHROS_ARTIFACT_URLS, presigned) win over the base, and
// the chain still holds through them. An error names a URL without its query,
// which may be a signature.
func TestFetchUVFromArtifactURLs(t *testing.T) {
	archive := uvArchive(t, []byte("#!uv"))
	const uvURL = "https://example.test/uv-x86_64-unknown-linux-gnu.tar.gz"
	lock := []byte("uv@0.12.19 amd64 " + sha(archive) + " " + uvURL + "\n")
	sums := []byte(sha(lock) + "  upstream.lock\n")
	urls := ParseArtifactURLs("SHA256SUMS=https://bucket.test/p/SHA256SUMS?X-Amz-Signature=SECRET&a=b \n upstream.lock=https://bucket.test/p/upstream.lock?X-Amz-Signature=SECRET")
	served := map[string][]byte{urls["SHA256SUMS"]: sums, urls["upstream.lock"]: lock, uvURL: archive}
	get := func(_ context.Context, url string) ([]byte, error) {
		body, ok := served[url]
		if !ok {
			return nil, fmt.Errorf("404 %s", url)
		}
		return body, nil
	}
	got, err := FetchUV(context.Background(), Fetch{ArtifactURLs: urls, SumsSHA256: sha(sums), Arch: "amd64", Dir: t.TempDir(), Get: get})
	if err != nil {
		t.Fatal(err)
	}
	if body, _ := os.ReadFile(got); string(body) != "#!uv" {
		t.Fatalf("uv = %q", body)
	}
	_, err = FetchUV(context.Background(), Fetch{ArtifactURLs: urls, SumsSHA256: strings.Repeat("0", 64), Arch: "amd64", Dir: t.TempDir(), Get: get})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch for https://bucket.test/p/SHA256SUMS:") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("wrong pin: %v", err)
	}
}
