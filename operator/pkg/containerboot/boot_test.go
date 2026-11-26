// SPDX-License-Identifier: Apache-2.0

package containerboot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// The script runs with a PATH that holds only what a test gives it, so each
// case says exactly which tools the image has. Fake curl and wget record
// that they ran and copy from the file:// base; python3, when given, is the
// real one. No image has zstd or gzip: the agent is unpacked with the
// zenthros-unzstd it downloads (cmd/unzstd, built once for these tests).

var (
	unzstdOnce sync.Once
	unzstdBody []byte
	unzstdErr  error
)

// unzstd is cmd/unzstd built for this machine.
func unzstd(t *testing.T) []byte {
	t.Helper()
	unzstdOnce.Do(func() {
		dir, err := os.MkdirTemp("", "unzstd")
		if err != nil {
			unzstdErr = err
			return
		}
		defer os.RemoveAll(dir)
		out, err := exec.Command("go", "build", "-o", filepath.Join(dir, "unzstd"), "../../cmd/unzstd").CombinedOutput()
		if err != nil {
			unzstdErr = fmt.Errorf("go build cmd/unzstd: %v\n%s", err, out)
			return
		}
		unzstdBody, unzstdErr = os.ReadFile(filepath.Join(dir, "unzstd"))
	})
	if unzstdErr != nil {
		t.Fatal(unzstdErr)
	}
	return unzstdBody
}

func pack(t *testing.T, body string) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return enc.EncodeAll([]byte(body), nil)
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type image struct {
	t      *testing.T
	bin    string // the image's PATH
	served string // the artifact base
	state  string
	log    string // which downloader ran
	ran    string // written by the agent when it runs
}

// coreutils is what every Debian and Ubuntu image has (busybox, on Alpine):
// no zstd, no gzip.
var coreutils = []string{"sh", "cat", "cp", "cut", "sed", "mkdir", "rm", "mv", "chmod", "uname", "sha256sum", "sleep"}

func newImage(t *testing.T, tools ...string) *image {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("the script is for Linux containers")
	}
	root := t.TempDir()
	im := &image{t: t, bin: filepath.Join(root, "bin"), served: filepath.Join(root, "served"),
		state: filepath.Join(root, "state"), log: filepath.Join(root, "downloads"), ran: filepath.Join(root, "ran")}
	for _, dir := range []string{im.bin, im.served} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, tool := range append(append([]string{}, coreutils...), tools...) {
		switch tool {
		case "curl", "wget":
			// curl ... URL -o DEST / wget ... -O DEST URL. A URL's query
			// is dropped, as a server would read a signature. Each call's
			// arguments go to the log's .args.
			script := "#!/bin/sh\necho " + tool + " >> " + im.log + "\necho \"$*\" >> " + im.log + ".args\n" +
				"while [ $# -gt 0 ]; do case \"$1\" in -o|-O) dest=\"$2\"; shift 2 ;; *://*) src=\"${1#file://}\"; shift ;; *) shift ;; esac; done\n" +
				"cp \"${src%%\\?*}\" \"${dest}\"\n"
			if err := os.WriteFile(filepath.Join(im.bin, tool), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
		default:
			real, err := exec.LookPath(tool)
			if err != nil {
				t.Skipf("no %s on this machine", tool)
			}
			if err := os.Symlink(real, filepath.Join(im.bin, tool)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The agent: a script that records it ran.
	im.publish("#!/bin/sh\necho downloaded > " + im.ran + "\n")
	return im
}

// publish serves agent, packed, with the unpacker and a SHA256SUMS that
// lists both.
func (im *image) publish(agent string) {
	arch := machineArch(im.t)
	packed, unpacker := pack(im.t, agent), unzstd(im.t)
	im.serve(AgentArtifact(arch), packed)
	im.serve(UnzstdArtifact(arch), unpacker)
	im.serve("SHA256SUMS", []byte(sha(packed)+"  "+AgentArtifact(arch)+"\n"+sha(unpacker)+"  "+UnzstdArtifact(arch)+"\n"))
}

func machineArch(t *testing.T) string {
	switch runtime.GOARCH {
	case "amd64", "arm64":
		return runtime.GOARCH
	}
	t.Skipf("arch %s", runtime.GOARCH)
	return ""
}

func (im *image) serve(name string, body []byte) {
	if err := os.WriteFile(filepath.Join(im.served, name), body, 0o644); err != nil {
		im.t.Fatal(err)
	}
}

func (im *image) sumsPin() string {
	body, _ := os.ReadFile(filepath.Join(im.served, "SHA256SUMS"))
	return sha(body)
}

// boot runs the container's command with env on top of the image's.
func (im *image) boot(env ...string) (string, error) {
	cmd := exec.Command(filepath.Join(im.bin, "sh"), "-c", Script)
	cmd.Env = append([]string{
		"PATH=" + im.bin,
		"ZENTHROS_STATE_DIR=" + im.state,
		EnvBakedAgent + "=" + filepath.Join(im.state, "no-baked-agent"),
		EnvInstallBase + "=file://" + im.served,
		EnvSumsSHA256 + "=" + im.sumsPin(),
	}, env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (im *image) agentRan() string {
	got, _ := os.ReadFile(im.ran)
	return strings.TrimSpace(string(got))
}

func (im *image) downloaders() string {
	got, _ := os.ReadFile(im.log)
	return strings.Join(strings.Fields(string(got)), " ")
}

// calls are the arguments of each download, in order.
func (im *image) calls() []string {
	got, _ := os.ReadFile(im.log + ".args")
	return strings.Split(strings.TrimSpace(string(got)), "\n")
}

// curl gets the URL first, then a connect timeout and the stall cut, both
// of zenthros_stall: a host that never answers the connection is cut as a
// stalled download is (with no connect timeout, curl waits 300 s a try),
// and a process list cut short (the harness's, at 200 bytes) still names
// the download.
func TestBootGivesCurlItsURLFirstAndAConnectTimeout(t *testing.T) {
	im := newImage(t, "curl")
	if out, err := im.boot("zenthros_stall=7"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	calls := im.calls()
	if len(calls) != 3 {
		t.Fatalf("curl calls %q, want the three downloads", calls)
	}
	for _, call := range calls {
		if !strings.HasPrefix(call, "file://") || !strings.Contains(call, " --connect-timeout 7 ") ||
			!strings.Contains(call, " --speed-limit 1024 --speed-time 7 ") {
			t.Errorf("curl %s: want its URL first, then --connect-timeout 7 and --speed-limit 1024 --speed-time 7", call)
		}
	}
}

// Downloads prefer curl, then wget, then python3: whatever the image has.
func TestBootDownloadsWithWhatTheImageHas(t *testing.T) {
	for _, c := range []struct {
		tools []string
		want  string
	}{
		{[]string{"curl", "wget", "python3"}, "curl curl curl"},
		{[]string{"wget", "python3"}, "wget wget wget"},
		{[]string{"python3"}, ""},
	} {
		im := newImage(t, c.tools...)
		if out, err := im.boot(); err != nil {
			t.Fatalf("tools %v: %v\n%s", c.tools, err, out)
		}
		if got := im.downloaders(); got != c.want {
			t.Fatalf("tools %v: downloaded with %q, want %q", c.tools, got, c.want)
		}
		if im.agentRan() != "downloaded" {
			t.Fatalf("tools %v: the downloaded agent did not run", c.tools)
		}
	}
}

// An agent baked into the image runs without any download, and so does
// one an earlier start of the container already verified.
func TestBootPrefersAnAgentAlreadyThere(t *testing.T) {
	im := newImage(t, "curl")
	baked := filepath.Join(t.TempDir(), "zenthros-agent")
	if err := os.WriteFile(baked, []byte("#!/bin/sh\necho baked > "+im.ran+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := im.boot(EnvBakedAgent + "=" + baked); err != nil || im.agentRan() != "baked" || im.downloaders() != "" {
		t.Fatalf("baked: %v ran %q downloads %q\n%s", err, im.agentRan(), im.downloaders(), out)
	}

	if out, err := im.boot(); err != nil || im.agentRan() != "downloaded" {
		t.Fatalf("first start: %v\n%s", err, out)
	}
	_ = os.Remove(im.log)
	if out, err := im.boot(); err != nil || im.downloaders() != "" {
		t.Fatalf("restart downloaded again (%q): %v\n%s", im.downloaders(), err, out)
	}
}

// Nothing unverified runs: a SHA256SUMS that is not the pinned one, or an
// agent that is not the one SHA256SUMS lists, stops the boot with a message
// that names it, and no agent is left in the state dir.
func TestBootRunsNothingUnverified(t *testing.T) {
	for name, tamper := range map[string]func(im *image) []string{
		"unpinned SHA256SUMS": func(im *image) []string {
			return []string{EnvSumsSHA256 + "=" + strings.Repeat("0", 64)}
		},
		"swapped agent": func(im *image) []string {
			im.serve(AgentArtifact(machineArch(t)), pack(t, "#!/bin/sh\necho evil > "+im.ran+"\n"))
			return nil
		},
		// The unpacker runs before the agent: it is checked as the agent is.
		"swapped unpacker": func(im *image) []string {
			im.serve(UnzstdArtifact(machineArch(t)), []byte("#!/bin/sh\necho evil > "+im.ran+"\ncat\n"))
			return nil
		},
		"no pin": func(im *image) []string { return []string{EnvSumsSHA256 + "="} },
		// Only the container installer trusts what it downloads.
		"no pin, trust from the environment": func(im *image) []string {
			return []string{EnvSumsSHA256 + "=", "zenthros_trust_sums=1"}
		},
	} {
		im := newImage(t, "curl")
		out, err := im.boot(tamper(im)...)
		if err == nil {
			t.Fatalf("%s: booted\n%s", name, out)
		}
		if !strings.Contains(out, "zenthros-boot: ") || (!strings.Contains(out, "checksum mismatch") && !strings.Contains(out, "refusing an unpinned download")) {
			t.Fatalf("%s: message %q", name, out)
		}
		if im.agentRan() != "" {
			t.Fatalf("%s: an unverified agent ran (%q)", name, im.agentRan())
		}
		if _, err := os.Stat(filepath.Join(im.state, "bin", "zenthros-agent")); err == nil {
			t.Fatalf("%s: an unverified agent was left to run on the next start", name)
		}
	}
}

// A job can write the state dir: an unpacker changed there after a start is
// not run on the next one; it is downloaded again and checked.
func TestBootChecksTheCachedUnpackerAgain(t *testing.T) {
	im := newImage(t, "curl")
	if out, err := im.boot(); err != nil || im.agentRan() != "downloaded" {
		t.Fatalf("first start: %v\n%s", err, out)
	}
	cached := filepath.Join(im.state, "bin", UnzstdArtifact(machineArch(t)))
	if err := os.WriteFile(cached, []byte("#!/bin/sh\necho evil > "+im.ran+"\ncat\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(im.log)
	_ = os.Remove(im.ran)
	if out, err := im.boot(); err != nil || im.agentRan() != "downloaded" || im.downloaders() == "" {
		t.Fatalf("restart ran %q, downloads %q: %v\n%s", im.agentRan(), im.downloaders(), err, out)
	}
}

// An image with neither a downloader nor an agent says so.
func TestBootWithoutAnyDownloader(t *testing.T) {
	im := newImage(t)
	out, err := im.boot()
	if err == nil || !strings.Contains(out, "no curl, wget or python3 to download the agent with") {
		t.Fatalf("%v %q", err, out)
	}
}

// Each artifact named in ZENTHROS_ARTIFACT_URLS comes from its own URL (a
// private bucket's presigned URLs), with no base at all. A failure names the
// URL without its query: a presigned URL's signature is a credential, and the
// message lands in the provider's log.
func TestBootFetchesFromArtifactURLs(t *testing.T) {
	im := newImage(t, "curl")
	elsewhere := t.TempDir()
	signed := func(name string) string {
		body, err := os.ReadFile(filepath.Join(im.served, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(elsewhere, "obj-"+name), body, 0o644); err != nil {
			t.Fatal(err)
		}
		return name + "=file://" + filepath.Join(elsewhere, "obj-"+name) + "?X-Amz-Signature=SECRET&X-Amz-Expires=60"
	}
	urls := EnvArtifactURLs + "=" + signed("SHA256SUMS") + "  " + signed(AgentArtifact(machineArch(t))) + "  " +
		signed(UnzstdArtifact(machineArch(t)))
	if out, err := im.boot(EnvInstallBase+"=", urls); err != nil || im.agentRan() != "downloaded" {
		t.Fatalf("boot from artifact URLs: %v ran %q\n%s", err, im.agentRan(), out)
	}

	im = newImage(t, "curl")
	out, err := im.boot(EnvInstallBase+"=", urls, EnvSumsSHA256+"="+strings.Repeat("0", 64))
	if err == nil || !strings.Contains(out, "zenthros-boot: checksum mismatch for SHA256SUMS from file://"+elsewhere+"/obj-SHA256SUMS\n") {
		t.Fatalf("wrong pin: %v %q", err, out)
	}
	if strings.Contains(out, "SECRET") {
		t.Fatalf("the message carries the URL's signature: %q", out)
	}

	// An artifact missing from the list, with no base to fall back on, is
	// named.
	im = newImage(t, "curl")
	out, err = im.boot(EnvInstallBase+"=", EnvArtifactURLs+"="+signed("SHA256SUMS"))
	if err == nil || !strings.Contains(out, "ZENTHROS_ARTIFACT_URLS has no "+UnzstdArtifact(machineArch(t))) {
		t.Fatalf("missing artifact: %v %q", err, out)
	}
}

// Attach runs the same script from a shell's stdin, its arguments the
// agent's: `sh -s attach`. Both the baked and the downloaded agent get
// them; the container command passes none.
func TestBootPassesItsArgumentsToTheAgent(t *testing.T) {
	im := newImage(t, "curl")
	recordArgs := "#!/bin/sh\necho \"$# $*\" > " + im.ran + "\n"
	im.publish(recordArgs)

	stdin := func(args ...string) (string, error) {
		cmd := exec.Command(filepath.Join(im.bin, "sh"), append([]string{"-s"}, args...)...)
		cmd.Stdin = strings.NewReader(Script)
		cmd.Env = []string{"PATH=" + im.bin, "ZENTHROS_STATE_DIR=" + im.state,
			EnvBakedAgent + "=" + filepath.Join(im.state, "no-baked-agent"),
			EnvInstallBase + "=file://" + im.served, EnvSumsSHA256 + "=" + im.sumsPin()}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := stdin("attach", "--wait", "1m"); err != nil || im.agentRan() != "3 attach --wait 1m" {
		t.Fatalf("downloaded agent ran with %q: %v\n%s", im.agentRan(), err, out)
	}
	if out, err := im.boot(); err != nil || im.agentRan() != "0" {
		t.Fatalf("the container command passed %q: %v\n%s", im.agentRan(), err, out)
	}
	baked := filepath.Join(t.TempDir(), "zenthros-agent")
	if err := os.WriteFile(baked, []byte(recordArgs), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(im.bin, "sh"), "-s", "attach")
	cmd.Stdin = strings.NewReader(Script)
	cmd.Env = []string{"PATH=" + im.bin, "ZENTHROS_STATE_DIR=" + im.state, EnvBakedAgent + "=" + baked}
	if out, err := cmd.CombinedOutput(); err != nil || im.agentRan() != "1 attach" {
		t.Fatalf("baked agent ran with %q: %v\n%s", im.agentRan(), err, out)
	}
}

// The SDK runs the same Fetch to take a local agent from a release: its copy
// in the wheel is this script, byte for byte. To write it again:
// ZENTHROS_WRITE_SDK_BOOT=1 go test ./pkg/containerboot/ -run SDKFetch
func TestSDKFetchIsThisOne(t *testing.T) {
	path := filepath.Join("..", "..", "..", "sdk", "python", "zenthros", "_data", "container-fetch.sh")
	if os.Getenv("ZENTHROS_WRITE_SDK_BOOT") != "" {
		if err := os.WriteFile(path, []byte(Fetch), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != Fetch {
		t.Fatalf("%s is not containerboot.Fetch: write it again (see this test)", path)
	}
}
