// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlanSkipsPresentBinaries(t *testing.T) {
	env := testInstallEnv(t)
	env.present["tailscale"] = true
	env.present["iptables"] = true
	plan, err := env.plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.missing) != 0 {
		t.Fatalf("missing = %#v, want none when tailscale and iptables are present", plan.missing)
	}
}

func TestPlanDownloadsOnlyMissingDebs(t *testing.T) {
	env := testInstallEnv(t)
	env.osRelease = map[string]string{"ID": "ubuntu", "VERSION_CODENAME": "jammy"}
	env.present["iptables"] = true
	plan, err := env.plan()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range plan.missing {
		if a.Name == "iptables" {
			t.Fatalf("iptables is present; must not be in missing: %#v", plan.missing)
		}
		if a.Kind != artifactDeb {
			t.Fatalf("ubuntu with dpkg must plan debs, got %#v", a)
		}
	}
	if !hasName(plan.missing, "tailscale") {
		t.Fatalf("missing = %#v, want tailscale", plan.missing)
	}
}

func TestPlanUsesStaticWhenNoDpkg(t *testing.T) {
	env := testInstallEnv(t)
	env.osRelease = map[string]string{"ID": "alpine"}
	env.hasDpkg = false
	plan, err := env.plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.missing) != 1 || plan.missing[0].Kind != artifactTGZ {
		t.Fatalf("missing = %#v, want one tgz", plan.missing)
	}
	if !strings.Contains(plan.missing[0].URL, "tailscale_") || !strings.HasSuffix(plan.missing[0].URL, ".tgz") {
		t.Fatalf("url = %q, want static tgz", plan.missing[0].URL)
	}
}

func TestEnsureTailscaleNoDownloadsWhenPresent(t *testing.T) {
	env := testInstallEnv(t)
	env.present["tailscale"] = true
	if err := env.ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := env.downloads.Load(); n != 0 {
		t.Fatalf("downloads = %d, want 0", n)
	}
	if env.installed.Load() != 0 {
		t.Fatalf("installs = %d, want 0", env.installed.Load())
	}
}

func TestEnsureTailscaleDownloadsMissingConcurrently(t *testing.T) {
	env := testInstallEnv(t)
	env.osRelease = map[string]string{"ID": "ubuntu", "VERSION_CODENAME": "jammy"}
	env.present["iptables"] = true
	env.present["tailscale"] = false
	env.downloadDelay = 80 * time.Millisecond

	if err := env.ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if env.downloads.Load() < 2 {
		t.Fatalf("downloads = %d, want tailscale deb + keyring concurrently", env.downloads.Load())
	}
	if env.maxInFlight.Load() < 2 {
		t.Fatalf("max in-flight downloads = %d, want concurrent >= 2", env.maxInFlight.Load())
	}
}

func TestEnsureTailscaleInstallsOnlyDownloadedFiles(t *testing.T) {
	env := testInstallEnv(t)
	env.osRelease = map[string]string{"ID": "debian", "VERSION_CODENAME": "bookworm"}
	env.present["iptables"] = true
	if err := env.ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(env.installedFiles) == 0 {
		t.Fatal("expected dpkg -i of downloaded debs")
	}
	joined := strings.Join(env.installedFiles, ",")
	if !strings.Contains(joined, "tailscale_") {
		t.Fatalf("installed %q, want tailscale deb", joined)
	}
}

func hasName(arts []artifact, name string) bool {
	for _, a := range arts {
		if a.Name == name {
			return true
		}
	}
	return false
}

type testEnv struct {
	installEnv
	present        map[string]bool
	hasDpkg        bool
	downloadDelay  time.Duration
	downloads      atomic.Int32
	inFlight       atomic.Int32
	maxInFlight    atomic.Int32
	installed      atomic.Int32
	installedFiles []string
	hashes         map[string]string
	pendingSHA     string
	mu             sync.Mutex
}

func testInstallEnv(t *testing.T) *testEnv {
	t.Helper()
	env := &testEnv{
		present: map[string]bool{},
		hasDpkg: true,
	}
	env.lookPath = func(name string) bool { return env.present[name] }
	env.dpkgQuery = func(name string) bool { return env.hasDpkg && env.present[name] }
	env.dpkgAvailable = func() bool { return env.hasDpkg }
	env.readOSRelease = func() (map[string]string, error) {
		if env.osRelease != nil {
			return env.osRelease, nil
		}
		return map[string]string{"ID": "ubuntu", "VERSION_CODENAME": "jammy"}, nil
	}
	env.goarch = "amd64"
	env.download = func(_ context.Context, url, dest string) error {
		n := env.inFlight.Add(1)
		for {
			cur := env.maxInFlight.Load()
			if n <= cur || env.maxInFlight.CompareAndSwap(cur, n) {
				break
			}
		}
		env.downloads.Add(1)
		if env.downloadDelay > 0 {
			time.Sleep(env.downloadDelay)
		}
		env.inFlight.Add(-1)
		return nil
	}
	env.installDebs = func(_ context.Context, files []string) error {
		env.installed.Add(1)
		env.mu.Lock()
		env.installedFiles = append(env.installedFiles, files...)
		env.present["tailscale"] = true
		env.mu.Unlock()
		return nil
	}
	env.installTGZ = func(context.Context, string) error {
		env.installed.Add(1)
		env.present["tailscale"] = true
		return nil
	}
	return env
}

func (e *testEnv) plan() (installPlan, error) {
	return e.planArtifacts()
}

func (e *testEnv) ensure(ctx context.Context) error {
	return e.ensureTailscale(ctx)
}

func TestEnsurePinnedSkipsWhenSHAMatches(t *testing.T) {
	env := testPinnedEnv(t)
	env.present["zenadm"] = true
	env.hashes["/usr/local/bin/zenadm"] = strings.Repeat("a", 64)
	bin := pinnedBinary{Name: "zenadm", URL: "https://example/zenadm", SHA256: strings.Repeat("a", 64), Dest: "/usr/local/bin/zenadm"}
	if err := env.ensurePinned(context.Background(), bin); err != nil {
		t.Fatal(err)
	}
	if env.downloads.Load() != 0 {
		t.Fatalf("downloads = %d, want 0 when sha matches", env.downloads.Load())
	}
}

func TestEnsurePinnedDownloadsWhenLookPathHitsWrongSHA(t *testing.T) {
	env := testPinnedEnv(t)
	env.present["zenthros-agent"] = true
	env.hashes["/usr/local/bin/zenthros-agent"] = strings.Repeat("b", 64)
	want := strings.Repeat("c", 64)
	env.pendingSHA = want
	bin := pinnedBinary{Name: "zenthros-agent", URL: "https://example/agent", SHA256: want, Dest: "/usr/local/bin/zenthros-agent"}
	if err := env.ensurePinned(context.Background(), bin); err != nil {
		t.Fatal(err)
	}
	if env.downloads.Load() != 1 {
		t.Fatalf("downloads = %d, want 1 when lookPath hits a different sha", env.downloads.Load())
	}
	if env.hashes["/usr/local/bin/zenthros-agent"] != want {
		t.Fatalf("installed sha = %q, want pin", env.hashes["/usr/local/bin/zenthros-agent"])
	}
}

func TestEnsurePinnedDownloadsWhenMissing(t *testing.T) {
	env := testPinnedEnv(t)
	want := strings.Repeat("d", 64)
	env.pendingSHA = want
	bin := pinnedBinary{Name: "zenadm", URL: "https://example/zenadm", SHA256: want, Dest: "/usr/local/bin/zenadm"}
	if err := env.ensurePinned(context.Background(), bin); err != nil {
		t.Fatal(err)
	}
	if env.downloads.Load() != 1 {
		t.Fatalf("downloads = %d, want 1", env.downloads.Load())
	}
}

func testPinnedEnv(t *testing.T) *testEnv {
	t.Helper()
	env := testInstallEnv(t)
	env.hashes = map[string]string{}
	env.which = func(name string) string {
		if env.present[name] {
			return "/usr/local/bin/" + name
		}
		return ""
	}
	env.hashFile = func(path string) (string, error) {
		env.mu.Lock()
		defer env.mu.Unlock()
		sum, ok := env.hashes[path]
		if !ok {
			return "", os.ErrNotExist
		}
		return sum, nil
	}
	env.download = func(_ context.Context, _, dest string) error {
		env.downloads.Add(1)
		env.mu.Lock()
		env.hashes[dest] = env.pendingSHA
		env.mu.Unlock()
		return nil
	}
	env.placeFile = func(src, dest string) error {
		env.mu.Lock()
		defer env.mu.Unlock()
		env.hashes[dest] = env.hashes[src]
		return nil
	}
	return env
}
