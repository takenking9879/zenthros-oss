// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The image name is spec data; the console event prints it and runs nothing.
func TestImageEventDoesNotRunTheImageName(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pwned")
	image := `repo/img:1"; touch ` + marker + `; echo "$(touch ` + marker + `)`
	command := imageEvent("job_image_pull_start", image, "")
	printer, _, found := strings.Cut(command, " | ")
	if !found {
		t.Fatalf("unexpected command shape: %s", command)
	}
	out, err := exec.Command("bash", "-c", printer).CombinedOutput()
	if err != nil {
		t.Fatalf("event command failed: %v\n%s", err, out)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the image name ran as a command")
	}
	if !strings.Contains(string(out), "image="+image+" ts=") {
		t.Fatalf("event line = %q, want the image verbatim", out)
	}
}

// A hostPath is checked where it really points: a link planted under an
// allowed root cannot reach the rest of the host.
func TestResolveExistingFollowsLinksInExistingPrefix(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{root, outside} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	got, err := resolveExisting(filepath.Join(root, "link", "not-yet", "created"))
	if err != nil {
		t.Fatal(err)
	}
	resolvedOutside, _ := filepath.EvalSymlinks(outside)
	if want := filepath.Join(resolvedOutside, "not-yet", "created"); got != want {
		t.Fatalf("resolveExisting = %q, want %q", got, want)
	}
}
