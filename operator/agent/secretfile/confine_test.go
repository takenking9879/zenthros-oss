// SPDX-License-Identifier: Apache-2.0

package secretfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

func fileBinding(path, keyID, value string) Binding {
	ref := v1.SecretSourceRef{
		Type:            v1.SecretSourceTypeEncryptedPayload,
		Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: path},
	}
	if keyID != "" {
		ref.EncryptedPayload = &v1.EncryptedPayloadRef{KeyID: keyID}
	}
	return Binding{Ref: ref, Value: value}
}

// The path of a file secret is chosen by whoever submits the job, and the
// agent runs as root. Apply once wrote it on the host as asked and
// CleanupRoot removed it: any host file. Every byte now stays under
// <root>/<jobID>; where the job sees the file is a mount (or a link), not a
// write.
func TestFileSecretStaysUnderJobRoot(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "etc", "passwd")
	if err := os.MkdirAll(filepath.Dir(outside), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, []byte("root:x:0:0"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := Apply(root, "job-1", []Binding{fileBinding(outside, "", "tenant value")})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(outside); string(got) != "root:x:0:0" {
		t.Fatalf("Apply wrote the host path: %q", got)
	}
	if len(result.Mounts) != 1 || result.Mounts[0].Target != outside {
		t.Fatalf("mounts = %+v, want one at %s", result.Mounts, outside)
	}
	source := result.Mounts[0].Source
	if !strings.HasPrefix(source, filepath.Join(root, "job-1")+"/") {
		t.Fatalf("source %s is outside the job root", source)
	}
	if got, _ := os.ReadFile(source); string(got) != "tenant value" {
		t.Fatalf("source holds %q", got)
	}
	for _, f := range result.Files {
		if !strings.HasPrefix(f, filepath.Join(root, "job-1")+"/") {
			t.Errorf("recorded %s outside the job root", f)
		}
	}
	if err := CleanupRoot(root, "job-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("CleanupRoot removed a host file: %v", err)
	}
}

// A job ID that is not one path element under the root is refused: its
// cleanup would remove the root or what lies outside it.
func TestCleanupRootRefusesAJobIDThatLeavesItsDirectory(t *testing.T) {
	root := t.TempDir()
	for _, id := range []string{"..", "../x", "a/b", "."} {
		if err := CleanupRoot(root, id); err == nil {
			t.Errorf("CleanupRoot accepted job id %q", id)
		}
	}
}

// Without a keyID every encrypted file secret defaults to
// /var/run/zenthros/secrets/encrypted/value: two jobs on one machine wrote
// the same file, and one's cleanup removed the other's.
func TestDefaultFileSecretsDoNotCollideAcrossJobs(t *testing.T) {
	root := t.TempDir()
	a, err := Apply(root, "job-a", []Binding{fileBinding("", "", "a")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Apply(root, "job-b", []Binding{fileBinding("", "", "b")})
	if err != nil {
		t.Fatal(err)
	}
	if a.Mounts[0].Source == b.Mounts[0].Source {
		t.Fatalf("both jobs write %s", a.Mounts[0].Source)
	}
	if a.Mounts[0].Target != b.Mounts[0].Target || a.Mounts[0].Target != "/var/run/zenthros/secrets/encrypted/value" {
		t.Fatalf("targets %s, %s: the documented default is /var/run/zenthros/secrets/encrypted/value", a.Mounts[0].Target, b.Mounts[0].Target)
	}
	if err := CleanupRoot(root, "job-a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(b.Mounts[0].Source); string(got) != "b" {
		t.Fatalf("job-b's secret after job-a's cleanup: %q", got)
	}
	if got := SecretsDir(root, "job-b"); filepath.Join(got, "encrypted", "value") != b.Mounts[0].Source {
		t.Fatalf("SecretsDir %s does not hold the default file %s", got, b.Mounts[0].Source)
	}
}

// A link planted where a secret is about to be written is not followed.
func TestWriteFileDoesNotFollowSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(link, "secret"); err == nil {
		t.Fatal("WriteFile followed a symlink")
	}
	if got, _ := os.ReadFile(target); string(got) != "keep" {
		t.Fatalf("target now holds %q", got)
	}
}

// A job without a mount namespace (systemd, the container agent) sees the
// host: an explicit path must be under one of its own directories, where a
// link to the file in the root stands; anywhere else is refused before
// anything is written there.
func TestLinkConfinesExplicitPathsToTheJobsDirectories(t *testing.T) {
	root := t.TempDir()
	work := t.TempDir()
	inside := filepath.Join(work, "conf", "token")
	result, err := Apply(root, "job-1", []Binding{fileBinding(inside, "", "v"), fileBinding("", "k1", "d")})
	if err != nil {
		t.Fatal(err)
	}
	if err := Link(result, []string{work}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(inside); string(got) != "v" {
		t.Fatalf("%s reads %q", inside, got)
	}
	if _, err := os.Lstat("/var/run/zenthros/secrets/k1/value"); err == nil {
		t.Fatal("a default-path secret was linked on the host")
	}
	// A resubmit links again over its own link.
	if err := Link(result, []string{work}); err != nil {
		t.Fatalf("relink: %v", err)
	}

	outside := filepath.Join(t.TempDir(), "x")
	result, err = Apply(root, "job-2", []Binding{fileBinding(outside, "", "v")})
	if err != nil {
		t.Fatal(err)
	}
	err = Link(result, []string{work})
	if err == nil || !strings.Contains(err.Error(), "working directory") {
		t.Fatalf("Link outside the job's directories: %v", err)
	}
	if _, statErr := os.Lstat(outside); statErr == nil {
		t.Fatal("Link created the refused path")
	}

	// An existing file is never replaced.
	existing := filepath.Join(work, "existing")
	if err := os.WriteFile(existing, []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err = Apply(root, "job-3", []Binding{fileBinding(existing, "", "v")})
	if err != nil {
		t.Fatal(err)
	}
	if err := Link(result, []string{work}); err == nil {
		t.Fatal("Link replaced an existing file")
	}
	if got, _ := os.ReadFile(existing); string(got) != "mine" {
		t.Fatalf("existing file now %q", got)
	}
}

// A re-run's secrets are attempt-scoped (<root>/<id>-a2/...), and its
// working directory is the job's, where the first attempt's link still
// stands, to <root>/<id>-a1/.... The re-run replaces its own job's stale
// link; it refused it as an existing file and failed INVALID (post-fix
// review 2026-10-03 R-2). A link to anything else is still never replaced.
func TestLinkReplacesAnEarlierAttemptsLink(t *testing.T) {
	root := t.TempDir()
	work := t.TempDir()
	target := filepath.Join(work, "conf", "token")
	first, err := Apply(root, "job-1-a1", []Binding{fileBinding(target, "", "one")})
	if err != nil {
		t.Fatal(err)
	}
	if err := Link(first, []string{work}); err != nil {
		t.Fatal(err)
	}
	if err := CleanupRoot(root, "job-1-a1"); err != nil {
		t.Fatal(err)
	}
	second, err := Apply(root, "job-1-a2", []Binding{fileBinding(target, "", "two")})
	if err != nil {
		t.Fatal(err)
	}
	if err := Link(second, []string{work}); err != nil {
		t.Fatalf("the second attempt could not link over the first's: %v", err)
	}
	if got, _ := os.ReadFile(target); string(got) != "two" {
		t.Fatalf("%s reads %q, want the second attempt's", target, got)
	}

	foreign := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.WriteFile(foreign, []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	planted := filepath.Join(work, "planted")
	if err := os.Symlink(foreign, planted); err != nil {
		t.Fatal(err)
	}
	third, err := Apply(root, "job-3", []Binding{fileBinding(planted, "", "v")})
	if err != nil {
		t.Fatal(err)
	}
	if err := Link(third, []string{work}); err == nil {
		t.Fatal("Link replaced a link that is not a secret's")
	}
	if dest, _ := os.Readlink(planted); dest != foreign {
		t.Fatalf("the planted link now points at %s", dest)
	}
}

// A path under the default root is the default place: kept in the root and
// read as $ZENTHROS_SECRETS_DIR/<the rest> where the job sees the host,
// never a link to make. One that only starts like it is the job's own path.
func TestAPathUnderTheDefaultRootIsReadFromTheSecretsDir(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	result, err := Apply(root, "job-1", []Binding{fileBinding("/var/run/zenthros/secrets/workspace/hf-token", "k1", "hf")})
	if err != nil {
		t.Fatal(err)
	}
	if result.Mounts[0].Explicit || result.Mounts[0].Target != "/var/run/zenthros/secrets/workspace/hf-token" {
		t.Fatalf("mount %+v: the default place, at its path in a container", result.Mounts[0])
	}
	if err := Link(result, []string{work}); err != nil {
		t.Fatalf("Link refused the default place: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(SecretsDir(root, "job-1"), "workspace", "hf-token")); string(got) != "hf" {
		t.Fatalf("$ZENTHROS_SECRETS_DIR/workspace/hf-token reads %q", got)
	}
	result, err = Apply(root, "job-2", []Binding{fileBinding("/var/run/zenthros/secrets-x/token", "", "v")})
	if err != nil {
		t.Fatal(err)
	}
	if err := Link(result, []string{work}); err == nil {
		t.Fatal("a path outside the default root was taken for it")
	}
}

// An empty job ID was skipped as nothing to remove: the job's plaintext
// stayed, and the caller heard it was gone (07#9).
func TestCleanupOfNoJobIsRefused(t *testing.T) {
	if err := CleanupRoot(t.TempDir(), ""); err == nil {
		t.Fatal("CleanupRoot of an empty job ID succeeded")
	}
	if err := CleanupRoot("", "job-1"); err == nil {
		t.Fatal("CleanupRoot under an empty root succeeded")
	}
}
