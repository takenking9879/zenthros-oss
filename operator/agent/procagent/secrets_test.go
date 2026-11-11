// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenthros/operator/agent/agepayload"
	v1 "github.com/zenthros/operator/api/v1"
	pkgage "github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/owner"
)

func sealedFileSecret(t *testing.T, bundle agepayload.Bundle, value, path string) v1.SecretSourceRef {
	t.Helper()
	ct, err := pkgage.Encrypt([]byte(value), bundle.Recipients())
	if err != nil {
		t.Fatal(err)
	}
	return v1.SecretSourceRef{
		Type:             v1.SecretSourceTypeEncryptedPayload,
		EncryptedPayload: &v1.EncryptedPayloadRef{Ciphertext: ct, Recipients: bundle.Recipients()},
		Materialization:  v1.SecretMaterialization{Mode: v1.SecretMaterializationFile, Path: path},
	}
}

// A container runs as a user that cannot write /var/run, and two
// jobs' default files once shared /var/run/zenthros/secrets/encrypted/value.
// Each job reads its own file under its directory, through
// $ZENTHROS_SECRETS_DIR, and the file goes with the job.
func TestDefaultFileSecretsDoNotCollideAcrossJobs(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := newTestExecutor(t, "")
	e.cfg.Age = agepayload.Config{Bundle: bundle}
	ctx := context.Background()
	var dirs []string
	for _, id := range []string{"job-a", "job-b"} {
		spec := shellJob(id, `cat "$ZENTHROS_SECRETS_DIR/encrypted/value"; echo; echo "$ZENTHROS_SECRETS_DIR"`)
		spec.SecretSources = []v1.SecretSourceRef{sealedFileSecret(t, bundle, "value-of-"+id, "")}
		h, err := e.Submit(ctx, spec, nil)
		if err != nil {
			t.Fatal(err)
		}
		if state := waitTerminal(t, e, h); state.Phase != v1.PhaseSucceeded {
			t.Fatalf("%s: %+v", id, state)
		}
		log, _ := os.ReadFile(filepath.Join(e.jobDir(h), logFile))
		lines := strings.Split(strings.TrimSpace(string(log)), "\n")
		if len(lines) != 2 || lines[0] != "value-of-"+id {
			t.Fatalf("%s read %q", id, log)
		}
		if !strings.HasPrefix(lines[1], e.cfg.StateDir+"/") {
			t.Fatalf("%s: secrets dir %s is outside the state dir", id, lines[1])
		}
		dirs = append(dirs, lines[1])
	}
	if dirs[0] == dirs[1] {
		t.Fatalf("both jobs read %s", dirs[0])
	}
	if err := e.Cancel(ctx, "job-a", owner.Ref{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dirs[0]); !os.IsNotExist(err) {
		t.Fatalf("job-a's secret outlived its Cancel: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dirs[1], "encrypted", "value")); string(got) != "value-of-job-b" {
		t.Fatalf("job-b's secret after job-a's cancel: %q", got)
	}
}

// The job and this agent share a user, and the path is the submitter's:
// outside the job's own directories it is refused before the job runs, and
// nothing is written there.
func TestExplicitFileSecretOutsideTheJobIsRefused(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := newTestExecutor(t, "")
	e.cfg.Age = agepayload.Config{Bundle: bundle}
	outside := filepath.Join(t.TempDir(), "victim")
	spec := shellJob("job-x", "true")
	spec.SecretSources = []v1.SecretSourceRef{sealedFileSecret(t, bundle, "v", outside)}
	_, err = e.Submit(context.Background(), spec, nil)
	if !errors.Is(err, backend.ErrInvalid) || !strings.Contains(err.Error(), "working directory") {
		t.Fatalf("Submit = %v, want ErrInvalid naming the working directory", err)
	}
	if _, statErr := os.Lstat(outside); statErr == nil {
		t.Fatal("the refused path was created")
	}

	work := t.TempDir()
	inside := filepath.Join(work, "conf", "token")
	spec = shellJob("job-y", `cat conf/token`)
	spec.WorkingDir = work
	spec.SecretSources = []v1.SecretSourceRef{sealedFileSecret(t, bundle, "in-workdir", inside)}
	h, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if state := waitTerminal(t, e, h); state.Phase != v1.PhaseSucceeded {
		t.Fatalf("%+v", state)
	}
	if log, _ := os.ReadFile(filepath.Join(e.jobDir(h), logFile)); string(log) != "in-workdir" {
		t.Fatalf("job read %q", log)
	}
}

// A Workspace secret is resolved to a path under the default root
// (/var/run/zenthros/secrets/workspace/<name>): the default place, read as
// $ZENTHROS_SECRETS_DIR/workspace/<name>, not a path of the job's own to
// refuse (found on a container here, 2026-10-16).
func TestAFileUnderTheDefaultRootIsReadFromTheSecretsDir(t *testing.T) {
	bundle, err := agepayload.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := newTestExecutor(t, "")
	e.cfg.Age = agepayload.Config{Bundle: bundle}
	spec := shellJob("job-w", `cat "$ZENTHROS_SECRETS_DIR/workspace/hf-token"`)
	spec.SecretSources = []v1.SecretSourceRef{sealedFileSecret(t, bundle, "hf", "/var/run/zenthros/secrets/workspace/hf-token")}
	h, err := e.Submit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if state := waitTerminal(t, e, h); state.Phase != v1.PhaseSucceeded {
		t.Fatalf("%+v", state)
	}
	if log, _ := os.ReadFile(filepath.Join(e.jobDir(h), logFile)); string(log) != "hf" {
		t.Fatalf("read %q", log)
	}
}
