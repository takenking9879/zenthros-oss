// SPDX-License-Identifier: Apache-2.0

package jobsource

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/jobspec"
)

type entry struct {
	name, body, link string
	typ              byte
}

func tarball(t *testing.T, entries ...entry) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		typ := e.typ
		if typ == 0 {
			typ = tar.TypeReg
		}
		hdr := &tar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.body)), Typeflag: typ, Linkname: e.link}
		if typ != tar.TypeReg {
			hdr.Size = 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if typ == tar.TypeReg {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	tw.Close()
	gz.Close()
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestInlineSourceUnpacksWithoutAShell(t *testing.T) {
	t.Setenv("PATH", "") // no tar, no base64, no sh: Go does it all
	dir := filepath.Join(t.TempDir(), "work")
	spec := jobspec.JobSpec{Source: v1.Source{Inline: tarball(t,
		entry{name: "pkg/", typ: tar.TypeDir},
		entry{name: "pkg/main.py", body: "print('hi')\n"},
		entry{name: "run.py", link: "pkg/main.py", typ: tar.TypeSymlink},
	)}}
	got, err := Prepare(context.Background(), spec, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != dir {
		t.Fatalf("work dir = %q, want %q", got, dir)
	}
	body, err := os.ReadFile(filepath.Join(dir, "run.py"))
	if err != nil || string(body) != "print('hi')\n" {
		t.Fatalf("run.py = %q, %v", body, err)
	}
}

func TestInlineSourceRefusesEscapes(t *testing.T) {
	for name, e := range map[string]entry{
		"dotdot":       {name: "../evil", body: "x"},
		"absolute":     {name: "/etc/evil", body: "x"},
		"link outside": {name: "l", link: "../../etc/passwd", typ: tar.TypeSymlink},
		"abs link":     {name: "l", link: "/etc/passwd", typ: tar.TypeSymlink},
		"device":       {name: "dev", typ: tar.TypeChar},
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "work")
			spec := jobspec.JobSpec{Source: v1.Source{Inline: tarball(t, e)}}
			if _, err := Prepare(context.Background(), spec, dir); err == nil {
				t.Fatal("an entry outside the work directory was accepted")
			}
		})
	}
}

func TestHostSourcesAreRefusedVerbatim(t *testing.T) {
	cases := map[string]v1.Source{
		"hostPath": {HostPath: "/data"},
		"s3":       {S3: "s3://bucket/code.tgz"},
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Prepare(context.Background(), jobspec.JobSpec{Source: src}, t.TempDir())
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", err)
			}
		})
	}
}

func TestGitWithoutGitIsRefusedWithReason(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := Prepare(context.Background(), jobspec.JobSpec{Source: v1.Source{Git: "https://example.com/r.git"}}, t.TempDir())
	if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "git binary") {
		t.Fatalf("err = %v", err)
	}
}

func TestNoSourceNoWorkDir(t *testing.T) {
	got, err := Prepare(context.Background(), jobspec.JobSpec{}, t.TempDir())
	if err != nil || got != "" {
		t.Fatalf("Prepare(no source) = %q, %v", got, err)
	}
}

// A git URL carrying a token keeps it out of a failed clone's error, which
// reaches the job's status.
func TestAFailedCloneQuotesNoToken(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	url := "https://user:ghp-secret-token@127.0.0.1:1/r.git"
	_, err := Prepare(context.Background(), jobspec.JobSpec{Source: v1.Source{Git: url}}, t.TempDir())
	if err == nil || strings.Contains(err.Error(), "ghp-secret-token") {
		t.Fatalf("err = %v", err)
	}
}
