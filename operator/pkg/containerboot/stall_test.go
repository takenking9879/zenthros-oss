// SPDX-License-Identifier: Apache-2.0

package containerboot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// stallingStore serves im's artifacts over HTTP. Its first stalls downloads
// of the agent send half of it and then nothing, until the client gives up.
// It counts the agent's downloads.
func stallingStore(t *testing.T, im *image, stalls int64) (string, *atomic.Int64) {
	t.Helper()
	var agents atomic.Int64
	quit := make(chan struct{})
	agent := AgentArtifact(machineArch(t))
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := filepath.Base(r.URL.Path)
		body, err := os.ReadFile(filepath.Join(im.served, name))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		if name != agent || agents.Add(1) > stalls {
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-quit:
		}
	}))
	t.Cleanup(store.Close)
	t.Cleanup(func() { close(quit) })
	return store.URL, &agents
}

// A download that stops moving is cut and tried again, with whichever of
// curl, wget or python3 the image has (the real ones): one that stalls once
// boots on its next try; one that stalls on every try ends the boot with
// Stalled and the artifact's URL, after three, never hanging. Each download
// is named in the log before it starts. Named case: on Vast.ai an agent
// download stalled past 5 minutes, and the machine waited out its boot
// (2026-10-28, hosts 45390 and 146320).
func TestBootCutsADownloadThatStoppedMoving(t *testing.T) {
	agent := AgentArtifact(machineArch(t))
	for _, tool := range []string{"curl", "wget", "python3"} {
		for _, c := range []struct {
			name   string
			stalls int64
		}{{"stalls once", 1}, {"stalls on every try", 3}} {
			t.Run(tool+" "+c.name, func(t *testing.T) {
				t.Parallel()
				real, err := exec.LookPath(tool)
				if err != nil {
					t.Skipf("no %s on this machine", tool)
				}
				im := newImage(t)
				if err := os.Symlink(real, filepath.Join(im.bin, tool)); err != nil {
					t.Fatal(err)
				}
				base, agents := stallingStore(t, im, c.stalls)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, filepath.Join(im.bin, "sh"), "-c", Script)
				// A boot that hangs ends with its downloader: the group goes.
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
				cmd.Env = []string{
					"PATH=" + im.bin,
					"ZENTHROS_STATE_DIR=" + im.state,
					EnvBakedAgent + "=" + filepath.Join(im.state, "no-baked-agent"),
					EnvInstallBase + "=" + base,
					EnvSumsSHA256 + "=" + im.sumsPin(),
					"zenthros_stall=1",
				}
				raw, err := cmd.CombinedOutput()
				out := string(raw)
				if ctx.Err() != nil {
					t.Fatalf("the boot hung on a download that stopped moving\n%s", out)
				}
				if !strings.Contains(out, "zenthros-boot downloading "+base+"/"+agent+"\n") {
					t.Errorf("the log does not name the agent's download before it starts:\n%s", out)
				}
				if c.stalls < 3 {
					if err != nil || im.agentRan() != "downloaded" || agents.Load() != c.stalls+1 {
						t.Fatalf("%v, ran %q after %d downloads of the agent; want it run on try %d\n%s",
							err, im.agentRan(), agents.Load(), c.stalls+1, out)
					}
					return
				}
				want := "zenthros-boot: " + Stalled + " " + base + "/" + agent + "\n"
				if err == nil || !strings.HasSuffix(out, want) || agents.Load() != 3 || im.agentRan() != "" {
					t.Fatalf("%v after %d downloads of the agent, ran %q; want three, then %q\n%s",
						err, agents.Load(), im.agentRan(), want, out)
				}
			})
		}
	}
}
