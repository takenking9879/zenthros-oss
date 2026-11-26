// SPDX-License-Identifier: Apache-2.0

package procagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/takenking9879/zenthros-oss/operator/pkg/containerfacts"
	"github.com/takenking9879/zenthros-oss/operator/pkg/runtimegate"
)

// cgroupTree is a container's cgroup and /proc as the agent reads them.
func cgroupTree(t *testing.T, cpuMax, memoryMax string) string {
	t.Helper()
	root := t.TempDir()
	write(t, filepath.Join(root, "sys/fs/cgroup/cpu.max"), cpuMax)
	write(t, filepath.Join(root, "sys/fs/cgroup/memory.max"), memoryMax)
	write(t, filepath.Join(root, "proc/meminfo"), "MemTotal:       32000000 kB\n")
	return root
}

func write(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A container resized in place (docker update, from `zenthros attach NAME
// --type local --cpus 3 --memory 6g`) declares its new size on the next
// heartbeat, with no restart: its environments stay, and a restart would end
// a job. The size was read once, at the agent's start (observaciones 17, 24).
func TestAContainerResizedInPlaceDeclaresItsNewSize(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(runtimegate.EnvReadyFile, filepath.Join(dir, "ready"))
	t.Setenv(runtimegate.EnvPendingFile, filepath.Join(dir, "pending"))
	root := cgroupTree(t, "200000 100000\n", "4294967296\n")
	e, err := New(Config{StateDir: filepath.Join(dir, "state"), KillGrace: time.Second, Logger: logr.Discard(),
		Facts: containerfacts.Sources{Root: root, Command: func(context.Context, string, ...string) (string, error) {
			return "", os.ErrNotExist
		}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, host := e.Declare(context.Background()); host.CPU != 2 || host.MemoryBytes != 4<<30 {
		t.Fatalf("before: %d CPUs, %d bytes; want 2 and 4 GiB", host.CPU, host.MemoryBytes)
	}
	write(t, filepath.Join(root, "sys/fs/cgroup/cpu.max"), "300000 100000\n")
	write(t, filepath.Join(root, "sys/fs/cgroup/memory.max"), "6442450944\n")
	if _, host := e.Declare(context.Background()); host.CPU != 3 || host.MilliCPU != 3000 || host.MemoryBytes != 6<<30 {
		t.Fatalf("after docker update: %d CPUs (%d m), %d bytes; want 3 and 6 GiB", host.CPU, host.MilliCPU, host.MemoryBytes)
	}
}
