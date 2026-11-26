// SPDX-License-Identifier: Apache-2.0

package containerfacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func tree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func noCommands(context.Context, string, ...string) (string, error) {
	return "", errors.New("not here")
}

// The container's limits, not the host's: a 2-CPU, 4 GiB container on a
// 64-CPU host must not advertise 64 CPUs.
func TestCgroupV2LimitsWinOverHost(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("needs at least 2 visible CPUs to observe a lower quota")
	}
	root := tree(t, map[string]string{
		"/sys/fs/cgroup/cpu.max":    "150000 100000\n",
		"/sys/fs/cgroup/memory.max": "4294967296\n",
		"/proc/meminfo":             "MemTotal:       67108864 kB\n",
		"/etc/os-release":           "ID=debian\nVERSION_ID=\"12\"\n",
	})
	host := Read(context.Background(), Sources{Root: root, Command: noCommands, Getenv: func(string) string { return "" }})
	if host.CPU != 2 {
		t.Errorf("CPU = %d, want 2 (1.5 CPUs of quota, rounded up)", host.CPU)
	}
	// What create(cpus=1.5) asked, exactly: targets() shows 1.5, not 2.
	if host.MilliCPU != 1500 {
		t.Errorf("MilliCPU = %d, want 1500", host.MilliCPU)
	}
	if host.MemoryBytes != 4<<30 {
		t.Errorf("MemoryBytes = %d, want 4 GiB", host.MemoryBytes)
	}
	if host.OSID != "debian" || host.OSVersion != "12" {
		t.Errorf("os = %s %s", host.OSID, host.OSVersion)
	}
}

func TestNoLimitFallsBackToWhatTheKernelShows(t *testing.T) {
	root := tree(t, map[string]string{
		"/sys/fs/cgroup/cpu.max":    "max 100000\n",
		"/sys/fs/cgroup/memory.max": "max\n",
		"/proc/meminfo":             "MemTotal:       1048576 kB\n",
	})
	host := Read(context.Background(), Sources{Root: root, Command: noCommands, Getenv: func(string) string { return "" }})
	if host.CPU != int32(runtime.NumCPU()) || host.MilliCPU != int32(runtime.NumCPU())*1000 || host.MemoryBytes != 1<<30 {
		t.Fatalf("CPU=%d milli=%d mem=%d", host.CPU, host.MilliCPU, host.MemoryBytes)
	}
}

func TestCgroupV1UnlimitedIsNotALimit(t *testing.T) {
	root := tree(t, map[string]string{
		"/sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
		"/sys/fs/cgroup/cpu/cpu.cfs_quota_us":         "-1\n",
		"/sys/fs/cgroup/cpu/cpu.cfs_period_us":        "100000\n",
		"/proc/meminfo":                               "MemTotal:       2097152 kB\n",
	})
	host := Read(context.Background(), Sources{Root: root, Command: noCommands, Getenv: func(string) string { return "" }})
	if host.MemoryBytes != 2<<30 {
		t.Fatalf("MemoryBytes = %d, want MemTotal", host.MemoryBytes)
	}
}

func TestGPUsImageAndTopologyFromTheContainer(t *testing.T) {
	env := map[string]string{
		"ZENTHROS_CONTAINER_IMAGE":   "runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404",
		"ZENTHROS_TOPOLOGY_PROVIDER": "runpod",
		"ZENTHROS_TOPOLOGY_ZONE":     "US-KS-2",
	}
	cmd := func(_ context.Context, name string, _ ...string) (string, error) {
		if name == "nvidia-smi" {
			return "NVIDIA GeForce RTX 4090\nNVIDIA GeForce RTX 4090\n", nil
		}
		return "", errors.New("no")
	}
	host := Read(context.Background(), Sources{Root: t.TempDir(), Command: cmd, Getenv: func(k string) string { return env[k] }})
	if host.GPUCount != 2 || host.GPUModel != "NVIDIA GeForce RTX 4090" || host.GPUVendor != "nvidia" {
		t.Errorf("gpu = %s %s x%d", host.GPUVendor, host.GPUModel, host.GPUCount)
	}
	if host.ContainerImage != env["ZENTHROS_CONTAINER_IMAGE"] || host.Provider != "runpod" || host.Zone != "US-KS-2" {
		t.Errorf("host = %+v", host)
	}
	if host.NvidiaContainerRuntime {
		t.Error("a container has no container runtime of its own to report")
	}
}

// A quota of 1.5 CPUs on a host that shows 2 rounds to the 2 it sees, and
// is still 1500 thousandths exactly.
func TestTheExactQuotaHoldsWhenItRoundsToTheVisibleCPUs(t *testing.T) {
	root := tree(t, map[string]string{"/sys/fs/cgroup/cpu.max": "150000 100000\n"})
	if cpu, milli := cpuLimit(root, 2); cpu != 2 || milli != 1500 {
		t.Fatalf("cpuLimit = %d, %d; want 2, 1500", cpu, milli)
	}
	if cpu, milli := cpuLimit(root, 1); cpu != 1 || milli != 1000 {
		t.Fatalf("on 1 visible CPU = %d, %d; want 1, 1000", cpu, milli)
	}
}
