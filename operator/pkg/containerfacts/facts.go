// SPDX-License-Identifier: Apache-2.0

// Package containerfacts reads what a container agent can observe about the
// container it runs in: the cgroup's CPU and memory limits (not the host's),
// the image's OS, GPUs when nvidia-smi is present, and the topology the
// environment states. It never asks a metadata service: a container has no
// reason to reach one, and waiting on it would delay every start.
package containerfacts

import (
	"bufio"
	"context"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/zenthros/operator/pkg/capability"
)

// Sources are where the facts come from; tests point them at a fake tree.
type Sources struct {
	// Root is prefixed to /sys/fs/cgroup, /proc and /etc paths. Empty is "/".
	Root string
	// Command runs a program; nil runs it for real when it is on PATH.
	Command func(ctx context.Context, name string, args ...string) (string, error)
	// Getenv reads the environment; nil is os.Getenv.
	Getenv func(string) string
}

// Read observes the container.
func Read(ctx context.Context, src Sources) capability.Host {
	if src.Command == nil {
		src.Command = run
	}
	if src.Getenv == nil {
		src.Getenv = os.Getenv
	}
	host := capability.Host{Arch: runtime.GOARCH}
	host.CPU, host.MilliCPU = cpuLimit(src.Root, runtime.NumCPU())
	host.MemoryBytes = memoryLimit(src.Root)
	host.OSID, host.OSVersion = OSRelease(filepath.Join(src.Root, "/etc/os-release"))
	if out, err := src.Command(ctx, "nvidia-smi", "--query-gpu=name", "--format=csv,noheader"); err == nil {
		var models []string
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				models = append(models, line)
			}
		}
		if len(models) > 0 {
			host.GPUVendor, host.GPUModel, host.GPUCount = "nvidia", models[0], int32(len(models))
		}
	}
	host.Zone = src.Getenv("ZENTHROS_TOPOLOGY_ZONE")
	host.Region = src.Getenv("ZENTHROS_TOPOLOGY_REGION")
	host.Provider = src.Getenv("ZENTHROS_TOPOLOGY_PROVIDER")
	host.NetworkID = src.Getenv("ZENTHROS_NETWORK_ID")
	host.PeerAddr = src.Getenv("ZENTHROS_PEER_ADDR")
	host.ContainerImage = src.Getenv("ZENTHROS_CONTAINER_IMAGE")
	return host
}

// cpuLimit is the cgroup's CPU quota rounded up to whole CPUs, and exactly
// in thousandths, never more than the CPUs the process can see; without a
// quota it is those CPUs.
func cpuLimit(root string, visible int) (int32, int32) {
	limit, milli := visible, int64(visible)*1000
	if quota, period, ok := cpuMax(root); ok && period > 0 {
		cpus := int(math.Ceil(float64(quota) / float64(period)))
		if cpus > 0 && cpus < limit {
			limit = cpus
		}
		if exact := quota * 1000 / period; exact > 0 && exact < milli {
			milli = exact
		}
	}
	return int32(limit), int32(milli)
}

// cpuMax reads cgroup v2 cpu.max ("max 100000" or "<quota> <period>"), then
// cgroup v1's cfs files. ok is false when there is no quota.
func cpuMax(root string) (quota, period int64, ok bool) {
	if raw, err := os.ReadFile(filepath.Join(root, "/sys/fs/cgroup/cpu.max")); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) == 2 && fields[0] != "max" {
			q, err1 := strconv.ParseInt(fields[0], 10, 64)
			p, err2 := strconv.ParseInt(fields[1], 10, 64)
			return q, p, err1 == nil && err2 == nil
		}
		return 0, 0, false
	}
	q, err1 := readInt(filepath.Join(root, "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
	p, err2 := readInt(filepath.Join(root, "/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
	if err1 != nil || err2 != nil || q <= 0 {
		return 0, 0, false
	}
	return q, p, true
}

// memoryLimit is the cgroup's memory limit, or the memory the kernel
// reports when the cgroup sets none.
func memoryLimit(root string) int64 {
	total := MemTotal(filepath.Join(root, "/proc/meminfo"))
	limit := int64(0)
	if raw, err := os.ReadFile(filepath.Join(root, "/sys/fs/cgroup/memory.max")); err == nil {
		if value := strings.TrimSpace(string(raw)); value != "max" {
			limit, _ = strconv.ParseInt(value, 10, 64)
		}
	} else if v1, err := readInt(filepath.Join(root, "/sys/fs/cgroup/memory/memory.limit_in_bytes")); err == nil {
		limit = v1
	}
	// cgroup v1 writes a huge number for "no limit".
	if limit > 0 && (total == 0 || limit < total) {
		return limit
	}
	return total
}

func readInt(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
}

// MemTotal is the MemTotal of a /proc/meminfo, in bytes; 0 when the file
// says none.
func MemTotal(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return 0
}

// OSRelease is the ID and VERSION_ID of an os-release file.
func OSRelease(path string) (id, version string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), "=")
		if !ok {
			continue
		}
		value = strings.Trim(value, `"'`)
		switch key {
		case "ID":
			id = value
		case "VERSION_ID":
			version = value
		}
	}
	return id, version
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	if _, err := exec.LookPath(name); err != nil {
		return "", err
	}
	out, err := exec.CommandContext(ctx, name, args...).Output()
	return string(out), err
}
