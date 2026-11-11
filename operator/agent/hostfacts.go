// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/containerfacts"
)

// hostFacts is what the agent knows of its host: read once, and read again
// in the background while that read was incomplete (factsCache).
var hostFacts = (&factsCache{read: func() (capability.Host, bool) {
	return readHostFacts(hostSources{
		osRelease: "/etc/os-release",
		meminfo:   "/proc/meminfo",
		command:   runCommand,
		metadata: func(ctx context.Context) cloudmeta.Facts {
			return cloudmeta.DetectFacts(ctx, cloudmeta.FactsSources())
		},
		say: func(msg string) { hostLog.Info(msg) },
	})
}, now: time.Now}).get

// hostLog is where the host's read says what its operator should know (a GPU
// it does not offer). main sets it before the agent starts.
var hostLog = logr.Discard()

// SetHostLog sets where the host's read says what it does not offer.
func SetHostLog(logger logr.Logger) { hostLog = logger.WithName("host") }

// factsRetry is how often an incomplete read of the host is made again.
const factsRetry = 5 * time.Minute

// factsCache keeps the host's facts. A read that left something out (the
// metadata service did not answer in time, nvidia-smi failed while the
// driver loaded) was kept for the agent's life: no provider, zone or GPU
// until a restart. It is read again every
// factsRetry, in the background, so a heartbeat never waits on it.
type factsCache struct {
	read func() (capability.Host, bool)
	now  func() time.Time

	mu                         sync.Mutex
	value                      capability.Host
	at                         time.Time
	read1, complete, rereading bool
}

func (c *factsCache) get() capability.Host {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.read1 {
		c.value, c.complete = c.read()
		c.at, c.read1 = c.now(), true
		return c.value
	}
	if !c.complete && !c.rereading && c.now().Sub(c.at) >= factsRetry {
		c.rereading = true
		go func() {
			value, complete := c.read()
			c.mu.Lock()
			defer c.mu.Unlock()
			c.value, c.complete, c.at, c.rereading = value, complete, c.now(), false
		}()
	}
	return c.value
}

// hostSources are where host facts come from; tests replace them.
type hostSources struct {
	osRelease, meminfo string
	command            func(ctx context.Context, name string, args ...string) (string, error)
	metadata           func(ctx context.Context) cloudmeta.Facts
	// say tells the operator what the read found and does not offer.
	say func(msg string)
}

// sourceTimeout bounds each source of host facts on its own: a slow first
// nvidia-smi spent the budget the metadata service then had none of.
var sourceTimeout = 5 * time.Second

// readHostFacts reads the host, and whether that read is complete: the
// metadata service (or the environment) named its provider, and nvidia-smi,
// where it is installed, answered.
func readHostFacts(src hostSources) (capability.Host, bool) {
	host := capability.Host{Arch: runtime.GOARCH, CPU: int32(runtime.NumCPU())}
	host.OSID, host.OSVersion = containerfacts.OSRelease(src.osRelease)
	host.MemoryBytes = containerfacts.MemTotal(src.meminfo)
	complete := true
	out, err := withTimeout(func(ctx context.Context) (string, error) {
		return src.command(ctx, "nvidia-smi", "--query-gpu=name", "--format=csv,noheader")
	})
	if err == nil {
		var models []string
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				models = append(models, line)
			}
		}
		if len(models) > 0 {
			host.GPUVendor, host.GPUModel, host.GPUCount = "nvidia", models[0], int32(len(models))
			// A machine of its owner's: the agent says it and offers no GPU,
			// asking again in factsRetry, as a GPU still coming up would be.
			switch cuda := containerfacts.AskCUDA(context.Background(), src.command, "python3"); {
			case cuda.Fails():
				src.say(fmt.Sprintf("%s: %s; offering no GPU", models[0], cuda.Failure()))
				host.GPUVendor, host.GPUModel, host.GPUCount = "", "", 0
				complete = false
			case !cuda.Asked:
				src.say(fmt.Sprintf("cannot ask whether the GPU starts (%s); offering the %s nvidia-smi lists", cuda.Why, models[0]))
			}
		}
	} else if !errors.Is(err, exec.ErrNotFound) {
		complete = false
	}
	if _, err := withTimeout(func(ctx context.Context) (string, error) { return src.command(ctx, "nvidia-ctk", "--version") }); err == nil {
		host.NvidiaContainerRuntime = true
	}
	host.PeerAddr = os.Getenv("ZENTHROS_PEER_ADDR")
	if host.PeerAddr == "" {
		host.PeerAddr = outboundAddr()
	}
	var cloud cloudmeta.Facts
	if src.metadata != nil {
		ctx, cancel := context.WithTimeout(context.Background(), sourceTimeout)
		cloud = src.metadata(ctx)
		cancel()
	}
	// The environment wins over metadata: an operator can place a machine
	// the metadata service does not describe (on-prem, another cloud).
	host.NetworkID = envOr("ZENTHROS_NETWORK_ID", cloud.NetworkID)
	host.Zone = envOr("ZENTHROS_TOPOLOGY_ZONE", cloud.Zone)
	host.Region = envOr("ZENTHROS_TOPOLOGY_REGION", cloud.Region)
	host.Provider = envOr("ZENTHROS_TOPOLOGY_PROVIDER", cloud.Provider)
	return host, complete && host.Provider != ""
}

func withTimeout(f func(context.Context) (string, error)) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sourceTimeout)
	defer cancel()
	return f(ctx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runCommand(ctx context.Context, name string, args ...string) (string, error) {
	if _, err := exec.LookPath(name); err != nil {
		return "", err
	}
	out, err := exec.CommandContext(ctx, name, args...).Output()
	return string(out), err
}

// outboundAddr is the address of the interface the default route uses. The
// UDP "dial" sends nothing.
func outboundAddr() string {
	conn, err := net.Dial("udp4", "192.0.2.1:9")
	if err != nil {
		return ""
	}
	defer conn.Close()
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return addr.IP.String()
	}
	return ""
}
