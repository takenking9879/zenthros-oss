// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/capability"
	"github.com/zenthros/operator/pkg/cloudmeta"
)

func noCommand(context.Context, string, ...string) (string, error) {
	return "", errors.New("not found")
}

// The metadata service places the host; ZENTHROS_TOPOLOGY_* overrides it
// field by field.
func TestHostFactsTopologyFromMetadataAndEnv(t *testing.T) {
	src := hostSources{command: noCommand, metadata: func(context.Context) cloudmeta.Facts {
		return cloudmeta.Facts{NetworkID: "vpc-1", Zone: "us-east-2a", Region: "us-east-2", Provider: "aws"}
	}}
	host, _ := readHostFacts(src)
	if host.NetworkID != "vpc-1" || host.Zone != "us-east-2a" || host.Region != "us-east-2" || host.Provider != "aws" {
		t.Fatalf("from metadata: %+v", host)
	}
	t.Setenv("ZENTHROS_TOPOLOGY_ZONE", "rack-7")
	t.Setenv("ZENTHROS_TOPOLOGY_PROVIDER", "onprem")
	host, _ = readHostFacts(src)
	if host.Zone != "rack-7" || host.Provider != "onprem" || host.Region != "us-east-2" {
		t.Fatalf("env overrides: %+v", host)
	}
}

// Each source has its own deadline: a first nvidia-smi that hung while the
// driver loaded spent the one budget, and the metadata service got none.
// Such a read is incomplete, to be made again.
func TestASlowSourceDoesNotStarveTheOthers(t *testing.T) {
	defer func(d time.Duration) { sourceTimeout = d }(sourceTimeout)
	sourceTimeout = 50 * time.Millisecond
	hung := func(ctx context.Context, name string, _ ...string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	host, complete := readHostFacts(hostSources{command: hung, metadata: func(ctx context.Context) cloudmeta.Facts {
		if ctx.Err() != nil {
			return cloudmeta.Facts{}
		}
		return cloudmeta.Facts{Provider: "aws", Zone: "us-east-2a"}
	}})
	if host.Provider != "aws" || host.Zone != "us-east-2a" {
		t.Fatalf("the metadata service had no time left: %+v", host)
	}
	if complete {
		t.Fatal("a read whose nvidia-smi did not answer is complete")
	}
}

// An incomplete read is made again, in the background, every factsRetry;
// a complete one is kept.
func TestIncompleteHostFactsAreReadAgain(t *testing.T) {
	now := time.Unix(0, 0)
	reads := make(chan struct{}, 4)
	providers := []string{"", "aws"}
	c := &factsCache{now: func() time.Time { return now }, read: func() (capability.Host, bool) {
		provider := providers[0]
		providers = providers[1:]
		reads <- struct{}{}
		return capability.Host{Provider: provider}, provider != ""
	}}
	if c.get().Provider != "" {
		t.Fatal("first read")
	}
	<-reads
	if c.get(); len(reads) != 0 {
		t.Fatal("read again before factsRetry")
	}
	now = now.Add(factsRetry)
	c.get()
	<-reads
	deadline := time.Now().Add(5 * time.Second)
	for c.get().Provider != "aws" {
		if time.Now().After(deadline) {
			t.Fatal("the second read never landed")
		}
		time.Sleep(time.Millisecond)
	}
	now = now.Add(10 * factsRetry)
	c.get()
	time.Sleep(10 * time.Millisecond)
	if len(reads) != 0 {
		t.Fatal("a complete read was made again")
	}
}

// A public address the first read did not get is asked for again, on its
// own deadline whatever the caller's context: every endpoint read Private
// until a restart.
func TestAPublicAddressIsAskedForUntilKnown(t *testing.T) {
	now := time.Unix(0, 0)
	answers := []string{"", "203.0.113.7"}
	asked := 0
	c := &addressCache{now: func() time.Time { return now }, detect: func(ctx context.Context) string {
		if ctx.Err() != nil {
			return ""
		}
		asked++
		ip := answers[0]
		answers = answers[1:]
		return ip
	}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if c.get(cancelled) != "" || c.get(cancelled) != "" || asked != 1 {
		t.Fatalf("asked %d times before addressRetry", asked)
	}
	now = now.Add(addressRetry)
	if got := c.get(cancelled); got != "203.0.113.7" {
		t.Fatalf("after addressRetry: %q", got)
	}
	now = now.Add(time.Hour)
	if c.get(cancelled); asked != 2 {
		t.Fatalf("a known address was asked for again (%d)", asked)
	}
}
