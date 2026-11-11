// SPDX-License-Identifier: Apache-2.0

// Package cloudmeta is the machine side of a provider: what the agent and
// zenadm read from the instance metadata service. A provider implements the
// sources it has (host facts, interruption notices, identity evidence) and
// registers once; the agent and zenadm never name a provider.
//
// Adding a provider is one file here: a type with Name and any of Facts,
// Notice and IdentityEvidence, registered in init. Its identity evidence must
// match an IdentityVerifier of the same name on the controller.
package cloudmeta

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Facts places a machine: the provider's name, region, zone and private
// network. Empty fields are unknown.
type Facts struct {
	NetworkID, Zone, Region, Provider string
	// PublicIPv4 is the host's public address, when the provider gave it
	// one: where a Service on it is reached from outside.
	PublicIPv4 string
}

// Notice is an interruption notice. Source names the channel it came from
// (liveness.Notice*); Deadline is when the provider acts, zero when the
// provider does not say.
type Notice struct {
	Preempted bool
	Source    string
	Action    string
	Deadline  time.Time
}

// Provider is one provider's metadata service.
type Provider interface {
	Name() string
}

// FactsSource reads host facts. ok is false off this provider (the metadata
// service did not answer), so the next source is tried.
type FactsSource interface {
	Provider
	Facts(ctx context.Context) (facts Facts, ok bool)
}

// NoticeSource polls for an interruption notice. A provider that is not
// there answers a zero Notice.
type NoticeSource interface {
	Provider
	Notice(ctx context.Context) Notice
}

// RebalanceSource polls for a rebalance recommendation: the provider's
// advice that the machine is at elevated risk of interruption, before any
// notice. at is when the provider issued it; ok is false while there is none,
// and off this provider.
type RebalanceSource interface {
	Provider
	Rebalance(ctx context.Context) (at time.Time, ok bool)
}

// IdentitySource produces the evidence the controller verifies at managed
// enrollment. It fails closed: no evidence, no enrollment.
type IdentitySource interface {
	Provider
	IdentityEvidence(ctx context.Context) (document, signature []byte, err error)
}

var (
	mu        sync.RWMutex
	providers []Provider
)

// Register adds a provider. Names are unique; registering one twice panics,
// as a duplicate would make which one answers depend on init order.
func Register(p Provider) {
	mu.Lock()
	defer mu.Unlock()
	for _, existing := range providers {
		if existing.Name() == p.Name() {
			panic(fmt.Sprintf("cloudmeta: provider %q registered twice", p.Name()))
		}
	}
	providers = append(providers, p)
	sort.SliceStable(providers, func(i, j int) bool { return providers[i].Name() < providers[j].Name() })
}

// Providers lists the registered providers, sorted by name.
func Providers() []Provider {
	mu.RLock()
	defer mu.RUnlock()
	return append([]Provider(nil), providers...)
}

// Names lists the registered provider names.
func Names() []string {
	var names []string
	for _, p := range Providers() {
		names = append(names, p.Name())
	}
	return names
}

// NoticeSources are the registered providers that can hear a notice.
func NoticeSources() []NoticeSource {
	var out []NoticeSource
	for _, p := range Providers() {
		if source, ok := p.(NoticeSource); ok {
			out = append(out, source)
		}
	}
	return out
}

// RebalanceSources are the registered providers that can recommend a
// rebalance (only EC2 does).
func RebalanceSources() []RebalanceSource {
	var out []RebalanceSource
	for _, p := range Providers() {
		if source, ok := p.(RebalanceSource); ok {
			out = append(out, source)
		}
	}
	return out
}

// FirstRebalance asks each source in turn and returns the first
// recommendation.
func FirstRebalance(ctx context.Context, sources []RebalanceSource) (time.Time, bool) {
	for _, source := range sources {
		if at, ok := source.Rebalance(ctx); ok {
			return at, true
		}
	}
	return time.Time{}, false
}

// FactsSources are the registered providers that can place a machine.
func FactsSources() []FactsSource {
	var out []FactsSource
	for _, p := range Providers() {
		if source, ok := p.(FactsSource); ok {
			out = append(out, source)
		}
	}
	return out
}

// DetectFacts asks each source in turn and returns the first that answers.
// Off every cloud it returns zero Facts.
func DetectFacts(ctx context.Context, sources []FactsSource) Facts {
	for _, source := range sources {
		if facts, ok := source.Facts(ctx); ok {
			return facts
		}
	}
	return Facts{}
}

// FirstNotice polls each source and returns the first notice heard.
func FirstNotice(ctx context.Context, sources []NoticeSource) Notice {
	for _, source := range sources {
		if notice := source.Notice(ctx); notice.Preempted {
			return notice
		}
	}
	return Notice{}
}

// IdentityEvidence reads the evidence of the provider registered as name.
func IdentityEvidence(ctx context.Context, name string) ([]byte, []byte, error) {
	for _, p := range Providers() {
		if p.Name() != name {
			continue
		}
		source, ok := p.(IdentitySource)
		if !ok {
			return nil, nil, fmt.Errorf("provider %q has no identity evidence", name)
		}
		return source.IdentityEvidence(ctx)
	}
	return nil, nil, fmt.Errorf("provider identity %q is not supported (registered: %v)", name, Names())
}
