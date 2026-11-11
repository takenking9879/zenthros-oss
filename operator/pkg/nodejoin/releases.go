// SPDX-License-Identifier: Apache-2.0

package nodejoin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/version"
)

// How a machine joins its cluster.
const (
	// MethodKubelet is Kubernetes' own: a kubelet and containerd that TLS
	// bootstrap with a token (kubeadm, kind, kubespray and the like).
	MethodKubelet = "kubelet"
	// MethodK3s is a k3s agent: k3s nodes run kube-proxy and the CNI inside
	// the agent, which a bare kubelet lacks.
	MethodK3s = "k3s"
)

// KubeletBootstrapGroup is the group of the bootstrap tokens X's agent
// issues outside k3s. X's agent chart binds it to node bootstrapping and
// to the automatic approval of a node's first client certificate.
const KubeletBootstrapGroup = "system:bootstrappers:zenthros"

// Release is a pinned binary or archive.
type Release struct {
	// Version is the component's: v<kubernetes>[+k3s<n>] for k3s and the
	// kubelet, the plugins' own for the CNI plugins.
	Version string `json:"version"`
	URL     string `json:"url"`
	SHA256  string `json:"sha256"`
}

func (r Release) check(component string) error {
	if !strings.HasPrefix(r.URL, "https://") && !strings.HasPrefix(r.URL, "http://") {
		return fmt.Errorf("%s %s: url %q", component, r.Version, r.URL)
	}
	if sum, err := hex.DecodeString(r.SHA256); err != nil || len(sum) != sha256.Size {
		return fmt.Errorf("%s %s: sha256 %q", component, r.Version, r.SHA256)
	}
	return nil
}

// Releases are a Kubernetes component's pinned releases (k3s, the
// kubelet), any number per minor.
type Releases struct {
	Component string
	Pins      []Release
}

// ParseReleases reads the gateway's pins of component (JSON) and checks them.
func ParseReleases(component, raw string) (Releases, error) {
	releases := Releases{Component: component}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return releases, nil
	}
	if err := json.Unmarshal([]byte(raw), &releases.Pins); err != nil {
		return releases, fmt.Errorf("%s releases: %w", component, err)
	}
	seen := map[string]bool{}
	for _, release := range releases.Pins {
		v, err := version.ParseSemantic(release.Version)
		if err != nil {
			return releases, fmt.Errorf("%s release %q: %w", component, release.Version, err)
		}
		if err := release.check(component); err != nil {
			return releases, err
		}
		key := fmt.Sprintf("%d.%d.%d", v.Major(), v.Minor(), v.Patch())
		if seen[key] {
			return releases, fmt.Errorf("%s releases pin %s twice", component, key)
		}
		seen[key] = true
	}
	return releases, nil
}

// For is the release a node of a cluster at serverVersion runs: of the
// server's minor, the newest pin not newer than the server (a node newer
// than its API server is outside Kubernetes' version skew policy). A
// cluster on 1.34.0 with pins 1.34.0 and 1.34.2 gets 1.34.0; with only
// 1.34.2, none.
func (r Releases) For(serverVersion string) (Release, error) {
	server, err := version.ParseGeneric(serverVersion)
	if err != nil {
		return Release{}, fmt.Errorf("cluster version %q: %w", serverVersion, err)
	}
	var candidates []Release
	newer := ""
	for _, release := range r.Pins {
		v, err := version.ParseSemantic(release.Version)
		if err != nil || v.Major() != server.Major() || v.Minor() != server.Minor() {
			continue
		}
		if v.Patch() > server.Patch() {
			newer = release.Version
			continue
		}
		candidates = append(candidates, release)
	}
	if len(candidates) == 0 {
		if newer != "" {
			return Release{}, fmt.Errorf("pinned %s %s is newer than the cluster's %s", r.Component, newer, serverVersion)
		}
		return Release{}, fmt.Errorf("no %s pinned for Kubernetes %d.%d (the cluster runs %s)", r.Component, server.Major(), server.Minor(), serverVersion)
	}
	return slices.MaxFunc(candidates, func(a, b Release) int {
		va, vb := version.MustParseSemantic(a.Version), version.MustParseSemantic(b.Version)
		switch {
		case va.LessThan(vb):
			return -1
		case vb.LessThan(va):
			return 1
		}
		return 0
	}), nil
}

// ParseRelease reads one pin of component (JSON), as the CNI plugins'.
// Empty is none.
func ParseRelease(component, raw string) (*Release, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" || raw == "null" {
		return nil, nil
	}
	release := &Release{}
	if err := json.Unmarshal([]byte(raw), release); err != nil {
		return nil, fmt.Errorf("%s release: %w", component, err)
	}
	if release.Version == "" {
		return nil, fmt.Errorf("%s release has no version", component)
	}
	return release, release.check(component)
}
