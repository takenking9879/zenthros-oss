// SPDX-License-Identifier: Apache-2.0

package cloudmeta

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

const (
	gcpMetadataBaseURL = "http://metadata.google.internal/computeMetadata/v1"
	// GCPPreemptedPath answers TRUE once a Spot/preemptible VM is being
	// stopped.
	GCPPreemptedPath = "/instance/preempted"
	// GCPIdentityAudience must match the provisioner's gcp.IdentityAudience;
	// this package does not import the provisioner.
	GCPIdentityAudience = "zenthros-managed"
	gcpMetadataFlavor   = "Google"
)

func init() { Register(&GCP{Base: gcpMetadataBaseURL}) }

// GCP reads the Compute Engine metadata server.
type GCP struct {
	// Base is the metadata root including /computeMetadata/v1.
	Base   string
	Client *http.Client
}

func (*GCP) Name() string { return "gcp" }

func (g *GCP) get(ctx context.Context, path string) (string, error) {
	body, err := metadataClient{client: g.Client}.fetch(ctx, http.MethodGet, g.Base+path,
		map[string]string{"Metadata-Flavor": gcpMetadataFlavor})
	return strings.TrimSpace(string(body)), err
}

// Facts reads the network and zone.
func (g *GCP) Facts(ctx context.Context) (Facts, bool) {
	ctx, cancel := context.WithTimeout(ctx, factsTimeout)
	defer cancel()
	network, err := g.get(ctx, "/instance/network-interfaces/0/network")
	if err != nil || network == "" {
		return Facts{}, false
	}
	zone, _ := g.get(ctx, "/instance/zone")
	return GCEFacts(network, zone), true
}

// GCEFacts places a GCE instance from its zone path
// (projects/N/zones/us-central1-a): the region is the zone less its suffix.
func GCEFacts(network, zonePath string) Facts {
	zone := zonePath[strings.LastIndex(zonePath, "/")+1:]
	region := ""
	if i := strings.LastIndex(zone, "-"); i > 0 {
		region = zone[:i]
	}
	return Facts{NetworkID: network, Zone: zone, Region: region, Provider: "gcp"}
}

// Notice polls instance/preempted.
func (g *GCP) Notice(ctx context.Context) Notice {
	ctx, cancel := context.WithTimeout(ctx, noticeTimeout)
	defer cancel()
	body, err := g.get(ctx, GCPPreemptedPath)
	if err != nil {
		return Notice{Source: traits.NoticeGCPMetadata}
	}
	return ParseGCPPreempted([]byte(body))
}

// ParseGCPPreempted parses instance/preempted ("TRUE" / "FALSE").
func ParseGCPPreempted(body []byte) Notice {
	if strings.EqualFold(strings.TrimSpace(string(body)), "TRUE") {
		return Notice{Preempted: true, Source: traits.NoticeGCPMetadata, Action: "preempted"}
	}
	return Notice{Source: traits.NoticeGCPMetadata}
}

// IdentityEvidence returns a format=full instance identity JWT. There is no
// unauthenticated fallback: a managed worker that cannot obtain the JWT must
// fail closed and retry bootstrap.
func (g *GCP) IdentityEvidence(ctx context.Context) ([]byte, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, identityTimeout)
	defer cancel()
	query := url.Values{"audience": {GCPIdentityAudience}, "format": {"full"}}
	token, err := g.get(ctx, "/instance/service-accounts/default/identity?"+query.Encode())
	if err != nil {
		return nil, nil, fmt.Errorf("retrieve GCE instance identity JWT: %w", err)
	}
	if strings.Count(token, ".") != 2 {
		return nil, nil, fmt.Errorf("GCE instance identity JWT is malformed")
	}
	return []byte(token), nil, nil
}
