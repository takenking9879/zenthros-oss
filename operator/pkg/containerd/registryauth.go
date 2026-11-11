// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/zenthros/operator/pkg/backend"
)

// AuthChain asks each RegistryAuth in turn; the first that knows the host
// logs in. A provider's registry is one more entry, not a change to the pull.
type AuthChain []RegistryAuth

func (c AuthChain) Login(ctx context.Context, host string) (string, string, bool, error) {
	for _, auth := range c {
		user, password, ok, err := auth.Login(ctx, host)
		if ok || err != nil {
			return user, password, ok, err
		}
	}
	return "", "", false, nil
}

// DefaultRegistryAuth logs in to each provider's registry with the machine's
// own identity: Amazon ECR through the instance role, Google Artifact
// Registry and Container Registry through the VM's service account.
func DefaultRegistryAuth() RegistryAuth {
	return AuthChain{ECRAuth{}, GARAuth{}}
}

const gceTokenURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

// GARAuth logs in to Google Artifact Registry (<region>-docker.pkg.dev) and
// Container Registry (gcr.io and its regional hosts) with the VM service
// account's access token from the metadata server. No key is configured on
// the machine; the service account needs read on the repository.
type GARAuth struct {
	// TokenURL overrides the metadata token endpoint (tests).
	TokenURL string
	Client   *http.Client
}

// IsGoogleRegistry reports whether host is Artifact Registry or Container
// Registry.
func IsGoogleRegistry(host string) bool {
	return strings.HasSuffix(host, "-docker.pkg.dev") || host == "gcr.io" || strings.HasSuffix(host, ".gcr.io")
}

func (a GARAuth) Login(ctx context.Context, host string) (string, string, bool, error) {
	if !IsGoogleRegistry(host) {
		return "", "", false, nil
	}
	url := a.TokenURL
	if url == "" {
		url = gceTokenURL
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", "", true, err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound || errors.Is(err, syscall.ECONNREFUSED) {
			// Off Google Cloud there is no metadata server: pull
			// anonymously, so a public gcr.io image pulls on any machine.
			return "", "", false, nil
		}
		// One that did not answer in time is not its absence: an
		// anonymous pull of a private image fails for good (c4-A7).
		return "", "", true, backend.Classified(backend.ErrDestinationUnavailable, fmt.Errorf("registry login for %s: metadata token: %w", host, err))
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return "", "", true, fmt.Errorf("registry login for %s: metadata token: HTTP %d: %s", host, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.AccessToken == "" {
		return "", "", true, fmt.Errorf("registry login for %s: malformed metadata token", host)
	}
	return "oauth2accesstoken", out.AccessToken, true, nil
}
