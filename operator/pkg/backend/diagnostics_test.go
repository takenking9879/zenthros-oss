// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	v1 "github.com/zenthros/operator/api/v1"
)

// The SDK's quickstart on k3d (2026-11-06): the network cut Docker Hub's
// answer mid-pull, the kubelet said ErrImagePull, and the job failed for
// good while the kubelet was about to pull again. A pull fails a job only
// when the registry refused it, as on a machine (pkg/containerd's pullClass,
// R4 ronda 3, G1); the kubelet's back-off carries the last pull's error
// (Kubernetes 1.30 and later), so both of its states say which it was.
func TestAPullFailsTheJobOnlyWhenTheRegistryRefused(t *testing.T) {
	for _, tc := range []struct {
		text string
		want string
	}{
		// The E2E's own words.
		{`ErrImagePull failed to pull and unpack image "docker.io/library/python:3.13-bookworm": failed to read expected number of bytes: unexpected EOF`, ""},
		{`ImagePullBackOff Back-off pulling image "python:3.13-bookworm": ErrImagePull: rpc error: code = Unknown desc = failed to pull and unpack image "docker.io/library/python:3.13-bookworm": failed to read expected number of bytes: unexpected EOF`, ""},
		{`ErrImagePull rpc error: code = Unknown desc = failed to pull and unpack image "x": dial tcp 1.2.3.4:443: i/o timeout`, ""},
		{`ErrImagePull rpc error: code = Unknown desc = failed to pull and unpack image "x": unexpected status from GET request: 500 Internal Server Error`, ""},
		{`ErrImagePull toomanyrequests: You have reached your unauthenticated pull rate limit`, ""},
		// What the kubelet said once it no longer knew why.
		{`ImagePullBackOff Back-off pulling image "x"`, ""},
		// The registry's refusals, as k3s 1.34's kubelet writes them.
		{`ImagePullBackOff Back-off pulling image "docker.io/library/python:0.0.0-nope": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image "docker.io/library/python:0.0.0-nope": failed to resolve reference "docker.io/library/python:0.0.0-nope": docker.io/library/python:0.0.0-nope: not found`, v1.ReasonImagePullFailed},
		{`ErrImagePull pull access denied, repository does not exist or may require authorization`, v1.ReasonImagePullFailed},
		{`ErrImagePull failed to authorize: failed to fetch anonymous token: unexpected status: 401 Unauthorized`, v1.ReasonImagePullFailed},
		{`Failed to pull image "ghcr.io/acme/app:v9": rpc error: code = Unknown desc = 403 Forbidden`, v1.ReasonImagePullFailed},
		{`ErrImagePull no match for platform in manifest: not found`, v1.ReasonImagePullFailed},
	} {
		if got := DiagnosticReasonCode(tc.text); got != tc.want {
			t.Errorf("DiagnosticReasonCode(%q) = %q, want %q", tc.text, got, tc.want)
		}
	}
}
