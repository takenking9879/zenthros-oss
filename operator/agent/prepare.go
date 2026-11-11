// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"

	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/errtext"
	"github.com/zenthros/operator/pkg/prewarm/cache"
)

// Preparer is an executor that warms its host's cache for a Prewarm. Prepare
// is idempotent and returns at once: the first call with a key starts the
// work, and every call reports each item's progress.
type Preparer interface {
	Prepare(ctx context.Context, req *agentpb.PrepareRequest) ([]*agentpb.PrepareItemStatus, error)
}

// Prepare answers a Prewarm. An executor that cannot warm refuses it, and
// an executor's refusal reaches the controller in its own words.
func (a *Agent) Prepare(ctx context.Context, req *agentpb.PrepareRequest) *agentpb.PrepareResponse {
	preparer, ok := a.executor.(Preparer)
	if !ok {
		err := fmt.Errorf("%w: backend %s cannot prewarm", backend.ErrUnsupportedBackend, a.cfg.BackendID)
		return &agentpb.PrepareResponse{RequestId: req.GetRequestId(), Error: err.Error(), ErrorClass: errorclass.Encode(err)}
	}
	items, err := preparer.Prepare(ctx, req)
	if err != nil {
		return &agentpb.PrepareResponse{RequestId: req.GetRequestId(), Error: errtext.Bound(err.Error(), errtext.MaxWire), ErrorClass: errorclass.Encode(err)}
	}
	return &agentpb.PrepareResponse{RequestId: req.GetRequestId(), Items: items}
}

// cacheItems maps a request's items for the cache.
func cacheItems(req *agentpb.PrepareRequest) []cache.Item {
	out := make([]cache.Item, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		out = append(out, cache.Item{
			Ref: it.GetRef(), Image: it.GetImage(), URI: it.GetArtifactUri(), SHA256: it.GetArtifactSha256(), Path: it.GetArtifactPath(),
			Toolchain: it.GetToolchainName(), ToolchainVersion: it.GetToolchainVersion(), ToolchainPackages: it.GetToolchainPackages(),
		})
	}
	return out
}

// prepareStatuses maps the cache's progress for the wire; each message is
// bounded as any error on the wire is.
func prepareStatuses(in []cache.Status) []*agentpb.PrepareItemStatus {
	out := make([]*agentpb.PrepareItemStatus, 0, len(in))
	for _, s := range in {
		out = append(out, &agentpb.PrepareItemStatus{
			Ref: s.Ref, Phase: s.Phase, Bytes: s.Bytes, DurationMillis: s.DurationMillis,
			Message: errtext.Bound(s.Message, errtext.MaxDetail),
		})
	}
	return out
}

// withEnv is env with key set to value, leaving env itself unchanged.
func withEnv(env map[string]string, key, value string) map[string]string {
	out := make(map[string]string, len(env)+1)
	for k, v := range env {
		out[k] = v
	}
	out[key] = value
	return out
}
