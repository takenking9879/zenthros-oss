// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// A running Service object's URLs are read by its endpointFrom, all of them
// or none; a cluster-local name is reached inside the cluster only.
func TestManifestEndpoints(t *testing.T) {
	contract := manifestContract{EndpointFrom: []v1.EndpointSource{
		{Name: "http", Path: "status.url"},
		{Name: "internal", Path: "status.address.url"},
	}}
	running := backend.JobState{Phase: v1.PhaseRunning}
	both := objectWithStatus("InferenceService", map[string]any{
		"url":     "https://llm.example.com",
		"address": map[string]any{"url": "http://llm.tenant-a.svc.cluster.local"},
	})
	got := manifestEndpoints(both, contract, running)
	if got.EndpointProblem != "" || len(got.Endpoints) != 2 {
		t.Fatalf("both published: %+v", got)
	}
	if e := got.Endpoints[0]; e.Name != "http" || e.URL != "https://llm.example.com" || e.Scope != v1.EndpointScopePublic {
		t.Errorf("public endpoint %+v", e)
	}
	if e := got.Endpoints[1]; e.Name != "internal" || e.Scope != v1.EndpointScopeCluster {
		t.Errorf("cluster endpoint %+v", e)
	}

	half := objectWithStatus("InferenceService", map[string]any{"url": "https://llm.example.com"})
	if got := manifestEndpoints(half, contract, running); len(got.Endpoints) != 0 || !strings.HasPrefix(got.EndpointProblem, "WaitingForPort: ") {
		t.Errorf("one path not set yet: %+v, want none and WaitingForPort", got)
	}
	relative := objectWithStatus("InferenceService", map[string]any{"url": "/v1", "address": map[string]any{"url": "http://x"}})
	if got := manifestEndpoints(relative, contract, running); len(got.Endpoints) != 0 || !strings.HasPrefix(got.EndpointProblem, "EndpointUnresolved: ") {
		t.Errorf("a relative URL: %+v, want none and EndpointUnresolved", got)
	}
	// Only a running object publishes; one without endpointFrom never does.
	if got := manifestEndpoints(both, contract, backend.JobState{Phase: v1.PhaseSubmitted}); got.Endpoints != nil || got.EndpointProblem != "" {
		t.Errorf("not running: %+v", got)
	}
	if got := manifestEndpoints(both, manifestContract{}, running); got.Endpoints != nil || got.EndpointProblem != "" {
		t.Errorf("no endpointFrom: %+v", got)
	}
}

// endpointFrom travels with the object (the contract annotation), so Status
// reads the URL from the handle alone.
func TestManifestStatusPublishesEndpointsFromTheHandle(t *testing.T) {
	e := manifestExecutor(t, nil)
	ctx := context.Background()
	spec := manifestSpec(`{"apiVersion":"e2e.zenthros.ai/v1","kind":"Widget","metadata":{}}`)
	spec.Manifest.Lifecycle = "Service"
	spec.Manifest.RunningWhen = []v1.ConditionMatch{{Type: "Ready", Status: "True"}}
	spec.Manifest.EndpointFrom = []v1.EndpointSource{{Name: "http", Path: "status.url"}}
	h, err := e.Submit(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(widgetGVK)
	if err := e.client.Get(ctx, client.ObjectKey{Namespace: "tenant-a", Name: "train"}, obj); err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedSlice(obj.Object, []any{map[string]any{"type": "Ready", "status": "True"}}, "status", "conditions")
	_ = unstructured.SetNestedField(obj.Object, "http://widget.tenant-a.svc:8080", "status", "url")
	if err := e.client.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	state, err := e.Status(ctx, h)
	if err != nil || state.Phase != v1.PhaseRunning || len(state.Endpoints) != 1 {
		t.Fatalf("status = %+v, %v", state, err)
	}
	if got := state.Endpoints[0]; got.URL != "http://widget.tenant-a.svc:8080" || got.Scope != v1.EndpointScopeCluster {
		t.Fatalf("endpoint %+v", got)
	}
}
