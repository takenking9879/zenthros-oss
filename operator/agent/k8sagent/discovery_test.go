// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/capability"
)

func fakeDiscovery(served map[string][]string, denied map[string]bool) *discovery {
	return &discovery{
		served: func(_ context.Context, gv string) ([]metav1.APIResource, error) {
			names, ok := served[gv]
			if !ok {
				return nil, apierrors.NewNotFound(schema.GroupResource{}, gv)
			}
			out := make([]metav1.APIResource, 0, len(names))
			for _, n := range names {
				out = append(out, metav1.APIResource{Name: n})
			}
			return out, nil
		},
		allowed: func(_ context.Context, _, resource string) (bool, error) { return !denied[resource], nil },
		version: func() string { return "v1.34.1" },
		nodes:   func(context.Context) ([]capability.Node, error) { return nil, nil },
	}
}

// A kind is declared when the cluster serves its resource and the agent may
// create it: KubeRay installed but no Spark operator declares the Ray kinds
// only, and an RBAC gap (the chart once granted only get on rayclusters) hides
// the kind instead of failing every submit. Manifest is always declared: what
// it may create is per resource, and a refusal is the API server's own text.
func TestDiscoverKindsFromAPIResources(t *testing.T) {
	for name, tc := range map[string]struct {
		served map[string][]string
		denied map[string]bool
		want   []string
	}{
		"kuberay only": {
			served: map[string][]string{"batch/v1": {"jobs"}, "ray.io/v1": {"rayjobs", "rayclusters", "rayjobs/status"}},
			want:   []string{v1.WorkloadKindJob, v1.WorkloadKindManifest, v1.WorkloadKindRayCluster, v1.WorkloadKindRayJob},
		},
		"kuberay and spark": {
			served: map[string][]string{"batch/v1": {"jobs"}, "ray.io/v1": {"rayjobs", "rayclusters"}, "spark.apache.org/v1": {"sparkapplications"}},
			want:   []string{v1.WorkloadKindJob, v1.WorkloadKindManifest, v1.WorkloadKindRayCluster, v1.WorkloadKindRayJob, v1.WorkloadKindSparkApplication},
		},
		"agent may not create rayclusters": {
			served: map[string][]string{"batch/v1": {"jobs"}, "ray.io/v1": {"rayjobs", "rayclusters"}},
			denied: map[string]bool{"rayclusters": true},
			want:   []string{v1.WorkloadKindJob, v1.WorkloadKindManifest, v1.WorkloadKindRayJob},
		},
		"no operators": {
			served: map[string][]string{"batch/v1": {"jobs"}},
			want:   []string{v1.WorkloadKindJob, v1.WorkloadKindManifest},
		},
		"agent may not create jobs": {
			served: map[string][]string{"batch/v1": {"jobs"}},
			denied: map[string]bool{"jobs": true},
			want:   []string{v1.WorkloadKindManifest},
		},
	} {
		t.Run(name, func(t *testing.T) {
			set, host := fakeDiscovery(tc.served, tc.denied).declare(context.Background())
			if got := set.Kinds(); !slices.Equal(got, tc.want) {
				t.Fatalf("kinds = %v, want %v", got, tc.want)
			}
			for _, w := range set {
				if w.Lifecycle != capability.LifecycleOf(w.Kind) || w.DefaultRuntime != capability.RuntimeKubernetes {
					t.Fatalf("declaration %+v", w)
				}
			}
			if host.OSVersion != "v1.34.1" {
				t.Fatalf("host = %+v", host)
			}
		})
	}
}

// RayCluster runs until cancelled: it is declared as a Service.
func TestRayClusterDeclaredAsService(t *testing.T) {
	set, _ := fakeDiscovery(map[string][]string{"ray.io/v1": {"rayjobs", "rayclusters"}}, nil).declare(context.Background())
	if w, ok := set.Find(v1.WorkloadKindRayCluster); !ok || w.Lifecycle != capability.Service {
		t.Fatalf("RayCluster = %+v, want Service", w)
	}
	if w, _ := set.Find(v1.WorkloadKindRayJob); w.Lifecycle != capability.RunToCompletion {
		t.Fatalf("RayJob = %+v, want RunToCompletion", w)
	}
}

// When the API server cannot be reached the agent keeps what it declared
// last rather than declaring nothing.
func TestDiscoveryKeepsLastDeclarationWhenUnreachable(t *testing.T) {
	d := fakeDiscovery(map[string][]string{"ray.io/v1": {"rayjobs"}}, nil)
	first, _ := d.declare(context.Background())
	d.checked, d.tried = d.checked.Add(-2*discoveryInterval), d.tried.Add(-2*discoveryInterval)
	d.served = func(context.Context, string) ([]metav1.APIResource, error) {
		return nil, errors.New("dial tcp 10.43.0.1:443: connect: connection refused")
	}
	d.declare(context.Background())
	d.settle()
	again, _ := d.declare(context.Background())
	if !slices.Equal(first.Kinds(), again.Kinds()) || len(again) == 0 {
		t.Fatalf("after an unreachable refresh kinds = %v, want %v", again.Kinds(), first.Kinds())
	}
}

// Every kind the API accepts has a translator with all its operations and,
// except Manifest, a resource for discovery; every handle a translator issues
// routes back to it. The RayJob handle has no prefix, so it must be the only
// one.
func TestTranslatorTableCoversDeclaredKinds(t *testing.T) {
	enum := []string{v1.WorkloadKindJob, v1.WorkloadKindRayJob, v1.WorkloadKindRayCluster, v1.WorkloadKindSparkApplication, v1.WorkloadKindManifest}
	for _, kind := range enum {
		tr, ok := translatorForKind(kind)
		if !ok {
			t.Fatalf("no translator for %s", kind)
		}
		if (tr.resource.Resource == "") != (kind == v1.WorkloadKindManifest) || tr.submit == nil || tr.read == nil || tr.pods == nil || tr.object == nil || tr.logs == nil ||
			(tr.container == "") != (kind == v1.WorkloadKindManifest) {
			t.Fatalf("translator %s is incomplete", kind)
		}
	}
	unprefixed := 0
	for _, tr := range translators {
		if tr.prefix == "" {
			unprefixed++
		}
		h := tr.handle("tenant-a", "train")
		if tr.kind == v1.WorkloadKindManifest {
			h = manifestHandle(schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "PyTorchJob"}, "tenant-a", "train")
		}
		got, r, err := translatorForHandle(h)
		if err != nil || got.kind != tr.kind || r.namespace != "tenant-a" || r.name != "train" {
			t.Fatalf("%s handle %q routed to %s %+v err=%v", tr.kind, h, got.kind, r, err)
		}
	}
	if unprefixed != 1 {
		t.Fatalf("%d translators without a handle prefix, want 1 (RayJob)", unprefixed)
	}
}

// A batch Job handle is job:ns/name and routes to the Job translator; a
// malformed one is an error, not a RayJob lookup.
func TestJobHandleRoundTrip(t *testing.T) {
	tr, r, err := translatorForHandle("job:tenant-a/train")
	if err != nil || tr.kind != v1.WorkloadKindJob || r.namespace != "tenant-a" || r.name != "train" {
		t.Fatalf("job handle -> %s %+v err=%v", tr.kind, r, err)
	}
	if _, _, err := translatorForHandle("job:tenant-a"); err == nil {
		t.Fatal("malformed job handle accepted")
	}
	if tr, _, _ := translatorForHandle("tenant-a/train"); tr.kind != v1.WorkloadKindRayJob {
		t.Fatalf("bare handle -> %s, want RayJob", tr.kind)
	}
}

// The declaration's host carries the nodes a job can land on (targets()
// shows a cluster by node). A read of the nodes that fails (a chart from
// before the agent read nodes, an API server that times out) keeps the
// nodes declared last and never hides the kinds.
func TestTheDeclarationCarriesTheNodesAndKeepsTheLastOnes(t *testing.T) {
	d := fakeDiscovery(map[string][]string{"batch/v1": {"jobs"}}, nil)
	nodes := []capability.Node{{Name: "big", MilliCPU: 8000, FreeMilliCPU: 4000}}
	d.nodes = func(context.Context) ([]capability.Node, error) { return nodes, nil }
	if _, host := d.declare(context.Background()); !reflect.DeepEqual(host.Nodes, nodes) {
		t.Fatalf("nodes = %+v, want %+v", host.Nodes, nodes)
	}
	d.checked, d.tried = d.checked.Add(-2*discoveryInterval), d.tried.Add(-2*discoveryInterval)
	d.nodes = func(context.Context) ([]capability.Node, error) {
		return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "", errors.New("no list"))
	}
	d.declare(context.Background())
	d.settle()
	set, host := d.declare(context.Background())
	if !reflect.DeepEqual(host.Nodes, nodes) || !slices.Contains(set.Kinds(), v1.WorkloadKindJob) {
		t.Fatalf("after a failed read: nodes = %+v, kinds = %v", host.Nodes, set.Kinds())
	}
	if !d.checked.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("a failed node read must not stop the refresh of the kinds")
	}
}

// A refresh that stalls on the API server does not hold up the heartbeat or
// a Submit: they get the last declaration at once, and the refresh lands
// when it ends (discovery held the lock they wait
// on, with calls that ignore their context).
func TestAStalledRefreshDoesNotHoldUpTheDeclaration(t *testing.T) {
	d := fakeDiscovery(map[string][]string{"batch/v1": {"jobs"}}, nil)
	first, _ := d.declare(context.Background())
	stall := make(chan struct{})
	served := d.served
	d.served = func(ctx context.Context, gv string) ([]metav1.APIResource, error) {
		<-stall
		return served(ctx, gv)
	}
	d.checked, d.tried = d.checked.Add(-2*discoveryInterval), d.tried.Add(-2*discoveryInterval)
	answered := make(chan capability.Set)
	go func() {
		set, _ := d.declare(context.Background())
		answered <- set
	}()
	select {
	case set := <-answered:
		if !slices.Equal(set.Kinds(), first.Kinds()) {
			t.Fatalf("kinds %v, want the last %v", set.Kinds(), first.Kinds())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the declaration waited on a stalled refresh")
	}
	close(stall)
	d.settle()
	if !d.checked.After(time.Now().Add(-time.Minute)) {
		t.Fatal("the refresh did not land once the API server answered")
	}
}

// One group that fails or hangs (an aggregated API down, a CRD's webhook
// slow) does not take another group's kinds with it, nor its own: a group
// that answered something other than 404 keeps the kinds it declared last,
// and one that does not answer within its deadline counts as failed
// (K8/06#4/c4-A6). With no earlier declaration nothing is declared yet.
func TestAFailingGroupKeepsItsKindsAndHoldsUpNoOther(t *testing.T) {
	d := fakeDiscovery(map[string][]string{"batch/v1": {"jobs"}, "ray.io/v1": {"rayjobs", "rayclusters"}}, nil)
	d.settle()
	if set, _ := d.declare(context.Background()); !slices.Contains(set.Kinds(), v1.WorkloadKindRayJob) {
		t.Fatalf("first declaration %v lacks RayJob", set)
	}
	healthy := d.served
	release := make(chan struct{})
	defer close(release)
	for name, broken := range map[string]func(context.Context, string) ([]metav1.APIResource, error){
		"503": func(ctx context.Context, gv string) ([]metav1.APIResource, error) {
			if gv == "ray.io/v1" {
				return nil, apierrors.NewServiceUnavailable("ray.io aggregated API down")
			}
			return healthy(ctx, gv)
		},
		"hang": func(ctx context.Context, gv string) ([]metav1.APIResource, error) {
			if gv == "ray.io/v1" {
				<-release // ServerResourcesForGroupVersion ignores ctx
			}
			return healthy(ctx, gv)
		},
	} {
		t.Run(name, func(t *testing.T) {
			d.served = broken
			start := time.Now()
			kinds, ok := d.discover(context.Background())
			if !ok || !slices.Contains(kinds, v1.WorkloadKindJob) || !slices.Contains(kinds, v1.WorkloadKindRayJob) || !slices.Contains(kinds, v1.WorkloadKindRayCluster) {
				t.Fatalf("with ray.io failing discovery = %v ok=%v, want Job and the Ray kinds kept", kinds, ok)
			}
			if took := time.Since(start); took > discoveryGroupTimeout+time.Second {
				t.Fatalf("one hung group held discovery %s", took)
			}

			// With none declared yet, a group that failed leaves the
			// declaration unknown: the kinds that answered alone would
			// read as an answer that the group's kinds are not there,
			// and the controller filtered this backend out for them.
			fresh := fakeDiscovery(nil, nil)
			fresh.served = broken
			if kinds, ok := fresh.discover(context.Background()); ok || len(kinds) != 0 {
				t.Fatalf("with no earlier declaration a failing group declared %v ok=%v, want none", kinds, ok)
			}
		})
	}
}
