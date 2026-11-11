// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"sync"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/zenthros/operator/pkg/capability"
)

// discoveryInterval is how often the agent re-reads which kinds the cluster
// serves; installing an operator shows up within it.
const discoveryInterval = 60 * time.Second

// discovery is the cluster's answer to "which of our kinds can this agent
// create", cached for discoveryInterval.
type discovery struct {
	// served lists a group/version's resources; allowed asks the API server
	// whether the agent may create a resource. Tests replace both.
	served  func(ctx context.Context, groupVersion string) ([]metav1.APIResource, error)
	allowed func(ctx context.Context, group, resource string) (bool, error)
	version func() string
	// namespace is where the agent asks: empty, the whole cluster. The agent
	// of no hub may create things only in its own.
	namespace string
	// nodes lists the nodes a job can land on (listNodes); nil reads none.
	nodes func(ctx context.Context) ([]capability.Node, error)

	mu sync.Mutex
	// checked is the last refresh that reached the API server; tried, the
	// last one begun; refreshing, whether one runs now.
	checked, tried time.Time
	refreshing     bool
	refreshed      chan struct{}
	set            capability.Set
	host           capability.Host
}

// discoveryGroupTimeout bounds one group's discovery: the client's call
// ignores its context, and one hung aggregated API held every other group's
// kinds back with it (K8/06#4).
const discoveryGroupTimeout = 3 * time.Second

// discoveryRetry is how soon a refresh that could not reach the API server
// is tried again.
const discoveryRetry = 10 * time.Second

func newDiscovery(clientset kubernetes.Interface) *discovery {
	var d *discovery
	d = &discovery{
		served: func(_ context.Context, groupVersion string) ([]metav1.APIResource, error) {
			list, err := clientset.Discovery().ServerResourcesForGroupVersion(groupVersion)
			if err != nil {
				return nil, err
			}
			return list.APIResources, nil
		},
		allowed: func(ctx context.Context, group, resource string) (bool, error) {
			review, err := clientset.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
				Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{
					Group: group, Resource: resource, Verb: "create", Namespace: d.namespace,
				}},
			}, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			return review.Status.Allowed, nil
		},
		version: func() string {
			info, err := clientset.Discovery().ServerVersion()
			if err != nil {
				return ""
			}
			return info.GitVersion
		},
		nodes: listNodes(clientset),
	}
	return d
}

// declare returns the cached declaration, refreshing it when it is older than
// discoveryInterval. A refresh that cannot reach the API server keeps the last
// declaration; before any success there is none, and the controller does not
// filter this backend.
//
// Only the agent's first declaration waits for the API server. Later
// refreshes run in the background: they held the lock every heartbeat and
// Submit wait on while discovery calls that ignore their context stalled on
// a slow API server, and heartbeats stopped past the jobs' grace.
func (d *discovery) declare(ctx context.Context) (capability.Set, capability.Host) {
	d.mu.Lock()
	first := d.tried.IsZero()
	stale := time.Since(d.checked) >= discoveryInterval && time.Since(d.tried) >= discoveryRetry
	if first || (stale && !d.refreshing) {
		d.tried, d.refreshing, d.refreshed = time.Now(), true, make(chan struct{})
		done := d.refreshed
		d.mu.Unlock()
		if first {
			d.refresh(ctx, done)
		} else {
			go d.refresh(context.Background(), done)
		}
		d.mu.Lock()
	}
	defer d.mu.Unlock()
	return d.set, d.host
}

// refresh asks the API server again and keeps what it answers, then closes
// done.
func (d *discovery) refresh(ctx context.Context, done chan struct{}) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	kinds, ok := d.discover(ctx)
	var host capability.Host
	var nodes []capability.Node
	nodesErr := error(nil)
	if ok {
		host = capability.Host{OSID: "kubernetes", OSVersion: d.version()}
		nodes, nodesErr = d.nodes(ctx)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	defer close(done)
	d.refreshing = false
	if !ok {
		return
	}
	d.set = capability.Kubernetes(kinds...)
	// A failed read keeps the nodes declared last.
	host.Nodes = d.host.Nodes
	if nodesErr == nil {
		host.Nodes = nodes
	}
	d.host = host
	d.checked = time.Now()
}

// settle waits for a refresh in flight; tests read after it.
func (d *discovery) settle() {
	d.mu.Lock()
	done := d.refreshed
	d.mu.Unlock()
	if done != nil {
		<-done
	}
}

// discover reports the kinds whose resource the cluster serves and the agent
// may create. ok is false when the API server could not be asked at all. A
// group that failed otherwise than with a 404 keeps the kinds it declared
// last (c4-A6): a failed read is not an answer that the kind is gone. Before
// any declaration there is nothing to keep, and ok is false too: the kinds
// that answered alone would say the failed group's are not there.
func (d *discovery) discover(ctx context.Context) (kinds []string, ok bool) {
	d.mu.Lock()
	previous, declared := d.set, !d.checked.IsZero()
	d.mu.Unlock()
	servedBy := map[string][]metav1.APIResource{}
	failed := map[string]bool{}
	reached := false
	for _, k := range translators {
		if k.resource.Empty() {
			kinds = append(kinds, k.kind)
			continue
		}
		gv := k.resource.GroupVersion().String()
		resources, seen := servedBy[gv]
		if !seen {
			var err error
			resources, err = d.servedWithin(ctx, gv)
			// A group the cluster does not serve answers 404: the API
			// server was reached and said no.
			if err == nil || apierrors.IsNotFound(err) {
				reached = true
			} else {
				failed[gv] = true
			}
			servedBy[gv] = resources
		}
		if failed[gv] {
			if !declared {
				return nil, false
			}
			if _, had := previous.Find(k.kind); had {
				kinds = append(kinds, k.kind)
			}
			continue
		}
		if !serves(resources, k.resource.Resource) {
			continue
		}
		allowed, err := d.allowed(ctx, k.resource.Group, k.resource.Resource)
		if err != nil {
			return nil, false
		}
		if allowed {
			kinds = append(kinds, k.kind)
		}
	}
	return kinds, reached
}

// servedWithin is served, given up after discoveryGroupTimeout.
func (d *discovery) servedWithin(ctx context.Context, gv string) ([]metav1.APIResource, error) {
	ctx, cancel := context.WithTimeout(ctx, discoveryGroupTimeout)
	defer cancel()
	type answer struct {
		resources []metav1.APIResource
		err       error
	}
	got := make(chan answer, 1)
	go func() {
		resources, err := d.served(ctx, gv)
		got <- answer{resources, err}
	}()
	select {
	case a := <-got:
		return a.resources, a.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func serves(resources []metav1.APIResource, name string) bool {
	for _, r := range resources {
		if r.Name == name {
			return true
		}
	}
	return false
}

// Declare is what this cluster runs for Zenthros: the kinds whose API the
// cluster serves and that the agent may create.
func (e *Executor) Declare(ctx context.Context) (capability.Set, capability.Host) {
	set, host := e.discovery.declare(ctx)
	host.CachedImages = e.warmImages()
	host.SecretStores = capability.SecretStoresKubernetes()
	return set, host
}
