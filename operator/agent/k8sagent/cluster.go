// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/nodejoin"
	"github.com/zenthros/operator/pkg/owner"
)

// The agent checks its cluster as a new node will find it and reports it to
// the hub (NodeSupply.ReportCluster, the BackendAccess status.nodeSupply):
// how a node joins (a k3s agent on k3s, a kubelet with TLS bootstrapping on
// any other cluster) and whether one can now. The hub creates no machine
// for a cluster that cannot take it: the machine would never become a node.
//
// For a kubelet the cluster must
//   - authenticate bootstrap tokens (the API server's
//     --enable-bootstrap-token-auth), checked with a probe token;
//   - let the bootstrap group ask for a node certificate and approve it,
//     and let nodes renew theirs (the agent chart's bindings, or kubeadm's);
//   - run its CNI (and kube-proxy, when it has one) as DaemonSets that
//     tolerate the pool taint: otherwise the node never turns Ready;
//   - have a DNS Service for the kubelet's clusterDNS.

// ClusterCheckInterval is how often the agent checks its cluster.
const ClusterCheckInterval = 30 * time.Second

// tokenProbeInterval is how often the bootstrap token authenticator is
// probed: each probe writes a token, so less often than the other checks.
const tokenProbeInterval = 10 * time.Minute

// ClusterCheck is the cluster as a new node finds it.
type ClusterCheck struct {
	// JoinMethod is nodejoin.MethodK3s or nodejoin.MethodKubelet.
	JoinMethod    string
	ServerVersion string
	ClusterDNS    []string
	ClusterDomain string
	Ready         bool
	// Reason (an infrav1alpha1.NodeSupplyReason*) and Message say why a
	// cluster cannot take a node.
	Reason, Message string
}

// Report is the check as the hub takes it.
func (c ClusterCheck) Report() *agentpb.ClusterReport {
	return &agentpb.ClusterReport{
		JoinMethod: c.JoinMethod, ServerVersion: c.ServerVersion, ClusterDns: c.ClusterDNS,
		ClusterDomain: c.ClusterDomain, Ready: c.Ready, Reason: c.Reason, Message: c.Message,
	}
}

// ClusterChecker checks the cluster the agent runs in.
type ClusterChecker struct {
	Client client.Client
	// ServerVersion reads the API server's gitVersion.
	ServerVersion func(context.Context) (string, error)
	// CanI says whether user in groups may do what attributes say (a
	// SubjectAccessReview).
	CanI func(ctx context.Context, user string, groups []string, attributes authorizationv1.ResourceAttributes) (bool, error)
	// WhoAmI is who the API server takes a bearer token for (a
	// SelfSubjectReview made with it); unauthorized is an error.
	WhoAmI func(ctx context.Context, token string) (user string, groups []string, err error)
	// ClusterDNS and ClusterDomain, when set, are the chart's: used instead
	// of the kube-dns Service and the kubelet-config ConfigMap.
	ClusterDNS    []string
	ClusterDomain string
	Now           func() time.Time
	// ProbeRetry is the pause between attempts to authenticate the probe
	// token; a second when zero.
	ProbeRetry time.Duration

	mu          sync.Mutex
	tokensOKAt  time.Time
	last        *ClusterCheck
	lastChecked time.Time
}

func (c *ClusterChecker) now() time.Time {
	if c.Now == nil {
		return time.Now()
	}
	return c.Now()
}

// Last is the latest check within ClusterCheckInterval, or a new one.
func (c *ClusterChecker) Last(ctx context.Context) ClusterCheck {
	c.mu.Lock()
	if c.last != nil && c.now().Sub(c.lastChecked) < ClusterCheckInterval {
		last := *c.last
		c.mu.Unlock()
		return last
	}
	c.mu.Unlock()
	return c.Check(ctx)
}

// Check checks the cluster now.
func (c *ClusterChecker) Check(ctx context.Context) ClusterCheck {
	check := c.check(ctx)
	c.mu.Lock()
	c.last, c.lastChecked = &check, c.now()
	c.mu.Unlock()
	return check
}

func (c *ClusterChecker) check(ctx context.Context) ClusterCheck {
	version, err := c.ServerVersion(ctx)
	if err != nil {
		return ClusterCheck{JoinMethod: nodejoin.MethodKubelet, ServerVersion: "unknown",
			Reason: infrav1alpha1.NodeSupplyReasonCheckFailed, Message: "read the API server version: " + err.Error()}
	}
	check := ClusterCheck{JoinMethod: nodejoin.MethodKubelet, ServerVersion: version}
	if strings.Contains(version, "+k3s") {
		// A k3s agent brings kube-proxy and the CNI and learns the DNS from
		// its server; k3s authenticates the token it issues for it.
		check.JoinMethod, check.Ready = nodejoin.MethodK3s, true
		return check
	}
	for _, step := range []func(context.Context, *ClusterCheck) bool{c.dns, c.networkDaemonSets, c.bootstrapBindings, c.bootstrapTokens} {
		if !step(ctx, &check) {
			return check
		}
	}
	check.Ready = true
	return check
}

func failCheck(check *ClusterCheck, reason, format string, args ...any) bool {
	check.Reason, check.Message = reason, fmt.Sprintf(format, args...)
	return false
}

// dns finds the kubelet's clusterDNS (the kube-dns Service, as kubeadm, kind
// and k3s name it) and clusterDomain (kubeadm's kubelet-config, else
// cluster.local).
func (c *ClusterChecker) dns(ctx context.Context, check *ClusterCheck) bool {
	check.ClusterDNS, check.ClusterDomain = c.ClusterDNS, c.ClusterDomain
	if len(check.ClusterDNS) == 0 {
		service := &corev1.Service{}
		err := c.Client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "kube-dns"}, service)
		switch {
		case apierrors.IsNotFound(err):
			return failCheck(check, infrav1alpha1.NodeSupplyReasonNoClusterDNS,
				"no kube-system/kube-dns Service; set the agent chart's nodeSupply.clusterDNS")
		case err != nil:
			return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "read kube-system/kube-dns: %v", err)
		case service.Spec.ClusterIP == "" || service.Spec.ClusterIP == corev1.ClusterIPNone:
			return failCheck(check, infrav1alpha1.NodeSupplyReasonNoClusterDNS, "kube-system/kube-dns has no cluster IP")
		}
		check.ClusterDNS = []string{service.Spec.ClusterIP}
	}
	if check.ClusterDomain == "" {
		check.ClusterDomain = "cluster.local"
		config := &corev1.ConfigMap{}
		if err := c.Client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "kubelet-config"}, config); err == nil {
			var kubelet struct {
				ClusterDomain string `json:"clusterDomain"`
			}
			if yaml.Unmarshal([]byte(config.Data["kubelet"]), &kubelet) == nil && kubelet.ClusterDomain != "" {
				check.ClusterDomain = kubelet.ClusterDomain
			}
		}
	}
	return true
}

// networkDaemonSets checks that the DaemonSets that wire a node into the
// pod network (the CNI's: they mount /etc/cni or /opt/cni from the host;
// kube-proxy) run on a joined node: they tolerate the pool taint.
func (c *ClusterChecker) networkDaemonSets(ctx context.Context, check *ClusterCheck) bool {
	list := &appsv1.DaemonSetList{}
	if err := c.Client.List(ctx, list); err != nil {
		return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "list DaemonSets: %v", err)
	}
	poolTaint := corev1.Taint{Key: infrav1alpha1.LabelVMPoolName, Value: "any-pool", Effect: corev1.TaintEffectNoSchedule}
	cni := false
	var untolerated []string
	for i := range list.Items {
		ds := &list.Items[i]
		isCNI, isProxy := networkRole(ds)
		if !isCNI && !isProxy {
			continue
		}
		cni = cni || isCNI
		if !slices.ContainsFunc(ds.Spec.Template.Spec.Tolerations, func(t corev1.Toleration) bool { return tolerates(t, poolTaint) }) {
			untolerated = append(untolerated, ds.Namespace+"/"+ds.Name)
		}
	}
	if !cni {
		return failCheck(check, infrav1alpha1.NodeSupplyReasonNoNetworkDaemonSet,
			"no DaemonSet installs a CNI on new nodes (one that mounts /etc/cni or /opt/cni from the host)")
	}
	if len(untolerated) > 0 {
		slices.Sort(untolerated)
		return failCheck(check, infrav1alpha1.NodeSupplyReasonNetworkUntolerated,
			"%s do not tolerate %s:NoSchedule: a joined node would never turn Ready", strings.Join(untolerated, ", "), infrav1alpha1.LabelVMPoolName)
	}
	return true
}

// tolerates is Kubernetes' toleration match, without the comparison
// operators.
func tolerates(t corev1.Toleration, taint corev1.Taint) bool {
	if t.Effect != "" && t.Effect != taint.Effect {
		return false
	}
	if t.Key == "" {
		return t.Operator == corev1.TolerationOpExists
	}
	if t.Key != taint.Key {
		return false
	}
	switch t.Operator {
	case corev1.TolerationOpExists:
		return true
	case "", corev1.TolerationOpEqual:
		return t.Value == taint.Value
	}
	return false
}

// networkRole says whether a DaemonSet is a CNI's (host network, mounts
// the host's CNI directories) or kube-proxy.
func networkRole(ds *appsv1.DaemonSet) (cni, proxy bool) {
	spec := ds.Spec.Template.Spec
	if ds.Name == "kube-proxy" || ds.Spec.Template.Labels["k8s-app"] == "kube-proxy" {
		return false, true
	}
	if !spec.HostNetwork {
		return false, false
	}
	for _, volume := range spec.Volumes {
		if volume.HostPath != nil && (strings.HasPrefix(volume.HostPath.Path, "/etc/cni") || strings.HasPrefix(volume.HostPath.Path, "/opt/cni")) {
			return true, false
		}
	}
	return false, false
}

// bootstrapBindings checks that a bootstrapping kubelet may ask for its
// client certificate and get it approved, and that a node may renew it.
func (c *ClusterChecker) bootstrapBindings(ctx context.Context, check *ClusterCheck) bool {
	bootstrapper := []string{nodejoin.KubeletBootstrapGroup, "system:bootstrappers", "system:authenticated"}
	for _, ask := range []struct {
		user        string
		groups      []string
		subresource string
		what        string
	}{
		{"system:bootstrap:zprobe", bootstrapper, "", "ask for a node certificate (system:node-bootstrapper)"},
		{"system:bootstrap:zprobe", bootstrapper, "nodeclient", "get a node certificate approved (certificatesigningrequests:nodeclient)"},
		{"system:node:zprobe", []string{"system:nodes", "system:authenticated"}, "selfnodeclient", "renew their certificate (certificatesigningrequests:selfnodeclient)"},
	} {
		allowed, err := c.CanI(ctx, ask.user, ask.groups, authorizationv1.ResourceAttributes{
			Group: "certificates.k8s.io", Resource: "certificatesigningrequests", Subresource: ask.subresource, Verb: "create",
		})
		if err != nil {
			return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "review %s: %v", ask.what, err)
		}
		if !allowed {
			return failCheck(check, infrav1alpha1.NodeSupplyReasonBootstrapBindingsMissing,
				"new nodes may not %s: install the agent chart with nodeSupply.enabled, or bind %s", ask.what, ask.groups[0])
		}
	}
	return true
}

// bootstrapTokens checks that the API server authenticates a bootstrap
// token of the group this agent issues, with a probe token that lives a
// few seconds. A success holds for tokenProbeInterval.
func (c *ClusterChecker) bootstrapTokens(ctx context.Context, check *ClusterCheck) bool {
	c.mu.Lock()
	fresh := !c.tokensOKAt.IsZero() && c.now().Sub(c.tokensOKAt) < tokenProbeInterval
	c.mu.Unlock()
	if fresh {
		return true
	}
	id, secret, err := newBootstrapToken()
	if err != nil {
		return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "draw a probe token: %v", err)
	}
	token := bootstrapTokenSecret(id, secret, c.now().Add(10*time.Minute), nodejoin.KubeletBootstrapGroup, "Zenthros node supply probe")
	if err := c.Client.Create(ctx, token); err != nil {
		return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "create a probe bootstrap token: %v", err)
	}
	// A probe token left behind expires within minutes.
	defer func() {
		cleanup, cancel := fault.Cleanup(ctx)
		defer cancel()
		_ = owner.DeleteIncarnation(cleanup, c.Client, token)
	}()
	// The API server's token authenticator reads Secrets through a cache.
	var user string
	var groups []string
	for attempt := range 5 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "probe bootstrap token: %v", ctx.Err())
			case <-time.After(cmp.Or(c.ProbeRetry, time.Second)):
			}
		}
		if user, groups, err = c.WhoAmI(ctx, id+"."+secret); err == nil {
			break
		}
	}
	if err != nil {
		// Only its 401 says the API server takes no bootstrap token; one
		// that did not answer says nothing of them.
		if fault.Of(err) != fault.Unauthorized {
			return failCheck(check, infrav1alpha1.NodeSupplyReasonCheckFailed, "probe bootstrap token: %v", err)
		}
		return failCheck(check, infrav1alpha1.NodeSupplyReasonBootstrapTokensDisabled,
			"the API server does not authenticate bootstrap tokens (--enable-bootstrap-token-auth): %v", err)
	}
	if user != "system:bootstrap:"+id || !slices.Contains(groups, nodejoin.KubeletBootstrapGroup) {
		return failCheck(check, infrav1alpha1.NodeSupplyReasonBootstrapTokensDisabled,
			"a bootstrap token authenticates as %s %v, not system:bootstrap:%s in %s", user, groups, id, nodejoin.KubeletBootstrapGroup)
	}
	c.mu.Lock()
	c.tokensOKAt = c.now()
	c.mu.Unlock()
	return true
}

// bootstrapTokenSecret is a Kubernetes bootstrap token for authentication
// in group, working until expires.
func bootstrapTokenSecret(id, secret string, expires time.Time, group, description string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bootstrap-token-" + id, Namespace: metav1.NamespaceSystem,
			Labels: map[string]string{JoinTokenLabel: "true"},
		},
		Type: corev1.SecretTypeBootstrapToken,
		Data: map[string][]byte{
			"token-id": []byte(id), "token-secret": []byte(secret),
			"expiration":                     []byte(expires.UTC().Format(time.RFC3339)),
			"usage-bootstrap-authentication": []byte("true"),
			"usage-bootstrap-signing":        []byte("true"),
			"auth-extra-groups":              []byte(group),
			"description":                    []byte(description),
		},
	}
}
