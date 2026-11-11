// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/nodejoin"
)

func networkDaemonSet(namespace, name string, cni bool, tolerations ...corev1.Toleration) *appsv1.DaemonSet {
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	ds.Spec.Template.Spec.HostNetwork = true
	ds.Spec.Template.Spec.Tolerations = tolerations
	if cni {
		ds.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "cni-cfg", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/etc/cni/net.d"}}}}
	} else {
		ds.Spec.Template.Labels = map[string]string{"k8s-app": "kube-proxy"}
	}
	return ds
}

var tolerateAll = corev1.Toleration{Operator: corev1.TolerationOpExists}

// kubeadmCluster is what kubeadm and kind leave: the kube-dns Service, the
// kubelet-config ConfigMap, a CNI and kube-proxy that tolerate any taint.
func kubeadmCluster() []client.Object {
	return []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-dns"}, Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.10"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kubelet-config"},
			Data: map[string]string{"kubelet": "apiVersion: kubelet.config.k8s.io/v1beta1\nclusterDomain: corp.local\n"}},
		networkDaemonSet("kube-system", "kindnet", true, tolerateAll),
		networkDaemonSet("kube-system", "kube-proxy", false, tolerateAll),
		// Not networking: its tolerations do not matter.
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "monitoring", Name: "node-exporter"}},
	}
}

type checkerFixture struct {
	checker  *ClusterChecker
	client   client.Client
	whoAmI   int
	reviews  []string
	tokenIDs []string
}

func newCheckerFixture(t *testing.T, version string, objects ...client.Object) *checkerFixture {
	t.Helper()
	f := &checkerFixture{client: nodeClient(t, nil, objects...)}
	f.checker = &ClusterChecker{
		Client:        f.client,
		Now:           func() time.Time { return sweepNow },
		ProbeRetry:    time.Millisecond,
		ServerVersion: func(context.Context) (string, error) { return version, nil },
		CanI: func(_ context.Context, user string, _ []string, attributes authorizationv1.ResourceAttributes) (bool, error) {
			f.reviews = append(f.reviews, user+" "+attributes.Subresource)
			return true, nil
		},
		WhoAmI: func(ctx context.Context, token string) (string, []string, error) {
			f.whoAmI++
			id := strings.SplitN(token, ".", 2)[0]
			// The token exists while it is probed.
			secret := &corev1.Secret{}
			if err := f.client.Get(ctx, client.ObjectKey{Namespace: "kube-system", Name: "bootstrap-token-" + id}, secret); err != nil {
				return "", nil, err
			}
			f.tokenIDs = append(f.tokenIDs, id)
			return "system:bootstrap:" + id, []string{string(secret.Data["auth-extra-groups"]), "system:bootstrappers", "system:authenticated"}, nil
		},
	}
	return f
}

// A kubeadm-style cluster takes a kubelet: the agent finds its DNS and
// domain, its CNI and kube-proxy tolerate the pool taint, the bootstrap
// group may get a node certificate approved and nodes may renew theirs, and
// a probe token in Zenthros' group authenticates. The probe token is gone
// afterwards; the next check within ten minutes does not write another.
func TestClusterCheckKubeadmTakesAKubelet(t *testing.T) {
	f := newCheckerFixture(t, "v1.34.0", kubeadmCluster()...)
	check := f.checker.Check(context.Background())
	if !check.Ready || check.JoinMethod != nodejoin.MethodKubelet || check.ServerVersion != "v1.34.0" ||
		len(check.ClusterDNS) != 1 || check.ClusterDNS[0] != "10.96.0.10" || check.ClusterDomain != "corp.local" {
		t.Fatalf("check %+v", check)
	}
	if want := []string{"system:bootstrap:zprobe ", "system:bootstrap:zprobe nodeclient", "system:node:zprobe selfnodeclient"}; strings.Join(f.reviews, ",") != strings.Join(want, ",") {
		t.Errorf("reviews %v, want %v", f.reviews, want)
	}
	if f.whoAmI != 1 {
		t.Fatalf("probe token reviewed %d times", f.whoAmI)
	}
	if tokens := bootstrapTokens(t, f.client); len(tokens) != 0 {
		t.Fatalf("the probe token stayed: %v", tokens)
	}
	f.checker.Now = func() time.Time { return sweepNow.Add(9 * time.Minute) }
	if check := f.checker.Check(context.Background()); !check.Ready || f.whoAmI != 1 {
		t.Fatalf("second check %+v probed again (%d)", check, f.whoAmI)
	}
	f.checker.Now = func() time.Time { return sweepNow.Add(11 * time.Minute) }
	if check := f.checker.Check(context.Background()); !check.Ready || f.whoAmI != 2 {
		t.Fatalf("after ten minutes %+v probed %d times, want 2", check, f.whoAmI)
	}
}

// A k3s agent brings its own network and DNS: on k3s the agent checks
// nothing else and writes no probe token.
func TestClusterCheckK3sTakesItsAgent(t *testing.T) {
	f := newCheckerFixture(t, "v1.34.1+k3s1")
	f.checker.CanI = nil
	f.checker.WhoAmI = nil
	check := f.checker.Check(context.Background())
	if !check.Ready || check.JoinMethod != nodejoin.MethodK3s || len(check.ClusterDNS) != 0 {
		t.Fatalf("check %+v", check)
	}
}

// Every way a cluster cannot take a kubelet, and what it reports.
func TestClusterCheckSaysWhyNoNodeCanJoin(t *testing.T) {
	without := func(name string) []client.Object {
		var out []client.Object
		for _, o := range kubeadmCluster() {
			if o.GetName() != name {
				out = append(out, o)
			}
		}
		return out
	}
	for name, c := range map[string]struct {
		objects []client.Object
		setup   func(*ClusterChecker)
		reason  string
		message string
	}{
		"no kube-dns": {objects: without("kube-dns"), reason: infrav1alpha1.NodeSupplyReasonNoClusterDNS},
		"no CNI":      {objects: without("kindnet"), reason: infrav1alpha1.NodeSupplyReasonNoNetworkDaemonSet},
		"CNI does not tolerate the pool": {
			objects: append(without("kindnet"), networkDaemonSet("kube-flannel", "kube-flannel-ds", true,
				corev1.Toleration{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists})),
			reason: infrav1alpha1.NodeSupplyReasonNetworkUntolerated, message: "kube-flannel/kube-flannel-ds"},
		"kube-proxy tolerates only NoExecute": {
			objects: append(without("kube-proxy"), networkDaemonSet("kube-system", "kube-proxy", false,
				corev1.Toleration{Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute})),
			reason: infrav1alpha1.NodeSupplyReasonNetworkUntolerated, message: "kube-system/kube-proxy"},
		"no auto-approval": {objects: kubeadmCluster(), setup: func(c *ClusterChecker) {
			c.CanI = func(_ context.Context, _ string, _ []string, a authorizationv1.ResourceAttributes) (bool, error) {
				return a.Subresource != "nodeclient", nil
			}
		}, reason: infrav1alpha1.NodeSupplyReasonBootstrapBindingsMissing, message: "nodeclient"},
		"bootstrap tokens disabled": {objects: kubeadmCluster(), setup: func(c *ClusterChecker) {
			c.WhoAmI = func(context.Context, string) (string, []string, error) {
				return "", nil, apierrors.NewUnauthorized("Unauthorized")
			}
		}, reason: infrav1alpha1.NodeSupplyReasonBootstrapTokensDisabled},
		// Only the API server's 401 says it does not take bootstrap tokens;
		// one that did not answer says nothing of them (R4 ronda 4, P2).
		"the probe got no answer": {objects: kubeadmCluster(), setup: func(c *ClusterChecker) {
			c.WhoAmI = func(context.Context, string) (string, []string, error) {
				return "", nil, apierrors.NewServiceUnavailable("etcd is down")
			}
		}, reason: infrav1alpha1.NodeSupplyReasonCheckFailed},
		"token not in the group": {objects: kubeadmCluster(), setup: func(c *ClusterChecker) {
			c.WhoAmI = func(_ context.Context, token string) (string, []string, error) {
				return "system:bootstrap:" + strings.SplitN(token, ".", 2)[0], []string{"system:bootstrappers"}, nil
			}
		}, reason: infrav1alpha1.NodeSupplyReasonBootstrapTokensDisabled},
		"version unreadable": {objects: kubeadmCluster(), setup: func(c *ClusterChecker) {
			c.ServerVersion = func(context.Context) (string, error) { return "", errors.New("connection refused") }
		}, reason: infrav1alpha1.NodeSupplyReasonCheckFailed},
	} {
		t.Run(name, func(t *testing.T) {
			f := newCheckerFixture(t, "v1.34.0", c.objects...)
			if c.setup != nil {
				c.setup(f.checker)
			}
			check := f.checker.Check(context.Background())
			if check.Ready || check.Reason != c.reason || !strings.Contains(check.Message, c.message) {
				t.Fatalf("check %+v, want %s (%q)", check, c.reason, c.message)
			}
			if tokens := bootstrapTokens(t, f.client); len(tokens) != 0 {
				t.Fatalf("a probe token stayed: %v", tokens)
			}
		})
	}
	// The chart's DNS replaces the lookup.
	f := newCheckerFixture(t, "v1.34.0", without("kube-dns")...)
	f.checker.ClusterDNS, f.checker.ClusterDomain = []string{"10.43.0.10"}, "lab.local"
	if check := f.checker.Check(context.Background()); !check.Ready || check.ClusterDNS[0] != "10.43.0.10" || check.ClusterDomain != "lab.local" {
		t.Fatalf("with the chart's DNS: %+v", check)
	}
}

// The agent tells the hub what it found, ready or not.
func TestReportClusterSendsTheCheck(t *testing.T) {
	var reports []*agentpb.ClusterReport
	f := newCheckerFixture(t, "v1.34.0", kubeadmCluster()[1:]...)
	r := &NodeReconciler{Client: f.client, Cluster: f.checker, Hub: fakeHub{calls: new([]string), clusters: &reports}}
	if err := r.ReportCluster(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].GetReady() || reports[0].GetReason() != infrav1alpha1.NodeSupplyReasonNoClusterDNS ||
		reports[0].GetJoinMethod() != nodejoin.MethodKubelet || reports[0].GetServerVersion() != "v1.34.0" {
		t.Fatalf("reports %+v", reports)
	}
}
