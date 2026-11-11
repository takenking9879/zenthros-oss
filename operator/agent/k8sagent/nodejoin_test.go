// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/nodejoin"
)

var testClusterCA = []byte("-----BEGIN CERTIFICATE-----\nMIIBcluster\n-----END CERTIFICATE-----\n")

func joinIssuer() *JoinIssuer {
	return &JoinIssuer{ServerURL: "https://10.0.0.1:6443", CACert: testClusterCA}
}

// checkedCluster is a cluster at version the agent just checked: k3s, or a
// kubelet cluster with its DNS.
func checkedCluster(version string) *ClusterChecker {
	check := ClusterCheck{JoinMethod: nodejoin.MethodKubelet, ServerVersion: version, Ready: true,
		ClusterDNS: []string{"10.96.0.10"}, ClusterDomain: "cluster.local"}
	if strings.Contains(version, "+k3s") {
		check = ClusterCheck{JoinMethod: nodejoin.MethodK3s, ServerVersion: version, Ready: true}
	}
	return &ClusterChecker{Now: func() time.Time { return sweepNow }, last: &check, lastChecked: sweepNow}
}

func bootstrapTokens(t *testing.T, c client.Client) []corev1.Secret {
	t.Helper()
	list := &corev1.SecretList{}
	if err := c.List(context.Background(), list, client.InNamespace(metav1.NamespaceSystem)); err != nil {
		t.Fatal(err)
	}
	return list.Items
}

// A slot whose machine asks for a credential gets a bootstrap token for that
// one node, sealed to the machine's key with the cluster's API URL and CA:
// k3s' agent group on k3s, working for 15 minutes. Once the Node exists, the
// hub hears it and the token goes.
func TestSweepIssuesASealedTokenThenDeletesItOnceRegistered(t *testing.T) {
	key, err := nodejoin.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	nodeID := infrav1alpha1.NodeProviderID("z", "fresh", "uid-fresh")
	var issued []*agentpb.IssueJoinRequest
	var registered []string
	hub := fakeHub{calls: new([]string), issued: &issued, registered: &registered, slots: []*agentpb.NodeSlot{
		{ProviderId: nodeID, Pool: "z", JoinPublicKey: key.PublicKey().Bytes()},
	}}
	c := nodeClient(t, nil)
	r := &NodeReconciler{Client: c, Hub: hub, Join: joinIssuer(), Cluster: checkedCluster("v1.34.1+k3s1"), Now: func() time.Time { return sweepNow }}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	tokens := bootstrapTokens(t, c)
	if len(tokens) != 1 || len(issued) != 1 {
		t.Fatalf("%d tokens, %d issued, want one of each", len(tokens), len(issued))
	}
	token := tokens[0]
	data := func(k string) string { return string(token.Data[k]) }
	if token.Type != corev1.SecretTypeBootstrapToken || token.Name != "bootstrap-token-"+data("token-id") ||
		data("auth-extra-groups") != nodejoin.K3sBootstrapGroup || data("usage-bootstrap-authentication") != "true" ||
		data("expiration") != sweepNow.Add(DefaultJoinTokenTTL).Format(time.RFC3339) {
		t.Fatalf("bootstrap token %s %v", token.Name, token.Data)
	}
	credential, err := nodejoin.Open(key, nodeID, issued[0].GetSealed())
	if err != nil {
		t.Fatalf("the machine cannot open what was issued: %v", err)
	}
	if credential.TokenID != data("token-id") || credential.TokenSecret != data("token-secret") ||
		credential.Server != "https://10.0.0.1:6443" || string(credential.CACert) != string(testClusterCA) {
		t.Fatalf("credential %+v does not carry the token issued", credential)
	}
	if issued[0].GetServerVersion() != "v1.34.1+k3s1" || issued[0].GetJoinMethod() != nodejoin.MethodK3s ||
		string(issued[0].GetPublicKey()) != string(key.PublicKey().Bytes()) {
		t.Fatalf("IssueJoin %+v", issued[0])
	}

	// The machine joined: its Node is there, the hub has not heard yet.
	if err := c.Create(context.Background(), supplyNode("fresh", nodeID, corev1.ConditionTrue, time.Minute)); err != nil {
		t.Fatal(err)
	}
	r.Hub = fakeHub{calls: new([]string), issued: &issued, registered: &registered, slots: []*agentpb.NodeSlot{{ProviderId: nodeID, Pool: "z"}}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 1 || registered[0] != nodeID {
		t.Fatalf("registered %v, want %s", registered, nodeID)
	}
	if tokens := bootstrapTokens(t, c); len(tokens) != 0 {
		t.Fatalf("the registered node's token is still there: %v", tokens)
	}
	// Reported once: the hub says so from then on.
	r.Hub = fakeHub{calls: new([]string), registered: &registered, slots: []*agentpb.NodeSlot{{ProviderId: nodeID, Pool: "z", Registered: true}}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(registered) != 1 {
		t.Fatalf("registered %v, want one report", registered)
	}
}

// A token the hub did not take is held by nobody: it goes at once.
func TestSweepDeletesATokenTheHubRefused(t *testing.T) {
	key, _ := nodejoin.NewKey()
	c := nodeClient(t, nil)
	r := &NodeReconciler{Client: c, Join: joinIssuer(), Cluster: checkedCluster("v1.34.1+k3s1"), Hub: fakeHub{calls: new([]string), issueErr: errors.New("no longer asks with that key"),
		slots: []*agentpb.NodeSlot{{ProviderId: infrav1alpha1.NodeProviderID("z", "s", "uid-s"), Pool: "z", JoinPublicKey: key.PublicKey().Bytes()}}}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tokens := bootstrapTokens(t, c); len(tokens) != 0 {
		t.Fatalf("tokens %v left behind", tokens)
	}
}

// A slot that goes before its Node registered (its join failed) takes its
// token with it: nobody may join with it any more.
func TestSweepDeletesTheTokenOfASlotThatWentUnregistered(t *testing.T) {
	key, _ := nodejoin.NewKey()
	failed := infrav1alpha1.NodeProviderID("z", "failed", "uid-failed")
	waiting := infrav1alpha1.NodeProviderID("z", "waiting", "uid-waiting")
	c := nodeClient(t, nil)
	var issued []*agentpb.IssueJoinRequest
	r := &NodeReconciler{Client: c, Join: joinIssuer(), Cluster: checkedCluster("v1.34.1+k3s1"), Hub: fakeHub{calls: new([]string), issued: &issued, slots: []*agentpb.NodeSlot{
		{ProviderId: failed, Pool: "z", JoinPublicKey: key.PublicKey().Bytes()},
		{ProviderId: waiting, Pool: "z", JoinPublicKey: key.PublicKey().Bytes()},
	}}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tokens := bootstrapTokens(t, c); len(tokens) != 2 {
		t.Fatalf("%d tokens, want two", len(tokens))
	}
	// The failed slot is gone; the other one still waits for its machine.
	r.Hub = fakeHub{calls: new([]string), slots: []*agentpb.NodeSlot{{ProviderId: waiting, Pool: "z"}}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokens := bootstrapTokens(t, c)
	if len(tokens) != 1 || tokens[0].Annotations[AnnotationProviderID] != waiting {
		t.Fatalf("tokens %v, want only the waiting slot's", tokens)
	}
}

// The token the agent deletes is the one it issued: one the cluster's token
// cleaner removed and that was issued again under its name is someone
// else's, and stays.
func TestSweepDeletesOnlyTheTokenItIssued(t *testing.T) {
	key, _ := nodejoin.NewKey()
	gone := infrav1alpha1.NodeProviderID("z", "gone", "uid-gone")
	c := nodeClient(t, nil)
	r := &NodeReconciler{Client: c, Join: joinIssuer(), Cluster: checkedCluster("v1.34.1+k3s1"), Hub: fakeHub{calls: new([]string), issued: new([]*agentpb.IssueJoinRequest),
		slots: []*agentpb.NodeSlot{{ProviderId: gone, Pool: "z", JoinPublicKey: key.PublicKey().Bytes()}}}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokens := bootstrapTokens(t, c)
	if len(tokens) != 1 {
		t.Fatalf("%d tokens, want one", len(tokens))
	}
	ctx := context.Background()
	if err := c.Delete(ctx, &tokens[0]); err != nil {
		t.Fatal(err)
	}
	namesake := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: tokens[0].Name}}
	if err := c.Create(ctx, namesake); err != nil {
		t.Fatal(err)
	}

	r.Hub = fakeHub{calls: new([]string)}
	if _, err := r.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if left := bootstrapTokens(t, c); len(left) != 1 || left[0].UID != namesake.UID {
		t.Fatalf("tokens %v, want the namesake kept", left)
	}
}

// Not k3s: the token is in Zenthros' bootstrap group, for a kubelet's TLS
// bootstrap. Without a server URL the agent issues nothing.
func TestSweepTokenGroupFollowsTheCluster(t *testing.T) {
	key, _ := nodejoin.NewKey()
	slot := []*agentpb.NodeSlot{{ProviderId: infrav1alpha1.NodeProviderID("z", "s", "uid-s"), Pool: "z", JoinPublicKey: key.PublicKey().Bytes()}}
	c := nodeClient(t, nil)
	var issued []*agentpb.IssueJoinRequest
	r := &NodeReconciler{Client: c, Join: joinIssuer(), Cluster: checkedCluster("v1.34.1"), Hub: fakeHub{calls: new([]string), slots: slot, issued: &issued}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tokens := bootstrapTokens(t, c); len(tokens) != 1 || string(tokens[0].Data["auth-extra-groups"]) != nodejoin.KubeletBootstrapGroup {
		t.Fatalf("tokens %v, want one in %s", tokens, nodejoin.KubeletBootstrapGroup)
	}
	// The hub learns how the node joins and the DNS its kubelet gives pods.
	if len(issued) != 1 || issued[0].GetJoinMethod() != nodejoin.MethodKubelet || issued[0].GetClusterDomain() != "cluster.local" ||
		len(issued[0].GetClusterDns()) != 1 || issued[0].GetClusterDns()[0] != "10.96.0.10" {
		t.Fatalf("IssueJoin %+v", issued)
	}

	// A kubelet cluster whose DNS the agent could not find gets no
	// credential: the kubelet would give pods no DNS.
	noDNS := nodeClient(t, nil)
	cluster := checkedCluster("v1.34.1")
	cluster.last.ClusterDNS, cluster.last.Ready, cluster.last.Reason = nil, false, "NoClusterDNS"
	r = &NodeReconciler{Client: noDNS, Join: joinIssuer(), Cluster: cluster, Hub: fakeHub{calls: new([]string), slots: slot}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tokens := bootstrapTokens(t, noDNS); len(tokens) != 0 {
		t.Fatalf("issued %v without the cluster DNS", tokens)
	}

	bare := nodeClient(t, nil)
	r = &NodeReconciler{Client: bare, Hub: fakeHub{calls: new([]string), slots: slot}}
	if _, err := r.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tokens := bootstrapTokens(t, bare); len(tokens) != 0 {
		t.Fatalf("an agent without a server URL issued %v", tokens)
	}
}

// cancelingHub ends the sweep's context when the agent hands it a node
// credential, as an agent shutting down mid-sweep does.
type cancelingHub struct {
	fakeHub
	cancel context.CancelFunc
}

func (h cancelingHub) IssueJoin(ctx context.Context, _ *agentpb.IssueJoinRequest, _ ...grpc.CallOption) (*agentpb.IssueJoinResponse, error) {
	h.cancel()
	return nil, ctx.Err()
}

// A token nobody was handed goes, whatever ended the hand-over: deleted
// under the sweep's context, a hand-over the context ended left a valid
// bootstrap token in the cluster until its TTL (fault.Cleanup).
func TestATokenTheHubNeverGotGoesEvenWhenTheSweepEnds(t *testing.T) {
	key, _ := nodejoin.NewKey()
	slot := []*agentpb.NodeSlot{{ProviderId: infrav1alpha1.NodeProviderID("z", "s", "uid-s"), Pool: "z", JoinPublicKey: key.PublicKey().Bytes()}}
	c := nodeClient(t, &interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return testutil.DeleteWithUIDPrecondition(ctx, c, obj, opts...)
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &NodeReconciler{Client: c, Join: joinIssuer(), Cluster: checkedCluster("v1.34.1"),
		Hub: cancelingHub{fakeHub: fakeHub{calls: new([]string), slots: slot}, cancel: cancel}}
	_, _ = r.Sweep(ctx)
	if tokens := bootstrapTokens(t, c); len(tokens) != 0 {
		t.Fatalf("%d tokens left after a hand-over the sweep's end cut", len(tokens))
	}
}
