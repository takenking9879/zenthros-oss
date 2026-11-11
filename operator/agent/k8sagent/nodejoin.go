// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/nodejoin"
	"github.com/zenthros/operator/pkg/owner"
)

// A joined pool's machine asks the hub for a node credential with a key of
// its own; the hub lists the request among this cluster's slots
// (NodeSlot.join_public_key). The agent issues a Kubernetes bootstrap token
// for that one node, seals it with the cluster's API URL and CA to the
// machine's key, and hands it back (IssueJoin). The hub never holds a token
// it could use; the token works for joinTokenTTL, and the agent deletes it
// once the node registered.

// DefaultJoinTokenTTL is how long a node's bootstrap token works.
const DefaultJoinTokenTTL = 15 * time.Minute

// JoinTokenLabel marks the bootstrap tokens this agent issues, and
// AnnotationProviderID names on each the provider ID of the node it joins.
const (
	JoinTokenLabel       = "infrastructure.zenthros.io/node-join"
	AnnotationProviderID = "infrastructure.zenthros.io/provider-id"
)

// JoinIssuer issues node credentials for this cluster.
type JoinIssuer struct {
	// ServerURL is the cluster's API as its nodes reach it (https).
	ServerURL string
	// CACert is the cluster CA bundle the agent itself trusts.
	CACert []byte
	// TTL is how long a token works; DefaultJoinTokenTTL when zero.
	TTL time.Duration
}

func (j *JoinIssuer) ttl() time.Duration {
	if j.TTL > 0 {
		return j.TTL
	}
	return DefaultJoinTokenTTL
}

// issueJoin issues the slot's node credential and hands it to the hub.
func (r *NodeReconciler) issueJoin(ctx context.Context, slot *agentpb.NodeSlot) error {
	if r.Join == nil || r.Cluster == nil {
		return fmt.Errorf("slot %s asks for a node credential, but this agent has no nodeSupply.serverURL", slot.GetProviderId())
	}
	// How the node joins and its DNS come from the cluster check. A check
	// that failed for another reason does not hold back a machine that
	// already exists: the hub stops creating them.
	check := r.Cluster.Last(ctx)
	if check.ServerVersion == "" || check.ServerVersion == "unknown" {
		return fmt.Errorf("the cluster's version is unknown: %s", check.Message)
	}
	group := nodejoin.KubeletBootstrapGroup
	switch {
	case check.JoinMethod == nodejoin.MethodK3s:
		group = nodejoin.K3sBootstrapGroup
	case len(check.ClusterDNS) == 0:
		return fmt.Errorf("a kubelet needs the cluster DNS: %s", check.Message)
	}
	id, secret, err := newBootstrapToken()
	if err != nil {
		return err
	}
	expires := r.now().Add(r.Join.ttl()).UTC()
	token := bootstrapTokenSecret(id, secret, expires, group, "Zenthros node "+slot.GetProviderId())
	token.Annotations = map[string]string{AnnotationProviderID: slot.GetProviderId()}
	if err := r.Client.Create(ctx, token); err != nil {
		return fmt.Errorf("create the bootstrap token: %w", err)
	}
	sealed, err := nodejoin.Seal(slot.GetJoinPublicKey(), slot.GetProviderId(), nodejoin.Credential{
		Server: r.Join.ServerURL, TokenID: id, TokenSecret: secret, CACert: r.Join.CACert, ExpiresAt: expires,
	})
	if err == nil {
		_, err = r.Hub.IssueJoin(ctx, &agentpb.IssueJoinRequest{
			ProviderId: slot.GetProviderId(), PublicKey: slot.GetJoinPublicKey(),
			Sealed: sealed, ServerVersion: check.ServerVersion, JoinMethod: check.JoinMethod,
			ClusterDns: check.ClusterDNS, ClusterDomain: check.ClusterDomain,
		})
	}
	if err != nil {
		// Nobody holds this token: it goes now, the next sweep issues another.
		r.deleteJoinToken(ctx, token)
		return fmt.Errorf("hand the node credential to the hub: %w", err)
	}
	if r.joinTokens == nil {
		r.joinTokens = map[string][]*corev1.Secret{}
	}
	r.joinTokens[slot.GetProviderId()] = append(r.joinTokens[slot.GetProviderId()], token)
	r.Logger.Info("issued a node credential", "providerID", slot.GetProviderId(), "expires", expires,
		"serverVersion", check.ServerVersion, "joinMethod", check.JoinMethod)
	return nil
}

// registered tells the hub the slot's Node exists and deletes the tokens
// issued for it: the node joined, nothing else may join with them.
func (r *NodeReconciler) registered(ctx context.Context, providerID string) error {
	if _, err := r.Hub.ReportRegistered(ctx, &agentpb.ReportRegisteredRequest{ProviderId: providerID}); err != nil {
		return err
	}
	for _, token := range r.joinTokens[providerID] {
		r.deleteJoinToken(ctx, token)
	}
	delete(r.joinTokens, providerID)
	return nil
}

// deleteJoinToken deletes a bootstrap token this agent issued, and not a
// namesake created since, as a compensation (fault.Cleanup): a sweep that
// ended still removes a token nobody holds. A failure leaves it to expire;
// the cluster's token cleaner removes it then.
func (r *NodeReconciler) deleteJoinToken(ctx context.Context, token *corev1.Secret) {
	ctx, cancel := fault.Cleanup(ctx)
	defer cancel()
	if err := owner.DeleteIncarnation(ctx, r.Client, token); err != nil {
		r.Logger.Error(err, "delete a node's bootstrap token; it expires on its own", "secret", token.Name)
	}
}

// newBootstrapToken is a token id ([a-z0-9]{6}) and secret ([a-z0-9]{16}).
func newBootstrapToken() (string, string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	draw := func(n int) (string, error) {
		out := make([]byte, n)
		for i := range out {
			k, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
			if err != nil {
				return "", err
			}
			out[i] = alphabet[k.Int64()]
		}
		return string(out), nil
	}
	id, err := draw(6)
	if err != nil {
		return "", "", err
	}
	secret, err := draw(16)
	return id, secret, err
}

// JoinIssuer issues node credentials for nodes that reach this cluster's API
// at serverURL, trusting the CA this agent itself trusts.
func (e *Executor) JoinIssuer(serverURL string) (*JoinIssuer, error) {
	if !strings.HasPrefix(serverURL, "https://") {
		return nil, fmt.Errorf("nodeSupply.serverURL %q is not an https URL", serverURL)
	}
	ca := e.restCfg.CAData
	if len(ca) == 0 && e.restCfg.CAFile != "" {
		var err error
		if ca, err = os.ReadFile(e.restCfg.CAFile); err != nil {
			return nil, fmt.Errorf("read the cluster CA: %w", err)
		}
	}
	if len(ca) == 0 {
		return nil, fmt.Errorf("the agent's cluster config has no CA to hand to nodes")
	}
	return &JoinIssuer{ServerURL: serverURL, CACert: ca}, nil
}

// ClusterChecker checks this cluster as new nodes find it. clusterDNS and
// clusterDomain, when set, are the chart's.
func (e *Executor) ClusterChecker(clusterDNS []string, clusterDomain string) *ClusterChecker {
	return &ClusterChecker{
		Client: e.client, ClusterDNS: clusterDNS, ClusterDomain: clusterDomain,
		ServerVersion: func(context.Context) (string, error) {
			info, err := e.clientset.Discovery().ServerVersion()
			if err != nil {
				return "", err
			}
			return info.GitVersion, nil
		},
		CanI: func(ctx context.Context, user string, groups []string, attributes authorizationv1.ResourceAttributes) (bool, error) {
			review, err := e.clientset.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
				Spec: authorizationv1.SubjectAccessReviewSpec{User: user, Groups: groups, ResourceAttributes: &attributes},
			}, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			return review.Status.Allowed, nil
		},
		WhoAmI: func(ctx context.Context, token string) (string, []string, error) {
			config := rest.AnonymousClientConfig(e.restCfg)
			config.BearerToken = token
			clientset, err := kubernetes.NewForConfig(config)
			if err != nil {
				return "", nil, err
			}
			review, err := clientset.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
			if err != nil {
				return "", nil, err
			}
			return review.Status.UserInfo.Username, review.Status.UserInfo.Groups, nil
		},
	}
}
