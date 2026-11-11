// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	externalgrpcpb "github.com/zenthros/operator/internal/grpc/gen/externalgrpc"
	"github.com/zenthros/operator/pkg/tlsconfig"
)

// CAProxy is Cluster Autoscaler's externalgrpc cloud provider inside its
// cluster (docs/design/K8S_NODE_SUPPLY.md). Every call goes to the hub's
// gateway on the agent's own connection: the gateway sees the agent's
// identity, Cluster Autoscaler never holds the agent's certificate, and a
// renewed agent certificate is used on the next handshake with no restart.
// Errors and their status codes pass through untouched: CA's backoff reads
// them.
type CAProxy struct {
	externalgrpcpb.UnimplementedCloudProviderServer
	Hub externalgrpcpb.CloudProviderClient
}

func (p CAProxy) NodeGroups(ctx context.Context, req *externalgrpcpb.NodeGroupsRequest) (*externalgrpcpb.NodeGroupsResponse, error) {
	return p.Hub.NodeGroups(ctx, req)
}

func (p CAProxy) NodeGroupForNode(ctx context.Context, req *externalgrpcpb.NodeGroupForNodeRequest) (*externalgrpcpb.NodeGroupForNodeResponse, error) {
	return p.Hub.NodeGroupForNode(ctx, req)
}

func (p CAProxy) PricingNodePrice(ctx context.Context, req *externalgrpcpb.PricingNodePriceRequest) (*externalgrpcpb.PricingNodePriceResponse, error) {
	return p.Hub.PricingNodePrice(ctx, req)
}

func (p CAProxy) PricingPodPrice(ctx context.Context, req *externalgrpcpb.PricingPodPriceRequest) (*externalgrpcpb.PricingPodPriceResponse, error) {
	return p.Hub.PricingPodPrice(ctx, req)
}

func (p CAProxy) GPULabel(ctx context.Context, req *externalgrpcpb.GPULabelRequest) (*externalgrpcpb.GPULabelResponse, error) {
	return p.Hub.GPULabel(ctx, req)
}

func (p CAProxy) GetAvailableGPUTypes(ctx context.Context, req *externalgrpcpb.GetAvailableGPUTypesRequest) (*externalgrpcpb.GetAvailableGPUTypesResponse, error) {
	return p.Hub.GetAvailableGPUTypes(ctx, req)
}

func (p CAProxy) Cleanup(ctx context.Context, req *externalgrpcpb.CleanupRequest) (*externalgrpcpb.CleanupResponse, error) {
	return p.Hub.Cleanup(ctx, req)
}

func (p CAProxy) Refresh(ctx context.Context, req *externalgrpcpb.RefreshRequest) (*externalgrpcpb.RefreshResponse, error) {
	return p.Hub.Refresh(ctx, req)
}

func (p CAProxy) NodeGroupTargetSize(ctx context.Context, req *externalgrpcpb.NodeGroupTargetSizeRequest) (*externalgrpcpb.NodeGroupTargetSizeResponse, error) {
	return p.Hub.NodeGroupTargetSize(ctx, req)
}

func (p CAProxy) NodeGroupIncreaseSize(ctx context.Context, req *externalgrpcpb.NodeGroupIncreaseSizeRequest) (*externalgrpcpb.NodeGroupIncreaseSizeResponse, error) {
	return p.Hub.NodeGroupIncreaseSize(ctx, req)
}

func (p CAProxy) NodeGroupDeleteNodes(ctx context.Context, req *externalgrpcpb.NodeGroupDeleteNodesRequest) (*externalgrpcpb.NodeGroupDeleteNodesResponse, error) {
	return p.Hub.NodeGroupDeleteNodes(ctx, req)
}

func (p CAProxy) NodeGroupDecreaseTargetSize(ctx context.Context, req *externalgrpcpb.NodeGroupDecreaseTargetSizeRequest) (*externalgrpcpb.NodeGroupDecreaseTargetSizeResponse, error) {
	return p.Hub.NodeGroupDecreaseTargetSize(ctx, req)
}

func (p CAProxy) NodeGroupNodes(ctx context.Context, req *externalgrpcpb.NodeGroupNodesRequest) (*externalgrpcpb.NodeGroupNodesResponse, error) {
	return p.Hub.NodeGroupNodes(ctx, req)
}

func (p CAProxy) NodeGroupTemplateNodeInfo(ctx context.Context, req *externalgrpcpb.NodeGroupTemplateNodeInfoRequest) (*externalgrpcpb.NodeGroupTemplateNodeInfoResponse, error) {
	return p.Hub.NodeGroupTemplateNodeInfo(ctx, req)
}

func (p CAProxy) NodeGroupGetOptions(ctx context.Context, req *externalgrpcpb.NodeGroupAutoscalingOptionsRequest) (*externalgrpcpb.NodeGroupAutoscalingOptionsResponse, error) {
	return p.Hub.NodeGroupGetOptions(ctx, req)
}

// CAProxyTLS is the proxy's own trust, local to the cluster (the agent
// chart makes it): a server certificate for its Service name and the CA
// that signed Cluster Autoscaler's client certificate. A caller without
// such a certificate is refused at the handshake.
func CAProxyTLS(cert, key, clientCA string) (*tls.Config, error) {
	cfg, err := tlsconfig.ServerConfig{CertFile: cert, KeyFile: key, CAFile: clientCA}.Config()
	if err != nil {
		return nil, fmt.Errorf("cluster autoscaler proxy: %w", err)
	}
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return cfg, nil
}

// ServeCAProxy serves the proxy on listen until ctx ends.
func ServeCAProxy(ctx context.Context, listen string, tlsCfg *tls.Config, hub externalgrpcpb.CloudProviderClient, log logr.Logger) error {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("cluster autoscaler proxy: %w", err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	externalgrpcpb.RegisterCloudProviderServer(server, CAProxy{Hub: hub})
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	log.Info("serving Cluster Autoscaler's cloud provider", "listen", lis.Addr().String())
	if err := server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("cluster autoscaler proxy: %w", err)
	}
	return nil
}
