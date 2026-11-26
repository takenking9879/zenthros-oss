// SPDX-License-Identifier: Apache-2.0

package cloudmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// EKSSTSIdentityKind tags the aws-eks evidence document.
const EKSSTSIdentityKind = "eks-sts"

func init() { Register(EKS{}) }

// EKS is the evidence of an enrollment Job on EKS: a presigned STS
// GetCallerIdentity URL made with the Job's Pod Identity credentials. The
// controller fetches the URL itself and never trusts a client-supplied
// account or role ARN. It has no host facts or notices: the node's own
// provider (aws) answers those.
type EKS struct{}

func (EKS) Name() string { return "aws-eks" }

func (EKS) IdentityEvidence(ctx context.Context) ([]byte, []byte, error) {
	clusterName := os.Getenv("ZENTHROS_EKS_CLUSTER_NAME")
	if clusterName == "" {
		return nil, nil, fmt.Errorf("ZENTHROS_EKS_CLUSTER_NAME is required for aws-eks identity")
	}
	sdkConfig, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load AWS config for eks identity: %w", err)
	}
	signed, err := sts.NewPresignClient(sts.NewFromConfig(sdkConfig)).PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, nil, fmt.Errorf("presign STS GetCallerIdentity: %w", err)
	}
	document, err := json.Marshal(map[string]string{
		"kind":         EKSSTSIdentityKind,
		"presignedUrl": signed.URL,
		"clusterName":  clusterName,
	})
	return document, nil, err
}
