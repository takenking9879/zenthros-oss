// SPDX-License-Identifier: Apache-2.0

package cloudmeta

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

const (
	awsIMDSBaseURL = "http://169.254.169.254/latest"
	// IMDSEndpointEnv is the AWS SDKs' own override of the metadata service
	// address (scheme://host[:port], no path). Only the Spot notice follows
	// it: the identity document that enrolls the machine, and the facts
	// placement trusts, come from the link-local address, which an
	// environment variable cannot point elsewhere.
	IMDSEndpointEnv = "AWS_EC2_METADATA_SERVICE_ENDPOINT"
	// AWSSpotActionPath is where IMDS announces a Spot interruption.
	AWSSpotActionPath = "/meta-data/spot/instance-action"
	// AWSRebalancePath is where IMDS posts an EC2 rebalance recommendation:
	// the Spot instance is at elevated risk of interruption.
	AWSRebalancePath = "/meta-data/events/recommendations/rebalance"
)

func init() { Register(NewAWS()) }

// AWS reads EC2's instance metadata service, IMDSv2 only.
type AWS struct {
	// Base is the IMDS root including /latest.
	Base string
	// NoticeBase is where the Spot notice is read; Base when empty.
	NoticeBase string
	Client     *http.Client
}

// NewAWS reads IMDS at the link-local address, and the Spot notice at
// AWS_EC2_METADATA_SERVICE_ENDPOINT when it is set.
func NewAWS() *AWS {
	aws := &AWS{Base: awsIMDSBaseURL}
	if endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(IMDSEndpointEnv)), "/"); endpoint != "" {
		aws.NoticeBase = endpoint + "/latest"
	}
	return aws
}

func (a *AWS) noticeBase() string {
	if a.NoticeBase != "" {
		return a.NoticeBase
	}
	return a.Base
}

func (*AWS) Name() string { return "aws" }

func (a *AWS) meta() metadataClient { return metadataClient{client: a.Client} }

func (a *AWS) token(ctx context.Context, base string, ttl time.Duration) (string, error) {
	body, err := a.meta().fetch(ctx, http.MethodPut, base+"/api/token",
		map[string]string{"X-aws-ec2-metadata-token-ttl-seconds": fmt.Sprint(int(ttl.Seconds()))})
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func (a *AWS) get(ctx context.Context, token, path string) (string, error) {
	body, err := a.meta().fetch(ctx, http.MethodGet, a.Base+path, map[string]string{"X-aws-ec2-metadata-token": token})
	return strings.TrimSpace(string(body)), err
}

// Facts reads the zone, region and VPC.
func (a *AWS) Facts(ctx context.Context) (Facts, bool) {
	ctx, cancel := context.WithTimeout(ctx, factsTimeout)
	defer cancel()
	token, err := a.token(ctx, a.Base, time.Minute)
	if err != nil || token == "" {
		return Facts{}, false
	}
	facts := Facts{Provider: a.Name()}
	facts.Zone, _ = a.get(ctx, token, "/meta-data/placement/availability-zone")
	facts.Region, _ = a.get(ctx, token, "/meta-data/placement/region")
	if mac, _ := a.get(ctx, token, "/meta-data/mac"); mac != "" {
		facts.NetworkID, _ = a.get(ctx, token, "/meta-data/network/interfaces/macs/"+mac+"/vpc-id")
	}
	// A 404 (no public address) leaves it empty.
	if ip, err := a.get(ctx, token, "/meta-data/public-ipv4"); err == nil {
		facts.PublicIPv4 = ip
	}
	return facts, true
}

// Notice polls the Spot instance-action document.
func (a *AWS) Notice(ctx context.Context) Notice {
	ctx, cancel := context.WithTimeout(ctx, noticeTimeout)
	defer cancel()
	base := a.noticeBase()
	token, err := a.token(ctx, base, time.Minute)
	if err != nil {
		return Notice{Source: traits.NoticeAWSIMDS}
	}
	status, body, err := a.meta().do(ctx, http.MethodGet, base+AWSSpotActionPath, map[string]string{"X-aws-ec2-metadata-token": token})
	if err != nil {
		return Notice{Source: traits.NoticeAWSIMDS}
	}
	return ParseAWSInstanceAction(status, body)
}

// Rebalance polls the rebalance recommendation document, at the same
// address as the Spot notice.
func (a *AWS) Rebalance(ctx context.Context) (time.Time, bool) {
	ctx, cancel := context.WithTimeout(ctx, noticeTimeout)
	defer cancel()
	base := a.noticeBase()
	token, err := a.token(ctx, base, time.Minute)
	if err != nil {
		return time.Time{}, false
	}
	status, body, err := a.meta().do(ctx, http.MethodGet, base+AWSRebalancePath, map[string]string{"X-aws-ec2-metadata-token": token})
	if err != nil {
		return time.Time{}, false
	}
	return ParseAWSRebalance(status, body)
}

// ParseAWSRebalance parses an IMDS events/recommendations/rebalance body,
// {"noticeTime": "<RFC 3339>"}. HTTP 404 (none) and any other non-200 are
// no recommendation.
func ParseAWSRebalance(status int, body []byte) (time.Time, bool) {
	if status != http.StatusOK {
		return time.Time{}, false
	}
	var payload struct {
		NoticeTime string `json:"noticeTime"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339, strings.TrimSpace(payload.NoticeTime))
	if err != nil {
		return time.Time{}, false
	}
	return at, true
}

// ParseAWSInstanceAction parses an IMDS spot/instance-action body. HTTP 404
// (no interruption) and any non-200 are not a notice.
func ParseAWSInstanceAction(status int, body []byte) Notice {
	if status != http.StatusOK {
		return Notice{Source: traits.NoticeAWSIMDS}
	}
	var payload struct {
		Action string `json:"action"`
		Time   string `json:"time"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Notice{Source: traits.NoticeAWSIMDS}
	}
	action := strings.TrimSpace(payload.Action)
	if action == "" {
		return Notice{Source: traits.NoticeAWSIMDS}
	}
	notice := Notice{Preempted: true, Source: traits.NoticeAWSIMDS, Action: action}
	if at, err := time.Parse(time.RFC3339, strings.TrimSpace(payload.Time)); err == nil {
		notice.Deadline = at
	}
	return notice
}

// IdentityEvidence returns the byte-exact instance identity document and its
// signature. There is deliberately no IMDSv1 fallback: a managed worker that
// cannot obtain authenticated metadata must fail closed and retry bootstrap.
func (a *AWS) IdentityEvidence(ctx context.Context) ([]byte, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, identityTimeout)
	defer cancel()
	token, err := a.token(ctx, a.Base, 5*time.Minute)
	if err != nil {
		return nil, nil, fmt.Errorf("retrieve AWS IMDSv2 token: %w", err)
	}
	header := map[string]string{"X-aws-ec2-metadata-token": token}
	document, err := a.meta().fetch(ctx, http.MethodGet, a.Base+"/dynamic/instance-identity/document", header)
	if err != nil {
		return nil, nil, fmt.Errorf("retrieve AWS instance identity document: %w", err)
	}
	encoded, err := a.meta().fetch(ctx, http.MethodGet, a.Base+"/dynamic/instance-identity/signature", header)
	if err != nil {
		return nil, nil, fmt.Errorf("retrieve AWS instance identity signature: %w", err)
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		return nil, nil, fmt.Errorf("decode AWS instance identity signature: %w", err)
	}
	return document, signature, nil
}
