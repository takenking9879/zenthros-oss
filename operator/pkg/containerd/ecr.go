// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

// RegistryAuth gives `ctr images pull` a registry login for the hosts it
// knows; ok is false for any other host, which is pulled anonymously.
type RegistryAuth interface {
	Login(ctx context.Context, host string) (user, password string, ok bool, err error)
}

var ecrHost = regexp.MustCompile(`^\d{12}\.dkr\.ecr(-fips)?\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?$`)

// ECRRegion is the region of an Amazon ECR registry host, or "" when host is
// not one.
func ECRRegion(host string) string {
	if m := ecrHost.FindStringSubmatch(host); m != nil {
		return m[2]
	}
	return ""
}

// ECRAuth logs in to Amazon ECR with the machine's own AWS identity: the
// default credential chain, on EC2 the instance role through IMDS. No key is
// configured on the machine; the role needs ecr:GetAuthorizationToken and
// read on the repository.
type ECRAuth struct {
	// Credentials overrides the default chain (tests).
	Credentials aws.CredentialsProvider
	// Endpoint overrides https://api.ecr.<region>.amazonaws.com (tests).
	Endpoint string
	Client   *http.Client
	Now      func() time.Time
}

// Login returns the ECR token for host's region as ctr's user and password.
func (a ECRAuth) Login(ctx context.Context, host string) (string, string, bool, error) {
	region := ECRRegion(host)
	if region == "" {
		return "", "", false, nil
	}
	creds := a.Credentials
	if creds == nil {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return "", "", true, fmt.Errorf("ECR login for %s: load AWS credentials: %w", host, err)
		}
		creds = cfg.Credentials
	}
	value, err := creds.Retrieve(ctx)
	if err != nil {
		return "", "", true, fmt.Errorf("ECR login for %s: no AWS credentials on this machine (instance role): %w", host, err)
	}
	endpoint := a.Endpoint
	if endpoint == "" {
		endpoint = "https://api.ecr." + region + ".amazonaws.com"
		if strings.HasSuffix(host, ".amazonaws.com.cn") {
			endpoint += ".cn"
		}
	}
	body := []byte("{}")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/", bytes.NewReader(body))
	if err != nil {
		return "", "", true, err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken")
	sum := sha256.Sum256(body)
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	if err := v4.NewSigner().SignHTTP(ctx, value, req, hex.EncodeToString(sum[:]), "ecr", region, now()); err != nil {
		return "", "", true, fmt.Errorf("ECR login for %s: sign: %w", host, err)
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", true, fmt.Errorf("ECR login for %s: %w", host, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		// ECR's own error, as it said it (AccessDeniedException: ...).
		return "", "", true, fmt.Errorf("ECR GetAuthorizationToken for %s: HTTP %d: %s", host, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		AuthorizationData []struct {
			AuthorizationToken string `json:"authorizationToken"`
		} `json:"authorizationData"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || len(out.AuthorizationData) == 0 {
		return "", "", true, fmt.Errorf("ECR GetAuthorizationToken for %s: unexpected answer", host)
	}
	decoded, err := base64.StdEncoding.DecodeString(out.AuthorizationData[0].AuthorizationToken)
	user, password, found := strings.Cut(string(decoded), ":")
	if err != nil || !found {
		return "", "", true, fmt.Errorf("ECR GetAuthorizationToken for %s: malformed token", host)
	}
	return user, password, true, nil
}
