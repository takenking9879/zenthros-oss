// SPDX-License-Identifier: Apache-2.0

package awssecret

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
)

// IdentityProbe reports whether this host has a worker instance profile.
type IdentityProbe func(ctx context.Context) (bool, error)

func requireWorkerIdentity(ctx context.Context, probe IdentityProbe) error {
	if probe == nil {
		probe = imdsInstanceProfilePresent
	}
	present, err := probe(ctx)
	if metadataAnswered(err) {
		// The metadata service is here and failing or throttling: this is
		// an AWS machine, so the job retries within its submit budget
		// (c4-A7) instead of failing as if the machine had no identity.
		return backend.Classified(backend.ErrDestinationUnavailable, fmt.Errorf("aws-secrets-manager: instance metadata: %w", err))
	}
	if err != nil || !present {
		return backend.NewSecretSourceError(v1.ReasonUnsupportedSecretSource, "aws-secrets-manager references require a Managed VM worker identity")
	}
	return nil
}

// metadataAnswered reports an error the metadata service itself answered
// (a 5xx, a 429); off AWS there is none to answer.
func metadataAnswered(err error) bool {
	var httpErr interface{ HTTPStatusCode() int }
	return errors.As(err, &httpErr) && httpErr.HTTPStatusCode() >= 400
}

func imdsInstanceProfilePresent(ctx context.Context) (bool, error) {
	client := imds.New(imds.Options{})
	_, err := client.GetIAMInfo(ctx, &imds.GetIAMInfoInput{})
	if err == nil {
		return true, nil
	}
	if isMissingInstanceProfile(err) {
		return false, nil
	}
	return false, err
}

func isMissingInstanceProfile(err error) bool {
	if err == nil {
		return false
	}
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "404") || strings.Contains(message, "not found") || errors.Is(err, io.EOF)
}

// noInstanceProfile reports the SDK's credential chain finding no role on
// the machine: a worker that is no Managed VM. The SDK gives it no type.
func noInstanceProfile(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "no ec2 instance profile")
}
