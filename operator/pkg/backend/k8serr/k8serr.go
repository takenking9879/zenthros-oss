// SPDX-License-Identifier: Apache-2.0

// Package k8serr classifies errors from a Kubernetes API server, including the
// ones an operator's admission webhook or a validating admission policy
// returns, into the pkg/backend sentinels that drive retry policy. The error
// text is never rewritten: the user reads exactly what the cluster said.
package k8serr

import (
	"errors"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/zenthros/operator/pkg/backend"
)

// Classify returns err marked with the sentinel that matches it, or err
// unchanged when it matches none (a transport error or a Conflict, which the
// next pass reads again, stays transient) or already carries a more specific
// backend sentinel.
func Classify(err error) error {
	if err == nil || backend.HasSentinel(err) {
		return err
	}
	if class := classOf(err); class != nil {
		return backend.Classified(class, err)
	}
	return err
}

func classOf(err error) error {
	switch {
	case meta.IsNoMatchError(err):
		return backend.ErrUnsupportedKind
	case apierrors.IsForbidden(err) && isAdmissionDenial(err):
		return backend.ErrAdmissionRejected
	case apierrors.IsForbidden(err):
		if strings.Contains(err.Error(), "exceeded quota") {
			return quotaDenial(err)
		}
		return backend.ErrForbidden
	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		return backend.ErrInvalid
	case apierrors.IsNotFound(err):
		return backend.ErrNotFound
	case apierrors.IsAlreadyExists(err):
		// An object of the job's name that the job does not own.
		return backend.ErrSpecMismatch
	case apierrors.IsTooManyRequests(err):
		// The agent is here; the cluster behind it throttles (c4-A1).
		return backend.ErrRateLimited
	case apierrors.IsServerTimeout(err), apierrors.IsTimeout(err),
		apierrors.IsServiceUnavailable(err), apierrors.IsInternalError(err), apierrors.IsUnexpectedServerError(err):
		// The agent is here; the cluster behind it is failing (c4-A1):
		// never the registry's "no agent".
		return backend.ErrDestinationUnavailable
	case notRegistered(err):
		return backend.ErrUnsupportedKind
	}
	return nil
}

// quotaWords is how the API server words a ResourceQuota's denial: "exceeded
// quota: q, requested: cpu=4,memory=1Gi, used: cpu=8,memory=2Gi, limited:
// cpu=8,memory=4Gi".
var quotaWords = regexp.MustCompile(`requested: (\S+), used: \S+, limited: ([^\s"]+)`)

// quotaDenial is the class of a ResourceQuota's denial. Full now, a job that
// ends makes room, and the job waits as for a busy backend; a request over a
// limit fits only once someone raises it.
func quotaDenial(err error) error {
	found := quotaWords.FindStringSubmatch(err.Error())
	if found == nil {
		return backend.ErrBackendBusy
	}
	limits := map[string]resource.Quantity{}
	for _, limit := range strings.Split(found[2], ",") {
		if name, value, ok := strings.Cut(limit, "="); ok {
			if quantity, parseErr := resource.ParseQuantity(value); parseErr == nil {
				limits[name] = quantity
			}
		}
	}
	for _, request := range strings.Split(found[1], ",") {
		name, value, ok := strings.Cut(request, "=")
		if !ok {
			continue
		}
		asked, parseErr := resource.ParseQuantity(value)
		if limit, known := limits[name]; parseErr == nil && known && asked.Cmp(limit) > 0 {
			return backend.ErrQuotaExceeded
		}
	}
	return backend.ErrBackendBusy
}

// QuotaFull reports a ResourceQuota's denial over resource (the quota's name
// for it, such as count/zenthrosjobs.zenthros.ai), and the most it allows as
// the API server wrote it.
func QuotaFull(err error, resource string) (most string, full bool) {
	if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "exceeded quota") {
		return "", false
	}
	found := quotaWords.FindStringSubmatch(err.Error())
	if found == nil {
		return "", false
	}
	for _, limit := range strings.Split(found[2], ",") {
		if name, value, ok := strings.Cut(limit, "="); ok && name == resource {
			return value, true
		}
	}
	return "", false
}

// isAdmissionDenial reports a validating or mutating webhook refusal, which the
// API server returns as Forbidden with the webhook's own message: like a
// quota's denial, it has no reason of its own, only the text.
func isAdmissionDenial(err error) bool {
	return strings.Contains(err.Error(), "admission webhook")
}

// notRegistered reports a typed client that does not know the kind, through
// the wraps around it (runtime.IsNotRegisteredError reads only err itself).
func notRegistered(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if runtime.IsNotRegisteredError(err) {
			return true
		}
	}
	return false
}

// CannotSelect reports a reader that cannot select on a field: an apiserver
// for which it is no selectable field (a BadRequest), or a controller-runtime
// cache without the index, whose error has no type, only its text.
func CannotSelect(err error) bool {
	return apierrors.IsBadRequest(err) || strings.Contains(err.Error(), "index with name")
}

// AlreadyIndexed reports a controller-runtime indexer that has the field
// already: another reconciler of the manager registered it. Its error has
// no type, only its text.
func AlreadyIndexed(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already a field indexer") ||
		strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "indexer conflict")
}

// ContainerStarting reports a log-open the apiserver refused because the
// container has not started yet (ContainerCreating, PodInitializing): it
// passes. Its error has no reason of its own, only the kubelet's text.
func ContainerStarting(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "waiting to start") ||
		strings.Contains(msg, "ContainerCreating") ||
		strings.Contains(msg, "PodInitializing")
}
