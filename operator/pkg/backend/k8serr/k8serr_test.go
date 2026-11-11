// SPDX-License-Identifier: Apache-2.0

package k8serr

import (
	"errors"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/zenthros/operator/pkg/backend"
)

var pytorch = schema.GroupResource{Group: "kubeflow.org", Resource: "pytorchjobs"}

// Every error an API server or an operator returns keeps its exact text; only
// its retry class is added.
func TestClassifyAPIErrorKeepsTextExactly(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class error
	}{
		{"forbidden", apierrors.NewForbidden(pytorch, "train", fmt.Errorf(`User "system:serviceaccount:zenthros-system:zenthros-agent" cannot patch resource "pytorchjobs"`)), backend.ErrForbidden},
		{"webhook", apierrors.NewForbidden(pytorch, "train", fmt.Errorf(`admission webhook "vpytorchjob.kb.io" denied the request: spec.pytorchReplicaSpecs: Required value`)), backend.ErrAdmissionRejected},
		{"quota", apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "p", fmt.Errorf("exceeded quota: tenant, requested: cpu=16, used: cpu=8, limited: cpu=8")), backend.ErrQuotaExceeded},
		{"invalid", apierrors.NewInvalid(schema.GroupKind{Group: "kubeflow.org", Kind: "PyTorchJob"}, "train", field.ErrorList{field.Invalid(field.NewPath("spec", "runPolicy", "backoffLimit"), -1, "must be >= 0")}), backend.ErrInvalid},
		{"vap", apierrors.NewInvalid(schema.GroupKind{Group: "e2e.zenthros.ai", Kind: "Widget"}, "w", field.ErrorList{field.Forbidden(field.NewPath(""), `ValidatingAdmissionPolicy 'e2e-size' with binding 'e2e-size' denied request: size too big (policy e2e)`)}), backend.ErrInvalid},
		{"badrequest", apierrors.NewBadRequest("the server rejected our request for an unknown reason"), backend.ErrInvalid},
		{"notfound", apierrors.NewNotFound(pytorch, "train"), backend.ErrNotFound},
		{"exists", apierrors.NewAlreadyExists(pytorch, "train"), backend.ErrSpecMismatch},
		{"toomany", apierrors.NewTooManyRequests("slow down", 2), backend.ErrRateLimited},
		{"timeout", apierrors.NewServerTimeout(pytorch, "create", 5), backend.ErrDestinationUnavailable},
		{"unavailable", apierrors.NewServiceUnavailable("etcd leader changed"), backend.ErrDestinationUnavailable},
		{"internal", apierrors.NewInternalError(fmt.Errorf("failed calling webhook")), backend.ErrDestinationUnavailable},
		{"nomatch", &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "kubeflow.org", Kind: "PyTorchJob"}, SearchedVersions: []string{"v1"}}, backend.ErrUnsupportedKind},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("create %s ns/train: %w", tc.name, tc.err)
			got := Classify(wrapped)
			if got.Error() != wrapped.Error() {
				t.Fatalf("text changed:\n got %q\nwant %q", got.Error(), wrapped.Error())
			}
			if !errors.Is(got, tc.class) {
				t.Fatalf("class of %q: not %v", got, tc.class)
			}
			if !errors.Is(got, tc.err) {
				t.Fatal("the original error is no longer reachable with errors.Is")
			}
		})
	}
}

func TestClassifyLeavesUnknownErrorsTransient(t *testing.T) {
	for _, err := range []error{
		errors.New("dial tcp 10.0.0.1:443: connect: connection refused"),
		// A write that raced another is read again on the next pass.
		apierrors.NewConflict(pytorch, "train", fmt.Errorf("the object has been modified")),
	} {
		got := Classify(err)
		if got != err || backend.Classify(got) != backend.ClassTransient {
			t.Fatalf("%v must pass through unchanged and transient, got %v", err, got)
		}
	}
}

// A throttled or failing cluster behind a connected agent is never the
// registry's "no agent", which fails a job not yet sent at once (c4-A1).
func TestAThrottledClusterIsNotAMissingAgent(t *testing.T) {
	for _, err := range []error{
		apierrors.NewTooManyRequests("slow down", 2),
		apierrors.NewServiceUnavailable("etcd leader changed"),
	} {
		got := Classify(err)
		if errors.Is(got, backend.ErrBackendUnavailable) || backend.Classify(got) != backend.ClassTransient {
			t.Fatalf("%v classified %v, want transient and not ErrBackendUnavailable", err, got)
		}
	}
}

// A typed client that does not know a kind (an operator not installed in
// the cluster) is an unsupported kind, read from the error's type, not its
// text.
func TestAKindTheClientDoesNotKnowIsUnsupported(t *testing.T) {
	_, err := runtime.NewScheme().New(schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayJob"})
	if got := Classify(fmt.Errorf("create RayJob: %w", err)); !errors.Is(got, backend.ErrUnsupportedKind) {
		t.Fatalf("%v classified %v, want unsupported kind", err, got)
	}
}

// A reader that cannot select on a field says so as a BadRequest (an
// apiserver) or in a cache's untyped text; throttled or timed out is not
// that, and listing every job then would load a server already refusing
// work.
func TestCannotSelectIsOnlyAMissingField(t *testing.T) {
	jobs := schema.GroupResource{Group: "zenthros.ai", Resource: "zenthrosjobs"}
	for err, want := range map[error]bool{
		apierrors.NewBadRequest(`field label not supported: status.scheduling.decision.backend`): true,
		fmt.Errorf("list: %w", errors.New("index with name field:spec.poolRef does not exist")):  true,
		apierrors.NewTooManyRequests("throttled", 1):                                             false,
		apierrors.NewTimeoutError("list", 1):                                                     false,
		apierrors.NewForbidden(jobs, "", errors.New("no")):                                       false,
	} {
		if got := CannotSelect(err); got != want {
			t.Errorf("CannotSelect(%v) = %v, want %v", err, got, want)
		}
	}
}

// An index another reconciler registered is no error; a cache that
// cannot index is.
func TestAlreadyIndexedIsOnlyARepeatedIndex(t *testing.T) {
	for err, want := range map[error]bool{
		errors.New("indexer conflict: map[field:spec.poolRef:{}]"): true,
		errors.New("index spec.poolRef already exists"):            true,
		errors.New("informer has already started"):                 false,
	} {
		if got := AlreadyIndexed(err); got != want {
			t.Errorf("AlreadyIndexed(%v) = %v, want %v", err, got, want)
		}
	}
}

// A cluster's ResourceQuota is the same wait as a cloud's: full now, a job
// that ends makes room, and the job waits as for a busy backend. Only a request
// over a limit, which nothing ending could make room for, is a refusal.
func TestAQuotaDenialWaitsUnlessItCouldNeverFit(t *testing.T) {
	deny := func(words string) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "batch", Resource: "jobs"}, "j", errors.New("exceeded quota: "+words))
	}
	for _, c := range []struct {
		name  string
		words string
		want  error
	}{
		{"full now", "tenant, requested: cpu=4, used: cpu=8, limited: cpu=8", backend.ErrBackendBusy},
		{"the request is the whole limit", "tenant, requested: cpu=8, used: cpu=1, limited: cpu=8", backend.ErrBackendBusy},
		{"several resources, within all", "t, requested: limits.cpu=2,limits.memory=4Gi, used: limits.cpu=8,limits.memory=1Gi, limited: limits.cpu=8,limits.memory=16Gi", backend.ErrBackendBusy},
		{"over a limit", "tenant, requested: cpu=16, used: cpu=0, limited: cpu=8", backend.ErrQuotaExceeded},
		{"over one limit of several, in other units", "t, requested: limits.cpu=2,limits.memory=32Gi, used: limits.cpu=0,limits.memory=0, limited: limits.cpu=8,limits.memory=16384Mi", backend.ErrQuotaExceeded},
		{"a limit of nothing", "t, requested: count/jobs.batch=1, used: count/jobs.batch=0, limited: count/jobs.batch=0", backend.ErrQuotaExceeded},
		{"no numbers in the words", "tenant", backend.ErrBackendBusy},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := Classify(deny(c.words)); !errors.Is(err, c.want) {
				t.Fatalf("Classify = %v, want %v", err, c.want)
			}
		})
	}
}

// A Workspace's job count at its most, in the API server's words
// (k8s.io/apiserver plugin/resourcequota, v0.35), which zenthros-api answers
// in its own: a 429 that says what to do.
func TestAQuotaOverOneResourceIsReadByItsName(t *testing.T) {
	const jobs = "count/zenthrosjobs.zenthros.ai"
	deny := func(words string) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "zenthros.ai", Resource: "zenthrosjobs"}, "train", errors.New(words))
	}
	full := deny("exceeded quota: zenthros-workspace-quota, requested: " + jobs + "=1, used: " + jobs + "=1000, limited: " + jobs + "=1000")
	if most, ok := QuotaFull(full, jobs); !ok || most != "1000" {
		t.Fatalf("QuotaFull = %q, %v; want \"1000\", true", most, ok)
	}
	for name, err := range map[string]error{
		"another resource's limit": deny("exceeded quota: q, requested: pods=1, used: pods=30, limited: pods=30"),
		"not a quota":              deny(`admission webhook "x" denied the request`),
		"not forbidden":            apierrors.NewConflict(schema.GroupResource{Resource: "zenthrosjobs"}, "train", errors.New("exceeded quota: q, requested: "+jobs+"=1, used: "+jobs+"=1, limited: "+jobs+"=1")),
	} {
		if _, ok := QuotaFull(err, jobs); ok {
			t.Errorf("%s: QuotaFull read it as the job count at its most", name)
		}
	}
}
