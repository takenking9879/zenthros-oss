// SPDX-License-Identifier: Apache-2.0

package fault_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/zenthros/operator/internal/grpc/errorclass"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/fault"
)

var gr = schema.GroupResource{Group: "zenthros.ai", Resource: "zenthrosjobs"}

type sqlErr string

func (e sqlErr) Error() string    { return "sql " + string(e) }
func (e sqlErr) SQLState() string { return string(e) }

func TestOfTable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want fault.Kind
	}{
		{"explicit kind wins over its cause", fault.Wrap(fault.Refused, "op", context.Canceled), fault.Refused},
		{"own context canceled", fmt.Errorf("call: %w", context.Canceled), fault.Canceled},
		{"deadline: the callee may have done it", context.DeadlineExceeded, fault.Unknown},
		{"k8s conflict", apierrors.NewConflict(gr, "j", errors.New("x")), fault.Conflict},
		{"k8s already exists", apierrors.NewAlreadyExists(gr, "j"), fault.Exists},
		{"k8s not found", apierrors.NewNotFound(gr, "j"), fault.Absent},
		{"k8s forbidden", apierrors.NewForbidden(gr, "j", errors.New("x")), fault.Unauthorized},
		{"k8s invalid", apierrors.NewBadRequest("x"), fault.Invalid},
		{"k8s unsupported", apierrors.NewMethodNotSupported(gr, "patch"), fault.Refused},
		{"k8s 429", apierrors.NewTooManyRequests("x", 1), fault.Transient},
		{"k8s 503", apierrors.NewServiceUnavailable("x"), fault.Transient},
		{"k8s 500", apierrors.NewInternalError(errors.New("x")), fault.Transient},
		{"k8s server timeout", apierrors.NewServerTimeout(gr, "create", 1), fault.Unknown},
		{"grpc unavailable", status.Error(codes.Unavailable, "x"), fault.Transient},
		{"grpc aborted", status.Error(codes.Aborted, "x"), fault.Conflict},
		{"grpc not found", status.Error(codes.NotFound, "x"), fault.Absent},
		{"grpc invalid", status.Error(codes.InvalidArgument, "x"), fault.Invalid},
		{"grpc precondition", status.Error(codes.FailedPrecondition, "x"), fault.Refused},
		{"grpc unauthenticated", status.Error(codes.Unauthenticated, "x"), fault.Unauthorized},
		{"grpc internal", status.Error(codes.Internal, "x"), fault.Unknown},
		{"wrapped grpc", fmt.Errorf("dial: %w", status.Error(codes.Unavailable, "x")), fault.Transient},
		{"a sentinel beats the generic wire code", errorclass.Decode(agentpb.ErrorClass_SPEC_MISMATCH, "x"), fault.Refused},
		{"backend busy", fmt.Errorf("submit: %w", backend.ErrBackendBusy), fault.Transient},
		{"plain error", errors.New("boom"), fault.Unknown},
	} {
		if got := fault.Of(tc.err); got != tc.want {
			t.Errorf("%s: Of = %s, want %s", tc.name, got, tc.want)
		}
	}
}

// Every class that crosses the wire and every backend sentinel has a kind of
// its own (each provider class too: pkg/infrastructure/provider's fault_test): a new one added without a kind is
// Unknown, which retries what may be a permanent refusal.
func TestEveryFamilyIsMapped(t *testing.T) {
	registered := fault.Sentinels()
	for _, sentinel := range []error{
		backend.ErrSpecMismatch, backend.ErrUnsupportedKind, backend.ErrUnsupportedBackend,
		backend.ErrBackendUnavailable, backend.ErrQuotaExceeded, backend.ErrAdmissionRejected, backend.ErrImagePull,
		backend.ErrSecretSource, backend.ErrRejected, backend.ErrBackendBusy, backend.ErrRateLimited, backend.ErrQueueFull,
		backend.ErrForbidden, backend.ErrInvalid, backend.ErrNotFound,
	} {
		if _, ok := registered[sentinel]; !ok {
			t.Errorf("backend sentinel %q has no kind", sentinel)
		}
		// backend.Classify must agree: permanent is exactly not Retryable.
		permanent := backend.Classify(sentinel) == backend.ClassPermanent
		if permanent == fault.Retryable(sentinel) {
			t.Errorf("backend sentinel %q: Classify permanent=%v but fault.Retryable=%v", sentinel, permanent, fault.Retryable(sentinel))
		}
	}
	for class := range agentpb.ErrorClass_name {
		c := agentpb.ErrorClass(class)
		if c == agentpb.ErrorClass_UNSPECIFIED {
			if got := fault.Of(errorclass.Decode(c, "x")); got != fault.Unknown {
				t.Errorf("UNSPECIFIED: Of = %s, want unknown", got)
			}
			continue
		}
		decoded := errorclass.Decode(c, "x")
		if _, ok := registered[errors.Unwrap(decoded)]; !ok {
			t.Errorf("wire class %s decodes to an error with no kind", c)
		}
	}
}

func TestGRPCIsTheOneWayOut(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{errors.New("store down"), codes.Unavailable},
		{apierrors.NewNotFound(gr, "j"), codes.NotFound},
		{apierrors.NewConflict(gr, "j", errors.New("x")), codes.Aborted},
		{fmt.Errorf("x: %w", backend.ErrInvalid), codes.FailedPrecondition},
		{fault.Wrap(fault.Unauthorized, "verify", errors.New("bad cert")), codes.PermissionDenied},
		{status.Error(codes.InvalidArgument, "kept"), codes.InvalidArgument},
		{context.Canceled, codes.Canceled},
	} {
		if got := status.Code(fault.GRPC(tc.err)); got != tc.want {
			t.Errorf("GRPC(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
	if fault.GRPC(nil) != nil {
		t.Error("GRPC(nil) must be nil")
	}
}

func TestHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{
		{errors.New("store down"), http.StatusServiceUnavailable},
		{fault.FromSQL(sqlErr("22P05")), http.StatusUnprocessableEntity},
		{fault.FromSQL(sqlErr("08006")), http.StatusServiceUnavailable},
		{apierrors.NewNotFound(gr, "j"), http.StatusNotFound},
		{fault.Wrap(fault.Unauthorized, "", errors.New("x")), http.StatusForbidden},
	} {
		if got := fault.HTTPStatus(tc.err); got != tc.want {
			t.Errorf("HTTPStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestFromHTTP(t *testing.T) {
	for _, tc := range []struct {
		code   int
		create bool
		want   fault.Kind
	}{
		{404, false, fault.Absent},
		{401, false, fault.Unauthorized},
		{403, true, fault.Unauthorized},
		{409, true, fault.Exists},
		{409, false, fault.Conflict},
		{429, true, fault.Transient},
		{429, false, fault.Transient},
		{503, true, fault.Transient},
		{500, false, fault.Transient},
		{500, true, fault.Unknown},
		{504, true, fault.Unknown},
		{422, false, fault.Invalid},
		{400, true, fault.Invalid},
		{405, false, fault.Refused},
	} {
		if got := fault.Of(fault.FromHTTP(tc.code, tc.create, nil)); got != tc.want {
			t.Errorf("FromHTTP(%d, create=%v) = %s, want %s", tc.code, tc.create, got, tc.want)
		}
	}
	cause := errors.New("body")
	if !errors.Is(fault.FromHTTP(500, false, cause), cause) {
		t.Error("FromHTTP must keep its cause")
	}
}

// TransientHTTP is the one table every client of an HTTP server asks again
// by: a server's timeout, a rate limit and any 5xx, never an answer or a
// success (R4 ronda 5, M3).
func TestTransientHTTP(t *testing.T) {
	for code, want := range map[int]bool{200: false, 304: false, 400: false, 404: false, 409: false,
		408: true, 429: true, 500: true, 503: true, 507: true} {
		if got := fault.TransientHTTP(code); got != want {
			t.Errorf("TransientHTTP(%d) = %t, want %t", code, got, want)
		}
	}
}

func TestFromSQLKeepsTheCause(t *testing.T) {
	err := fault.FromSQL(sqlErr("23505"))
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) || fault.Of(err) != fault.Refused {
		t.Fatalf("FromSQL lost the cause or the kind: %v", err)
	}
	if fault.FromSQL(nil) != nil || fault.Of(fault.FromSQL(errors.New("x"))) != fault.Unknown {
		t.Fatal("FromSQL of nil or of a non-SQL error")
	}
}

func TestCleanupOutlivesTheCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cleanup, done := fault.Cleanup(ctx)
	defer done()
	if cleanup.Err() != nil {
		t.Fatal("a compensation must run after the caller is canceled")
	}
	if _, ok := cleanup.Deadline(); !ok {
		t.Fatal("a compensation must be bounded")
	}
}

func TestCreateOwned(t *testing.T) {
	ours := func(o client.Object) bool { return o.GetLabels()["owner"] == "me" }
	secret := func(owner string) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns", Labels: map[string]string{"owner": owner}},
			Data: map[string][]byte{"k": []byte(owner)}}
	}
	t.Run("ours already there is success", func(t *testing.T) {
		c := fake.NewClientBuilder().WithObjects(secret("me")).Build()
		obj := secret("me")
		obj.Data = nil
		if err := fault.CreateOwned(context.Background(), c, obj, ours); err != nil {
			t.Fatal(err)
		}
		if string(obj.Data["k"]) != "me" || obj.ResourceVersion == "" {
			t.Fatalf("obj not filled with what is there: %+v", obj)
		}
	})
	t.Run("someone else's is Exists", func(t *testing.T) {
		c := fake.NewClientBuilder().WithObjects(secret("other")).Build()
		err := fault.CreateOwned(context.Background(), c, secret("me"), ours)
		if fault.Of(err) != fault.Exists || !errors.Is(err, fault.ErrNotOurs) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a timed-out create that landed is success", func(t *testing.T) {
		base := fake.NewClientBuilder().Build()
		c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := cl.Create(ctx, obj, opts...); err != nil {
					return err
				}
				return apierrors.NewServerTimeout(gr, "create", 1)
			},
		})
		if err := fault.CreateOwned(context.Background(), c, secret("me"), ours); err != nil {
			t.Fatal(err)
		}
	})
	// A 500 on a create (etcd "request timed out") may have persisted it:
	// read back, it is ours. Taken for nothing done, the retry under a new
	// nonce found it and refused it as someone else's: an enrollment with
	// a valid token denied for good (R4 ronda 4, M1).
	t.Run("a create that landed and answered 500 is success", func(t *testing.T) {
		base := fake.NewClientBuilder().Build()
		c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := cl.Create(ctx, obj, opts...); err != nil {
					return err
				}
				return apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
			},
		})
		if err := fault.CreateOwned(context.Background(), c, secret("me"), ours); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a refusal is returned as it is", func(t *testing.T) {
		c := interceptor.NewClient(fake.NewClientBuilder().Build().(client.WithWatch), interceptor.Funcs{
			Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return apierrors.NewBadRequest("no")
			},
		})
		if err := fault.CreateOwned(context.Background(), c, secret("me"), ours); fault.Of(err) != fault.Invalid {
			t.Fatalf("err = %v", err)
		}
	})
}

// Forbidden is a caller who may not do it (a 403, PermissionDenied), through
// any wrapping; a credential missing or refused (a 401, Unauthenticated) is
// not, though Of says Unauthorized of all four.
func TestForbiddenIsNotACredentialRefused(t *testing.T) {
	jobs := schema.GroupResource{Group: "zenthros.ai", Resource: "zenthrosjobs"}
	for name, tc := range map[string]struct {
		err       error
		forbidden bool
	}{
		"a 403":                    {apierrors.NewForbidden(jobs, "train", errors.New("no")), true},
		"a 403, wrapped":           {fmt.Errorf("get: %w", apierrors.NewForbidden(jobs, "train", errors.New("no"))), true},
		"PermissionDenied":         {status.Error(codes.PermissionDenied, "no"), true},
		"PermissionDenied wrapped": {fmt.Errorf("logs: %w", status.Error(codes.PermissionDenied, "no")), true},
		"a 401":                    {apierrors.NewUnauthorized("Unauthorized"), false},
		"Unauthenticated":          {status.Error(codes.Unauthenticated, "no certificate"), false},
	} {
		if got := fault.Forbidden(tc.err); got != tc.forbidden {
			t.Errorf("%s: Forbidden %v, want %v", name, got, tc.forbidden)
		}
		if got := fault.Of(tc.err); got != fault.Unauthorized {
			t.Errorf("%s: Of %v, want Unauthorized", name, got)
		}
	}
}
