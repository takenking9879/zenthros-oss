// SPDX-License-Identifier: Apache-2.0

package cloudmeta

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "preempt", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// The agent and zenadm reach every provider through the registry: the ones
// shipped today are registered, sorted, with the sources each one has.
func TestRegistryHasShippedProviders(t *testing.T) {
	if got := strings.Join(Names(), ","); got != "aws,aws-eks,gcp" {
		t.Fatalf("providers = %s", got)
	}
	var notices, facts []string
	for _, source := range NoticeSources() {
		notices = append(notices, source.Name())
	}
	for _, source := range FactsSources() {
		facts = append(facts, source.Name())
	}
	if strings.Join(notices, ",") != "aws,gcp" || strings.Join(facts, ",") != "aws,gcp" {
		t.Fatalf("notice sources = %v, facts sources = %v", notices, facts)
	}
	if _, _, err := IdentityEvidence(context.Background(), "azure"); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unknown provider identity: %v", err)
	}
}

func TestRegisterTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate provider name was accepted")
		}
	}()
	Register(&GCP{})
}

type fakeSource struct {
	name   string
	facts  Facts
	answer bool
	notice Notice
}

func (f fakeSource) Name() string                        { return f.name }
func (f fakeSource) Facts(context.Context) (Facts, bool) { return f.facts, f.answer }
func (f fakeSource) Notice(context.Context) Notice       { return f.notice }

func TestFirstAnsweringSourceWins(t *testing.T) {
	facts := DetectFacts(context.Background(), []FactsSource{
		fakeSource{name: "a"},
		fakeSource{name: "b", facts: Facts{Provider: "b"}, answer: true},
		fakeSource{name: "c", facts: Facts{Provider: "c"}, answer: true},
	})
	if facts.Provider != "b" {
		t.Fatalf("facts = %+v", facts)
	}
	notice := FirstNotice(context.Background(), []NoticeSource{
		fakeSource{name: "a", notice: Notice{Source: "a"}},
		fakeSource{name: "b", notice: Notice{Preempted: true, Source: "b"}},
	})
	if !notice.Preempted || notice.Source != "b" {
		t.Fatalf("notice = %+v", notice)
	}
}

func TestParseAWSInstanceActionCannedPayloads(t *testing.T) {
	got := ParseAWSInstanceAction(http.StatusOK, fixture(t, "aws-instance-action.json"))
	if !got.Preempted || got.Source != traits.NoticeAWSIMDS || got.Action != "terminate" {
		t.Fatalf("terminate = %+v, want preempted aws-imds terminate", got)
	}
	// The provider's time bounds how long the worker may still checkpoint.
	if !got.Deadline.Equal(time.Date(2017, 9, 18, 8, 22, 0, 0, time.UTC)) {
		t.Fatalf("deadline = %v, want the payload's time", got.Deadline)
	}
	if got := ParseAWSInstanceAction(http.StatusOK, fixture(t, "aws-instance-action-stop.json")); !got.Preempted || got.Action != "stop" {
		t.Fatalf("stop = %+v, want preempted stop", got)
	}
	for name, notice := range map[string]Notice{
		"404":            ParseAWSInstanceAction(http.StatusNotFound, nil),
		"invalid json":   ParseAWSInstanceAction(http.StatusOK, []byte(`{`)),
		"missing action": ParseAWSInstanceAction(http.StatusOK, []byte(`{"time":"2017-09-18T08:22:00Z"}`)),
	} {
		if notice.Preempted {
			t.Errorf("%s must not be preempted", name)
		}
	}
}

func TestParseGCPPreemptedCannedPayloads(t *testing.T) {
	got := ParseGCPPreempted(fixture(t, "gcp-preempted-true.txt"))
	if !got.Preempted || got.Source != traits.NoticeGCPMetadata || got.Action != "preempted" {
		t.Fatalf("TRUE = %+v, want preempted gcp-metadata", got)
	}
	if ParseGCPPreempted(fixture(t, "gcp-preempted-false.txt")).Preempted {
		t.Fatal("FALSE must not be preempted")
	}
	if !ParseGCPPreempted([]byte("true")).Preempted {
		t.Fatal("case-insensitive TRUE must be preempted")
	}
	if ParseGCPPreempted(nil).Preempted {
		t.Fatal("empty GCP body must not be preempted")
	}
}

// imds serves IMDSv2: a PUT token, then GETs that must carry it.
func imds(t *testing.T, paths map[string]string) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut && request.URL.Path == "/latest/api/token" {
			_, _ = io.WriteString(writer, "imdsv2-token")
			return
		}
		body, ok := paths[strings.TrimPrefix(request.URL.Path, "/latest")]
		if !ok || request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Header.Get("X-aws-ec2-metadata-token") != "imdsv2-token" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(writer, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestAWSReadsIMDSv2(t *testing.T) {
	server := imds(t, map[string]string{
		AWSSpotActionPath:                                 string(fixture(t, "aws-instance-action.json")),
		"/meta-data/placement/availability-zone":          "us-east-2a",
		"/meta-data/placement/region":                     "us-east-2",
		"/meta-data/mac":                                  "0a:1b",
		"/meta-data/network/interfaces/macs/0a:1b/vpc-id": "vpc-1",
		"/dynamic/instance-identity/document":             `{"instanceId":"i-test"}`,
		"/dynamic/instance-identity/signature":            base64.StdEncoding.EncodeToString([]byte("signature")),
	})
	aws := &AWS{Base: server.URL + "/latest", Client: server.Client()}
	if notice := aws.Notice(context.Background()); !notice.Preempted || notice.Action != "terminate" {
		t.Fatalf("notice = %+v", notice)
	}
	facts, ok := aws.Facts(context.Background())
	if !ok || facts != (Facts{Provider: "aws", Zone: "us-east-2a", Region: "us-east-2", NetworkID: "vpc-1"}) {
		t.Fatalf("facts = %+v ok=%v", facts, ok)
	}
	document, signature, err := aws.IdentityEvidence(context.Background())
	if err != nil || string(document) != `{"instanceId":"i-test"}` || string(signature) != "signature" {
		t.Fatalf("identity = %q %q %v", document, signature, err)
	}
}

// Off AWS (no token) nothing is a notice and no facts are claimed.
func TestAWSOffCloud(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	aws := &AWS{Base: server.URL + "/latest", Client: server.Client()}
	if aws.Notice(context.Background()).Preempted {
		t.Fatal("notice without IMDS")
	}
	if _, ok := aws.Facts(context.Background()); ok {
		t.Fatal("facts claimed without IMDS")
	}
	if _, _, err := aws.IdentityEvidence(context.Background()); err == nil {
		t.Fatal("identity without IMDS must fail closed")
	}
}

// AWS_EC2_METADATA_SERVICE_ENDPOINT moves the Spot notice only. The identity
// document and the facts stay on the link-local address: Floci sets the
// variable on its instances, and an enrollment that followed it would skip
// the signed document (e2e: every Floci machine stuck AwaitingEnrollment).
func TestIMDSEndpointMovesOnlyTheNotice(t *testing.T) {
	t.Setenv(IMDSEndpointEnv, "http://127.0.0.1:18080/")
	aws := NewAWS()
	if aws.Base != "http://169.254.169.254/latest" {
		t.Fatalf("identity/facts base = %q, want link-local", aws.Base)
	}
	if aws.noticeBase() != "http://127.0.0.1:18080/latest" {
		t.Fatalf("notice base = %q", aws.noticeBase())
	}
	t.Setenv(IMDSEndpointEnv, "")
	if aws := NewAWS(); aws.noticeBase() != "http://169.254.169.254/latest" {
		t.Fatalf("default notice base = %q", aws.noticeBase())
	}
}

// The notice is read from NoticeBase and identity from Base, each with a
// token from its own server.
func TestAWSNoticeAndIdentityUseTheirOwnServers(t *testing.T) {
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("tok"))
		case "/latest/dynamic/instance-identity/document":
			_, _ = w.Write([]byte(`{"instanceId":"i-1"}`))
		case "/latest/dynamic/instance-identity/signature":
			_, _ = w.Write([]byte(base64.StdEncoding.EncodeToString([]byte("sig"))))
		default:
			http.NotFound(w, r)
		}
	}))
	defer identity.Close()
	notice := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("tok"))
		case "/latest" + AWSSpotActionPath:
			_, _ = w.Write([]byte(`{"action":"terminate","time":"2026-09-27T12:00:00Z"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer notice.Close()
	aws := &AWS{Base: identity.URL + "/latest", NoticeBase: notice.URL + "/latest", Client: identity.Client()}
	if doc, sig, err := aws.IdentityEvidence(context.Background()); err != nil || string(doc) != `{"instanceId":"i-1"}` || string(sig) != "sig" {
		t.Fatalf("identity = %q %q %v", doc, sig, err)
	}
	if got := aws.Notice(context.Background()); !got.Preempted || got.Action != "terminate" {
		t.Fatalf("notice = %+v, want the notice server's terminate", got)
	}
}

func TestGCPReadsMetadataServer(t *testing.T) {
	var identityQuery string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Metadata-Flavor") != "Google" {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		switch strings.TrimPrefix(request.URL.Path, "/computeMetadata/v1") {
		case GCPPreemptedPath:
			_, _ = writer.Write(fixture(t, "gcp-preempted-true.txt"))
		case "/instance/network-interfaces/0/network":
			_, _ = io.WriteString(writer, "projects/1/networks/default")
		case "/instance/zone":
			_, _ = io.WriteString(writer, "projects/1/zones/us-central1-a")
		case "/instance/service-accounts/default/identity":
			identityQuery = request.URL.RawQuery
			_, _ = io.WriteString(writer, "aaa.bbb.ccc")
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	gcp := &GCP{Base: server.URL + "/computeMetadata/v1", Client: server.Client()}
	if notice := gcp.Notice(context.Background()); !notice.Preempted {
		t.Fatalf("notice = %+v", notice)
	}
	facts, ok := gcp.Facts(context.Background())
	if !ok || facts != (Facts{NetworkID: "projects/1/networks/default", Zone: "us-central1-a", Region: "us-central1", Provider: "gcp"}) {
		t.Fatalf("facts = %+v", facts)
	}
	token, _, err := gcp.IdentityEvidence(context.Background())
	if err != nil || string(token) != "aaa.bbb.ccc" {
		t.Fatalf("identity = %q %v", token, err)
	}
	if !strings.Contains(identityQuery, "audience="+GCPIdentityAudience) || !strings.Contains(identityQuery, "format=full") {
		t.Fatalf("identity query = %q", identityQuery)
	}
}

func TestEKSIdentityRequiresClusterName(t *testing.T) {
	t.Setenv("ZENTHROS_EKS_CLUSTER_NAME", "")
	if _, _, err := IdentityEvidence(context.Background(), "aws-eks"); err == nil {
		t.Fatal("missing cluster name was accepted")
	}
}

// An EC2 rebalance recommendation is read from IMDS next to the Spot notice:
// {"noticeTime": ...} when there is one, 404 until then.
func TestAWSReadsRebalanceRecommendation(t *testing.T) {
	if _, ok := ParseAWSRebalance(http.StatusNotFound, nil); ok {
		t.Fatal("404 is a recommendation")
	}
	if _, ok := ParseAWSRebalance(http.StatusOK, []byte(`{"noticeTime":"soon"}`)); ok {
		t.Fatal("an unreadable time is a recommendation")
	}
	at, ok := ParseAWSRebalance(http.StatusOK, []byte(`{"noticeTime": "2026-10-06T12:00:00Z"}`))
	if !ok || !at.Equal(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("recommendation = %v %v", at, ok)
	}

	server := imds(t, map[string]string{AWSRebalancePath: `{"noticeTime":"2026-10-06T12:00:00Z"}`})
	aws := &AWS{Base: server.URL + "/latest", Client: server.Client()}
	if at, ok := aws.Rebalance(context.Background()); !ok || at.IsZero() {
		t.Fatalf("Rebalance = %v %v", at, ok)
	}
	quiet := imds(t, map[string]string{})
	if _, ok := (&AWS{Base: quiet.URL + "/latest", Client: quiet.Client()}).Rebalance(context.Background()); ok {
		t.Fatal("a recommendation without one in IMDS")
	}
	var names []string
	for _, source := range RebalanceSources() {
		names = append(names, source.Name())
	}
	if len(names) != 1 || names[0] != "aws" {
		t.Fatalf("rebalance sources = %v, want only aws", names)
	}
}
