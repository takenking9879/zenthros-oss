// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zenthros/operator/pkg/backend"
)

type fixedAuth struct {
	host string
	err  error
}

func (f fixedAuth) Login(_ context.Context, host string) (string, string, bool, error) {
	if host != f.host {
		return "", "", false, nil
	}
	return "u-" + f.host, "p", true, f.err
}

// The first entry that knows the host answers, error included; a host no
// entry knows is pulled anonymously.
func TestAuthChainFirstKnownHostAnswers(t *testing.T) {
	boom := errors.New("denied")
	chain := AuthChain{fixedAuth{host: "a"}, fixedAuth{host: "b", err: boom}, fixedAuth{host: "a"}}
	if user, _, ok, err := chain.Login(context.Background(), "a"); !ok || err != nil || user != "u-a" {
		t.Fatalf("a: %q %v %v", user, ok, err)
	}
	if _, _, ok, err := chain.Login(context.Background(), "b"); !ok || !errors.Is(err, boom) {
		t.Fatalf("b: ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := chain.Login(context.Background(), "docker.io"); ok || err != nil {
		t.Fatalf("unknown host: ok=%v err=%v", ok, err)
	}
}

func TestGARAuthUsesServiceAccountToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Metadata-Flavor") != "Google" {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(writer, `{"access_token":"ya29.token","expires_in":3599,"token_type":"Bearer"}`)
	}))
	t.Cleanup(server.Close)
	auth := GARAuth{TokenURL: server.URL, Client: server.Client()}
	for _, host := range []string{"us-central1-docker.pkg.dev", "gcr.io", "eu.gcr.io"} {
		user, password, ok, err := auth.Login(context.Background(), host)
		if err != nil || !ok || user != "oauth2accesstoken" || password != "ya29.token" {
			t.Fatalf("%s: %q %q %v %v", host, user, password, ok, err)
		}
	}
	if _, _, ok, _ := auth.Login(context.Background(), "123456789012.dkr.ecr.us-east-1.amazonaws.com"); ok {
		t.Fatal("GAR auth claimed an ECR host")
	}
	server.Close()
	if _, _, ok, err := auth.Login(context.Background(), "gcr.io"); ok || err != nil {
		t.Fatalf("off Google Cloud: ok=%v err=%v, want an anonymous pull", ok, err)
	}
	if _, _, ok, _ := DefaultRegistryAuth().Login(context.Background(), "docker.io"); ok {
		t.Fatal("default chain claimed docker.io")
	}
}

// A metadata server that does not answer in time is on Google Cloud and
// slow, not absent: the pull waits for it instead of going anonymous and
// failing for good on a private image (c4-A7).
func TestASlowMetadataServerIsNotAnAnonymousPull(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	client := server.Client()
	client.Timeout = 50 * time.Millisecond
	_, _, ok, err := GARAuth{TokenURL: server.URL, Client: client}.Login(context.Background(), "gcr.io")
	if !ok || backend.Classify(err) != backend.ClassTransient || !errors.Is(err, backend.ErrDestinationUnavailable) {
		t.Fatalf("slow metadata: ok=%v err=%v, want a transient login failure", ok, err)
	}
}
