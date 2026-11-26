// SPDX-License-Identifier: Apache-2.0

package objectput

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
)

// fakeBucket is an S3 or Cloud Storage server that keeps objects in memory:
// it checks each request's credential (verify), honors a conditional PUT and
// keeps the SHA-256 header the client sends.
type fakeBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	meta    map[string]string
	header  string // the conditional header: If-None-Match or X-Goog-If-Generation-Match
	metaKey string
	verify  func(r *http.Request, body []byte) bool
	refuse  int // when set, every request gets it
	puts    int
	// replace takes an unconditional PUT over what is at the key (a
	// job's output, PutFile).
	replace bool
}

func newFakeBucket(gs bool, verify func(*http.Request, []byte) bool) *fakeBucket {
	b := &fakeBucket{objects: map[string][]byte{}, meta: map[string]string{}, verify: verify,
		header: "If-None-Match", metaKey: "X-Amz-Meta-Sha256"}
	if gs {
		b.header, b.metaKey = "X-Goog-If-Generation-Match", "X-Goog-Meta-Sha256"
	}
	return b
}

func (b *fakeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.refuse != 0 {
		http.Error(w, "refused", b.refuse)
		return
	}
	if !b.verify(r, body) {
		http.Error(w, "SignatureDoesNotMatch", http.StatusForbidden)
		return
	}
	switch r.Method {
	case http.MethodPut:
		b.puts++
		if b.replace && r.Header.Get(b.header) == "" {
			b.objects[r.URL.Path], b.meta[r.URL.Path] = body, r.Header.Get(b.metaKey)
			return
		}
		if want := map[string]string{"If-None-Match": "*", "X-Goog-If-Generation-Match": "0"}[b.header]; r.Header.Get(b.header) != want {
			http.Error(w, "unconditional PUT", http.StatusBadRequest)
			return
		}
		if _, ok := b.objects[r.URL.Path]; ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		b.objects[r.URL.Path], b.meta[r.URL.Path] = body, r.Header.Get(b.metaKey)
	case http.MethodHead:
		if _, ok := b.objects[r.URL.Path]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set(b.metaKey, b.meta[r.URL.Path])
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			// A page of two keys at most, so that a listing goes on.
			bucket, prefix, after := strings.TrimSuffix(r.URL.Path, "/")+"/", r.URL.Query().Get("prefix"), r.URL.Query().Get("continuation-token")
			var keys []string
			for path := range b.objects {
				if key := strings.TrimPrefix(path, bucket); strings.HasPrefix(key, prefix) && key > after {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
			for i, key := range keys {
				if i == 2 {
					fmt.Fprintf(w, "<IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken>", keys[1])
					break
				}
				fmt.Fprintf(w, "<Contents><Key>%s</Key><LastModified>2026-11-14T10:00:0%dZ</LastModified></Contents>", key, i)
			}
			fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		object, ok := b.objects[r.URL.Path]
		if !ok {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		_, _ = w.Write(object)
	default:
		http.Error(w, "no", http.StatusMethodNotAllowed)
	}
}

// verifySigV4 re-signs what the request says it signed with the secret, as
// S3 does, and compares: the key S3 reads (the path, decoded) written in
// the canonical request once, each byte but the unreserved ones and '/' as
// %XX.
func verifySigV4(t *testing.T, id, secret, region string) func(*http.Request, []byte) bool {
	return func(r *http.Request, body []byte) bool {
		auth := r.Header.Get("Authorization")
		_, rest, ok := strings.Cut(auth, "AWS4-HMAC-SHA256 ")
		if !ok {
			return false
		}
		fields := map[string]string{}
		for _, part := range strings.Split(rest, ", ") {
			k, v, _ := strings.Cut(part, "=")
			fields[k] = v
		}
		if !strings.HasPrefix(fields["Credential"], id+"/") || !strings.Contains(fields["Credential"], "/"+region+"/s3/") {
			return false
		}
		at, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
		if err != nil {
			return false
		}
		target := "https://" + r.Host + s3Escape(r.URL.Path)
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
		again, _ := http.NewRequest(r.Method, target, strings.NewReader(string(body)))
		for _, name := range strings.Split(fields["SignedHeaders"], ";") {
			if name != "host" {
				again.Header.Set(name, r.Header.Get(name))
			}
		}
		again.Header.Del("Authorization")
		sum := r.Header.Get("X-Amz-Content-Sha256")
		once := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
		if err := once.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: id, SecretAccessKey: secret}, again, sum, "s3", region, at); err != nil {
			t.Error(err)
			return false
		}
		_, theirs, _ := strings.Cut(auth, "Signature=")
		_, ours, _ := strings.Cut(again.Header.Get("Authorization"), "Signature=")
		return theirs == ours
	}
}

// s3Escape writes path as S3's canonical request does.
func s3Escape(path string) string {
	var b strings.Builder
	for i := 0; i < len(path); i++ {
		c := path[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || strings.IndexByte("-_.~/", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// serve is fake behind TLS, and the CA that trusts it.
func serve(t *testing.T, h http.Handler) (string, []byte) {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	return server.URL, ca
}

// An object is written once, signed: sent again with the same bytes it is
// written (an agent that restarted before it knew), with other bytes it is
// refused for good, and a signature the server does not take is a refusal
// of the credential, never asked again.
func TestAnObjectIsWrittenOnceSigned(t *testing.T) {
	ctx := context.Background()
	bucket := newFakeBucket(false, verifySigV4(t, "AKID", "secret", "eu-west-3"))
	endpoint, ca := serve(t, bucket)
	store, err := Open(ctx, Config{Destination: "s3://logs/zenthros/", Endpoint: endpoint, Region: "eu-west-3", CAData: ca,
		Credentials: map[string]string{EnvAccessKeyID: "AKID", EnvSecretAccessKey: "secret", EnvSessionToken: "tok"}})
	if err != nil {
		t.Fatal(err)
	}
	key := store.Key("job", "uid", "g1-a0", "h", "00000000.log.zst")
	if key != "zenthros/job/uid/g1-a0/h/00000000.log.zst" {
		t.Fatalf("key %q", key)
	}
	if err := store.Put(ctx, key, "application/zstd", []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, key, "application/zstd", []byte("frame")); err != nil {
		t.Fatalf("the same object sent again: %v", err)
	}
	err = store.Put(ctx, key, "application/zstd", []byte("other"))
	if !errors.Is(err, ErrDifferent) || fault.Retryable(err) {
		t.Fatalf("other bytes at the key: %v (retryable %v)", err, fault.Retryable(err))
	}
	if got := string(bucket.objects["/logs/"+key]); got != "frame" {
		t.Fatalf("the bucket holds %q", got)
	}
	// A pod's handle, escaped into one segment of the key: its ':' and '%'
	// are signed as S3 reads them, not escaped twice.
	pod := store.Key("job", "uid", "g1-a0", url.PathEscape("job:tenant-a/train"), "00000000.log.zst")
	if err := store.Put(ctx, pod, "application/zstd", []byte("pod")); err != nil {
		t.Fatalf("a key with a pod's handle: %v", err)
	}
	if got := string(bucket.objects["/logs/zenthros/job/uid/g1-a0/job:tenant-a%2Ftrain/00000000.log.zst"]); got != "pod" {
		t.Fatalf("the bucket holds %q for the pod's key; it holds %v", got, bucket.objects)
	}
	wrong, err := Open(ctx, Config{Destination: "s3://logs", Endpoint: endpoint, Region: "eu-west-3", CAData: ca,
		Credentials: map[string]string{EnvAccessKeyID: "AKID", EnvSecretAccessKey: "not it"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.Put(ctx, "x", "application/zstd", []byte("x")); fault.Of(err) != fault.Unauthorized {
		t.Fatalf("a wrong secret: %v (%s)", err, fault.Of(err))
	}
}

// What was written is read back, as a person's zctl reads a log's bucket:
// a listing page after page, and each object, signed like a write; one
// that is not there is absent.
func TestWhatWasWrittenIsReadBack(t *testing.T) {
	ctx := context.Background()
	bucket := newFakeBucket(false, verifySigV4(t, "AKID", "secret", "us-east-1"))
	endpoint, ca := serve(t, bucket)
	store, err := Open(ctx, Config{Destination: "s3://logs/zenthros", Endpoint: endpoint, CAData: ca,
		Credentials: map[string]string{EnvAccessKeyID: "AKID", EnvSecretAccessKey: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	written := map[string]string{}
	for i := 0; i < 5; i++ {
		key := store.Key("job", "uid", "g1-a0", url.PathEscape("job:ns/pod"), fmt.Sprintf("%08d.log.zst", i))
		written[key] = fmt.Sprintf("object %d", i)
		if err := store.Put(ctx, key, "application/zstd", []byte(written[key])); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Put(ctx, store.Key("other", "x"), "application/zstd", []byte("x")); err != nil {
		t.Fatal(err)
	}
	objects, err := store.List(ctx, store.Key("job", "uid")+"/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 5 {
		t.Fatalf("listed %v, want the 5 objects under job/uid/", objects)
	}
	for _, object := range objects {
		got, err := store.Get(ctx, object.Key)
		if err != nil || string(got) != written[object.Key] || object.Modified.IsZero() {
			t.Fatalf("%s (written %s) reads %q (%v), want %q", object.Key, object.Modified, got, err, written[object.Key])
		}
	}
	if _, err := store.Get(ctx, store.Key("job", "none")); fault.Of(err) != fault.Absent {
		t.Fatalf("a key with nothing at it: %v (%s)", err, fault.Of(err))
	}
}

// What the bucket answers says whether to try again: a missing bucket or a
// refused credential never succeed; a busy or broken server may.
func TestARefusalSaysWhetherToTryAgain(t *testing.T) {
	ctx := context.Background()
	bucket := newFakeBucket(false, func(*http.Request, []byte) bool { return true })
	endpoint, ca := serve(t, bucket)
	store, err := Open(ctx, Config{Destination: "s3://logs", Endpoint: endpoint, CAData: ca,
		Credentials: map[string]string{EnvAccessKeyID: "a", EnvSecretAccessKey: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	for code, retry := range map[int]bool{404: false, 403: false, 400: false, 503: true, 500: true, 429: true} {
		bucket.refuse = code
		err := store.Put(ctx, "k", "application/zstd", []byte("x"))
		if err == nil || fault.Retryable(err) != retry {
			t.Errorf("%d: %v, retryable %v, want %v", code, err, fault.Retryable(err), retry)
		}
	}
	// No answer at all: asked again.
	bucket.refuse = 0
	closed, err := Open(ctx, Config{Destination: "s3://logs", Endpoint: "https://127.0.0.1:1", Credentials: map[string]string{EnvAccessKeyID: "a", EnvSecretAccessKey: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Put(ctx, "k", "application/zstd", []byte("x")); err == nil || !fault.Retryable(err) {
		t.Fatalf("a server that does not answer: %v", err)
	}
}

// gs:// takes an OAuth2 token made from a service account's key, from the
// key's own token endpoint, and writes with Cloud Storage's precondition.
func TestCloudStorageWritesWithATokenFromTheKey(t *testing.T) {
	ctx := context.Background()
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(private)
	tokens := 0
	tokenURL, _ := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" || r.Form.Get("assertion") == "" {
			http.Error(w, "bad grant", http.StatusBadRequest)
			return
		}
		tokens++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"ya29.fake","token_type":"Bearer","expires_in":3600}`))
	}))
	bucket := newFakeBucket(true, func(r *http.Request, _ []byte) bool { return r.Header.Get("Authorization") == "Bearer ya29.fake" })
	endpoint, ca := serve(t, bucket)
	key, _ := json.Marshal(map[string]string{"type": "service_account", "client_email": "job@p.iam.gserviceaccount.com",
		"private_key_id": "k1", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"token_uri": tokenURL + "/token"})
	// httptest's servers share one certificate: ca trusts the token's too.
	store, err := Open(ctx, Config{Destination: "gs://logs/p", Endpoint: endpoint, CAData: ca, Credentials: map[string]string{EnvGoogleKey: string(key)}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.Put(ctx, store.Key("o"), "application/zstd", []byte("frame")); err != nil {
			t.Fatal(err)
		}
	}
	if tokens != 1 || bucket.puts != 2 || string(bucket.objects["/logs/p/o"]) != "frame" {
		t.Fatalf("tokens %d, puts %d, objects %v", tokens, bucket.puts, bucket.objects)
	}
}

// A destination, an endpoint and credentials a machine would not write with
// are refused before any write.
func TestWhatAMachineMustNotWriteToIsRefused(t *testing.T) {
	ctx := context.Background()
	for _, endpoint := range []string{"http://minio:9000", "https://169.254.169.254", "https://metadata.google.internal",
		"https://[fe80::1]:9000", "https://100.100.100.200", "https://user:pw@s3.example", "https://s3.example/path", "ftp://x"} {
		if err := ValidateEndpoint(endpoint); fault.Of(err) != fault.Invalid {
			t.Errorf("endpoint %s: %v", endpoint, err)
		}
	}
	if err := ValidateEndpoint("https://s3.example:9000/"); err != nil {
		t.Errorf("an S3-compatible server: %v", err)
	}
	for _, dest := range []string{"s3://", "http://b/p", "s3://b/../x", "s3://b/p?x=1", "file:///tmp"} {
		if _, err := Open(ctx, Config{Destination: dest}); fault.Of(err) != fault.Invalid {
			t.Errorf("destination %s: %v", dest, err)
		}
	}
	for name, creds := range map[string]map[string]string{
		"an unknown name":   {"PASSWORD": "x"},
		"half a key":        {EnvAccessKeyID: "a"},
		"a Google key, s3:": {EnvGoogleKey: "{}"},
		"a token alone":     {EnvSessionToken: "t"},
		"an empty value":    {EnvAccessKeyID: "", EnvSecretAccessKey: "b"},
	} {
		if _, err := Open(ctx, Config{Destination: "s3://b", Credentials: creds}); fault.Of(err) != fault.Invalid {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A name that resolves to a metadata address is not dialed either.
	_, err := (&http.Client{Transport: newTransport(nil)}).Get("https://169.254.169.254:1/")
	if fault.Of(err) != fault.Refused {
		t.Fatalf("a dial to a link-local address: %v (%s)", err, fault.Of(err))
	}
}

// A conditional PUT is sent again safely: a 409 (S3's two writes of one
// key at once) is asked again, never a refusal of the log. A credential that
// may only write (s3:PutObject alone) cannot look behind a 412: what is at
// the key is the object sent before, and the write is done.
func TestAWriteSentAgainIsNeverRefusedForWhatItCannotSee(t *testing.T) {
	ctx := context.Background()
	bucket := newFakeBucket(false, func(*http.Request, []byte) bool { return true })
	conflicts := 1
	endpoint, ca := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && conflicts > 0:
			conflicts--
			http.Error(w, "ConditionalRequestConflict", http.StatusConflict)
		case r.Method == http.MethodHead:
			http.Error(w, "AccessDenied", http.StatusForbidden) // a write-only credential
		default:
			bucket.ServeHTTP(w, r)
		}
	}))
	store, err := Open(ctx, Config{Destination: "s3://logs", Endpoint: endpoint, CAData: ca,
		Credentials: map[string]string{EnvAccessKeyID: "a", EnvSecretAccessKey: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "k", "application/zstd", []byte("frame")); err == nil || !fault.Retryable(err) {
		t.Fatalf("a 409 to a conditional PUT: %v (retryable %v), want one to send again", err, fault.Retryable(err))
	}
	if err := store.Put(ctx, "k", "application/zstd", []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "k", "application/zstd", []byte("frame")); err != nil {
		t.Fatalf("sent again with a credential that may only write: %v", err)
	}
}

// A machine's token, or a person's, is a bearer's credential: gs:// with an
// endpoint of the job's own is written only with credentials given for it,
// never with the token Cloud Storage takes.
func TestAMachinesTokenGoesOnlyToCloudStorage(t *testing.T) {
	if err := Validate("gs://b/logs", "https://storage.example", nil, nil); fault.Of(err) != fault.Invalid {
		t.Fatalf("gs:// at an endpoint with the machine's own token: %v", err)
	}
	if _, err := Open(context.Background(), Config{Destination: "gs://b/logs", Endpoint: "https://storage.example", Person: true}); fault.Of(err) != fault.Invalid {
		t.Fatalf("gs:// at an endpoint with a person's own token: %v", err)
	}
	for _, names := range [][]string{{EnvGoogleKey}, {EnvAccessKeyID, EnvSecretAccessKey}} {
		if err := Validate("gs://b/logs", "https://storage.example", nil, names); err != nil {
			t.Errorf("gs:// at an endpoint with %v: %v", names, err)
		}
	}
	if err := Validate("gs://b/logs", "", nil, nil); err != nil {
		t.Errorf("gs:// at Cloud Storage with the machine's own token: %v", err)
	}
}

// A job's output is a file sent as it is on disk, signed with its SHA-256
// (which the object keeps), over what an earlier attempt left at its key.
// Its store may be plain http, the job's own, only with the job's access
// key: a machine's own credentials go only over https (19).
func TestAFileIsSentWholeOverWhatWasThere(t *testing.T) {
	ctx := context.Background()
	bucket := newFakeBucket(false, verifySigV4(t, "AKID", "secret", "us-east-1"))
	bucket.replace = true
	server := httptest.NewServer(bucket)
	t.Cleanup(server.Close)
	keys := map[string]string{EnvAccessKeyID: "AKID", EnvSecretAccessKey: "secret"}
	if _, err := Open(ctx, Config{Destination: "s3://out/runs/r1", Endpoint: server.URL, Credentials: keys}); fault.Of(err) != fault.Invalid {
		t.Fatalf("plain http without AllowHTTP: %v", err)
	}
	if _, err := Open(ctx, Config{Destination: "s3://out/runs/r1", Endpoint: server.URL, AllowHTTP: true}); fault.Of(err) != fault.Invalid {
		t.Fatalf("plain http with the machine's own credentials: %v", err)
	}
	if _, err := Open(ctx, Config{Destination: "s3://out/r", Endpoint: "http://169.254.169.254", AllowHTTP: true, Credentials: keys}); fault.Of(err) != fault.Invalid {
		t.Fatalf("a metadata service over http: %v", err)
	}
	store, err := Open(ctx, Config{Destination: "s3://out/runs/r1", Endpoint: server.URL, AllowHTTP: true, Credentials: keys})
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("0123456789", 100_000) // 1 MB, more than one read
	for _, content := range []string{"an earlier attempt's", body} {
		file, err := os.CreateTemp(t.TempDir(), "out")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString(content); err != nil {
			t.Fatal(err)
		}
		if err := store.PutFile(ctx, store.Key("plots/a b.png"), file, int64(len(content))); err != nil {
			t.Fatal(err)
		}
		file.Close()
	}
	got := bucket.objects["/out/runs/r1/plots/a b.png"]
	if string(got) != body {
		t.Fatalf("the bucket holds %d bytes, want the last file's %d", len(got), len(body))
	}
	digest := sha256.Sum256([]byte(body))
	if bucket.meta["/out/runs/r1/plots/a b.png"] != hex.EncodeToString(digest[:]) {
		t.Fatalf("its SHA-256 is %q", bucket.meta["/out/runs/r1/plots/a b.png"])
	}
	// An empty file is an empty object, not a chunked request.
	empty, err := os.CreateTemp(t.TempDir(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if err := store.PutFile(ctx, store.Key("done"), empty, 0); err != nil {
		t.Fatal(err)
	}
	if got, ok := bucket.objects["/out/runs/r1/done"]; !ok || len(got) != 0 {
		t.Fatalf("the empty file: %q, %v", got, ok)
	}
}

// A job's own process writes its bucket where and with what the AWS SDKs
// read from its environment; an agent that writes for it reads the same:
// the S3 endpoint before the general one, the region before the default,
// and the keys it was given (19).
func TestAJobsEnvSaysWhereItsOwnProcessWrites(t *testing.T) {
	env := map[string]string{
		"AWS_ENDPOINT_URL_S3": "http://10.0.0.5:4566", "AWS_ENDPOINT_URL": "https://other",
		"AWS_DEFAULT_REGION": "us-east-1", EnvAccessKeyID: "test", EnvSecretAccessKey: "secret", "PATH": "/bin",
	}
	endpoint, region, keys := JobEnv(env)
	if endpoint != "http://10.0.0.5:4566" || region != "us-east-1" {
		t.Fatalf("endpoint %q, region %q", endpoint, region)
	}
	if len(keys) != 2 || keys[EnvAccessKeyID] != "test" || keys[EnvSecretAccessKey] != "secret" {
		t.Fatalf("keys %v", keys)
	}
	endpoint, region, keys = JobEnv(map[string]string{"AWS_ENDPOINT_URL": "https://general", "AWS_REGION": "eu-west-1",
		"AWS_DEFAULT_REGION": "us-east-1"})
	if endpoint != "https://general" || region != "eu-west-1" || len(keys) != 0 {
		t.Fatalf("endpoint %q, region %q, keys %v", endpoint, region, keys)
	}
}
