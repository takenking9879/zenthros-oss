// SPDX-License-Identifier: Apache-2.0

// Package objectput writes objects to a bucket of the user's from a machine:
// a job's log, as the agent saves it (agent/logpersist). It only writes, each
// object once: a PUT that succeeds only where nothing is yet (If-None-Match:
// * on S3, x-goog-if-generation-match: 0 on Cloud Storage), so an object sent
// again after an agent restart is never overwritten, and a 412 over the same
// bytes (their SHA-256, kept with the object) counts as written.
//
// s3:// is Amazon S3 or any S3-compatible endpoint, signed with Signature V4:
// with the keys given, or with the machine's own role. gs:// is Cloud Storage's
// XML API, with an OAuth2 token: from a service account's key, or from the
// machine's own account; or signed with an HMAC key, as for s3://. A token
// is a bearer's credential: the machine's own, or a person's, goes to Cloud
// Storage only, never to an endpoint of the job's.
//
// A credential that may only write (s3:PutObject alone) is enough: a 412
// it cannot look behind is the object sent before.
package objectput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/urlredact"
)

// The credentials a bucket is written with, by the names a job's environment
// gives them (the SDK's machine_access): an access key, with its session
// token when it is temporary, or a Cloud Storage service account's key.
const (
	EnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	EnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	EnvSessionToken    = "AWS_SESSION_TOKEN"
	EnvGoogleKey       = "GOOGLE_STORE_KEY"
)

// CredentialNames are the names Config.Credentials may hold.
var CredentialNames = []string{EnvAccessKeyID, EnvSecretAccessKey, EnvSessionToken, EnvGoogleKey}

// cloudStorage is Cloud Storage's XML API, where gs:// is written.
const cloudStorage = "https://storage.googleapis.com"

// storageScope is what a Cloud Storage token may do: write objects.
const storageScope = "https://www.googleapis.com/auth/devstorage.read_write"

// putTimeout bounds one PUT: an object is ~1 MiB.
const putTimeout = 2 * time.Minute

// ErrDifferent is a 412 over other bytes: another object is already at the
// key, and nothing writes over it.
var ErrDifferent = errors.New("another object is already at this key")

func init() { fault.Register(ErrDifferent, fault.Refused) }

// Config is where objects go and with what.
type Config struct {
	// Destination is s3://bucket/prefix or gs://bucket/prefix.
	Destination string
	// Endpoint is an S3-compatible server, https://host[:port]. Empty is
	// Amazon S3 in Region for s3://, Cloud Storage for gs://.
	Endpoint string
	// Region signs for s3://; empty is us-east-1.
	Region string
	// CAData is PEM certificates trusted for Endpoint besides the system's:
	// a server of the user's own with a private CA.
	CAData []byte
	// Credentials, by CredentialNames. Empty: the machine's own identity.
	Credentials map[string]string
	// Person reads from a person's computer (zctl logs --to): without
	// Credentials, their own default ones (for gs://, Google's application
	// default credentials), not a machine's metadata server.
	Person bool
}

// Store writes objects under one bucket's prefix.
type Store struct {
	scheme, bucket, prefix string
	endpoint, region       string
	client                 *http.Client
	authorize              func(ctx context.Context, req *http.Request, sum string) error
	now                    func() time.Time
}

// Validate refuses a destination, an endpoint, a CA or credentials' names a
// machine would not write with; names is what Credentials will hold. The
// job's spec is checked with it as it is sent and as its agent receives it.
func Validate(destination, endpoint string, caData []byte, names []string) error {
	scheme, _, _, err := ParseDestination(destination)
	if err != nil {
		return err
	}
	if err := ValidateEndpoint(endpoint); err != nil {
		return err
	}
	if len(caData) > 0 && !x509.NewCertPool().AppendCertsFromPEM(caData) {
		return fault.Wrap(fault.Invalid, "caData", errors.New("no PEM certificate in it"))
	}
	has := map[string]bool{}
	for _, name := range names {
		if !known(name) {
			return fault.Wrap(fault.Invalid, "credentials", fmt.Errorf("%q is not a bucket's credential (want %s)", name, strings.Join(CredentialNames, ", ")))
		}
		has[name] = true
	}
	switch {
	case scheme == "gs" && endpoint != "" && !has[EnvGoogleKey] && !has[EnvAccessKeyID]:
		return fault.Wrap(fault.Invalid, "endpoint", fmt.Errorf("a gs:// endpoint of its own (%s) takes the credentials it is written with, %s or an HMAC key: "+
			"the machine's own token goes only to Cloud Storage", urlredact.URL(endpoint), EnvGoogleKey))
	case has[EnvGoogleKey] && scheme != "gs":
		return fault.Wrap(fault.Invalid, "credentials", fmt.Errorf("%s is for gs://, not %s://", EnvGoogleKey, scheme))
	case has[EnvGoogleKey] && (has[EnvAccessKeyID] || has[EnvSecretAccessKey]):
		return fault.Wrap(fault.Invalid, "credentials", fmt.Errorf("%s or an access key, not both", EnvGoogleKey))
	case has[EnvAccessKeyID] != has[EnvSecretAccessKey], has[EnvSessionToken] && !has[EnvAccessKeyID]:
		return fault.Wrap(fault.Invalid, "credentials", fmt.Errorf("%s and %s go together, and %s with them", EnvAccessKeyID, EnvSecretAccessKey, EnvSessionToken))
	}
	return nil
}

// Open checks cfg and readies its credentials. It reaches nothing: a bad
// credential shows on the first Put.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	names := make([]string, 0, len(cfg.Credentials))
	for name, value := range cfg.Credentials {
		if value == "" {
			return nil, fault.Wrap(fault.Invalid, "credentials", fmt.Errorf("%s is empty", name))
		}
		names = append(names, name)
	}
	if err := Validate(cfg.Destination, cfg.Endpoint, cfg.CAData, names); err != nil {
		return nil, err
	}
	scheme, bucket, prefix, _ := ParseDestination(cfg.Destination)
	s := &Store{scheme: scheme, bucket: bucket, prefix: prefix, endpoint: strings.TrimRight(cfg.Endpoint, "/"),
		region: cfg.Region, now: time.Now}
	if s.region == "" {
		s.region = "us-east-1"
	}
	if s.endpoint == "" && scheme == "gs" {
		s.endpoint, s.region = cloudStorage, "auto"
	}
	transport := newTransport(cfg.CAData)
	s.client = &http.Client{Transport: transport, Timeout: putTimeout,
		// A redirect would carry the signature elsewhere.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	key, keyed := cfg.Credentials[EnvGoogleKey]
	id, secret := cfg.Credentials[EnvAccessKeyID], cfg.Credentials[EnvSecretAccessKey]
	switch {
	case keyed:
		// The token is asked for long after Open, so not with its ctx; and
		// through the transport that trusts CAData (a test's token server).
		tokenCtx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport, Timeout: 30 * time.Second})
		creds, err := google.CredentialsFromJSON(tokenCtx, []byte(key), storageScope)
		if err != nil {
			return nil, fault.Wrap(fault.Invalid, "credentials", fmt.Errorf("%s: %w", EnvGoogleKey, err))
		}
		s.authorize = bearer(creds.TokenSource)
	case id != "":
		s.authorize = s.sigV4(credentials.NewStaticCredentialsProvider(id, secret, cfg.Credentials[EnvSessionToken]))
	case scheme == "gs" && cfg.Person:
		found, err := google.FindDefaultCredentials(ctx, storageScope)
		if err != nil {
			return nil, fault.Wrap(fault.Unauthorized, "credentials", fmt.Errorf("no Google credentials here for %s: "+
				"gcloud auth application-default login: %w", cfg.Destination, err))
		}
		s.authorize = bearer(found.TokenSource)
	case scheme == "gs":
		// The machine's own account, from its metadata server.
		s.authorize = bearer(google.ComputeTokenSource("", storageScope))
	default:
		// The machine's own role: its instance profile, or what its
		// environment names.
		loaded, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(s.region))
		if err != nil {
			return nil, fmt.Errorf("the machine's own credentials: %w", err)
		}
		s.authorize = s.sigV4(loaded.Credentials)
	}
	return s, nil
}

func known(name string) bool {
	for _, n := range CredentialNames {
		if n == name {
			return true
		}
	}
	return false
}

// ParseDestination is a destination's scheme, bucket and prefix.
func ParseDestination(destination string) (scheme, bucket, prefix string, err error) {
	u, err := url.Parse(destination)
	if err != nil || (u.Scheme != "s3" && u.Scheme != "gs") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", "", "", fault.Wrap(fault.Invalid, "destination", fmt.Errorf("%q: want s3://bucket/prefix or gs://bucket/prefix", destination))
	}
	prefix = strings.Trim(u.Path, "/")
	for _, part := range strings.Split(prefix, "/") {
		if part == "." || part == ".." {
			return "", "", "", fault.Wrap(fault.Invalid, "destination", fmt.Errorf("%q: a prefix of . or ..", destination))
		}
	}
	return u.Scheme, u.Host, prefix, nil
}

// ValidateEndpoint refuses an endpoint a machine must not write to: one not
// https, or a metadata service (the machine's own credentials live there).
// Empty is the scheme's own service.
func ValidateEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return fault.Wrap(fault.Invalid, "endpoint", fmt.Errorf("%q: want https://host[:port]", urlredact.URL(endpoint)))
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if metadataHost(host) {
		return fault.Wrap(fault.Invalid, "endpoint", fmt.Errorf("%q is a metadata service", host))
	}
	if ip := net.ParseIP(host); ip != nil && metadataIP(ip) {
		return fault.Wrap(fault.Invalid, "endpoint", fmt.Errorf("%s is a link-local address, where metadata services answer", host))
	}
	return nil
}

// metadataHost is a name the clouds' metadata services answer on.
func metadataHost(host string) bool {
	switch host {
	case "metadata", "metadata.google.internal", "instance-data", "instance-data.ec2.internal", "metadata.azure.com":
		return true
	}
	return false
}

// metadataIP is a link-local address (169.254.169.254, fd00:ec2::254 aside,
// where metadata services answer) or Alibaba's 100.100.100.200.
func metadataIP(ip net.IP) bool {
	return ip.IsLinkLocalUnicast() || ip.Equal(net.ParseIP("fd00:ec2::254")) || ip.Equal(net.ParseIP("100.100.100.200"))
}

// newTransport dials no metadata address, whatever a name resolves to, and
// trusts caData besides the system's roots.
func newTransport(caData []byte) *http.Transport {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if ip := net.ParseIP(host); ip != nil && metadataIP(ip) {
				return fault.Wrap(fault.Refused, "dial", fmt.Errorf("%s is a link-local address, where metadata services answer", host))
			}
			return nil
		}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext
	if len(caData) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		// Validate checked it holds one.
		pool.AppendCertsFromPEM(caData)
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return transport
}

func (s *Store) sigV4(provider aws.CredentialsProvider) func(context.Context, *http.Request, string) error {
	cache := aws.NewCredentialsCache(provider)
	// The path is the key escaped once (objectURL), as S3 reads it in the
	// canonical request: escaped again, a ':' or '%' fails the signature.
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	return func(ctx context.Context, req *http.Request, sum string) error {
		creds, err := cache.Retrieve(ctx)
		if err != nil {
			return fault.Wrap(fault.Unauthorized, "credentials", err)
		}
		req.Header.Set("X-Amz-Content-Sha256", sum)
		return signer.SignHTTP(ctx, creds, req, sum, "s3", s.region, s.now())
	}
}

func bearer(source oauth2.TokenSource) func(context.Context, *http.Request, string) error {
	source = oauth2.ReuseTokenSource(nil, source)
	return func(_ context.Context, req *http.Request, _ string) error {
		token, err := source.Token()
		if err != nil {
			// The metadata server or the token endpoint: unreachable now, or
			// a key it refuses. Asked again.
			return fmt.Errorf("a Cloud Storage token: %w", err)
		}
		token.SetAuthHeader(req)
		return nil
	}
}

// Key is the object key of parts under the destination's prefix.
func (s *Store) Key(parts ...string) string {
	return strings.Trim(strings.Join(append([]string{s.prefix}, parts...), "/"), "/")
}

// URI is the object's s3:// or gs:// name.
func (s *Store) URI(key string) string { return s.scheme + "://" + s.bucket + "/" + key }

// objectURL addresses the object path-style on an endpoint, and virtual-host
// style on Amazon S3.
func (s *Store) objectURL(key string) string {
	escaped := escapeKey("/" + key)
	if s.endpoint == "" {
		return "https://" + s.bucket + ".s3." + s.region + ".amazonaws.com" + escaped
	}
	return s.endpoint + "/" + s.bucket + escaped
}

// escapeKey writes a key as S3 and Cloud Storage read it in a signed
// request: each byte but the unreserved ones and '/' as %XX, once.
func escapeKey(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || strings.IndexByte("-_.~/", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// metaHeader is the header the object's SHA-256 is kept in.
func (s *Store) metaHeader() string {
	if s.scheme == "gs" {
		return "X-Goog-Meta-Sha256"
	}
	return "X-Amz-Meta-Sha256"
}

// Put writes body at key, once: written already with the same bytes is
// written. An error says by its kind (pkg/fault) whether to try again: a
// missing bucket, a refused credential or ErrDifferent never succeed.
func (s *Store) Put(ctx context.Context, key, contentType string, body []byte) error {
	digest := sha256.Sum256(body)
	sum := hex.EncodeToString(digest[:])
	code, err := s.do(ctx, http.MethodPut, key, body, sum, func(req *http.Request) {
		req.Header.Set("Content-Type", contentType)
		req.Header.Set(s.metaHeader(), sum)
		if s.scheme == "gs" {
			req.Header.Set("X-Goog-If-Generation-Match", "0")
		} else {
			req.Header.Set("If-None-Match", "*")
		}
	})
	if err != nil || code != http.StatusPreconditionFailed {
		return err
	}
	// Something is at the key: this object, sent before.
	_, err = s.do(ctx, http.MethodHead, key, nil, emptySum, func(*http.Request) {}, func(resp *http.Response) error {
		if resp.Header.Get(s.metaHeader()) != sum {
			return fmt.Errorf("%w: %s", ErrDifferent, s.URI(key))
		}
		return nil
	})
	if fault.Of(err) == fault.Unauthorized {
		// A credential that may only write cannot look: what is at the key
		// is this object, sent before without an answer (keys are a run's
		// own). Each object vouches for its bytes, which its readers check.
		return nil
	}
	return err
}

// maxRead bounds what Get and one listing page read.
const maxRead = 64 << 20

// Get reads the object at key; one with nothing at it is fault.Absent.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	var out []byte
	_, err := s.do(ctx, http.MethodGet, key, nil, emptySum, func(*http.Request) {}, func(resp *http.Response) error {
		var err error
		out, err = ReadBounded(resp.Body, s.URI(key))
		return err
	})
	return out, err
}

// Object is a key a listing gave, and when its object was written.
type Object struct {
	Key      string
	Modified time.Time `xml:"LastModified"`
}

// List is every object under prefix (a key's start, "" the bucket's), in
// the order the bucket gives them: ListObjectsV2, page by page, which S3 and
// Cloud Storage's XML API both answer.
func (s *Store) List(ctx context.Context, prefix string) ([]Object, error) {
	var objects []Object
	after := ""
	for {
		query := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if after != "" {
			query.Set("continuation-token", after)
		}
		var page []Object
		_, err := s.send(ctx, http.MethodGet, s.objectURL("")+"?"+strings.ReplaceAll(query.Encode(), "+", "%20"), s.URI(prefix),
			nil, emptySum, func(*http.Request) {}, func(resp *http.Response) error {
				var err error
				page, after, err = ReadListing(resp.Body, s.URI(prefix))
				return err
			})
		if err != nil {
			return nil, err
		}
		objects = append(objects, page...)
		if after == "" {
			return objects, nil
		}
	}
}

// ReadListing is one page of a ListObjectsV2 answer of uri, its objects
// bucket-relative, and the token of the next page ("" at the end).
func ReadListing(body io.Reader, uri string) ([]Object, string, error) {
	raw, err := ReadBounded(body, uri)
	if err != nil {
		return nil, "", err
	}
	var page struct {
		Contents []Object
		More     bool   `xml:"IsTruncated"`
		Next     string `xml:"NextContinuationToken"`
	}
	if err := xml.Unmarshal(raw, &page); err != nil {
		return nil, "", fault.Wrap(fault.Invalid, "list", fmt.Errorf("%s: not a listing: %w", uri, err))
	}
	if !page.More {
		page.Next = ""
	}
	return page.Contents, page.Next, nil
}

// emptySum is the SHA-256 of no body, what a GET signs.
var emptySum = func() string {
	digest := sha256.Sum256(nil)
	return hex.EncodeToString(digest[:])
}()

// ReadBounded reads an object's or a listing's answer, refusing one past
// 64 MiB: a log's object is ~1 MiB.
func ReadBounded(body io.Reader, uri string) ([]byte, error) {
	out, err := io.ReadAll(io.LimitReader(body, maxRead+1))
	if err == nil && len(out) > maxRead {
		err = fault.Wrap(fault.Invalid, "read", fmt.Errorf("%s: more than %d MiB", uri, maxRead>>20))
	}
	return out, err
}

// do sends one request for key and answers its status: 2xx, or the 412 a
// conditional PUT gets, are answers; anything else is an error classified by
// its status (fault.FromHTTP). check, when given, reads a 2xx answer.
func (s *Store) do(ctx context.Context, method, key string, body []byte, sum string, prepare func(*http.Request), check ...func(*http.Response) error) (int, error) {
	return s.send(ctx, method, s.objectURL(key), s.URI(key), body, sum, prepare, check...)
}

// send is do for a target URL; uri names it in errors.
func (s *Store) send(ctx context.Context, method, target, uri string, body []byte, sum string, prepare func(*http.Request), check ...func(*http.Response) error) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.ContentLength = int64(len(body))
	prepare(req)
	if err := s.authorize(ctx, req, sum); err != nil {
		return 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, fault.Wrap(fault.Canceled, method, ctx.Err())
		}
		// Not answered: unknown, which a conditional PUT may send again.
		return 0, urlredact.Error(err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		for _, c := range check {
			if err := c(resp); err != nil {
				return resp.StatusCode, err
			}
		}
		return resp.StatusCode, nil
	case resp.StatusCode == http.StatusPreconditionFailed && method == http.MethodPut:
		return resp.StatusCode, nil
	}
	said, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	// Not a create: a conditional PUT is sent again safely, and its 409 (two
	// writes of the key at once) is a conflict, asked again.
	return resp.StatusCode, fault.FromHTTP(resp.StatusCode, false,
		fmt.Errorf("%s %s: %s: %s", method, uri, resp.Status, strings.TrimSpace(string(said))))
}
