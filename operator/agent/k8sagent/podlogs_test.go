// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/internal/testutil"
	"github.com/zenthros/operator/pkg/backend"
)

// testCA makes a CA as the chart does, and certificates of it: the reader's
// (server), the agent's (client CN) and one of another name.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "zenthros-node-logs"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue writes name's certificate and key, and the CA, to dir as prefix.crt,
// prefix.key and ca.crt.
func (ca *testCA) issue(t *testing.T, dir, prefix, name string, dns ...string) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, DNSNames: dns,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	for name, data := range map[string][]byte{
		prefix + ".crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		prefix + ".key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		"ca.crt":        ca.pem,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

const testPodUID = "0b5f6c56-1d2e-4c8f-9a3b-2f4e6d8c0a1b"

// nodeReaderCluster is a cluster with a reader on node-1, serving root,
// and a running pod on that node whose container's log is p.
type nodeReaderCluster struct {
	e    *Executor
	pod  *corev1.Pod
	log  *podLog
	ca   *testCA
	addr string
}

func newNodeReaderCluster(t *testing.T, ready bool) *nodeReaderCluster {
	t.Helper()
	root := t.TempDir()
	ca := newTestCA(t)
	serverDir, clientDir := t.TempDir(), t.TempDir()
	ca.issue(t, serverDir, "server", NodeLogReaderName, NodeLogReaderName)
	ca.issue(t, clientDir, "client", NodeLogClientName)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- RunNodeLogReader(ctx, NodeLogReaderConfig{
			Listen: fmt.Sprintf("127.0.0.1:%d", port), Health: "127.0.0.1:0", Root: root, Dir: serverDir,
		}, logr.Discard())
	}()
	t.Cleanup(func() {
		cancel()
		<-served
	})
	// The reader listens in its goroutine: a dial before it does is refused.
	testutil.Eventually(t, "the node's reader listening", func() bool {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			conn.Close()
		}
		return err == nil
	})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "zj-0", Namespace: "tenant-default", UID: types.UID(testPodUID)},
		Spec:       corev1.PodSpec{NodeName: "node-1", RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "main"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}},
	}
	condition := corev1.ConditionFalse
	if ready {
		condition = corev1.ConditionTrue
	}
	reader := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "reader-x", Namespace: "zenthros-node-logs", Labels: map[string]string{NodeLogReaderLabel: NodeLogReaderComponent}},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status:     corev1.PodStatus{PodIP: "127.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: condition}}},
	}
	e := newTestExecutor(t, pod, reader)
	readers, err := NewNodeLogReaders("zenthros-node-logs", port, clientDir)
	if err != nil {
		t.Fatal(err)
	}
	readers.creds = readers.creds.Clone()
	e.NodeLogs = readers
	e.clientset = (&apiStreams{}).clientset()

	dir := filepath.Join(root, "tenant-default_zj-0_"+testPodUID, "main")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return &nodeReaderCluster{e: e, pod: pod, log: &podLog{t: t, dir: dir, at: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)}, ca: ca, addr: fmt.Sprintf("127.0.0.1:%d", port)}
}

// readChunks reads chunks to their end, or fails after a timeout.
func readChunks(t *testing.T, chunks <-chan backend.LogChunk) ([]backend.LogChunk, string) {
	t.Helper()
	var got []backend.LogChunk
	var b strings.Builder
	timeout := time.After(20 * time.Second)
	for {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				return got, b.String()
			}
			got = append(got, chunk)
			b.Write(chunk.Data)
		case <-timeout:
			t.Fatalf("after 20 s the read had not ended: %q", b.String())
		}
	}
}

// A pod's log is read from its node's files, through the node's reader:
// the history, then what the container writes, to what it wrote right
// before it ended. The end comes from the pod, and a last read after it
// takes what the stream had not brought yet.
func TestAPodsLogIsReadThroughItsNodesReader(t *testing.T) {
	c := newNodeReaderCluster(t, true)
	c.log.write(0, lines(0, 10)...)
	ctx := context.Background()
	if _, got := readChunks(t, mustLogs(t, c.e, ctx, backend.LogRequest{TailLines: -1})); got != text(0, 10) {
		t.Fatalf("read %q", got)
	}

	chunks := mustLogs(t, c.e, ctx, backend.LogRequest{Follow: true, TailLines: -1})
	_, first := collect(t, chunks, len(text(0, 10)))
	c.log.write(0, lines(10, 20)...)
	// It ends: what it wrote last lands as the pod says so.
	ended := c.pod.DeepCopy()
	ended.Status.Phase = corev1.PodSucceeded
	ended.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}
	c.log.write(0, lines(20, 25)...)
	if err := c.e.client.Status().Update(ctx, ended); err != nil {
		t.Fatal(err)
	}
	if _, rest := readChunks(t, chunks); first+rest != text(0, 25) {
		t.Fatalf("read %q then %q", first, rest)
	}
}

func mustLogs(t *testing.T, e *Executor, ctx context.Context, req backend.LogRequest) <-chan backend.LogChunk {
	t.Helper()
	var pod corev1.Pod
	if err := e.client.Get(ctx, types.NamespacedName{Namespace: "tenant-default", Name: "zj-0"}, &pod); err != nil {
		t.Fatal(err)
	}
	chunks, err := e.podLogs(ctx, &pod, "main", req)
	if err != nil {
		t.Fatal(err)
	}
	return chunks
}

// A read whose node has no reader ready goes through the API; one that goes
// on after a cursor of the node's reader waits for it.
func TestANodeWithoutAReadyReaderIsReadThroughTheAPI(t *testing.T) {
	c := newNodeReaderCluster(t, false)
	streams := &apiStreams{answers: []string{apiLine(time.Unix(1, 0), "through the API")}}
	c.e.clientset = streams.clientset()
	chunks := mustLogs(t, c.e, context.Background(), backend.LogRequest{TailLines: -1})
	if _, got := readChunks(t, chunks); got != "through the API\n" || len(streams.asked) != 1 {
		t.Fatalf("read %q", got)
	}
	var pod corev1.Pod
	c.e.client.Get(context.Background(), types.NamespacedName{Namespace: "tenant-default", Name: "zj-0"}, &pod)
	_, err := c.e.podLogs(context.Background(), &pod, "main", backend.LogRequest{After: nodeCursor{first: time.Unix(1, 0).UTC().Format(time.RFC3339Nano)}.String()})
	if !errors.Is(err, backend.ErrBackendUnavailable) {
		t.Fatalf("a node cursor with no reader: %v", err)
	}
}

// The reader serves the cluster's agent only: a certificate of the chart's
// CA under another name is refused, and one of no CA never connects.
func TestTheNodesReaderServesTheAgentOnly(t *testing.T) {
	c := newNodeReaderCluster(t, true)
	c.log.write(0, "secret")
	other := t.TempDir()
	c.ca.issue(t, other, "client", "someone-else")
	readers, err := NewNodeLogReaders("zenthros-node-logs", 0, other)
	if err != nil {
		t.Fatal(err)
	}
	readers.Port = c.e.NodeLogs.Port
	reader, err := readers.on(context.Background(), c.e.client, "node-1")
	if err != nil || reader == nil {
		t.Fatalf("reader %v: %v", reader, err)
	}
	_, err = openReaderStream(context.Background(), reader, &agentpb.PodLogRequest{
		Namespace: "tenant-default", Pod: "zj-0", Uid: testPodUID, Container: "main", TailLines: -1})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("another name: %v", err)
	}
}

// The reader is ready only once it can read the node's files: a node where
// it may not is read through the API.
func TestTheNodesReaderIsReadyOnlyWhenItCanRead(t *testing.T) {
	root := t.TempDir()
	cfg := NodeLogReaderConfig{Root: root, Namespace: "zenthros-node-logs", Pod: "reader-x", UID: testPodUID, Container: "reader"}
	dir := filepath.Join(root, "zenthros-node-logs_reader-x_"+testPodUID, "reader")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0.log"), []byte("x stdout F up\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := canRead(cfg); err != nil {
		t.Fatalf("a readable log: %v", err)
	}
	if err := os.Chmod(filepath.Join(dir, "0.log"), 0); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 && canRead(cfg) == nil {
		t.Fatal("ready with a log it may not read")
	}
}

func apiLine(at time.Time, text string) string {
	return at.UTC().Format(time.RFC3339Nano) + " " + text + "\n"
}

// apiStreams answers the API's log reads in turn, and records each read's
// options.
type apiStreams struct {
	mu      sync.Mutex
	answers []string
	asked   []*corev1.PodLogOptions
}

func (s *apiStreams) answer(opts *corev1.PodLogOptions) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, opts)
	answer := ""
	if len(s.answers) > 0 {
		answer, s.answers = s.answers[0], s.answers[1:]
	}
	return answer
}

// clientset is a clientset whose pod logs are s's answers.
func (s *apiStreams) clientset() kubernetes.Interface {
	return scriptedLogs{Interface: kubefake.NewSimpleClientset(), streams: s}
}

type scriptedLogs struct {
	kubernetes.Interface
	streams *apiStreams
}

func (c scriptedLogs) CoreV1() corev1client.CoreV1Interface {
	return scriptedCore{CoreV1Interface: c.Interface.CoreV1(), streams: c.streams}
}

type scriptedCore struct {
	corev1client.CoreV1Interface
	streams *apiStreams
}

func (c scriptedCore) Pods(namespace string) corev1client.PodInterface {
	return scriptedPods{PodInterface: c.CoreV1Interface.Pods(namespace), streams: c.streams}
}

type scriptedPods struct {
	corev1client.PodInterface
	streams *apiStreams
}

func (p scriptedPods) GetLogs(name string, opts *corev1.PodLogOptions) *restclient.Request {
	answer := p.streams.answer(opts)
	rest := &restfake.RESTClient{
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
		Client: restfake.CreateHTTPClient(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(answer))}, nil
		}),
	}
	return rest.Get().Resource("pods").Name(name).SubResource("log")
}

// The kubelet closes a followed stream opened as the container starts,
// before any line (P12): the read opens it again after the last line it
// gave, skips what that second brings again, and ends when the pod does.
func TestAnAPIReadGoesOnWhenTheKubeletClosesItsStream(t *testing.T) {
	c := newNodeReaderCluster(t, false)
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 500, time.UTC)
	streams := &apiStreams{answers: []string{
		apiLine(t0, "one"),
		"", // closed at once
		apiLine(t0, "one") + apiLine(t0.Add(time.Millisecond), "two"),
	}}
	c.e.clientset = streams.clientset()
	chunks := mustLogs(t, c.e, context.Background(), backend.LogRequest{Follow: true, TailLines: -1})
	_, got := collect(t, chunks, len("one\ntwo\n"))
	if got != "one\ntwo\n" {
		t.Fatalf("read %q", got)
	}
	ended := c.pod.DeepCopy()
	ended.Status.Phase = corev1.PodSucceeded
	if err := c.e.client.Status().Update(context.Background(), ended); err != nil {
		t.Fatal(err)
	}
	if _, rest := readChunks(t, chunks); rest != "" {
		t.Fatalf("after the end: %q", rest)
	}
	streams.mu.Lock()
	defer streams.mu.Unlock()
	if last := streams.asked[len(streams.asked)-1]; last.SinceTime == nil || !last.Timestamps {
		t.Fatalf("opened again with %+v", last)
	}
}

// Through the API the history is only the kubelet's latest file (P14): one
// that starts at that file's start says an earlier part may be missing; one
// cut within it says how much it left out; an empty log says nothing. A
// read after an API cursor skips what it gave.
func TestAnAPIHistorySaysWhatItCannotShow(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	file := apiLine(t0, "aaaa") + apiLine(t0.Add(time.Millisecond), "bbbb") + apiLine(t0.Add(2*time.Millisecond), "cccc")
	for _, tc := range []struct {
		name    string
		answer  string
		req     backend.LogRequest
		want    string
		omitted int64
	}{
		{"the whole file", file, backend.LogRequest{TailLines: -1}, "aaaa\nbbbb\ncccc\n", backend.Uncounted},
		{"cut within it", file, backend.LogRequest{TailLines: -1, HistoryBytes: 10}, "bbbb\ncccc\n", 5},
		{"empty", "", backend.LogRequest{TailLines: -1}, "", 0},
		{"lines", apiLine(t0, "cccc"), backend.LogRequest{TailLines: 1}, "cccc\n", backend.Uncounted},
		{"after a cursor", file, backend.LogRequest{After: apiCursorPrefix + t0.Add(time.Millisecond).Format(time.RFC3339Nano)}, "cccc\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newNodeReaderCluster(t, false)
			c.e.clientset = (&apiStreams{answers: []string{tc.answer}}).clientset()
			got, read := readChunks(t, mustLogs(t, c.e, context.Background(), tc.req))
			if read != tc.want {
				t.Fatalf("read %q", read)
			}
			if len(got) > 0 && got[0].Omitted != tc.omitted {
				t.Fatalf("omitted %d, want %d", got[0].Omitted, tc.omitted)
			}
			if len(got) > 0 && !strings.HasPrefix(got[len(got)-1].Cursor, apiCursorPrefix) {
				t.Fatalf("cursor %q", got[len(got)-1].Cursor)
			}
		})
	}
}

// A history read whole and then followed, of a container that ended before
// its stream was opened again, still gets the lines written in between: a
// last read without following, after the last line it gave.
func TestAnAPIReadGetsTheLinesWrittenBeforeTheEnd(t *testing.T) {
	c := newNodeReaderCluster(t, false)
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 500, time.UTC)
	streams := &apiStreams{answers: []string{
		apiLine(t0, "one"),
		apiLine(t0, "one") + apiLine(t0.Add(time.Millisecond), "two"),
	}}
	c.e.clientset = streams.clientset()
	ended := c.pod.DeepCopy()
	ended.Status.Phase = corev1.PodSucceeded
	if err := c.e.client.Status().Update(context.Background(), ended); err != nil {
		t.Fatal(err)
	}
	if _, got := readChunks(t, mustLogs(t, c.e, context.Background(), backend.LogRequest{Follow: true, TailLines: -1})); got != "one\ntwo\n" {
		t.Fatalf("read %q", got)
	}
	streams.mu.Lock()
	defer streams.mu.Unlock()
	if last := streams.asked[len(streams.asked)-1]; last.Follow || last.SinceTime == nil {
		t.Fatalf("the last read %+v, want one after the last line, not followed", last)
	}
}

// A history with a line past a chunk's size keeps its order: the lines
// before the long one go first, and no chunk passes backend.MaxLogChunk.
func TestAnAPIHistoryKeepsItsOrderAroundALongLine(t *testing.T) {
	c := newNodeReaderCluster(t, false)
	t0 := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	var file, want strings.Builder
	add := func(at time.Time, text string) {
		file.WriteString(apiLine(at, text))
		want.WriteString(text + "\n")
	}
	for i := range 100 {
		add(t0.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("short %03d %s", i, strings.Repeat("y", 1000)))
	}
	add(t0.Add(time.Second), strings.Repeat("x", 300<<10))
	add(t0.Add(2*time.Second), "after")
	c.e.clientset = (&apiStreams{answers: []string{file.String()}}).clientset()
	chunks, got := readChunks(t, mustLogs(t, c.e, context.Background(), backend.LogRequest{TailLines: -1}))
	if got != want.String() {
		t.Fatalf("read %d bytes out of order (starting %q), want the %d of the log in order", len(got), got[:20], want.Len())
	}
	for _, chunk := range chunks {
		if len(chunk.Data) > backend.MaxLogChunk {
			t.Fatalf("a chunk of %d bytes, past %d", len(chunk.Data), backend.MaxLogChunk)
		}
	}
}
