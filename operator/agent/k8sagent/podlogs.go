// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/zenthros/operator/agent/logfile"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/backend"
)

// A pod's log is read from the files on its node, through that node's log
// reader (the chart's DaemonSet, NodeLogs): ~5 mCPU a reader, the rotated
// files too. Where the node has no reader ready (a node that runs no
// DaemonSet, a policy that refused its hostPath), or with the chart's
// logs.nodeReader false, it is read through the Kubernetes API: ~560 mCPU
// with one reader, and only the kubelet's latest file
// (benchmarks/loglab/PLAN.md, decision 10). A cursor names the way it was
// read, and goes on only that way.

// NodeLogReaders are the node log readers the agent reads pods' logs
// through: the ready pod of the DaemonSet on each node.
type NodeLogReaders struct {
	// Namespace is the readers' own.
	Namespace string
	// Port is their NodeLogs port.
	Port  int
	creds credentials.TransportCredentials

	mu sync.Mutex
	// conns is the connection to each node's reader, and its address.
	conns map[string]readerConn
}

type readerConn struct {
	addr string
	conn *grpc.ClientConn
}

// NodeLogReaderLabel is on the readers' pods.
const NodeLogReaderLabel = "app.kubernetes.io/component"

// NodeLogReaderComponent is that label's value.
const NodeLogReaderComponent = "node-log-reader"

// NewNodeLogReaders reads through the readers in namespace, on port, with
// the agent's certificate of the chart's CA in dir (client.crt, client.key,
// ca.crt).
func NewNodeLogReaders(namespace string, port int, dir string) (*NodeLogReaders, error) {
	cert, err := tls.LoadX509KeyPair(dir+"/client.crt", dir+"/client.key")
	if err != nil {
		return nil, fmt.Errorf("node log readers: %w", err)
	}
	ca, err := os.ReadFile(dir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("node log readers: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("node log readers: no certificate in %s/ca.crt", dir)
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   NodeLogReaderName,
		MinVersion:   tls.VersionTLS13,
	})
	return &NodeLogReaders{Namespace: namespace, Port: port, creds: creds, conns: map[string]readerConn{}}, nil
}

// on is the reader of node, nil when none is ready there.
func (n *NodeLogReaders) on(ctx context.Context, c client.Client, node string) (agentpb.NodeLogsClient, error) {
	if node == "" {
		return nil, nil
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(n.Namespace),
		client.MatchingLabels{NodeLogReaderLabel: NodeLogReaderComponent}, client.MatchingFields{"spec.nodeName": node}); err != nil {
		return nil, fmt.Errorf("list the node log readers on %s: %w", node, err)
	}
	addr := ""
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == node && pod.Status.PodIP != "" && podReady(&pod) {
			addr = fmt.Sprintf("%s:%d", pod.Status.PodIP, n.Port)
			break
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	known, ok := n.conns[node]
	if ok && known.addr == addr {
		return agentpb.NewNodeLogsClient(known.conn), nil
	}
	if ok {
		known.conn.Close()
		delete(n.conns, node)
	}
	if addr == "" {
		return nil, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(n.creds))
	if err != nil {
		return nil, fmt.Errorf("dial the node log reader on %s: %w", node, err)
	}
	n.conns[node] = readerConn{addr: addr, conn: conn}
	return agentpb.NewNodeLogsClient(conn), nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podLogs reads one container of pod as req asks: following, until ctx
// ends or the container ended for good.
func (e *Executor) podLogs(ctx context.Context, pod *corev1.Pod, container string, req backend.LogRequest) (<-chan backend.LogChunk, error) {
	if container == "" {
		container = pod.Spec.Containers[0].Name
	}
	var ended <-chan struct{}
	if req.Follow {
		ended = e.containerEnded(ctx, pod, container)
	}
	if e.NodeLogs != nil && !strings.HasPrefix(req.After, apiCursorPrefix) {
		// Its node and its files are there once it started (the API's
		// read waits for that itself).
		pod, err := e.waitStarted(ctx, pod, container)
		if err != nil {
			return nil, err
		}
		reader, err := e.NodeLogs.on(ctx, e.client, pod.Spec.NodeName)
		switch {
		case err != nil:
			return nil, err
		case reader != nil:
			return e.nodeLogs(ctx, pod, container, req, ended)
		case strings.HasPrefix(req.After, nodeCursorPrefix):
			return nil, backend.Classified(backend.ErrBackendUnavailable,
				fmt.Errorf("the log reader of node %s is not ready, and this read goes on only through it", pod.Spec.NodeName))
		}
	}
	return e.apiLogs(ctx, pod, container, req, ended)
}

// waitStarted is pod once its container has started (its log is there), or
// after containerStartGrace as it is then.
func (e *Executor) waitStarted(ctx context.Context, pod *corev1.Pod, container string) (*corev1.Pod, error) {
	deadline := time.Now().Add(containerStartGrace)
	for !started(pod, container) && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		var now corev1.Pod
		if err := e.client.Get(ctx, client.ObjectKeyFromObject(pod), &now); err != nil {
			return nil, fmt.Errorf("get pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if now.UID != pod.UID {
			return nil, backend.Classified(backend.ErrNotFound, fmt.Errorf("pod %s/%s is another pod now", pod.Namespace, pod.Name))
		}
		pod = &now
	}
	return pod, nil
}

func containerStatus(pod *corev1.Pod, container string) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == container {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// started is whether the container ran at all: it has a log.
func started(pod *corev1.Pod, container string) bool {
	s := containerStatus(pod, container)
	return s != nil && (s.State.Running != nil || s.State.Terminated != nil || s.LastTerminationState.Terminated != nil)
}

// done is whether the container ended and will not run again.
func done(pod *corev1.Pod, container string) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	s := containerStatus(pod, container)
	if s == nil || s.State.Terminated == nil {
		return false
	}
	switch pod.Spec.RestartPolicy {
	case corev1.RestartPolicyNever:
		return true
	case corev1.RestartPolicyOnFailure:
		return s.State.Terminated.ExitCode == 0
	}
	return false
}

// endedPoll is how often a followed read asks whether its container ended.
const endedPoll = time.Second

// containerEnded is closed once the container of pod ended for good, or the
// pod is gone: the end of a followed read comes from the pod, never from
// what the log says (P13). It stops asking when ctx ends.
func (e *Executor) containerEnded(ctx context.Context, pod *corev1.Pod, container string) <-chan struct{} {
	ended := make(chan struct{})
	go func() {
		key := client.ObjectKeyFromObject(pod)
		tick := time.NewTicker(endedPoll)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			var now corev1.Pod
			err := e.statusReader().Get(ctx, key, &now)
			if apierrors.IsNotFound(err) {
				// A cache's not found is confirmed live.
				err = e.client.Get(ctx, key, &now)
			}
			if apierrors.IsNotFound(err) || err == nil && (now.UID != pod.UID || done(&now, container)) {
				close(ended)
				return
			}
		}
	}()
	return ended
}

// statusReader is where a read asks for its pod: the status cache, when the
// agent made the pod, before the apiserver.
func (e *Executor) statusReader() client.Reader {
	if e.StatusCache != nil {
		return cachedReads{Client: e.client, cache: e.StatusCache, events: &e.events}
	}
	return e.client
}

// nodeLogs reads the container through its node's reader. A read the reader
// broke (it restarted, the connection dropped) is opened again after the
// last cursor; once the container ended, the read stops and a last one
// takes what it wrote until then.
func (e *Executor) nodeLogs(ctx context.Context, pod *corev1.Pod, container string, req backend.LogRequest, ended <-chan struct{}) (<-chan backend.LogChunk, error) {
	open := func(ctx context.Context, req backend.LogRequest) (*readerStream, error) {
		reader, err := e.NodeLogs.on(ctx, e.client, pod.Spec.NodeName)
		if err != nil {
			return nil, err
		}
		if reader == nil {
			return nil, backend.Classified(backend.ErrBackendUnavailable, fmt.Errorf("the log reader of node %s is not ready", pod.Spec.NodeName))
		}
		return openReaderStream(ctx, reader, &agentpb.PodLogRequest{
			Namespace: pod.Namespace, Pod: pod.Name, Uid: string(pod.UID), Container: container,
			Follow: req.Follow, TailLines: int32(req.TailLines), HistoryBytes: req.HistoryBytes, After: req.After,
		})
	}
	stream, err := open(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make(chan backend.LogChunk, 4)
	go func() {
		defer close(out)
		cursor := req.After
		forward := func(chunk backend.LogChunk) bool {
			select {
			case out <- chunk:
				cursor = chunk.Cursor
				return true
			case <-ctx.Done():
				return false
			}
		}
		broke := 0
		for {
			select {
			case chunk, ok := <-stream.chunks:
				if ok {
					if !forward(chunk) {
						stream.stop()
						return
					}
					broke = 0
					continue
				}
			case <-ended:
				// What it brought until now, then a last read to the end.
				stream.stop()
				for chunk := range stream.chunks {
					if !forward(chunk) {
						return
					}
				}
				e.lastRead(ctx, open, req, cursor, forward)
				return
			case <-ctx.Done():
				stream.stop()
				return
			}
			if ctx.Err() != nil || !req.Follow && stream.err == nil {
				return
			}
			select {
			case <-ended:
				e.lastRead(ctx, open, req, cursor, forward)
				return
			default:
			}
			// Broken: opened again after what it gave.
			for {
				broke++
				select {
				case <-ctx.Done():
					return
				case <-ended:
					e.lastRead(ctx, open, req, cursor, forward)
					return
				case <-time.After(time.Duration(min(broke, 5)) * time.Second):
				}
				again := req
				if cursor != "" {
					again.After = cursor
				}
				if stream, err = open(ctx, again); err == nil {
					break
				}
				e.logger.V(1).Info("opening a pod's log again", "pod", pod.Name, "error", err.Error())
			}
		}
	}()
	return out, nil
}

// lastRead reads the container's log from cursor to its end, once the
// container ended: what the runtime wrote of it a moment after.
func (e *Executor) lastRead(ctx context.Context, open func(context.Context, backend.LogRequest) (*readerStream, error), req backend.LogRequest, cursor string, forward func(backend.LogChunk) bool) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(logfile.Pace):
	}
	last := backend.LogRequest{TailLines: req.TailLines, HistoryBytes: req.HistoryBytes, After: cursor}
	stream, err := open(ctx, last)
	if err != nil {
		e.logger.V(1).Info("the last read of a pod's log", "error", err.Error())
		return
	}
	for chunk := range stream.chunks {
		if !forward(chunk) {
			stream.stop()
			return
		}
	}
}

// readerStream is one read of a node's reader: its chunks, and why it ended
// once they close.
type readerStream struct {
	chunks <-chan backend.LogChunk
	err    error
	stop   context.CancelFunc
}

// openReaderStream opens a read and waits for the reader to take it: a read
// it refuses (a cursor it does not know, a container it has no files of)
// fails here, not as an empty log.
func openReaderStream(ctx context.Context, reader agentpb.NodeLogsClient, req *agentpb.PodLogRequest) (*readerStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := reader.ReadPodLog(ctx, req)
	if err == nil {
		var md map[string][]string
		if md, err = stream.Header(); err == nil && md == nil {
			// It ended before it began: its status says why.
			if _, err = stream.Recv(); errors.Is(err, io.EOF) {
				err = errors.New("the node's log reader ended the read before it began")
			}
		}
	}
	if err != nil {
		cancel()
		return nil, err
	}
	chunks := make(chan backend.LogChunk, 4)
	r := &readerStream{chunks: chunks, stop: cancel}
	go func() {
		defer close(chunks)
		for {
			chunk, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.err = err
				}
				return
			}
			select {
			case chunks <- backend.LogChunkOf(chunk):
			case <-ctx.Done():
				r.err = ctx.Err()
				return
			}
		}
	}()
	return r, nil
}

// apiChunk is the most of the log one chunk read through the API carries.
const apiChunk = 128 << 10

// apiLogs reads the container through the Kubernetes API, which serves
// only the kubelet's latest file of its log: a history that starts at that
// file's start says an earlier part may be missing (P14). A followed read
// whose stream the kubelet closed while the container runs (it closes one
// opened as the container starts, P12) is opened again after the last line
// it gave, by the line's time; once the container ended, a last time
// without following, for the lines written since (a history is read
// whole, then followed).
func (e *Executor) apiLogs(ctx context.Context, pod *corev1.Pod, container string, req backend.LogRequest, ended <-chan struct{}) (<-chan backend.LogChunk, error) {
	r := &apiReader{omitted: backend.Uncounted}
	opts := &corev1.PodLogOptions{Container: container, Timestamps: true}
	byBytes := false
	switch {
	case req.After != "":
		ts, ok := strings.CutPrefix(req.After, apiCursorPrefix)
		at, err := time.Parse(time.RFC3339Nano, ts)
		if !ok || err != nil {
			return nil, backend.Classified(backend.ErrInvalid, fmt.Errorf("%w: %q", logfile.ErrCursor, req.After))
		}
		r.last, r.cursor, r.omitted = at, req.After, 0
		opts.SinceTime, opts.Follow = &metav1.Time{Time: at}, req.Follow
	case req.TailLines >= 0:
		n := int64(req.TailLines)
		opts.TailLines, opts.Follow = &n, req.Follow
	default:
		// The whole file, of which the history keeps the end.
		byBytes = true
	}
	stream, err := e.openLogStream(ctx, pod.Namespace, pod.Name, opts)
	if err != nil {
		return nil, fmt.Errorf("open log stream for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	out := make(chan backend.LogChunk, 4)
	go func() {
		defer close(out)
		r.out = out
		if byBytes {
			ok := r.history(ctx, stream, req.History())
			stream.Close()
			if !ok || !req.Follow {
				return
			}
		} else {
			ok := r.copy(ctx, stream)
			stream.Close()
			if !ok || !req.Follow {
				return
			}
		}
		for last := false; !last; {
			// The kubelet ends a followed stream when the container ends;
			// one that ended while it runs is opened again.
			select {
			case <-ctx.Done():
				return
			case <-ended:
				last = true
			case <-time.After(2 * endedPoll):
			}
			opts := &corev1.PodLogOptions{Container: container, Timestamps: true, Follow: !last}
			switch {
			case !r.last.IsZero():
				opts.SinceTime = &metav1.Time{Time: r.last}
			case req.After == "" && req.TailLines >= 0:
				// Nothing came yet: the history it asked for.
				n := int64(req.TailLines)
				opts.TailLines = &n
			}
			stream, err := e.openLogStream(ctx, pod.Namespace, pod.Name, opts)
			if err != nil {
				e.logger.V(1).Info("opening a pod's log again", "pod", pod.Name, "error", err.Error())
				continue
			}
			ok := r.copy(ctx, stream)
			stream.Close()
			if !ok {
				return
			}
		}
	}()
	return out, nil
}

// apiReader turns what the API sends ("<time> <line>", the time once a
// line) into chunks, each to the end of a line with its time as the cursor.
// What it already sent comes again when a stream is opened from a second
// (SinceTime has no finer): it skips every line up to the last it sent.
type apiReader struct {
	out     chan<- backend.LogChunk
	last    time.Time
	cursor  string
	omitted int64
	sent    bool
}

// apiLines calls fn with each line of r: its time and its text, a piece at
// a time for a long one (whole is whether the piece ends it, more whether
// more of r is there already). fn returning false stops it; apiLines
// returns false then, or when ctx ended. A stream that broke is a stream
// that ended: the caller opens it again.
func apiLines(ctx context.Context, r io.Reader, fn func(ts string, at time.Time, piece []byte, whole, more bool) bool) bool {
	br := bufio.NewReaderSize(r, 64<<10)
	var ts string
	var at time.Time
	inLine := false
	for {
		piece, err := br.ReadSlice('\n')
		if len(piece) > 0 {
			whole := piece[len(piece)-1] == '\n'
			if !inLine {
				stamp, text, _ := bytes.Cut(piece, []byte(" "))
				ts, piece = string(stamp), text
				at, _ = time.Parse(time.RFC3339Nano, ts)
			}
			inLine = !whole
			if !fn(ts, at, piece, whole, br.Buffered() > 0) {
				return false
			}
		}
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return ctx.Err() == nil
		}
	}
}

// copy sends what r brings, past the last line sent. It returns false when
// the read should end.
func (r *apiReader) copy(ctx context.Context, stream io.Reader) bool {
	var pending []byte
	skipping := false
	flush := func(cursor string) bool {
		if len(pending) == 0 {
			return true
		}
		chunk := backend.LogChunk{Stream: "stdout", Data: pending, Timestamp: time.Now(), Cursor: cursor}
		pending = nil
		return r.send(ctx, chunk)
	}
	return apiLines(ctx, stream, func(ts string, at time.Time, piece []byte, whole, more bool) bool {
		if !r.last.IsZero() && !at.After(r.last) {
			skipping = true
		}
		if !skipping {
			pending = append(pending, piece...)
		}
		if whole {
			if !skipping {
				r.last, r.cursor = at, apiCursorPrefix+ts
			}
			skipping = false
		}
		switch {
		case whole && !more:
			return flush(r.cursor)
		case len(pending) >= apiChunk:
			cursor := ""
			if whole {
				cursor = r.cursor
			}
			return flush(cursor)
		}
		return true
	})
}

// history reads the whole of r and sends its last keep bytes (all of them
// for backend.AllHistory), from the start of a line, then what it left out.
// A line not ended yet is left for the read that follows.
func (r *apiReader) history(ctx context.Context, stream io.Reader, keep int64) bool {
	type line struct {
		ts   string
		at   time.Time
		data []byte
	}
	var (
		kept    []line
		total   int64
		dropped int64
		cur     []byte
	)
	ok := apiLines(ctx, stream, func(ts string, at time.Time, piece []byte, whole, _ bool) bool {
		cur = append(cur, piece...)
		if !whole {
			return true
		}
		kept = append(kept, line{ts: ts, at: at, data: cur})
		total += int64(len(cur))
		cur = nil
		for keep != backend.AllHistory && total > keep && len(kept) > 0 {
			total -= int64(len(kept[0].data))
			dropped += int64(len(kept[0].data))
			kept = kept[1:]
		}
		return true
	})
	if !ok {
		return false
	}
	if dropped > 0 {
		r.omitted = dropped
	}
	// In order, each chunk to the end of a line and at most apiChunk, but
	// for the pieces of a line past backend.MaxLogLine.
	var pending []byte
	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		ok := r.send(ctx, backend.LogChunk{Stream: "stdout", Data: pending, Timestamp: time.Now(), Cursor: r.cursor})
		pending = nil
		return ok
	}
	for _, l := range kept {
		if len(pending)+len(l.data) > apiChunk && !flush() {
			return false
		}
		for len(l.data) > backend.MaxLogLine {
			if !r.send(ctx, backend.LogChunk{Stream: "stdout", Data: l.data[:backend.MaxLogLine], Timestamp: time.Now()}) {
				return false
			}
			l.data = l.data[backend.MaxLogLine:]
		}
		pending = append(pending, l.data...)
		r.last, r.cursor = l.at, apiCursorPrefix+l.ts
	}
	if !flush() {
		return false
	}
	if len(kept) == 0 && dropped == 0 {
		r.omitted = 0 // an empty log: nothing before it to miss
	}
	return true
}

func (r *apiReader) send(ctx context.Context, chunk backend.LogChunk) bool {
	if !r.sent {
		chunk.Omitted, r.sent = r.omitted, true
	}
	select {
	case r.out <- chunk:
		return true
	case <-ctx.Done():
		return false
	}
}
