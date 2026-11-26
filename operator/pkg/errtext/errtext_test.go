// SPDX-License-Identifier: Apache-2.0

package errtext

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestBoundKeepsShortTextExactly(t *testing.T) {
	msg := `pytorchjobs.kubeflow.org "x" is forbidden: User "system:serviceaccount:zenthros-system:agent" cannot patch resource`
	if got := Bound(msg, MaxDetail); got != msg {
		t.Fatalf("Bound changed a message that fits:\n got %q\nwant %q", got, msg)
	}
}

// The cause of an error is at its head: an oversized message keeps the head,
// says how much was cut, and keeps the end.
func TestBoundKeepsHead(t *testing.T) {
	head := "admission webhook \"vpytorchjob.kb.io\" denied the request: spec.pytorchReplicaSpecs.Master.replicas: Invalid value"
	msg := head + strings.Repeat(" detail", 5000) + " END"
	got := Bound(msg, 2048)
	if len(got) > 2048 {
		t.Fatalf("Bound returned %d bytes, max 2048", len(got))
	}
	if !strings.HasPrefix(got, head) {
		t.Fatalf("head lost: %q", got[:120])
	}
	if !strings.HasSuffix(got, " END") || !strings.Contains(got, "…[truncated ") {
		t.Fatalf("tail or marker missing: %q", got[len(got)-80:])
	}
}

func TestBoundStaysValidUTF8(t *testing.T) {
	msg := strings.Repeat("é", 4000)
	got := Bound(msg, 1001)
	if !utf8.ValidString(got) || len(got) > 1001 {
		t.Fatalf("invalid or oversized result: len=%d valid=%v", len(got), utf8.ValidString(got))
	}
}

// A stray byte that is not UTF-8 near the start cut the head back to it, and
// the cause with it, in time quadratic in the length. It becomes U+FFFD; the
// head keeps what fits; the result is UTF-8 whether it was cut or not. The
// time bound leaves room for -race on a loaded machine: quadratic work on
// 80 KB takes seconds.
func TestAStrayByteKeepsTheHead(t *testing.T) {
	cause := "cause: the image could not be pulled"
	s := "x\xff" + cause + strings.Repeat("é", 40<<10)
	start := time.Now()
	got := Bound(s, MaxWire)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Bound took %s", took)
	}
	if !strings.Contains(got, cause) || !utf8.ValidString(got) || len(got) > MaxWire {
		t.Fatalf("head lost or invalid: %d bytes, valid %v, starts %q", len(got), utf8.ValidString(got), got[:min(len(got), 60)])
	}
	if short := Bound("ok\xffok", MaxWire); !utf8.ValidString(short) || !strings.HasPrefix(short, "ok") {
		t.Fatalf("a short invalid message = %q", short)
	}
}
