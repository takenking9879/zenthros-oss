// SPDX-License-Identifier: Apache-2.0

package runtimegate

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestGateClosesUntilReady(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvReadyFile, filepath.Join(dir, "ready"))
	t.Setenv(EnvPendingFile, filepath.Join(dir, "pending"))
	if !Ready() {
		t.Fatal("with no markers nothing is being prepared: ready")
	}
	if err := MarkPending(); err != nil {
		t.Fatal(err)
	}
	if Ready() {
		t.Fatal("pending marker present: not ready")
	}
	if err := MarkReady(); err != nil {
		t.Fatal(err)
	}
	if !Ready() {
		t.Fatal("ready marker written: ready")
	}
	// A second preparation (a prewarm toolchain) closes it again.
	if err := MarkPending(); err != nil {
		t.Fatal(err)
	}
	if Ready() {
		t.Fatal("MarkPending must remove the old ready marker")
	}
}

// A preparation that failed keeps the gate closed and says why, until the next
// one: the hub reads it to tell whoever is waiting for the machine.
func TestAFailureKeepsTheGateClosedAndSaysWhy(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvReadyFile, filepath.Join(dir, "ready"))
	t.Setenv(EnvPendingFile, filepath.Join(dir, "pending"))
	if Failure() != "" {
		t.Fatal("nothing failed yet")
	}
	if err := MarkPending(); err != nil {
		t.Fatal(err)
	}
	if err := MarkFailed("adding pip==23.0 would change the image's pip"); err != nil {
		t.Fatal(err)
	}
	if Ready() {
		t.Fatal("a failed preparation opened the gate")
	}
	if got := Failure(); got != "adding pip==23.0 would change the image's pip" {
		t.Fatalf("Failure() = %q", got)
	}
	if err := MarkFailed(strings.Repeat("x", 3*maxFailure)); err != nil {
		t.Fatal(err)
	}
	if got := len(Failure()); got != maxFailure {
		t.Fatalf("a heartbeat would carry %d bytes of it", got)
	}
	if err := MarkPending(); err != nil {
		t.Fatal(err)
	}
	if Failure() != "" {
		t.Fatal("a new preparation still says the last one's failure")
	}
}
