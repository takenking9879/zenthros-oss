// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// It gives back what was packed, and fails on what is not zstd or is cut
// short: an installer must not run half an agent.
func TestUnpackGivesBackWhatWasPacked(t *testing.T) {
	body := make([]byte, 3<<20)
	if _, err := rand.Read(body[:1<<20]); err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatal(err)
	}
	packed := enc.EncodeAll(body, nil)
	var out bytes.Buffer
	if err := unpack(&out, bytes.NewReader(packed)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), body) {
		t.Fatal("unpacked bytes differ")
	}
	if err := unpack(&bytes.Buffer{}, bytes.NewReader(packed[:len(packed)/2])); err == nil {
		t.Error("a cut-short file unpacked")
	}
	if err := unpack(&bytes.Buffer{}, bytes.NewReader([]byte("#!/bin/sh\necho not zstd\n"))); err == nil {
		t.Error("a file that is not zstd unpacked")
	}
}
