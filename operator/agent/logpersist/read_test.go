// SPDX-License-Identifier: Apache-2.0

package logpersist

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
)

// noisy is n lines that pack little, so that a run takes several objects.
func noisy(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	var b bytes.Buffer
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "step %d %016x%016x\n", i, r.Uint64(), r.Uint64())
	}
	return b.Bytes()
}

// saveRun saves text as job's run and waits for its END.json.
func saveRun(t *testing.T, store *fakeStore, job Job, text []byte) {
	t.Helper()
	src := newFakeLog()
	s := newTestSaver(t, t.TempDir(), src, store)
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	for len(text) > 0 {
		n := min(len(text), 3000)
		src.write(text[:n])
		text = text[n:]
	}
	src.end()
	if end := waitEnd(t, store, job); end.Status != StatusComplete {
		t.Fatalf("END.json %+v", end)
	}
}

// What the bucket holds of a job's last run is read back whole, as zctl
// logs --to reads it: the highest generation, then attempt, its handle as a
// log read names it, and the trailer of its last object to go on from.
func TestASavedLogIsReadBackWhole(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	first, last := testJob("job:ns/pod-a"), testJob("job:ns/pod-b")
	first.Attempt, last.Attempt = 0, 1
	saveRun(t, store, first, noisy(1, 500))
	want := noisy(2, 4000)
	saveRun(t, store, last, want)

	saved, err := Find(ctx, store, "train", "u-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil || saved.Handle != "job:ns/pod-b" || !saved.Ended || len(saved.Objects) < 2 {
		t.Fatalf("found %+v, want the last run's, ended, of several objects", saved)
	}
	var got bytes.Buffer
	through, err := saved.Read(ctx, store, &got, noGap(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) || through.Bytes != int64(len(want)) || through.Cursor == "" {
		t.Fatalf("read %d bytes through %+v, want the %d of the run", got.Len(), through, len(want))
	}
	end, err := saved.End(ctx, store)
	if err != nil || end.Status != StatusComplete || end.Bytes != int64(len(want)) {
		t.Fatalf("END.json %+v (%v)", end, err)
	}
	if none, err := Find(ctx, store, "train", "other-uid", ""); none != nil || err != nil {
		t.Fatalf("another incarnation's: %+v (%v)", none, err)
	}
}

// A bucket is not this side's to trust: an object that is not the bytes its
// trailer vouches for, one missing from the sequence, or one that unpacks
// to more than it says (a bomb: read no further than one byte past it) is
// corrupt, and the read stops there.
func TestACorruptSavedLogIsRefused(t *testing.T) {
	ctx := context.Background()
	job := testJob("h1")
	fresh := func(t *testing.T) (*fakeStore, *Saved) {
		store := newFakeStore()
		saveRun(t, store, job, noisy(3, 3000))
		saved, err := Find(ctx, store, job.Name, job.UID, "")
		if err != nil || len(saved.Objects) < 3 {
			t.Fatalf("found %+v (%v)", saved, err)
		}
		return store, saved
	}
	refused := func(t *testing.T, store *fakeStore, saved *Saved, why string) {
		t.Helper()
		var out bytes.Buffer
		if _, err := saved.Read(ctx, store, &out, noGap(t)); fault.Of(err) != fault.Invalid || !strings.Contains(err.Error(), why) {
			t.Fatalf("read: %v (%s), want corrupt: %s", err, fault.Of(err), why)
		}
	}

	t.Run("a flipped byte", func(t *testing.T) {
		store, saved := fresh(t)
		store.objects[saved.Objects[1]][10] ^= 0xff
		refused(t, store, saved, "vouches for")
	})
	t.Run("one missing", func(t *testing.T) {
		store, saved := fresh(t)
		delete(store.objects, saved.Objects[1])
		if _, err := Find(ctx, store, job.Name, job.UID, ""); fault.Of(err) != fault.Invalid {
			t.Fatalf("a gap in the objects: %v", err)
		}
	})
	bomb := func(t *testing.T, window int, why string) {
		store, saved := fresh(t)
		enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(window))
		if err != nil {
			t.Fatal(err)
		}
		store.objects[saved.Objects[0]] = AppendTrailer(enc.EncodeAll(make([]byte, 64<<20), nil), Trailer{Bytes: 100})
		refused(t, store, saved, why)
	}
	t.Run("a bomb", func(t *testing.T) { bomb(t, 1<<20, "more than the 100 bytes") })
	t.Run("a big window", func(t *testing.T) { bomb(t, 8<<20, "window size exceeded") })
	t.Run("short of what it says", func(t *testing.T) {
		store, saved := fresh(t)
		frames, said, err := ReadTrailer(store.objects[saved.Objects[0]])
		if err != nil {
			t.Fatal(err)
		}
		said.Bytes += 10
		store.objects[saved.Objects[0]] = AppendTrailer(frames, said)
		refused(t, store, saved, "not the")
	})
}

// noGap is a read's missing that no part may call.
func noGap(t *testing.T) func(int64) error {
	return func(n int64) error {
		t.Errorf("%d bytes said missing", n)
		return nil
	}
}

// The run read is the one the job is on, by its handle: an earlier
// attempt's copy is not its log, and a run that saved nothing yet has none
// in the bucket (its machine has it all).
func TestASavedLogIsTheRunTheJobIsOn(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	first, second := testJob("job:ns/pod-a"), testJob("job:ns/pod-b")
	first.Attempt, second.Attempt = 0, 1
	saveRun(t, store, first, noisy(1, 500))
	if saved, err := Find(ctx, store, "train", "u-1", second.Handle); saved != nil || err != nil {
		t.Fatalf("a run that saved nothing yet found %+v (%v)", saved, err)
	}
	saveRun(t, store, second, noisy(2, 500))
	for _, job := range []Job{first, second} {
		saved, err := Find(ctx, store, "train", "u-1", job.Handle)
		if err != nil || saved == nil || saved.Handle != job.Handle || saved.Folder != store.Key(job.Prefix())+"/" {
			t.Fatalf("the run on %s found %+v (%v)", job.Handle, saved, err)
		}
	}
}

// A part missing is said where it is: before the object it starts, by the
// count of what never came in its trailer.
func TestASavedLogSaysAPartMissingWhereItIs(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	src := newFakeLog()
	s := newTestSaver(t, t.TempDir(), src, store)
	log := lines(0, 3000)
	src.write(log)
	src.breakAfter, src.freeOnBreak = 40<<10, 60<<10
	src.end()
	job := testJob("h1")
	if err := s.Start(job); err != nil {
		t.Fatal(err)
	}
	waitEnd(t, store, job)
	saved, err := Find(ctx, store, job.Name, job.UID, job.Handle)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	through, err := saved.Read(ctx, store, &got, func(n int64) error {
		fmt.Fprintf(&got, "[%d missing]", n)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := string(log[:40<<10]) + "[20480 missing]" + string(log[60<<10:])
	if got.String() != want || through.Gap != 20<<10 {
		t.Fatalf("read %d bytes through %+v, want the gap said after the first %d", got.Len(), through, 40<<10)
	}
}
