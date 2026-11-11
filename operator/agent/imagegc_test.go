// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/zenthros/operator/pkg/containerd/imagegc"
	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

// fakeCtr answers the sweep's commands as a machine would: its images, the
// images its containers use, and the store's size, which each removal
// lowers by what that image alone held.
type fakeCtr struct {
	images     []string
	containers map[string]string // container -> image
	sizes      map[string]int64
	disk       int64
	brokenList bool
	commands   []string
	// on runs before the command it names (a substring of it), once.
	on map[string]func()
}

func (f *fakeCtr) Run(_ context.Context, cmd string) (string, error) {
	f.commands = append(f.commands, cmd)
	for part, hook := range f.on {
		if strings.Contains(cmd, part) {
			delete(f.on, part)
			hook()
		}
	}
	switch {
	case strings.Contains(cmd, "containers ls"):
		if f.brokenList {
			return "", errors.New("ctr: connection refused")
		}
		out := "CONTAINER    IMAGE    RUNTIME\n"
		for c, image := range f.containers {
			out += c + "    " + image + "    io.containerd.runc.v2\n"
		}
		return out, nil
	case strings.Contains(cmd, "images ls -q"):
		return strings.Join(f.images, "\n"), nil
	case strings.Contains(cmd, "images rm --sync"):
		ref := strings.Trim(cmd[strings.LastIndex(cmd, " ")+1:], "'")
		f.images = slices.DeleteFunc(f.images, func(i string) bool { return i == ref })
		return "", nil
	case strings.Contains(cmd, "du -sxb"):
		var used int64
		for _, image := range f.images {
			used += f.sizes[image]
		}
		return fmt.Sprintf("%d\n%d\n", used, f.disk), nil
	}
	return "", fmt.Errorf("unexpected command %q", cmd)
}

func (f *fakeCtr) removed() []string {
	var out []string
	for _, c := range f.commands {
		if strings.Contains(c, "images rm --sync") {
			out = append(out, strings.Trim(c[strings.LastIndex(c, " ")+1:], "'"))
		}
	}
	return out
}

// ledgerAtPull is a machine that reads the ledger when the job's image is
// pulled.
type ledgerAtPull struct {
	*recordingRunner
	path       string
	atPull     string
	gc         *ImageGC
	heldAtPull bool
}

func (r *ledgerAtPull) Run(ctx context.Context, cmd string) (string, error) {
	if strings.Contains(cmd, "images pull") {
		raw, _ := os.ReadFile(r.path)
		r.atPull = string(raw)
		for _, hold := range r.gc.holds {
			r.heldAtPull = r.heldAtPull || hold.starting
		}
	}
	return r.recordingRunner.Run(ctx, cmd)
}

// noUnits is a machine where no job unit runs.
func noUnits(context.Context) (map[string]bool, error) { return map[string]bool{}, nil }

func newTestGC(t *testing.T, ctr *fakeCtr, policy imagegc.Policy, now time.Time, used map[string]time.Time) (*ImageGC, *[]string) {
	t.Helper()
	ledger := imagegc.New(filepath.Join(t.TempDir(), "images.json"))
	for ref, at := range used {
		if err := ledger.Note(ref, at); err != nil {
			t.Fatal(err)
		}
	}
	var forgotten []string
	gc := NewImageGC(ctr, "", ledger, policy, func(ref string) { forgotten = append(forgotten, ref) }, noUnits, logr.Discard())
	gc.Now = func() time.Time { return now }
	return gc, &forgotten
}

// An image no job used for the age is removed, and the warm cache forgets
// it; one a container uses stays however old, even over the disk's limit
// (what a container uses is read before anything is removed); over the
// limit the least recently used goes, one at a time until under it; an
// image the ledger does not know stays; and the ledger read again after a
// restart still says when each was used.
func TestTheSweepRemovesOnlyWhatNoContainerUses(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	week := imagegc.Policy{MaxAge: 7 * 24 * time.Hour, Fraction: 0.4}
	day := 24 * time.Hour

	t.Run("old goes, in use stays", func(t *testing.T) {
		ctr := &fakeCtr{images: []string{"old", "running-old"}, containers: map[string]string{"zj-1": "running-old"},
			sizes: map[string]int64{"old": 10, "running-old": 10}, disk: 1000}
		gc, forgotten := newTestGC(t, ctr, week, now, map[string]time.Time{"old": now.Add(-8 * day), "running-old": now.Add(-30 * day)})
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := ctr.removed(); !slices.Equal(got, []string{"old"}) || !slices.Equal(*forgotten, []string{"old"}) {
			t.Errorf("removed %v, forgotten %v; want only the old image no container uses", got, *forgotten)
		}
		listed, firstRemoval := -1, -1
		for i, c := range ctr.commands {
			if strings.Contains(c, "containers ls") && listed < 0 {
				listed = i
			}
			if strings.Contains(c, "images rm") && firstRemoval < 0 {
				firstRemoval = i
			}
		}
		if listed < 0 || listed > firstRemoval {
			t.Errorf("an image was removed before reading what the containers use: %v", ctr.commands)
		}
	})

	t.Run("over the limit, the least recently used, until under it", func(t *testing.T) {
		ctr := &fakeCtr{images: []string{"a", "b", "c", "running"}, containers: map[string]string{"zj-1": "running"},
			sizes: map[string]int64{"a": 300, "b": 200, "c": 200, "running": 500}, disk: 2000}
		gc, _ := newTestGC(t, ctr, week, now, map[string]time.Time{
			"a": now.Add(-3 * time.Hour), "b": now.Add(-5 * time.Hour), "c": now.Add(-4 * time.Hour), "running": now.Add(-6 * time.Hour)})
		// 1200 of a limit of 800: b (least recent), then c, brings it to 800.
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := ctr.removed(); !slices.Equal(got, []string{"b", "c"}) {
			t.Errorf("removed %v, want b then c, the least recently used, and no more once under the limit", got)
		}
	})

	t.Run("in use and just named stay over the limit", func(t *testing.T) {
		ctr := &fakeCtr{images: []string{"running", "pulling", "unknown"}, containers: map[string]string{"zj-1": "running"},
			sizes: map[string]int64{"running": 900, "pulling": 900, "unknown": 900}, disk: 1000}
		gc, _ := newTestGC(t, ctr, week, now, map[string]time.Time{"running": now.Add(-30 * day), "pulling": now.Add(-time.Minute)})
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := ctr.removed(); len(got) != 0 {
			t.Errorf("removed %v: an image in use, one a job just named, or one seen for the first time", got)
		}
	})

	t.Run("cannot read the containers", func(t *testing.T) {
		ctr := &fakeCtr{images: []string{"old"}, brokenList: true, disk: 1000}
		gc, _ := newTestGC(t, ctr, week, now, map[string]time.Time{"old": now.Add(-30 * day)})
		if err := gc.Sweep(context.Background()); err == nil || len(ctr.removed()) != 0 {
			t.Errorf("a sweep that cannot read what the containers use: %v, removed %v; want an error and nothing removed", err, ctr.removed())
		}
	})

	t.Run("the ledger outlives the agent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "images.json")
		first := imagegc.New(path)
		if err := first.Note("old", now.Add(-8*day)); err != nil {
			t.Fatal(err)
		}
		restarted, err := imagegc.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		ctr := &fakeCtr{images: []string{"old"}, disk: 1000}
		gc := NewImageGC(ctr, "", restarted, week, nil, noUnits, logr.Discard())
		gc.Now = func() time.Time { return now }
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := ctr.removed(); !slices.Equal(got, []string{"old"}) {
			t.Errorf("after a restart the sweep removed %v, want the image unused for 8 days", got)
		}
	})
}

// A job that names an image while a sweep runs keeps it: the sweep decided
// from what it read before, and removed it under the job, whose image was
// there when it looked (code review 2026-11-08). Each removal reads the
// ledger again. A Prewarm's image is noted by the name containerd lists it
// under, or the ledger never knew it was asked for again.
func TestAnImageNamedDuringASweepStays(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	week := imagegc.Policy{MaxAge: 7 * 24 * time.Hour, Fraction: 0.4}

	t.Run("over the limit", func(t *testing.T) {
		a, b := "docker.io/library/a:1", "docker.io/library/b:1"
		ctr := &fakeCtr{images: []string{a, b}, sizes: map[string]int64{a: 600, b: 600}, disk: 2000}
		gc, _ := newTestGC(t, ctr, week, now, map[string]time.Time{a: now.Add(-5 * time.Hour), b: now.Add(-4 * time.Hour)})
		// The space is measured before a, the least recently used, goes: a
		// job names a then.
		ctr.on = map[string]func(){"du -sxb": func() { gc.Note("a:1") }}
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := ctr.removed(); !slices.Equal(got, []string{b}) {
			t.Errorf("removed %v, want b only: a job named a during the sweep", got)
		}
	})

	t.Run("stale", func(t *testing.T) {
		ctr := &fakeCtr{images: []string{"old1", "old2"}, disk: 1000}
		gc, _ := newTestGC(t, ctr, week, now, map[string]time.Time{"old1": now.Add(-9 * 24 * time.Hour), "old2": now.Add(-8 * 24 * time.Hour)})
		ctr.on = map[string]func(){"images rm --sync": func() { _ = gc.Ledger.Note("old2", now) }}
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := ctr.removed(); !slices.Equal(got, []string{"old1"}) {
			t.Errorf("removed %v, want old1 only: old2 was named after the sweep read the ledger", got)
		}
	})

	t.Run("a prewarm's short name", func(t *testing.T) {
		gc, _ := newTestGC(t, &fakeCtr{}, week, now, nil)
		gc.Note("python:3.12")
		if at := gc.Ledger.LastUsed("docker.io/library/python:3.12"); !at.Equal(now) {
			t.Errorf("python:3.12 noted under another name than containerd lists (%v)", at)
		}
	})
}

// A container job's image is noted before its pull, so a sweep keeps it
// until its container uses it, and a sweep is asked for once the job is
// installed. A host job's names no image.
func TestAContainerJobNotesItsImageBeforeItsPull(t *testing.T) {
	cfg := systemd.Config{Scope: systemd.ScopeSystem, UseSudo: true}
	path := filepath.Join(t.TempDir(), "images.json")
	runner := &ledgerAtPull{recordingRunner: &recordingRunner{}, path: path}
	gc := NewImageGC(runner, "", imagegc.New(path), imagegc.Policy{MaxAge: time.Hour}, nil, noUnits, logr.Discard())
	runner.gc = gc
	noted := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	gc.Now = func() time.Time { return noted }
	exec := SystemdExecutor{Manager: systemd.NewManager(cfg, runner, logr.Discard()), Images: gc}
	if _, err := exec.Submit(context.Background(), containerdSpec(), nil); err != nil {
		t.Fatal(err)
	}
	ledger, err := imagegc.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	image := writtenTeeBody(t, runner.commands, "ctr-run")
	ref := image[strings.Index(image, "image='")+len("image='"):]
	ref = ref[:strings.Index(ref, "'")]
	present, _ := ledger.Present([]string{ref}, noted.Add(time.Hour))
	if !present[ref].Equal(noted) || !strings.Contains(runner.atPull, ref) {
		t.Errorf("the job's image %s is not noted before its pull: %v; the ledger at the pull: %q", ref, present, runner.atPull)
	}
	scoped, _ := jobspec.AttemptScoped(containerdSpec(), nil)
	unit := systemd.UnitName(scoped.ID)
	if !runner.heldAtPull || gc.holds[unit] != (imageHold{ref: ref}) {
		t.Errorf("the job's unit %s does not hold %s from before its pull (held then: %v) to after its submit: %+v", unit, ref, runner.heldAtPull, gc.holds)
	}
	select {
	case <-gc.kick:
	default:
		t.Error("no sweep asked for after a container job's submit")
	}

	exec.Manager = systemd.NewManager(cfg, &recordingRunner{}, logr.Discard())
	spec := vmTestSpec()
	spec.Runtime = "systemd"
	if _, err := exec.Submit(context.Background(), spec, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gc.kick:
		t.Error("a host job asked for an image sweep")
	default:
	}
}

// A job holds its image from its name, before its pull, while its submit is
// under way and while its unit runs, however old the image's last use: a
// container is made by the unit after the submit, and with an age of one
// second the full suite of 2026-11-08 saw a job's image removed under it.
// The hold goes with the unit, and the image with the age. What runs is read
// after the holds: a submit that ends in between still holds. A sweep that
// cannot read what runs removes nothing.
func TestAJobsImageStaysFromItsNameUntilItsUnitEnds(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	second := imagegc.Policy{MaxAge: time.Second, Fraction: 0.4}
	ref, unit := "docker.io/library/python:3.13-slim", "zenthros-j1-a0.service"
	running := map[string]bool{}
	ctr := &fakeCtr{images: []string{ref}, disk: 1000}
	gc, _ := newTestGC(t, ctr, second, now, nil)
	gc.Running = func(context.Context) (map[string]bool, error) { return running, nil }
	at := now
	gc.Now = func() time.Time { return at }
	sweep := func(what string, wantGone bool) {
		t.Helper()
		at = at.Add(time.Minute)
		if err := gc.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if gone := !slices.Contains(ctr.images, ref); gone != wantGone {
			t.Fatalf("%s: removed = %v, want %v (holds %+v)", what, gone, wantGone, gc.holds)
		}
	}
	gc.Hold(unit, "python:3.13-slim")
	sweep("while its submit pulls it", false)
	gc.Started(unit)
	running[unit] = true
	sweep("its unit runs, before its container", false)

	// The submit ends after the holds were read and before the units were:
	// the unit was not there yet, and the job still holds its image.
	gc.Hold(unit, ref)
	gc.Running = func(context.Context) (map[string]bool, error) {
		gc.Started(unit)
		return map[string]bool{}, nil
	}
	sweep("a submit that ended while the sweep read the units", false)
	if gc.holds[unit] != (imageHold{ref: ref}) {
		t.Fatalf("the hold of a job whose submit just ended: %+v, want it held while its unit runs", gc.holds)
	}
	gc.Running = func(context.Context) (map[string]bool, error) { return running, nil }
	delete(running, unit)
	sweep("its unit ended", true)
	if len(gc.holds) != 0 {
		t.Fatalf("the hold of a unit that ended stayed: %+v", gc.holds)
	}

	ctr.images = []string{ref}
	gc.Hold(unit, ref)
	gc.Started(unit)
	gc.Running = func(context.Context) (map[string]bool, error) { return nil, errors.New("dbus: timeout") }
	at = at.Add(time.Hour)
	if err := gc.Sweep(context.Background()); err == nil || !slices.Contains(ctr.images, ref) {
		t.Fatalf("a sweep that cannot read what runs: %v, images %v; want an error and nothing removed", err, ctr.images)
	}
}
