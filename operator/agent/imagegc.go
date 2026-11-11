// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"

	ctd "github.com/zenthros/operator/pkg/containerd"
	"github.com/zenthros/operator/pkg/containerd/imagegc"
	"github.com/zenthros/operator/pkg/systemd"
)

// ImagesLedger is where a machine keeps when each job image was last used.
const ImagesLedger = "/var/lib/zenthros/images.json"

// Sweeps: every hour, and once a job's container is up after a submit (a
// pull may have filled the disk). Kicks in that wait are one sweep.
const (
	imageSweepEvery = time.Hour
	imageSweepAfter = 30 * time.Second
)

// ImageGC removes the machine's job images no job uses (imagegc): those
// unused for the policy's age, then, while containerd's store takes more of
// the disk than the policy allows, the least recently used. An image is in
// use while a container uses it, read from containerd at each sweep, and
// while a job holds it: from its name, before its pull, while the job's
// submit is under way and then while its unit runs, read from systemd at
// each sweep (Running). A container is made by the job's unit, after its
// submit: the policy's age alone kept the image in between (the full suite
// of 2026-11-08, with an age of one second, removed a job's image under
// it). A sweep that cannot read either removes nothing. An image a job
// names while a sweep runs is never removed under it: a Hold or Note and
// each removal take turns (mu), and a removal first reads the ledger again.
type ImageGC struct {
	Runner systemd.Runner
	Sudo   string
	Ledger *imagegc.Ledger
	Policy imagegc.Policy
	// Forget drops a removed image from what the machine reports warm.
	Forget func(ref string)
	// Running is the job units that run, or may (RunningUnits).
	Running func(context.Context) (map[string]bool, error)
	Logger  logr.Logger
	Now     func() time.Time
	kick    chan struct{}
	mu      sync.Mutex
	// holds are the images jobs hold, by the job's unit.
	holds map[string]imageHold
}

// imageHold is a job's image; starting while its submit is under way, before
// its unit is installed.
type imageHold struct {
	ref      string
	starting bool
}

// NewImageGC is the sweeper of a machine, with its ledger and what runs.
func NewImageGC(runner systemd.Runner, sudo string, ledger *imagegc.Ledger, policy imagegc.Policy, forget func(string),
	running func(context.Context) (map[string]bool, error), logger logr.Logger) *ImageGC {
	return &ImageGC{Runner: runner, Sudo: sudo, Ledger: ledger, Policy: policy, Forget: forget, Running: running,
		Logger: logger, Now: time.Now, kick: make(chan struct{}, 1), holds: map[string]imageHold{}}
}

// RunningUnits is what a machine's job units run, or may: the units
// SystemdExecutor.InstalledJobs lists.
func RunningUnits(manager *systemd.Manager) func(context.Context) (map[string]bool, error) {
	return func(ctx context.Context) (map[string]bool, error) {
		jobs, err := SystemdExecutor{Manager: manager}.InstalledJobs(ctx, "")
		if err != nil {
			return nil, err
		}
		units := make(map[string]bool, len(jobs))
		for _, job := range jobs {
			units[job.Handle] = true
		}
		return units, nil
	}
}

// Note records that a Prewarm uses ref now, before its pull, by the name
// containerd lists it under (python:3.12 is docker.io/library/python:3.12).
// A removal under way ends first: the caller's pull then finds the image
// gone.
func (g *ImageGC) Note(ref string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.note(ctd.NormalizeImageRef(ref))
}

func (g *ImageGC) note(ref string) {
	if err := g.Ledger.Note(ref, g.Now()); err != nil {
		g.Logger.Error(err, "record an image's use", "image", ref)
	}
}

// Hold records that the job of unit uses ref from now, before its pull, as
// Note does, and keeps it while the job's submit is under way (Started ends
// that) and then while its unit runs.
func (g *ImageGC) Hold(unit, ref string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ref = ctd.NormalizeImageRef(ref)
	g.holds[unit] = imageHold{ref: ref, starting: true}
	g.note(ref)
}

// Started ends the submit of the job of unit, however it ended: its hold
// lasts while its unit runs (a failed install may still have started it).
func (g *ImageGC) Started(unit string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if hold, ok := g.holds[unit]; ok {
		hold.starting = false
		g.holds[unit] = hold
	}
}

// held is what the jobs hold: what was held before running was read, of jobs
// starting then or whose unit runs. A hold of a unit that ended goes. One
// taken after the snapshot is safe without it: its Hold wrote the ledger,
// which a removal reads again.
func (g *ImageGC) held(ctx context.Context) (map[string]bool, error) {
	g.mu.Lock()
	snapshot := make(map[string]imageHold, len(g.holds))
	for unit, hold := range g.holds {
		snapshot[unit] = hold
	}
	g.mu.Unlock()
	running, err := g.Running(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the job units that run: %w", err)
	}
	refs := map[string]bool{}
	g.mu.Lock()
	defer g.mu.Unlock()
	for unit, hold := range snapshot {
		if hold.starting || running[unit] {
			refs[hold.ref] = true
		} else if current, ok := g.holds[unit]; ok && current == hold {
			delete(g.holds, unit)
		}
	}
	return refs, nil
}

// Kick asks for a sweep once the job just started has its container.
func (g *ImageGC) Kick() {
	select {
	case g.kick <- struct{}{}:
	default:
	}
}

// Run sweeps until ctx ends.
func (g *ImageGC) Run(ctx context.Context) {
	tick := time.NewTicker(imageSweepEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-g.kick:
			select {
			case <-ctx.Done():
				return
			case <-time.After(imageSweepAfter):
			}
			// The kicks in the wait were for this sweep.
			select {
			case <-g.kick:
			default:
			}
		}
		if probeRuntimeClass() != runtimeClassContainerd {
			continue
		}
		if err := g.Sweep(ctx); err != nil {
			g.Logger.Error(err, "image sweep")
		}
	}
}

// Sweep removes what the policy says: first the stale images, then, while
// the store is over its limit, the least recently used, measuring again
// after each (layers shared with another image free nothing).
func (g *ImageGC) Sweep(ctx context.Context) error {
	refs, err := ctd.ListImages(ctx, g.Runner, g.Sudo)
	if err != nil {
		return err
	}
	inUse, err := ctd.ContainerImages(ctx, g.Runner, g.Sudo)
	if err != nil {
		return err
	}
	held, err := g.held(ctx)
	if err != nil {
		return err
	}
	for ref := range held {
		inUse[ref] = true
	}
	now := g.Now()
	present, err := g.Ledger.Present(refs, now)
	if err != nil {
		return err
	}
	removed := map[string]bool{}
	for _, ref := range imagegc.Stale(present, inUse, now, g.Policy) {
		if err := g.remove(ctx, ref, present[ref], "unused for "+now.Sub(present[ref]).Round(time.Second).String()); err != nil {
			return err
		}
		removed[ref] = true
	}
	for _, ref := range imagegc.Candidates(present, inUse, now, g.Policy) {
		if removed[ref] {
			continue
		}
		used, disk, err := ctd.ImageSpace(ctx, g.Runner, g.Sudo)
		if err != nil {
			return err
		}
		if used <= g.Policy.Limit(disk) {
			return nil
		}
		if err := g.remove(ctx, ref, present[ref], "the images take more of the disk than they may"); err != nil {
			return err
		}
	}
	return nil
}

// remove removes ref, unless a job named it since the sweep saw it last
// used at seen.
func (g *ImageGC) remove(ctx context.Context, ref string, seen time.Time, why string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.Ledger.LastUsed(ref).Equal(seen) {
		return nil
	}
	if err := ctd.RemoveImage(ctx, g.Runner, g.Sudo, ref); err != nil {
		return err
	}
	g.Logger.Info("removed an image", "image", ref, "why", why)
	if g.Forget != nil {
		g.Forget(ref)
	}
	return g.Ledger.Forget(ref)
}
