// SPDX-License-Identifier: Apache-2.0

// Package logfile keeps and reads a job's log file on a machine or in a
// container. The job writes the file itself (its stdout and stderr, opened
// O_APPEND for it); the agent never copies a byte. Past the file's cap the
// agent frees the oldest part with a punch hole: the file keeps its size and
// every byte its position, and the freed part reads as zeros and takes no
// disk. Each reader reads the file on its own: a history (the end of the log
// as it is), then, when it follows, what the job writes next, woken by
// inotify (benchmarks/loglab/PLAN.md, "Las fuentes").
package logfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
)

const (
	// DefaultMax is what a job's file keeps: the history and the bucket's
	// reader need only the recent part, and twenty kept logs of a container
	// stay well inside its overlay. It is the kubelet's 5 x 10 MiB.
	DefaultMax = 50 << 20

	// block is what a hole is punched in: an unaligned punch zeroes the
	// start of a partial block it does not free, which a reader would then
	// take for data (W0, RESULTS.md).
	block = 4096

	// trimEvery is how often the keeper looks at its files' sizes: a file
	// passes its cap by what its job writes in that time.
	trimEvery = 2 * time.Second
)

// punch frees [off, off+n) of fd, keeping its size.
var punch = func(fd int, off, n int64) error {
	return unix.Fallocate(fd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, n)
}

// File is one job's log file as the agent keeps it.
type File struct {
	path string
	// floor is where the kept part starts: [0, floor) is freed. It only
	// grows, and it is stored before the bytes under it are freed, so a
	// reader that loads it after a read knows whether what it read could
	// have been freed (Read).
	floor atomic.Int64
}

// Path is the file's path.
func (f *File) Path() string { return f.path }

// Floor is where the kept part of the file starts.
func (f *File) Floor() int64 { return f.floor.Load() }

// Keeper holds the files of a machine's jobs to their cap.
type Keeper struct {
	max    int64
	logger logr.Logger

	mu    sync.Mutex
	files map[string]*File
	// dirs are folders whose files it keeps without being asked for each:
	// a machine's job units go on writing while the agent restarts.
	dirs []watchedDir
	stop chan struct{}
	once sync.Once
}

type watchedDir struct{ dir, suffix string }

// NewKeeper keeps every file it is asked for to max bytes (DefaultMax when
// 0), looking every trimEvery until Close.
func NewKeeper(max int64, logger logr.Logger) *Keeper {
	k := newKeeper(max, logger)
	go k.loop(trimEvery)
	return k
}

func newKeeper(max int64, logger logr.Logger) *Keeper {
	if max <= 0 {
		max = DefaultMax
	}
	return &Keeper{max: max, logger: logger, files: map[string]*File{}, stop: make(chan struct{})}
}

// Close stops the keeper's look at its files.
func (k *Keeper) Close() { k.once.Do(func() { close(k.stop) }) }

func (k *Keeper) loop(every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-k.stop:
			return
		case <-tick.C:
			k.trimAll()
		}
	}
}

// Within keeps every file of dir whose name ends in suffix, those written
// after this call too.
func (k *Keeper) Within(dir, suffix string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.dirs = append(k.dirs, watchedDir{dir: dir, suffix: suffix})
}

func (k *Keeper) trimAll() {
	k.mu.Lock()
	dirs := append([]watchedDir(nil), k.dirs...)
	k.mu.Unlock()
	for _, d := range dirs {
		entries, _ := os.ReadDir(d.dir)
		for _, entry := range entries {
			if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), d.suffix) {
				_, _ = k.File(filepath.Join(d.dir, entry.Name()))
			}
		}
	}
	k.mu.Lock()
	files := make([]*File, 0, len(k.files))
	for _, f := range k.files {
		files = append(files, f)
	}
	k.mu.Unlock()
	for _, f := range files {
		if err := k.trim(f); err != nil && !errors.Is(err, os.ErrNotExist) {
			k.logger.Error(err, "could not free the old part of a job's log", "path", f.path)
		}
	}
}

// File is the kept file at path. The first time, a file freed by an earlier
// session of the agent gives back where its kept part starts.
func (k *Keeper) File(path string) (*File, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if f, ok := k.files[path]; ok {
		return f, nil
	}
	floor, err := keptFrom(path)
	if err != nil {
		return nil, err
	}
	f := &File{path: path}
	f.floor.Store(floor)
	k.files[path] = f
	return f, nil
}

// Forget stops keeping path, for a file about to be removed.
func (k *Keeper) Forget(path string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.files, path)
}

// keptFrom is where the data of a file freed before starts: the first
// byte SEEK_DATA finds, or its size when everything is freed.
func keptFrom(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	off, err := unix.Seek(int(file.Fd()), 0, unix.SEEK_DATA)
	if errors.Is(err, unix.ENXIO) {
		// No data past 0: empty, or all of it freed.
		info, statErr := file.Stat()
		if statErr != nil {
			return 0, statErr
		}
		return info.Size() &^ (block - 1), nil
	}
	if err != nil {
		return 0, fmt.Errorf("find the kept part of %s: %w", path, err)
	}
	return off &^ (block - 1), nil
}

// trim frees what is older than the cap: [floor, size - max), in whole
// blocks. It never waits for a reader: the cap keeps the disk, and a reader
// behind it is told what it lost.
func (k *Keeper) trim(f *File) error {
	info, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	to := (info.Size() - k.max) &^ (block - 1)
	from := f.floor.Load()
	if to <= from {
		return nil
	}
	file, err := os.OpenFile(f.path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	// Before the bytes go: a reader that loads the floor after its read sees
	// this one whenever its read could have met the hole.
	f.floor.Store(to)
	if err := punch(int(file.Fd()), from, to-from); err != nil {
		return fmt.Errorf("free [%d, %d) of %s: %w", from, to, f.path, err)
	}
	return nil
}
