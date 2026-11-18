// SPDX-License-Identifier: Apache-2.0

package logfile

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Tail is the last n lines of the file, joined by " | ", for a failure
// message: at most 4 KiB of them, never from its freed part.
func (f *File) Tail(n int) string {
	file, err := os.Open(f.path)
	if err != nil {
		return ""
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return ""
	}
	from := tailOffset(file, info.Size(), f.Floor(), n)
	raw, err := io.ReadAll(io.NewSectionReader(file, from, min(4096, info.Size()-from)))
	if err != nil {
		return ""
	}
	var lines []string
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, " | ")
}

// Prune removes the oldest files of dir ending in suffix past the newest
// keep of them, but those live says are still in use (a job that runs, or
// whose log is still being read for its bucket). It forgets each one it
// removes.
func (k *Keeper) Prune(dir, suffix string, keep int, live func(name string) bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type kept struct {
		name string
		info os.FileInfo
	}
	var files []kept
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), suffix) || !entry.Type().IsRegular() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, kept{name: entry.Name(), info: info})
	}
	if len(files) <= keep {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].info.ModTime().Before(files[j].info.ModTime()) })
	for _, f := range files[:len(files)-keep] {
		if live(f.name) {
			continue
		}
		path := filepath.Join(dir, f.name)
		k.Forget(path)
		_ = os.Remove(path)
	}
}
