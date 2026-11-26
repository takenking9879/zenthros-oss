// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	v1 "github.com/takenking9879/zenthros-oss/operator/api/v1"
	"github.com/takenking9879/zenthros-oss/operator/pkg/backend"
	"github.com/takenking9879/zenthros-oss/operator/pkg/fault"
	"github.com/takenking9879/zenthros-oss/operator/pkg/jobspec"
	"github.com/takenking9879/zenthros-oss/operator/pkg/objectput"
)

// A job on a machine writes its outputs to a folder of its own
// (ZENTHROS_DATA_OUTPUT, backend.LocalOutput), and its agent saves the folder
// to the output's URI once the job ended, before it reports the end: open(),
// savefig() and a shell's > write there as anywhere, and the job's next step
// finds them in the bucket (19). What the agent saves of each job is a plan
// on its disk (Config.OutputStateDir), so a restarted agent saves what it
// had not, and nothing twice. Saving never fails the job: what it could not
// save is the job's OUTPUTS_NOT_SAVED reason.

// outputFolders is an executor whose jobs run on the machine's own disk:
// a VM's (systemd, containerd) and a container's (procagent). A pod's is
// its own, and keeps the URI.
type outputFolders interface {
	// OutputDir is where spec's run (its attempt) writes its outputs. The
	// executor makes it, the job's to write.
	OutputDir(spec jobspec.JobSpec) string
}

// outputTries is how many times one file is sent when its bucket answers
// that it may take it later; outputBackoff is the first wait between.
const (
	outputTries   = 4
	outputBackoff = time.Second
)

// outputDepth bounds how deep the folder is walked.
const outputDepth = 64

// outputPlan is what is saved of one job's run, and once it was, how.
type outputPlan struct {
	Folder      string               `json:"folder"`
	Destination string               `json:"destination"`
	Endpoint    string               `json:"endpoint,omitempty"`
	Region      string               `json:"region,omitempty"`
	JobID       string               `json:"jobID"`
	Namespace   string               `json:"namespace,omitempty"`
	Credentials []v1.SecretSourceRef `json:"credentials,omitempty"`
	// Keys are the ones the job's env gives as values; the file is the
	// agent's alone (0600), as the job's env on this machine is.
	Keys    map[string]string `json:"keys,omitempty"`
	Saved   bool              `json:"saved,omitempty"`
	Problem string            `json:"problem,omitempty"`
}

// outputSaver saves the jobs' outputs folders.
type outputSaver struct {
	dir      string
	executor Executor
	logger   Logger
	// wake asks for the poll that pushes a job's end once it is saved.
	wake func()
	// open is the job's bucket: openStore, objectput's.
	open func(ctx context.Context, plan outputPlan) (fileStore, error)

	mu     sync.Mutex
	saving map[string]bool
}

// fileStore is where a job's outputs go.
type fileStore interface {
	Key(parts ...string) string
	PutFile(ctx context.Context, key string, file *os.File, size int64) error
}

func newOutputSaver(cfg Config, executor Executor, logger Logger, wake func()) *outputSaver {
	if cfg.OutputStateDir == "" {
		return nil
	}
	if _, ok := executor.(outputFolders); !ok {
		return nil
	}
	s := &outputSaver{dir: cfg.OutputStateDir, executor: executor, logger: logger.WithName("output-saver"), wake: wake,
		saving: map[string]bool{}}
	s.open = s.openStore
	return s
}

// localOutput is env with the job's outputs folder for its output URI, and
// the URI, when the job names a bucket and this agent saves the folder.
func (a *Agent) localOutput(spec jobspec.JobSpec, env map[string]string) (map[string]string, string) {
	if a.outputs == nil {
		return env, ""
	}
	return backend.LocalOutput(env, a.executor.(outputFolders).OutputDir(spec))
}

// planOutputs records what is saved of the run under handle, before the
// job is tracked: the first poll that sees it ended finds the plan.
func (a *Agent) planOutputs(spec jobspec.JobSpec, handle string, env map[string]string, uri string) error {
	if a.outputs == nil || uri == "" {
		return nil
	}
	endpoint, region, keys := objectput.JobEnv(env)
	plan := outputPlan{Folder: env[backend.DataOutputEnv], Destination: uri, Endpoint: endpoint, Region: region,
		Keys: keys, JobID: spec.ID, Namespace: spec.Namespace}
	for _, source := range spec.SecretSources {
		if source.Materialization.Mode == v1.SecretMaterializationEnv && slices.Contains(objectput.CredentialNames, source.Materialization.EnvName) {
			plan.Credentials = append(plan.Credentials, source)
		}
	}
	return a.outputs.write(handle, plan)
}

// withOutputs holds an ended job's end while its outputs are saved: it runs
// on, saving them, and its end comes once they are, with what was not.
func (a *Agent) withOutputs(handle string, state backend.JobState) backend.JobState {
	if a.outputs == nil || !state.Phase.Terminal() {
		return state
	}
	plan, ok := a.outputs.read(handle)
	if !ok {
		return state
	}
	if plan.Saved {
		if plan.Problem != "" {
			state.Reasons = append(append([]v1.Reason(nil), state.Reasons...), v1.Reason{Code: v1.ReasonOutputsNotSaved, Detail: plan.Problem})
		}
		return state
	}
	a.outputs.start(handle, plan)
	return backend.JobState{Phase: v1.PhaseRunning, StartTime: state.StartTime, ResolvedImage: state.ResolvedImage,
		Message: "its process ended; saving its outputs to " + plan.Destination}
}

// forgetOutputs drops the run's plan with its Cancel.
func (a *Agent) forgetOutputs(handle string) {
	if a.outputs != nil {
		a.outputs.forget(handle)
	}
}

func (s *outputSaver) path(handle string) string {
	return filepath.Join(s.dir, url.PathEscape(handle)+".json")
}

func (s *outputSaver) write(handle string, plan outputPlan) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	tmp := s.path(handle) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(handle))
}

func (s *outputSaver) read(handle string) (outputPlan, bool) {
	raw, err := os.ReadFile(s.path(handle))
	if err != nil {
		return outputPlan{}, false
	}
	var plan outputPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		s.logger.Error(err, "an outputs plan that does not read: the job's end goes without them", "handle", handle)
		return outputPlan{}, false
	}
	return plan, true
}

func (s *outputSaver) forget(handle string) {
	if err := os.Remove(s.path(handle)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.logger.Error(err, "the outputs plan stays", "handle", handle)
	}
}

// start saves the run's folder, once at a time.
func (s *outputSaver) start(handle string, plan outputPlan) {
	s.mu.Lock()
	if s.saving[handle] {
		s.mu.Unlock()
		return
	}
	s.saving[handle] = true
	s.mu.Unlock()
	go func() {
		plan.Problem = s.save(context.Background(), plan)
		plan.Saved = true
		if err := s.write(handle, plan); err != nil {
			s.logger.Error(err, "what was saved of the outputs is not recorded: they are saved again", "handle", handle)
		}
		s.mu.Lock()
		delete(s.saving, handle)
		s.mu.Unlock()
		s.wake()
	}()
}

// save sends every file of plan's folder, and says what it could not.
func (s *outputSaver) save(ctx context.Context, plan outputPlan) string {
	started := time.Now()
	var store fileStore
	var sent, bytes int64
	var problems []string
	left, err := walkOutputs(plan.Folder, func(rel string, file *os.File, size int64) error {
		if store == nil {
			var err error
			if store, err = s.open(ctx, plan); err != nil {
				return fmt.Errorf("%s does not take them: %w", plan.Destination, err)
			}
		}
		if err := s.send(ctx, store, rel, file, size); err != nil {
			return fmt.Errorf("%s and the files after it are not in %s: %w", rel, plan.Destination, err)
		}
		sent, bytes = sent+1, bytes+size
		return nil
	})
	if err != nil {
		problems = append(problems, err.Error())
	}
	if len(left) > 0 {
		problems = append(problems, "left out, not files the job wrote: "+strings.Join(left, ", "))
	}
	s.logger.Info("saved the job's outputs", "job_id", plan.JobID, "destination", plan.Destination,
		"files", sent, "bytes", bytes, "took", time.Since(started).Round(time.Millisecond).String(), "problems", len(problems))
	return strings.Join(problems, "; ")
}

// send sends one file, again while its bucket says it may take it later.
func (s *outputSaver) send(ctx context.Context, store fileStore, rel string, file *os.File, size int64) error {
	wait := outputBackoff
	for try := 1; ; try++ {
		err := store.PutFile(ctx, store.Key(rel), file, size)
		if err == nil || !fault.Retryable(err) || try == outputTries {
			return err
		}
		time.Sleep(wait)
		wait *= 2
	}
}

// openStore is the job's bucket, with the credentials its executor
// resolves here, or the machine's own; plain http only for the job's own
// access key (objectput.Config.AllowHTTP).
func (s *outputSaver) openStore(ctx context.Context, plan outputPlan) (fileStore, error) {
	// The job's secrets are materialized over its env's values, as here.
	creds := maps.Clone(plan.Keys)
	if len(plan.Credentials) > 0 {
		resolver, ok := s.executor.(LogCredentialer)
		if !ok {
			return nil, errors.New("this machine resolves no credentials for a bucket")
		}
		resolved, err := resolver.LogCredentials(ctx, jobspec.JobSpec{ID: plan.JobID, Namespace: plan.Namespace, SecretSources: plan.Credentials})
		if err != nil {
			return nil, err
		}
		if creds == nil {
			creds = map[string]string{}
		}
		maps.Copy(creds, resolved)
	}
	return objectput.Open(ctx, objectput.Config{Destination: plan.Destination, Endpoint: plan.Endpoint, Region: plan.Region,
		Credentials: creds, AllowHTTP: true})
}

// walkOutputs hands visit every file of folder, open, in the order of
// their names, a component at a time and never through a link: the agent
// may read what the job may not, and the job chose every name in it. A file
// goes only when it is a regular file of the folder's owner, the job's
// user: a link the job made to another's file (a hard link keeps its
// owner), a FIFO, a socket or a device is left out, and named in left. The
// walk stops at visit's first error. No folder is nothing to save.
func walkOutputs(folder string, visit func(rel string, file *os.File, size int64) error) (left []string, err error) {
	root, err := unix.Open(folder, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("the outputs folder: %w", err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(root, &st); err != nil {
		unix.Close(root)
		return nil, err
	}
	owner := st.Uid
	var walk func(dir int, rel string, depth int) error
	walk = func(dir int, rel string, depth int) error {
		handle := os.NewFile(uintptr(dir), rel)
		defer handle.Close()
		names, err := handle.Readdirnames(-1)
		if err != nil {
			return err
		}
		sort.Strings(names)
		for _, name := range names {
			child := path.Join(rel, name)
			fd, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
			if err != nil {
				// ELOOP: a symbolic link.
				left = append(left, child)
				continue
			}
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil {
				unix.Close(fd)
				return err
			}
			switch {
			case st.Uid != owner:
				unix.Close(fd)
				left = append(left, child)
			case st.Mode&unix.S_IFMT == unix.S_IFDIR && depth < outputDepth:
				if err := walk(fd, child, depth+1); err != nil {
					return err
				}
			case st.Mode&unix.S_IFMT == unix.S_IFREG:
				// Blocking reads from here on: O_NONBLOCK was for a FIFO's open.
				if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFL, unix.O_RDONLY); err != nil {
					unix.Close(fd)
					return err
				}
				file := os.NewFile(uintptr(fd), child)
				err := visit(child, file, st.Size)
				file.Close()
				if err != nil {
					return err
				}
			default:
				unix.Close(fd)
				left = append(left, child)
			}
		}
		return nil
	}
	return left, walk(root, "", 0)
}
