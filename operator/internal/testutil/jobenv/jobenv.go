// SPDX-License-Identifier: Apache-2.0

// Package jobenv reads the list of every variable a job finds in its
// environment, by who sets it: the "job_env" of pkg/jobcli/testdata/wire.json,
// which the SDK's tests read too. Each Go setter's test holds it to exactly
// its keys, so a variable renamed or dropped on one side fails without a
// cluster.
package jobenv

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// Setters name who puts a variable in the job's environment.
const (
	Controller = "controller"
	Task       = "task"
	Child      = "child"
	Agent      = "agent"
	User       = "user"
	SDK        = "sdk"
)

// Of is the variables the setters put, sorted.
func Of(t testing.TB, setters ...string) []string {
	t.Helper()
	want := map[string]bool{}
	for _, s := range setters {
		want[s] = true
	}
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for name, by := range all {
		if want[by] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Owned is the names of env that Zenthros owns (ZENTHROS_*, TASK_INDEX and
// TASK_COUNT), sorted, leaving out what the job's own author set.
func Owned(env map[string]string) []string {
	var out []string
	for name := range env {
		if strings.HasPrefix(name, "ZENTHROS_") || name == "TASK_INDEX" || name == "TASK_COUNT" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// All is every variable of job_env, by its setter.
func All() (map[string]string, error) {
	_, here, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(here), "..", "..", "..", "pkg", "jobcli", "testdata", "wire.json"))
	if err != nil {
		return nil, err
	}
	var doc struct {
		JobEnv map[string]string `json:"job_env"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	delete(doc.JobEnv, "_about")
	for name, by := range doc.JobEnv {
		switch by {
		case Controller, Task, Child, Agent, User, SDK:
		default:
			return nil, fmt.Errorf("wire.json job_env: %s is set by %q, which is no setter", name, by)
		}
	}
	return doc.JobEnv, nil
}
