// SPDX-License-Identifier: Apache-2.0

package jobcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/fault"
)

// spawnBody is POST /v1/children exactly as the SDK writes it: job_id,
// generation, group and count always; the rest only when set.
type spawnBody struct {
	JobID      string            `json:"job_id"`
	Generation int64             `json:"generation"`
	Group      string            `json:"group"`
	Count      int32             `json:"count"`
	Command    []string          `json:"command,omitempty"`
	Same       bool              `json:"same,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	List       bool              `json:"list,omitempty"`
}

// Child is one child as the controller last saw it.
type Child struct {
	Group   string `json:"group"`
	Index   int32  `json:"index"`
	Name    string `json:"name"`
	Phase   string `json:"phase"`
	Message string `json:"message"`
}

type childrenReply struct {
	Accepted bool    `json:"accepted"`
	Children []Child `json:"children"`
}

type envFlags map[string]string

func (e envFlags) String() string { return "" }

func (e envFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("%q is not KEY=VALUE", v)
	}
	e[k] = val
	return nil
}

// ChildPoll is how often --wait asks how the children are doing.
var ChildPoll = 5 * time.Second

// spawn: job spawn --name P --count N (--same | --cmd ARG...) [--env K=V]...
// [--wait] [-o json]. Everything after --cmd is the children's argv.
func (c *CLI) spawn(ctx context.Context, args []string) int {
	var command []string
	for i, a := range args {
		if a == "--cmd" || a == "-cmd" {
			args, command = args[:i], args[i+1:]
			if len(command) == 0 {
				return c.usageErr(errors.New("--cmd needs the children's command"))
			}
			break
		}
	}
	fs := c.flags("spawn")
	name := fs.String("name", "", "the group's name (a DNS label)")
	count := fs.Int("count", 0, "how many children")
	same := fs.Bool("same", false, "children run this job's own command")
	wait := fs.Bool("wait", false, "wait until every child has ended; fail if one failed")
	output := fs.String("o", "", "json prints the controller's answer")
	env := envFlags{}
	fs.Var(env, "env", "KEY=VALUE added to each child's env (repeatable)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	switch {
	case fs.NArg() > 0:
		return c.usageErr(fmt.Errorf("unexpected %q: flags go before --cmd", fs.Arg(0)))
	case *name == "":
		return c.usageErr(errors.New("spawn needs --name"))
	case *count < 1:
		return c.usageErr(errors.New("spawn needs --count of at least 1"))
	case *same == (len(command) > 0):
		return c.usageErr(errors.New("spawn needs exactly one of --same or --cmd"))
	}
	body := spawnBody{JobID: c.Env(EnvJobID), Generation: c.generation(), Group: *name, Count: int32(*count),
		Command: command, Same: *same}
	if len(env) > 0 {
		body.Env = env
	}
	children, err := c.children(ctx, body)
	if err != nil {
		return c.fail(err)
	}
	if *wait {
		return c.waitChildren(ctx, *name, *output)
	}
	return c.printChildren(children, *output)
}

// childrenCmd: job children [--name P] [--wait] [-o json].
func (c *CLI) childrenCmd(ctx context.Context, args []string) int {
	fs := c.flags("children")
	name := fs.String("name", "", "one group; every group when empty")
	wait := fs.Bool("wait", false, "wait until every child has ended; fail if one failed")
	output := fs.String("o", "", "json prints the controller's answer")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if fs.NArg() > 0 {
		return c.usageErr(fmt.Errorf("unexpected %q", fs.Arg(0)))
	}
	if *wait {
		return c.waitChildren(ctx, *name, *output)
	}
	children, err := c.list(ctx, *name)
	if err != nil {
		return c.fail(err)
	}
	return c.printChildren(children, *output)
}

func (c *CLI) list(ctx context.Context, group string) ([]Child, error) {
	return c.children(ctx, spawnBody{JobID: c.Env(EnvJobID), Generation: c.generation(), Group: group, List: true})
}

func (c *CLI) children(ctx context.Context, body spawnBody) ([]Child, error) {
	raw, _ := json.Marshal(body)
	reply, err := c.post(ctx, "/v1/children", raw)
	if err != nil {
		return nil, err
	}
	var out childrenReply
	if err := json.Unmarshal(reply, &out); err != nil {
		return nil, fmt.Errorf("agent answered %q: %w", reply, err)
	}
	return out.Children, nil
}

// waitChildren polls until every child has ended. A child that failed (or
// was cancelled) fails the wait, in its own words, one line each. One call
// gives up after 45 s, the wait does not: an agent or controller away for
// longer is waited out.
func (c *CLI) waitChildren(ctx context.Context, group, output string) int {
	for {
		children, err := c.list(ctx, group)
		if err != nil && fault.Of(err) != fault.Transient {
			return c.fail(err)
		}
		done := len(children) > 0
		for _, ch := range children {
			if !v1.Phase(ch.Phase).Terminal() {
				done = false
			}
		}
		if done {
			code := c.printChildren(children, output)
			for _, ch := range children {
				if ch.Phase != string(v1.PhaseSucceeded) {
					fmt.Fprintf(c.Stderr, "CHILD_FAILED: %s ended %s: %s\n", ch.Name, ch.Phase, ch.Message)
					code = ExitFailed
				}
			}
			return code
		}
		select {
		case <-ctx.Done():
			return c.fail(ctx.Err())
		case <-time.After(ChildPoll):
		}
	}
}

func (c *CLI) printChildren(children []Child, output string) int {
	if output == "json" {
		raw, _ := json.Marshal(children)
		fmt.Fprintln(c.Stdout, string(raw))
		return ExitOK
	}
	for _, ch := range children {
		phase := ch.Phase
		if phase == "" {
			phase = "-"
		}
		fmt.Fprintf(c.Stdout, "%s %s\n", ch.Name, phase)
	}
	return ExitOK
}
