// SPDX-License-Identifier: Apache-2.0

package containerfacts

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CUDA is what the driver said when asked to start CUDA.
type CUDA struct {
	// Asked: a Python loaded libcuda.so.1 and called cuInit. When false, Why
	// says what was missing, and nothing is known of the GPU.
	Asked bool
	// Result is cuInit's CUresult, 0 when CUDA started, and Name its name;
	// -1 when cuInit did not return.
	Result int
	Name   string
	Why    string
}

// Fails reports whether CUDA was asked and did not start.
func (c CUDA) Fails() bool { return c.Asked && c.Result != 0 }

// Failure is what to say of a GPU that does not start.
func (c CUDA) Failure() string {
	if c.Result < 0 {
		return "the GPU does not start: cuInit did not return in " + cudaTimeout.String()
	}
	return fmt.Sprintf("the GPU does not start: cuInit returned %d (%s)", c.Result, c.Name)
}

// cudaTimeout bounds the ask: a first cuInit on eight GPUs without
// persistence mode takes tens of seconds; one that has not returned in two
// minutes is a GPU that does not start.
var cudaTimeout = 2 * time.Minute

// cudaProbe is a job's first CUDA call, through the standard library.
const cudaProbe = `import ctypes
try:
    cuda = ctypes.CDLL("libcuda.so.1")
except OSError as err:
    print("unloadable", err)
else:
    code = cuda.cuInit(0)
    name = ctypes.c_char_p()
    cuda.cuGetErrorName(code, ctypes.byref(name))
    print("cuInit", code, (name.value or b"unnamed").decode())
`

// AskCUDA asks python to start CUDA as a job's first CUDA call would. The
// GPU nvidia-smi lists answers through NVML even where CUDA does not start
// (a GPU fallen off the bus, a fabric manager that never came up), and every
// job that used it would fail. The agent is static Go and cannot load
// libcuda itself. command runs a program; nil runs it for real.
func AskCUDA(ctx context.Context, command func(ctx context.Context, name string, args ...string) (string, error), python string) CUDA {
	if command == nil {
		command = run
	}
	ctx, cancel := context.WithTimeout(ctx, cudaTimeout)
	defer cancel()
	out, err := command(ctx, python, "-I", "-c", cudaProbe)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return CUDA{Asked: true, Result: -1, Name: "timeout"}
	}
	if err != nil {
		return CUDA{Why: fmt.Sprintf("%s did not run: %v", python, err)}
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	said := strings.TrimSpace(lines[len(lines)-1])
	if rest, ok := strings.CutPrefix(said, "unloadable "); ok {
		return CUDA{Why: "libcuda.so.1 does not load: " + rest}
	}
	if fields := strings.Fields(said); len(fields) == 3 && fields[0] == "cuInit" {
		if code, err := strconv.Atoi(fields[1]); err == nil {
			return CUDA{Asked: true, Result: code, Name: fields[2]}
		}
	}
	return CUDA{Why: fmt.Sprintf("%s said %q", python, said)}
}
