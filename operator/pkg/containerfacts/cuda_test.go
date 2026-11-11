// SPDX-License-Identifier: Apache-2.0

package containerfacts

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

func cudaSays(out string, err error) func(context.Context, string, ...string) (string, error) {
	return func(_ context.Context, name string, args ...string) (string, error) {
		if name != "python3" || !slices.Equal(args[:2], []string{"-I", "-c"}) {
			return "", errors.New("asked " + name + " " + strings.Join(args, " "))
		}
		return out, err
	}
}

// nvidia-smi answers through NVML on a host whose CUDA does not start: only
// cuInit, which a job's first CUDA call makes, says so.
func TestCUDAIsAskedAsAJobWould(t *testing.T) {
	started := AskCUDA(context.Background(), cudaSays("cuInit 0 CUDA_SUCCESS\n", nil), "python3")
	if !started.Asked || started.Fails() {
		t.Fatalf("a GPU that starts: %+v", started)
	}
	lost := AskCUDA(context.Background(), cudaSays("cuInit 999 CUDA_ERROR_UNKNOWN\n", nil), "python3")
	if !lost.Fails() || lost.Failure() != "the GPU does not start: cuInit returned 999 (CUDA_ERROR_UNKNOWN)" {
		t.Fatalf("a GPU that does not start: %+v %q", lost, lost.Failure())
	}
}

// What cannot ask says nothing of the GPU: no python, or a libcuda the
// image's libc cannot load (the image's, not the host's).
func TestCUDANotAskedSaysWhy(t *testing.T) {
	for name, ask := range map[string]func(context.Context, string, ...string) (string, error){
		"no python":   cudaSays("", exec.ErrNotFound),
		"no libcuda":  cudaSays("unloadable libcuda.so.1: cannot open shared object file\n", nil),
		"other words": cudaSays("Traceback (most recent call last):\n", nil),
	} {
		cuda := AskCUDA(context.Background(), ask, "python3")
		if cuda.Asked || cuda.Fails() || cuda.Why == "" {
			t.Errorf("%s: %+v", name, cuda)
		}
	}
}

// A cuInit that never returns is a GPU that does not start.
func TestCUDAThatHangsDoesNotStart(t *testing.T) {
	defer func(d time.Duration) { cudaTimeout = d }(cudaTimeout)
	cudaTimeout = 20 * time.Millisecond
	hung := func(ctx context.Context, _ string, _ ...string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	cuda := AskCUDA(context.Background(), hung, "python3")
	if !cuda.Fails() || !strings.Contains(cuda.Failure(), "cuInit did not return in 20ms") {
		t.Fatalf("hung: %+v %q", cuda, cuda.Failure())
	}
}

// The probe is real Python: run here, it loads libcuda or says it cannot,
// and never fails to run.
func TestTheCUDAProbeRunsInARealPython(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("no python3 here")
	}
	cuda := AskCUDA(context.Background(), nil, "python3")
	if !cuda.Asked && !strings.HasPrefix(cuda.Why, "libcuda.so.1 does not load") {
		t.Fatalf("the probe did not run: %+v", cuda)
	}
}
