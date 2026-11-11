// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"strings"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
)

// IntelligentLogFilter reduces Ray/train/tune stdout to a useful summary.
// Rules come from external/kuberay/sample-logs/synth_run.log plus the
// pipeline prints [GPU_DEBUG] and Prometheus already-bound.
//
// Ray flags: synth_run.py uses log_to_driver=True and RAY_DEDUP_LOGS=0;
// RAY_BACKEND_LOG_LEVEL is unset (INFO). This filter does not duplicate
// those flags. Each StreamLogs RPC must construct a new window.
type progressSeries struct {
	first string
	last  string
}

type IntelligentLogFilter struct {
	inResourceBlock bool
	order           []string
	series          map[string]*progressSeries
}

// lineFilter is what a StreamLogs request's lines pass through.
type lineFilter interface {
	Feed(line string) []string
	FeedAll(lines []string) []string
	Flush() []string
}

// lineFilterFor summarizes only when the request asks (zctl logs
// --summary). The summary drops lines by Ray/train/tune patterns; applied to
// every job it hid lines of jobs that are neither, with no way to see them.
func lineFilterFor(req *agentpb.StreamLogsRequest) lineFilter {
	if req.GetSummarize() {
		return NewIntelligentLogFilter()
	}
	return rawLines{}
}

// rawLines passes every line through as the job wrote it.
type rawLines struct{}

func (rawLines) Feed(line string) []string {
	if line == "" {
		return nil
	}
	return []string{line}
}

func (rawLines) FeedAll(lines []string) []string { return lines }

func (rawLines) Flush() []string { return nil }

func NewIntelligentLogFilter() *IntelligentLogFilter {
	return &IntelligentLogFilter{
		series: map[string]*progressSeries{},
	}
}

// Feed classifies one line (with or without a trailing newline) and returns
// any lines that should be sent now. Collapse series are independent so
// interleaved Reporting/Epoch ticks still keep first+last of each.
func (f *IntelligentLogFilter) Feed(line string) []string {
	class, key := f.classify(line)
	switch class {
	case classDrop:
		return nil
	case classKeep:
		out := f.flushHeld()
		return append(out, line)
	case classCollapse:
		return f.hold(key, line)
	default:
		return nil
	}
}

func (f *IntelligentLogFilter) hold(key, line string) []string {
	if f.series == nil {
		f.series = map[string]*progressSeries{}
	}
	s, ok := f.series[key]
	if !ok {
		f.series[key] = &progressSeries{first: line}
		f.order = append(f.order, key)
		return []string{line}
	}
	s.last = line
	return nil
}

// FeedAll feeds each line in order and concatenates emitted output.
func (f *IntelligentLogFilter) FeedAll(lines []string) []string {
	var out []string
	for _, line := range lines {
		out = append(out, f.Feed(line)...)
	}
	return out
}

// Flush emits a held last-progress tick. Call when the source stream ends.
func (f *IntelligentLogFilter) Flush() []string {
	return f.flushHeld()
}

// takeLines splits chunk data on newlines. A chunk with no newline is one
// line (preserves the existing per-chunk StreamLogs contract). A trailing
// fragment after the last newline is held in partial until the next chunk.
func takeLines(data []byte, partial *string) []string {
	s := *partial + string(data)
	*partial = ""
	if !strings.Contains(s, "\n") {
		if s == "" {
			return nil
		}
		return []string{s}
	}
	parts := strings.SplitAfter(s, "\n")
	if !strings.HasSuffix(s, "\n") {
		*partial = parts[len(parts)-1]
		parts = parts[:len(parts)-1]
	} else if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func (f *IntelligentLogFilter) flushHeld() []string {
	if len(f.order) == 0 {
		return nil
	}
	var out []string
	for _, key := range f.order {
		s := f.series[key]
		if s.last != "" && s.last != s.first {
			out = append(out, s.last)
		}
	}
	f.order = nil
	f.series = map[string]*progressSeries{}
	return out
}

type lineClass int

const (
	classDrop lineClass = iota
	classKeep
	classCollapse
)

func (f *IntelligentLogFilter) classify(line string) (lineClass, string) {
	bare := stripWorkerPrefix(stripANSI(line))
	if strings.TrimSpace(bare) == "" {
		if f.inResourceBlock {
			return classKeep, ""
		}
		return classDrop, ""
	}
	if f.inResourceBlock {
		if isResourceBlockLine(bare) {
			return classKeep, ""
		}
		f.inResourceBlock = false
	}
	if strings.Contains(bare, "[RAY CLUSTER RESOURCES]") {
		f.inResourceBlock = true
		return classKeep, ""
	}
	if isKeepLine(bare) {
		return classKeep, ""
	}
	if key, ok := collapseKey(bare); ok {
		return classCollapse, key
	}
	if isDropLine(bare) {
		return classDrop, ""
	}
	return classKeep, ""
}

var keepFragments = []string{
	"[zenthros-resources]",
	"torch.cuda=",
	"[synth] phase=",
	"[synth] wrote",
	"[synth-data] read_parquet",
	"[synth-data] rows=",
	"[synth] Starting training pipeline",
	"[synth] Starting hyperparameter tuning",
	"[synth] Training completed",
	"[synth] Tuning completed",
	"[synth] Training job failed",
	"[synth-train] start",
	"[synth-train] completed",
	"[synth-tune] trial start",
	"[synth-tune] trial completed",
	"Current best trial:",
	" started with configuration",
	" completed after ",
}

func isKeepLine(bare string) bool {
	if containsAny(bare, keepFragments) {
		return true
	}
	return isUserError(bare)
}

func collapseKey(bare string) (string, bool) {
	if isEpochTick(bare) {
		if strings.Contains(bare, "[synth-tune]") {
			return "epoch:tune", true
		}
		if strings.Contains(bare, "[synth-train]") {
			return "epoch:train", true
		}
		return "epoch", true
	}
	if strings.Contains(bare, "Reporting training result") {
		return "report", true
	}
	if strings.Contains(strings.TrimSpace(bare), "Trial status:") {
		return "trial-status", true
	}
	return "", false
}

var dropFragments = []string{
	"[GPU_DEBUG]",
	"Prometheus server start failed (likely already bound)",
	"Failed to query Ray Train Controller actor state",
	"core_worker.cc",
	"RayDeprecationWarning",
	"tqdm is not installed",
	"Setting up process group",
	"Starting execution of Dataset",
	"Execution plan of Dataset",
	"A new progress UI is available",
	"execution finished",
	"Requesting resources:",
	"Attempting to start training worker group",
	"Started training worker group",
	"Wrote the latest version of all result files",
	"View detailed results here:",
	"To visualize your results with TensorBoard",
	"Logical resource usage:",
	"Current time:",
	"Progress will be logged because stdout is a non-interactive terminal",
	"Skipping Prometheus server",
	"world_rank=",
	"self.exclude_resources",
}

func isDropLine(bare string) bool {
	if containsAny(bare, dropFragments) {
		return true
	}
	return isRayLoggerLine(bare) || isSchemaDumpLine(bare) || isTuneBoxLine(bare)
}

func containsAny(s string, frags []string) bool {
	for _, frag := range frags {
		if strings.Contains(s, frag) {
			return true
		}
	}
	return false
}

func isUserError(bare string) bool {
	if strings.Contains(bare, "Failed to query Ray Train Controller actor state") {
		return false
	}
	if strings.Contains(bare, "RayDeprecationWarning") || strings.Contains(bare, "core_worker.cc") {
		return false
	}
	if strings.Contains(bare, "Prometheus server start failed") {
		return false
	}
	if strings.Contains(bare, "Training job failed") {
		return true
	}
	if strings.Contains(bare, "Traceback") {
		return true
	}
	if strings.Contains(bare, " ERROR ") || strings.Contains(bare, "Error:") || strings.Contains(bare, "Exception") {
		return true
	}
	return false
}

func isEpochTick(s string) bool {
	i := strings.Index(s, "Epoch ")
	if i < 0 {
		return false
	}
	rest := s[i+len("Epoch "):]
	return rest != "" && rest[0] >= '0' && rest[0] <= '9'
}

func isResourceBlockLine(bare string) bool {
	trimmed := strings.TrimSpace(bare)
	if trimmed == "" {
		return true
	}
	switch {
	case strings.HasPrefix(trimmed, "CPU"):
		return true
	case strings.HasPrefix(trimmed, "GPU"):
		return true
	case strings.HasPrefix(trimmed, "Memory"):
		return true
	case strings.HasPrefix(trimmed, "Object Store"):
		return true
	case strings.HasPrefix(trimmed, "accelerator_type:"):
		return true
	case strings.Contains(trimmed, "──"):
		return true
	default:
		return false
	}
}

func isRayLoggerLine(bare string) bool {
	if !strings.Contains(bare, ".py:") {
		return false
	}
	return strings.Contains(bare, "INFO ") || strings.Contains(bare, "WARNING ")
}

func isSchemaDumpLine(bare string) bool {
	trimmed := strings.TrimSpace(bare)
	if trimmed == "------  ----" || trimmed == "Column  Type" {
		return true
	}
	if strings.HasPrefix(trimmed, "label") && strings.Contains(trimmed, "int") {
		return true
	}
	if len(trimmed) < 3 || trimmed[0] != 'f' {
		return false
	}
	i := 1
	for i < len(trimmed) && trimmed[i] >= '0' && trimmed[i] <= '9' {
		i++
	}
	if i == 1 {
		return false
	}
	rest := strings.TrimSpace(trimmed[i:])
	return rest == "float"
}

func isTuneBoxLine(bare string) bool {
	trimmed := strings.TrimSpace(bare)
	if trimmed == "" {
		return false
	}
	r := []rune(trimmed)[0]
	switch r {
	case '╭', '╰', '│', '├', '─':
		return true
	default:
		return false
	}
}

func stripANSI(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			for j < len(s) && s[j] != 'm' {
				j++
			}
			if j < len(s) {
				i = j
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func stripWorkerPrefix(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	body := s[start:]
	if body == "" || body[0] != '(' {
		return s
	}
	end := strings.Index(body, ") ")
	if end < 0 {
		return s
	}
	return body[end+2:]
}
