// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"os"
	"strings"
	"testing"
)

// The SDK reads a pending cleanup from the same two marks (JobStatus
// cleanup_pending): renaming one here leaves the SDK saying "cleaning up"
// for ever.
func TestTheSDKReadsTheSameCleanupMarks(t *testing.T) {
	src, err := os.ReadFile("../../../sdk/python/zenthros/jobs/handle.py")
	if err != nil {
		t.Fatal(err)
	}
	for _, mark := range []string{AnnotationWorkloadCleaned, AnnotationOrphanReaped} {
		if !strings.Contains(string(src), `"`+mark+`"`) {
			t.Errorf("the SDK does not read %s", mark)
		}
	}
	job := &ZenthrosJob{Status: ZenthrosJobStatus{Phase: PhaseCancelled}}
	if !CleanupPending(job) {
		t.Fatal("a cancelled job with no mark is cleaning up")
	}
	job.Annotations = map[string]string{AnnotationWorkloadCleaned: "t"}
	if CleanupPending(job) {
		t.Fatal("a cleaned job is not")
	}
}
