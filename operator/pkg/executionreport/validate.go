// SPDX-License-Identifier: Apache-2.0

package executionreport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
)

var allowedArtifactSchemes = map[string]struct{}{
	"s3":    {},
	"gs":    {},
	"abfs":  {},
	"abfss": {},
	"https": {},
	"http":  {},
}

// ValidateSnapshot checks envelope fields, payload limits, and artifact URIs.
// Sequence may be zero: the Agent assigns it at first persist.
func ValidateSnapshot(snapshot Snapshot) error {
	if snapshot.APIVersion != APIVersion {
		return snapshotError(CodeInvalidSnapshot, "apiVersion must be %s", APIVersion)
	}
	if snapshot.Kind != Kind {
		return snapshotError(CodeInvalidSnapshot, "kind must be %s", Kind)
	}
	if err := requireID(snapshot.JobID, "job_id", MaxJobIDLength); err != nil {
		return err
	}
	if err := requireID(snapshot.ProducerID, "producer_id", MaxProducerIDLength); err != nil {
		return err
	}
	if err := requireID(snapshot.ReportID, "report_id", MaxReportIDLength); err != nil {
		return err
	}
	if snapshot.ProducedAt.IsZero() {
		return snapshotError(CodeInvalidSnapshot, "produced_at is required")
	}
	if snapshot.Sequence < 0 {
		return snapshotError(CodeInvalidSnapshot, "sequence must be >= 0")
	}
	if err := validatePayload(snapshot.Payload); err != nil {
		return err
	}
	if len(snapshot.Artifacts) > MaxArtifactCount {
		return snapshotError(CodeInvalidArtifactURI, "at most %d artifacts", MaxArtifactCount)
	}
	for _, artifact := range snapshot.Artifacts {
		if err := validateArtifact(artifact); err != nil {
			return err
		}
	}
	return nil
}

func validatePayload(payload json.RawMessage) error {
	if len(payload) == 0 {
		return snapshotError(CodeInvalidJSON, "payload is required")
	}
	if len(payload) > MaxPayloadBytes {
		return snapshotError(CodePayloadTooLarge, "payload exceeds %d bytes", MaxPayloadBytes)
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return snapshotError(CodeInvalidJSON, "payload must be a JSON object")
	}
	var object map[string]any
	if err := json.Unmarshal(trimmed, &object); err != nil {
		return snapshotError(CodeInvalidJSON, "payload is not valid JSON")
	}
	if holdsNUL(object) {
		// The store's JSON cannot keep it: refused here, where the job
		// hears why, instead of retried against the store forever (20#3).
		return snapshotError(CodeInvalidJSON, "payload holds a NUL character (\\u0000), which reports cannot keep")
	}
	return nil
}

// holdsNUL reports a NUL in any string or key of a decoded JSON value.
func holdsNUL(value any) bool {
	switch v := value.(type) {
	case string:
		return strings.ContainsRune(v, 0)
	case []any:
		for _, item := range v {
			if holdsNUL(item) {
				return true
			}
		}
	case map[string]any:
		for key, item := range v {
			if strings.ContainsRune(key, 0) || holdsNUL(item) {
				return true
			}
		}
	}
	return false
}

func validateArtifact(artifact ArtifactRef) error {
	if strings.TrimSpace(artifact.URI) == "" {
		return snapshotError(CodeInvalidArtifactURI, "artifact uri is required")
	}
	if len(artifact.URI) > MaxURILength {
		return snapshotError(CodeInvalidArtifactURI, "artifact uri exceeds %d bytes", MaxURILength)
	}
	if artifact.Name != "" {
		if err := requireID(artifact.Name, "artifact name", MaxNameLength); err != nil {
			return snapshotError(CodeInvalidArtifactURI, "%s", err.Error())
		}
	}
	return ValidateStoreURI(artifact.URI)
}

// ValidateStoreURI is whether a job's machine may name uri as something it
// stored: one of the schemes a store or a download reads.
func ValidateStoreURI(uri string) error {
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme == "" {
		return snapshotError(CodeInvalidArtifactURI, "artifact uri must include a scheme")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if _, ok := allowedArtifactSchemes[scheme]; !ok {
		return snapshotError(CodeInvalidArtifactURI, "artifact scheme %q is not allowed", parsed.Scheme)
	}
	if scheme == "http" || scheme == "https" {
		if parsed.Host == "" {
			return snapshotError(CodeInvalidArtifactURI, "http artifact uri requires a host")
		}
	}
	return nil
}

// ValidateCheckpointURI is whether a job's machine may name uri as a checkpoint
// of a job whose spec chose output: a store, or a place under that output,
// whatever its scheme (a folder its machines share). A replacement reads the
// URI, so a machine names nothing else of its own choosing.
func ValidateCheckpointURI(uri, output string) error {
	if underOutput(uri, output) {
		return nil
	}
	return ValidateStoreURI(uri)
}

func underOutput(uri, output string) bool {
	parsed, err := url.Parse(uri)
	base, baseErr := url.Parse(output)
	if err != nil || baseErr != nil || output == "" {
		return false
	}
	return parsed.Scheme == base.Scheme && parsed.Host == base.Host &&
		strings.HasPrefix(path.Clean(parsed.Path), strings.TrimRight(path.Clean(base.Path), "/")+"/")
}

func requireID(value, field string, max int) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return snapshotError(CodeInvalidSnapshot, "%s is required", field)
	}
	if trimmed != value {
		return snapshotError(CodeInvalidSnapshot, "%s must not have leading or trailing space", field)
	}
	if strings.ContainsAny(trimmed, "\x00\n\r") {
		return snapshotError(CodeInvalidSnapshot, "%s contains invalid characters", field)
	}
	if utf8.RuneCountInString(trimmed) > max {
		return snapshotError(CodeInvalidSnapshot, "%s exceeds %d characters", field, max)
	}
	return nil
}

type Error struct {
	Code    string
	Message string
	// Err is the cause, when the error was another's (a store's).
	Err error
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, format string, args ...interface{}) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func snapshotError(code, format string, args ...interface{}) *Error {
	return NewError(code, format, args...)
}

// CodeOf is the contract code of err, through any wrapping. An error
// without one is STORE_UNAVAILABLE: retried, never read as the report's
// own fault (c4-C2).
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if typed := (*Error)(nil); errors.As(err, &typed) {
		return typed.Code
	}
	return CodeStoreUnavailable
}
