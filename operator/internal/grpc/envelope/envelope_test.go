// SPDX-License-Identifier: Apache-2.0

package envelope

import (
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"strings"
	"testing"

	"filippo.io/age"

	v1 "github.com/zenthros/operator/api/v1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/agepayload"
	"github.com/zenthros/operator/pkg/jobspec"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := jobspec.JobSpec{
		ID:           "uid-123",
		Name:         "vm-hello",
		Namespace:    "tenant-default",
		Tenant:       "default",
		Backend:      "vm",
		Kind:         v1.WorkloadKindRayJob,
		Entrypoint:   `python3 -c "print('hi')"`,
		Image:        "ray:latest",
		Env:          map[string]string{"DATASET_TOKEN": "s3cr3t", "FOO": "bar"},
		Workers:      v1.WorkerGroup{Min: 0, Max: 2, ResourceClass: "cpu", CPU: "2"},
		BackoffLimit: 3,
		Source:       v1.Source{HostPath: "/work"},
	}

	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if env.Id != original.ID || env.Name != original.Name || env.Namespace != original.Namespace || env.Kind != original.Kind {
		t.Errorf("envelope identity = %s/%s/%s/%s, want %s/%s/%s/%s", env.Id, env.Name, env.Namespace, env.Kind, original.ID, original.Name, original.Namespace, original.Kind)
	}
	if env.ApiVersion != apiVersion {
		t.Errorf("apiVersion = %q, want %q", env.ApiVersion, apiVersion)
	}

	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ID != original.ID || got.Name != original.Name || got.Namespace != original.Namespace || got.Kind != original.Kind {
		t.Errorf("decoded identity mismatch: %+v", got)
	}
	if got.Entrypoint != original.Entrypoint || got.Image != original.Image {
		t.Errorf("decoded entrypoint/image mismatch: %q %q", got.Entrypoint, got.Image)
	}
	if got.Env["DATASET_TOKEN"] != "s3cr3t" || got.Env["FOO"] != "bar" {
		t.Errorf("decoded env = %+v", got.Env)
	}
	if got.Workers.Max != 2 || got.Workers.ResourceClass != "cpu" {
		t.Errorf("decoded workers = %+v", got.Workers)
	}
	if got.BackoffLimit != 3 {
		t.Errorf("decoded backoffLimit = %d", got.BackoffLimit)
	}
	if got.Source.HostPath != "/work" {
		t.Errorf("decoded source = %+v", got.Source)
	}
}

func TestDecodeNilEnvelopeErrors(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Error("Decode(nil) should error")
	}
}

func TestEncodeDecodeGitSource(t *testing.T) {
	original := jobspec.JobSpec{
		ID:         "uid-git",
		Name:       "git-job",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "python train.py",
		Image:      "ray:latest",
		Source: v1.Source{
			Git:          "git@github.com:o/r.git",
			GitRef:       "master",
			GitSecretRef: "zenthros-git-ssh",
		},
	}

	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Source != original.Source {
		t.Errorf("decoded source = %+v, want %+v", got.Source, original.Source)
	}
}

// An agent older than its control plane must not run a job without a setting
// it was asked to honour: an unknown spec field fails the decode, and the
// decoder's own message names the field.
func TestDecodeUnknownFieldIsRejectedVerbatim(t *testing.T) {
	env, err := Encode(jobspec.JobSpec{ID: "x", Kind: v1.WorkloadKindRayJob, Entrypoint: "echo hi"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	env.SpecJson = `{"entrypoint":"echo hi","futureField":42}`
	_, err = Decode(env)
	if err == nil || !strings.Contains(err.Error(), `json: unknown field "futureField"`) {
		t.Fatalf("Decode with unknown field = %v, want the decoder's unknown-field message", err)
	}
}

// fill sets every field reachable from v to a distinct non-zero value.
func fill(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	if v.Type() == reflect.TypeOf(runtime.RawExtension{}) {
		// An embedded object travels as its JSON bytes.
		v.Set(reflect.ValueOf(runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + path + `"}}`)}))
		return
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString("v-" + path)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(7 + len(path)))
	case reflect.Ptr:
		v.Set(reflect.New(v.Type().Elem()))
		fill(t, v.Elem(), path)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				fill(t, v.Field(i), path+"."+v.Type().Field(i).Name)
			}
		}
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fill(t, v.Index(0), path+"[0]")
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		key := reflect.New(v.Type().Key()).Elem()
		fill(t, key, path+".key")
		value := reflect.New(v.Type().Elem()).Elem()
		fill(t, value, path+".value")
		v.SetMapIndex(key, value)
	default:
		t.Fatalf("fill: unsupported kind %s at %s; teach fill or keep the field off the wire", v.Kind(), path)
	}
}

// Every JobSpec field the controller computes must reach the agent unchanged.
// A parallel wire struct once dropped Lifespan, so Retain never reached a spoke
// cluster; the codec now carries the JobSpec itself.
func TestEncodeDecodeIsIdentity(t *testing.T) {
	var spec jobspec.JobSpec
	fill(t, reflect.ValueOf(&spec).Elem(), "spec")
	raw, err := marshalSpec(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := unmarshalSpec(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got.ID, got.Name, got.Namespace, got.Kind = spec.ID, spec.Name, spec.Namespace, spec.Kind
	if !reflect.DeepEqual(got, spec) {
		t.Fatalf("round trip lost data:\n got %+v\nwant %+v", got, spec)
	}
}

// A field without a json tag would travel under its Go name and never match
// what the other side expects; json:"-" is reserved for the identity fields
// the envelope carries itself.
func TestEveryJobSpecFieldHasJSONTag(t *testing.T) {
	identity := map[string]bool{"ID": true, "Name": true, "Namespace": true, "Kind": true}
	typ := reflect.TypeOf(jobspec.JobSpec{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag, ok := field.Tag.Lookup("json")
		if !ok || tag == "" {
			t.Errorf("JobSpec.%s has no json tag", field.Name)
			continue
		}
		if (tag == "-") != identity[field.Name] {
			t.Errorf("JobSpec.%s json tag %q: only the envelope identity fields are \"-\"", field.Name, tag)
		}
	}
}

func TestEncodeDecodeCarriesLifespanAndRuntime(t *testing.T) {
	spec := jobspec.JobSpec{
		ID: "uid-r", Name: "retain", Namespace: "tenant-a", Tenant: "a", Kind: v1.WorkloadKindRayJob,
		Entrypoint: "python x.py", Image: "ray:2", Lifespan: v1.LifespanRetain, Runtime: v1.RuntimeContainerd,
	}
	env, err := Encode(spec)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Lifespan != v1.LifespanRetain || got.Runtime != v1.RuntimeContainerd || got.Tenant != "a" {
		t.Fatalf("decoded lifespan=%q runtime=%q tenant=%q", got.Lifespan, got.Runtime, got.Tenant)
	}
}

func TestEncodeDecodeInlineSource(t *testing.T) {
	original := jobspec.JobSpec{
		ID:         "uid-inline",
		Name:       "inline-job",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "python script.py",
		Image:      "ray:latest",
		Source:     v1.Source{Inline: "H4sIAAAAAAAA/wEAAP//dGVzdA=="},
	}

	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Source.Inline != original.Source.Inline {
		t.Errorf("decoded inline = %q, want %q", got.Source.Inline, original.Source.Inline)
	}
	if got.Source.Git != "" || got.Source.HostPath != "" {
		t.Errorf("unexpected non-inline fields: %+v", got.Source)
	}
}

func TestEncodeDecodeWorkspace(t *testing.T) {
	original := jobspec.JobSpec{
		ID:         "uid-ws",
		Name:       "ws-job",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindRayJob,
		Entrypoint: "python train.py",
		Image:      "ray:latest",
		Workspace:  "ml-research",
	}

	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Workspace != "ml-research" {
		t.Errorf("decoded workspace = %q, want %q", got.Workspace, "ml-research")
	}
}

func TestEncodeDecodeRayClusterReference(t *testing.T) {
	original := jobspec.JobSpec{
		ID: "uid-shared", Name: "shared-job", Namespace: "tenant-default",
		Kind: v1.WorkloadKindRayJob, Entrypoint: "python train.py", Image: "ray:latest",
		RayCluster: &v1.RayClusterReference{Name: "shared-ray", Namespace: "tenant-default"},
	}
	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.RayCluster == nil || *got.RayCluster != *original.RayCluster {
		t.Fatalf("decoded RayCluster reference = %+v, want %+v", got.RayCluster, original.RayCluster)
	}
}

func TestEncodeDecodeStructuredExecution(t *testing.T) {
	original := jobspec.JobSpec{
		ID:         "uid-structured",
		Name:       "structured-job",
		Namespace:  "tenant-default",
		Kind:       v1.WorkloadKindRayJob,
		Command:    []string{"python", "train.py"},
		Args:       []string{"--epochs", "10"},
		WorkingDir: "/work",
		Image:      "ray:latest",
	}

	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Command) != 2 || got.Command[0] != "python" || got.Command[1] != "train.py" {
		t.Errorf("decoded command = %v, want [python train.py]", got.Command)
	}
	if len(got.Args) != 2 || got.Args[0] != "--epochs" || got.Args[1] != "10" {
		t.Errorf("decoded args = %v, want [--epochs 10]", got.Args)
	}
	if got.WorkingDir != "/work" {
		t.Errorf("decoded workingDir = %q, want /work", got.WorkingDir)
	}
	if got.Entrypoint != "" {
		t.Errorf("decoded entrypoint = %q, want empty for a structured spec", got.Entrypoint)
	}
}

func TestEncodeDecodeRetainWorkdir(t *testing.T) {
	original := jobspec.JobSpec{
		ID:            "uid-rw",
		Name:          "rw-job",
		Namespace:     "tenant-default",
		Kind:          v1.WorkloadKindRayJob,
		Entrypoint:    "python train.py",
		Image:         "ray:latest",
		RetainWorkdir: true,
	}

	env, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(env)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !got.RetainWorkdir {
		t.Error("decoded retainWorkdir = false, want true")
	}
}

func TestEncodeDecodeSecretSourceCarriesReferenceOnly(t *testing.T) {
	const plaintext = "never-cross-the-control-plane"
	original := jobspec.JobSpec{
		ID: "secret-job", Name: "secret-job", Namespace: "tenant-default",
		Kind: v1.WorkloadKindRayJob, Image: "ray:latest",
		SecretSources: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeKubernetes, Namespace: "tenant-default",
			Name: "api-token", Key: "token",
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "API_TOKEN"},
		}},
	}
	wire, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if strings.Contains(wire.SpecJson, plaintext) {
		t.Fatal("envelope contains plaintext secret")
	}
	if !strings.Contains(wire.SpecJson, `"name":"api-token"`) {
		t.Fatalf("envelope missing reference metadata: %s", wire.SpecJson)
	}
	decoded, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(decoded.SecretSources, original.SecretSources) {
		t.Errorf("secret sources = %#v, want %#v", decoded.SecretSources, original.SecretSources)
	}
}

func TestEncodeDecodeAWSSecretSourceCarriesReferenceOnly(t *testing.T) {
	const plaintext = "never-cross-the-control-plane"
	original := jobspec.JobSpec{
		ID: "secret-job", Name: "secret-job", Namespace: "tenant-default",
		Kind: v1.WorkloadKindRayJob, Image: "ray:latest",
		SecretSources: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeAWSSecretsManager,
			AWSSecretsManager: &v1.AWSSecretsManagerRef{
				SecretID: "workload/token", Region: "us-east-2", JSONKey: "token",
			},
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T14_SECRET"},
		}},
	}
	wire, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if strings.Contains(wire.SpecJson, plaintext) {
		t.Fatal("envelope contains plaintext secret")
	}
	if !strings.Contains(wire.SpecJson, `"secretId":"workload/token"`) || !strings.Contains(wire.SpecJson, `"region":"us-east-2"`) {
		t.Fatalf("envelope missing AWS reference metadata: %s", wire.SpecJson)
	}
	decoded, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(decoded.SecretSources, original.SecretSources) {
		t.Errorf("secret sources = %#v, want %#v", decoded.SecretSources, original.SecretSources)
	}
}

func TestEncodeDecodeEncryptedPayloadCarriesCiphertextOnly(t *testing.T) {
	const plaintext = "never-cross-the-control-plane"
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := agepayload.Encrypt([]byte(plaintext), []string{identity.Recipient().String()})
	if err != nil {
		t.Fatal(err)
	}
	original := jobspec.JobSpec{
		ID: "secret-job", Name: "secret-job", Namespace: "tenant-default",
		Kind: v1.WorkloadKindRayJob, Image: "ray:latest",
		SecretSources: []v1.SecretSourceRef{{
			Type: v1.SecretSourceTypeEncryptedPayload,
			EncryptedPayload: &v1.EncryptedPayloadRef{
				Ciphertext: ciphertext,
				Recipients: []string{identity.Recipient().String()},
				KeyID:      agepayload.KeyID(identity.Recipient().String()),
			},
			Materialization: v1.SecretMaterialization{Mode: v1.SecretMaterializationEnv, EnvName: "T15_SECRET"},
		}},
	}
	wire, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if strings.Contains(wire.SpecJson, plaintext) {
		t.Fatal("envelope contains plaintext secret")
	}
	if !strings.Contains(wire.SpecJson, `"type":"encrypted-payload"`) || !strings.Contains(wire.SpecJson, agepayload.ArmorBegin) {
		t.Fatalf("envelope missing ciphertext: %s", wire.SpecJson)
	}
	decoded, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(decoded.SecretSources, original.SecretSources) {
		t.Errorf("secret sources = %#v, want %#v", decoded.SecretSources, original.SecretSources)
	}
}

func TestDecodeRejectsInvalidSecretSource(t *testing.T) {
	wire := &agentpb.JobEnvelope{
		Id: "x", Kind: v1.WorkloadKindRayJob, ApiVersion: apiVersion,
		SpecJson: `{"image":"ray:latest","workers":{"min":0,"max":0},"secretSources":[{"type":"k8s-secret","namespace":"tenant-default","name":"token","key":"value","materialization":{"mode":"inline"}}]}`,
	}
	if _, err := Decode(wire); err == nil {
		t.Fatal("Decode accepted an invalid secret materialization")
	}
}

func TestSparkEncodeDecodeRoundTrip(t *testing.T) {
	original := jobspec.JobSpec{
		ID: "spark-1", Name: "spark-pi", Namespace: "tenant-default",
		Kind: v1.WorkloadKindSparkApplication, Image: "zenthros/spark-base:4.1.1",
		Source: v1.Source{Git: "https://example.invalid/repo.git"}, Workspace: "data",
		Spark: &v1.SparkSpec{
			MainApplicationFile: "local:///app.py", Arguments: []string{"--rows", "10"},
			SparkConf: map[string]string{"spark.sql.shuffle.partitions": "2"},
			Driver:    v1.SparkResourceSpec{CPU: "1", Memory: "512m"},
			Executor:  v1.SparkResourceSpec{CPU: "2", Memory: "1g"}, ExecutorCount: 3,
			RestartPolicy: v1.SparkRestartPolicy{Type: v1.SparkRestartOnFailure, MaxRestartAttempts: 2},
		},
	}
	wire, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got.Spark, original.Spark) || got.Source != original.Source || got.Workspace != original.Workspace {
		t.Fatalf("decoded Spark spec = %+v, source=%+v, workspace=%q", got.Spark, got.Source, got.Workspace)
	}
}

func TestDecodeRejectsUnsupportedMajorVersion(t *testing.T) {
	wire := &agentpb.JobEnvelope{
		Id: "x", Kind: v1.WorkloadKindRayJob, ApiVersion: "zenthros.ai/v2",
		SpecJson: `{"entrypoint":"echo hi"}`,
	}
	if _, err := Decode(wire); err == nil || !strings.Contains(err.Error(), "unsupported apiVersion") {
		t.Fatalf("Decode error = %v, want unsupported apiVersion", err)
	}
}
