// SPDX-License-Identifier: Apache-2.0

package containerd

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/zenthros/operator/pkg/jobspec"
	"github.com/zenthros/operator/pkg/systemd"
)

func TestECRRegion(t *testing.T) {
	for host, want := range map[string]string{
		"123456789012.dkr.ecr.us-east-2.amazonaws.com":          "us-east-2",
		"123456789012.dkr.ecr-fips.us-gov-west-1.amazonaws.com": "us-gov-west-1",
		"123456789012.dkr.ecr.cn-north-1.amazonaws.com.cn":      "cn-north-1",
		"public.ecr.aws": "",
		"docker.io":      "",
		"evil.com/123456789012.dkr.ecr.us-east-2.amazonaws.com": "",
	} {
		if got := ECRRegion(host); got != want {
			t.Errorf("ECRRegion(%q) = %q, want %q", host, got, want)
		}
	}
}

// pullRunner answers ctr's image listing with nothing and records, for each
// pull, the command and the login the command would read.
type pullRunner struct {
	commands []string
	logins   []string
}

var catFile = regexp.MustCompile(`\$\(cat '([^']+)'\)`)

func (r *pullRunner) Run(_ context.Context, cmd string) (string, error) {
	r.commands = append(r.commands, cmd)
	if m := catFile.FindStringSubmatch(cmd); m != nil {
		b, err := os.ReadFile(m[1])
		if err != nil {
			return "", err
		}
		r.logins = append(r.logins, string(b))
	}
	return "", nil
}

// An ECR image is pulled with a token the machine's own role obtains
// (GetAuthorizationToken, SigV4 for the registry's region); the password is
// read from a private file, never written into the command, and the file is
// gone after the pull. Other registries get no login.
func TestECRHostUsesInstanceRoleToken(t *testing.T) {
	var target, authz string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, authz = r.Header.Get("X-Amz-Target"), r.Header.Get("Authorization")
		token := base64.StdEncoding.EncodeToString([]byte("AWS:s3cr3t-registry-password"))
		_, _ = w.Write([]byte(`{"authorizationData":[{"authorizationToken":"` + token + `","proxyEndpoint":"https://x"}]}`))
	}))
	defer api.Close()
	auth := ECRAuth{
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKIAROLEFROMIMDS", "secret", "session")),
		Endpoint:    api.URL,
	}
	ref := "123456789012.dkr.ecr.us-east-2.amazonaws.com/team/train:1.4"
	runner := &pullRunner{}
	pulled, err := EnsureImageWith(context.Background(), runner, "sudo ", ref, auth)
	if err != nil || !pulled {
		t.Fatalf("pulled=%v err=%v", pulled, err)
	}
	if target != "AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken" {
		t.Fatalf("ECR call %q", target)
	}
	if !strings.Contains(authz, "Credential=AKIAROLEFROMIMDS/") || !strings.Contains(authz, "/us-east-2/ecr/aws4_request") {
		t.Fatalf("not signed with the machine's role for the registry's region: %q", authz)
	}
	pull := runner.commands[len(runner.commands)-1]
	if !strings.Contains(pull, "images pull --user \"$(cat '") || !strings.HasSuffix(pull, "'"+ref+"'") {
		t.Fatalf("pull command %q", pull)
	}
	if strings.Contains(strings.Join(runner.commands, "\n"), "s3cr3t") {
		t.Fatal("the registry password is in a command")
	}
	if len(runner.logins) != 1 || runner.logins[0] != "AWS:s3cr3t-registry-password" {
		t.Fatalf("ctr read login %q", runner.logins)
	}
	if path := catFile.FindStringSubmatch(pull)[1]; fileExists(path) {
		t.Fatal("the login file outlived the pull")
	}

	runner = &pullRunner{}
	target = ""
	if _, err := EnsureImageWith(context.Background(), runner, "sudo ", "ghcr.io/org/app:1", auth); err != nil {
		t.Fatal(err)
	}
	if target != "" || strings.Contains(runner.commands[len(runner.commands)-1], "--user") {
		t.Fatal("a non-ECR registry got an ECR login")
	}
}

// ECR's own refusal reaches the job as ECR said it.
func TestECRDeniedIsVerbatim(t *testing.T) {
	denied := `{"__type":"AccessDeniedException","message":"User: arn:aws:sts::123456789012:assumed-role/worker/i-0 is not authorized to perform: ecr:GetAuthorizationToken"}`
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(denied))
	}))
	defer api.Close()
	auth := ECRAuth{Credentials: credentials.NewStaticCredentialsProvider("A", "B", ""), Endpoint: api.URL}
	runner := &pullRunner{}
	_, err := EnsureImageWith(context.Background(), runner, "", "123456789012.dkr.ecr.us-east-2.amazonaws.com/x:1", auth)
	if err == nil || !strings.Contains(err.Error(), denied) {
		t.Fatalf("err = %v, want ECR's answer verbatim", err)
	}
	for _, c := range runner.commands {
		if strings.Contains(c, "images pull") {
			t.Fatal("pulled without a login after ECR refused one")
		}
	}
}

// A job that asks for GPUs gets them in its container: ctr run --gpus per
// device, the NVIDIA toolkit's hook doing the rest.
func TestGPURequestAddsCtrGpus(t *testing.T) {
	spec := jobspec.JobSpec{ID: "j1", Image: "nvcr.io/nvidia/cuda:12.4.0-base-ubuntu22.04"}
	spec.Workers.GPUsPerNode = 2
	script := RenderRunScript(RunOptsFromSpec(spec, systemd.Config{}, nil, ""))
	if !strings.Contains(script, "args+=(--gpus 0)\nargs+=(--gpus 1)\n") || strings.Contains(script, "--gpus 2") {
		t.Fatalf("two GPUs not rendered:\n%s", script)
	}
	if strings.Index(script, "--gpus 0") > strings.Index(script, `args+=("$image" "$name")`) {
		t.Fatal("--gpus after the image: ctr would pass it to the job")
	}
	spec.Workers.GPUsPerNode = 0
	if script := RenderRunScript(RunOptsFromSpec(spec, systemd.Config{}, nil, "")); strings.Contains(script, "--gpus") {
		t.Fatal("a CPU job got --gpus")
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
