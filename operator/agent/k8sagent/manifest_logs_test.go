// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Each container's log opens on its own: an open waits up to a grace for
// its container to start, and one after another, pods still creating held
// back every line for a grace each (06#7). Here each open takes 300 ms.
func TestManifestLogsOpenEveryContainerAtOnce(t *testing.T) {
	root := &unstructured.Unstructured{}
	root.SetGroupVersionKind(widgetGVK)
	root.SetNamespace("tenant-a")
	root.SetName("train")
	root.SetUID("uid-widget")
	var pods []client.Object
	var objects []runtime.Object
	for i := range 4 {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: fmt.Sprintf("train-%d", i),
				OwnerReferences: owned("Widget", "e2e.zenthros.ai/v1", "train", "uid-widget")},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		}
		pods = append(pods, pod)
		objects = append(objects, pod)
	}
	e := manifestExecutor(t, nil, append([]client.Object{root}, pods...)...)
	e.clientset = slowLogs{Interface: kubefake.NewSimpleClientset(objects...), open: 300 * time.Millisecond}

	start := time.Now()
	ch, err := e.Logs(context.Background(), manifestHandle(widgetGVK, "tenant-a", "train"), false, -1)
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for range ch {
		lines++
	}
	if took := time.Since(start); lines != 4 || took > 900*time.Millisecond {
		t.Fatalf("%d lines in %s, want 4 in about one open (300 ms), not four", lines, took)
	}
}

// slowLogs is a clientset whose pod log opens each take open. The fake
// clientset's reactors run under one lock, so a reactor that waits makes
// every open wait for the others; this waits in the HTTP round trip.
type slowLogs struct {
	kubernetes.Interface
	open time.Duration
}

func (s slowLogs) CoreV1() corev1client.CoreV1Interface {
	return slowCore{CoreV1Interface: s.Interface.CoreV1(), open: s.open}
}

type slowCore struct {
	corev1client.CoreV1Interface
	open time.Duration
}

func (c slowCore) Pods(namespace string) corev1client.PodInterface {
	return slowPods{PodInterface: c.CoreV1Interface.Pods(namespace), open: c.open}
}

type slowPods struct {
	corev1client.PodInterface
	open time.Duration
}

func (p slowPods) GetLogs(name string, _ *corev1.PodLogOptions) *restclient.Request {
	rest := &restfake.RESTClient{
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
		Client: restfake.CreateHTTPClient(func(*http.Request) (*http.Response, error) {
			time.Sleep(p.open)
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(name + " line\n"))}, nil
		}),
	}
	return rest.Get().Resource("pods").Name(name).SubResource("log")
}
