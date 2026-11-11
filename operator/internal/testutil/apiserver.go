// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/zenthros/operator/api/v1"
)

// APIServerJobSelectors is a fake client's List as the API server answers a
// ZenthrosJob list, for readers that go to it directly (zctl, the gateway's
// admin): it selects on the CRD's selectable fields (SelectableJobFields),
// with = and !=, and refuses any other field (spec.target.backends) with a
// BadRequest. The fake alone selects only on indexes, and only with =.
func APIServerJobSelectors(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	jobs, ok := list.(*v1.ZenthrosJobList)
	o := (&client.ListOptions{}).ApplyOptions(opts)
	if !ok || o.FieldSelector == nil || o.FieldSelector.Empty() {
		return c.List(ctx, list, opts...)
	}
	selectable, err := SelectableJobFields()
	if err != nil {
		return err
	}
	for _, requirement := range o.FieldSelector.Requirements() {
		if !selectable[requirement.Field] {
			return apierrors.NewBadRequest("field label not supported: " + requirement.Field)
		}
	}
	selector := o.FieldSelector
	o.FieldSelector = nil
	if err := c.List(ctx, jobs, o); err != nil {
		return err
	}
	kept := jobs.Items[:0]
	for i := range jobs.Items {
		set, err := jobFieldSet(&jobs.Items[i], selectable)
		if err != nil {
			return err
		}
		if selector.Matches(set) {
			kept = append(kept, jobs.Items[i])
		}
	}
	jobs.Items = kept
	return nil
}

// jobFieldSet is job's value of each selectable field, "" where it has none.
func jobFieldSet(job *v1.ZenthrosJob, selectable map[string]bool) (fields.Set, error) {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(job)
	if err != nil {
		return nil, err
	}
	set := fields.Set{}
	for path := range selectable {
		value, _, _ := unstructured.NestedString(obj, strings.Split(path, ".")...)
		set[path] = value
	}
	return set, nil
}

var (
	selectableOnce sync.Once
	selectable     map[string]bool
	selectableErr  error
)

// SelectableJobFields are the fields the API server selects ZenthrosJobs on:
// the CRD's selectableFields (config/crd), and the name and namespace every
// object has. Any other field selector fails the LIST.
func SelectableJobFields() (map[string]bool, error) {
	selectableOnce.Do(func() {
		_, file, _, _ := goruntime.Caller(0)
		var raw []byte
		if raw, selectableErr = os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "config", "crd", "zenthros.ai_zenthrosjobs.yaml")); selectableErr != nil {
			return
		}
		var crd struct {
			Spec struct {
				Versions []struct {
					SelectableFields []struct {
						JSONPath string `json:"jsonPath"`
					} `json:"selectableFields"`
				} `json:"versions"`
			} `json:"spec"`
		}
		if selectableErr = yaml.Unmarshal(raw, &crd); selectableErr != nil {
			return
		}
		selectable = map[string]bool{"metadata.name": true, "metadata.namespace": true}
		for _, version := range crd.Spec.Versions {
			for _, field := range version.SelectableFields {
				selectable[strings.TrimPrefix(field.JSONPath, ".")] = true
			}
		}
		if len(selectable) == 2 {
			selectableErr = errors.New("the ZenthrosJob CRD declares no selectableFields")
		}
	})
	return selectable, selectableErr
}
