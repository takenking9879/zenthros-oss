// SPDX-License-Identifier: Apache-2.0

package capwire

import (
	"reflect"
	"testing"

	"github.com/zenthros/operator/pkg/capability"
)

// Every Host field crosses the wire: a field added to Host but not to the
// proto mapping would be dropped silently, and the controller would decide
// on a fact it never received (a container's image, say).
func TestHostRoundTripsEveryField(t *testing.T) {
	var host capability.Host
	fill(t, reflect.ValueOf(&host).Elem())
	if got := HostFromProto(HostToProto(host)); !reflect.DeepEqual(got, host) {
		t.Fatalf("round trip lost fields:\n got %+v\nwant %+v", got, host)
	}
}

// fill sets every field of the struct v to a value of its own, a slice to
// one element (a struct's, filled the same way).
func fill(t *testing.T, v reflect.Value) {
	t.Helper()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(v.Type().Field(i).Name)
		case reflect.Int32, reflect.Int64:
			f.SetInt(int64(i + 1))
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Slice:
			one := reflect.New(f.Type().Elem()).Elem()
			switch one.Kind() {
			case reflect.String:
				one.SetString(v.Type().Field(i).Name)
			case reflect.Struct:
				fill(t, one)
			default:
				t.Fatalf("field %s is a slice of %s; teach this test to fill it", v.Type().Field(i).Name, one.Kind())
			}
			f.Set(reflect.Append(reflect.MakeSlice(f.Type(), 0, 1), one))
		default:
			t.Fatalf("field %s has kind %s; teach this test to fill it", v.Type().Field(i).Name, f.Kind())
		}
	}
}

// Every Workload field crosses the wire too: an agent's Endpoints dropped
// here would make the controller refuse every Service it runs.
func TestWorkloadRoundTripsEveryField(t *testing.T) {
	var w capability.Workload
	v := reflect.ValueOf(&w).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(v.Type().Field(i).Name)
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Slice:
			f.Set(reflect.ValueOf([]string{v.Type().Field(i).Name}))
		default:
			t.Fatalf("field %s has kind %s; teach this test to fill it", v.Type().Field(i).Name, f.Kind())
		}
	}
	set := capability.Set{w}
	if got := FromProto(ToProto(set)); !reflect.DeepEqual(got, set) {
		t.Fatalf("round trip lost fields:\n got %+v\nwant %+v", got, set)
	}
}
