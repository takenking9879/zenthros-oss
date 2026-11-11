// SPDX-License-Identifier: Apache-2.0

// Package v1alpha1 contains the infrastructure.zenthros.io API used by the
// managed-compute control plane.
// +kubebuilder:object:generate=true
// +groupName=infrastructure.zenthros.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

const (
	GroupName = "infrastructure.zenthros.io"
	Version   = "v1alpha1"
)

var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

var AddToScheme = SchemeBuilder.AddToScheme
