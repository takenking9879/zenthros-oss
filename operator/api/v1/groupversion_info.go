// SPDX-License-Identifier: Apache-2.0

// Package v1 contains the zenthros.ai/v1 API types shared by the controller and the agent.
// +kubebuilder:object:generate=true
// +groupName=zenthros.ai
package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

const (
	GroupName = "zenthros.ai"
	Version   = "v1"
)

// GroupVersion identifies the zenthros.ai/v1 API group.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// SchemeBuilder registers the API types with a runtime scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the API types to the given scheme.
var AddToScheme = SchemeBuilder.AddToScheme
