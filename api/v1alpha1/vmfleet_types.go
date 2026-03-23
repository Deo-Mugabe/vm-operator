/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VmFleetSpec defines the desired state of VmFleet
type VmFleetSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html
	
	// controller-gen reads +k.. and bakes them into the CRD YAML as OpenAPI validation rules. 
	
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4
	CPU int32 `json:"cpu"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=8
	Memory int32 `json:"memory"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=100
	DiskGB int32 `json:"diskGB"`

	// +kubebuilder:validation:Required
	Template string `json:"template"`
}

type FleetPhase string

const (
	RunningFleetPhase FleetPhase = "RUNNING"
	PendingFleetPhase FleetPhase = "PENDING"
	ErrorFleetPhase   FleetPhase = "ERROR"
)


//Status is what the operator writes back to Kubernetes after doing work. 
// It's how you communicate "here's what actually exists" vs what was requested in the Spec.
// VmFleetStatus defines the observed state of VmFleet.
type VmFleetStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the VmFleet resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// +kubebuilder:validation:Optional
	Phase           FleetPhase `json:"phase,omitempty"`
	CurrentReplicas *int32     `json:"currentReplicas,omitempty"`
	DesiredReplicas int32      `json:"desiredReplicas"`
	LastMessage     string     `json:"lastMessage,omitempty"`
}
// printcolumns These define what columns appear when you run kubectl get vmfleets
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName={"vf"}
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.currentReplicas`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="CPU",type=integer,JSONPath=`.spec.cpu`
// +kubebuilder:printcolumn:name="Memory",type=integer,JSONPath=`.spec.memory`
// +kubebuilder:printcolumn:name="DiskGB",type=integer,JSONPath=`.spec.diskGB`
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=`.spec.template`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.lastMessage`


// VmFleet is the Schema for the vmfleets API
type VmFleet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of VmFleet - what you want
	// +required
	Spec VmFleetSpec `json:"spec,omitempty"`

	// status defines the observed state of VmFleet -  what exists
	// +optional
	Status VmFleetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VmFleetList contains a list of VmFleet
type VmFleetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VmFleet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VmFleet{}, &VmFleetList{})
}
