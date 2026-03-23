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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"golang.org/x/sync/errgroup"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmv1alpha1 "vmfleet/operator/api/v1alpha1"
)

const (
	// fleetFinalizer is added to every VmFleet so we can clean up
	// vCenter resources before the CR is deleted from Kubernetes
	fleetFinalizer = "vmfleet.vm.operator.io/finalizer"

	// defaultRequeue is how long to wait before retrying after a failure
	defaultRequeue = 20 * time.Second

	// successMsg is logged when reconciliation completes cleanly
	successMsg = "successfully reconciled VmFleet"
)

// VmFleetReconciler reconciles a VmFleet object
type VmFleetReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	VC           *govmomi.Client      // vCenter connection owned by this reconciler
	Finder       *find.Finder         // searches vCenter inventory by path
	ResourcePool *object.ResourcePool // where new VMs are placed
	Log          logr.Logger          // structured logger
}
// +kubebuilder:rbac:groups=vm.vm.operator.io,resources=vmfleets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=vm.vm.operator.io,resources=vmfleets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=vm.vm.operator.io,resources=vmfleets/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VmFleet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *VmFleetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("vmfleet", req.NamespacedName)

	// ── Step 1: Fetch the VmFleet object from Kubernetes ──────────────────
	fleet := &vmv1alpha1.VmFleet{}
	if err := r.Get(ctx, req.NamespacedName, fleet); err != nil {
		if k8serr.IsNotFound(err) {
			// Object was deleted before we could reconcile — nothing to do
			return ctrl.Result{}, nil
		}
		// Unexpected error fetching the object — log and requeue
		log.Error(err, "unable to fetch VmFleet")
		return ctrl.Result{}, err
	}

	log.Info("received reconcile request",
		"name", fleet.Name,
		"namespace", fleet.Namespace,
		"replicas", fleet.Spec.Replicas,
	)

	// ── Step 2: Handle deletion ────────────────────────────────────────────
	if !fleet.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, log, fleet)
	}

	// ── Step 3: Register finalizer if not present ──────────────────────────
	if !containsString(fleet.Finalizers, fleetFinalizer) {
		log.Info("adding finalizer", "finalizer", fleetFinalizer)
		fleet.Finalizers = append(fleet.Finalizers, fleetFinalizer)
		if err := r.Update(ctx, fleet); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to add finalizer")
		}
		return ctrl.Result{}, nil
	}

	// ── Step 4: Reconcile vCenter state ───────────────────────────────────
	return r.reconcileFleet(ctx, log, fleet)
}

// reconcileFleet does the actual work of making vCenter match the desired spec.
func (r *VmFleetReconciler) reconcileFleet(ctx context.Context, log logr.Logger, fleet *vmv1alpha1.VmFleet) (ctrl.Result, error) {
	folderName := fleetFolderName(fleet.Namespace, fleet.Name)
	desired := fleet.Spec.Replicas
	var notFound *find.NotFoundError

	// ── Step 4a: Ensure the fleet folder exists in vCenter ────────────────
	_, err := getFleet(ctx, r.Finder, folderName)
	if err != nil {
		if !errors.As(err, &notFound) {
			// Unexpected error — not a "not found" — something is wrong
			msg := fmt.Sprintf("unexpected error checking fleet folder %q", folderName)
			log.Error(err, msg)
			return r.setStatus(ctx, fleet, vmv1alpha1.ErrorFleetPhase, msg, err, nil, desired)
		}

		// Folder does not exist — create it
		log.Info("fleet folder not found, creating", "folder", folderName)
		if _, err := createFleet(ctx, r.Finder, folderName); err != nil {
			msg := "failed to create fleet folder in vCenter"
			log.Error(err, msg)
			return r.setStatus(ctx, fleet, vmv1alpha1.ErrorFleetPhase, msg, err, nil, desired)
		}
		log.Info("fleet folder created", "folder", folderName)
	}

	// ── Step 4b: Get current VMs in the fleet folder ──────────────────────
	vms, err := getReplicas(ctx, r.Finder, folderName)
	folderEmpty := false
	if err != nil {
		if !errors.As(err, &notFound) {
			msg := fmt.Sprintf("failed to list VMs in fleet folder %q", folderName)
			log.Error(err, msg)
			return r.setStatus(ctx, fleet, vmv1alpha1.ErrorFleetPhase, msg, err, nil, desired)
		}
		// NotFoundError here means the folder exists but has no VMs yet
		folderEmpty = true
	}

	// ── Step 4c: Scale up, scale down, or check power state ───────────────
	current := int32(len(vms))
	eg, egCtx := errgroup.WithContext(ctx)
	lim := newLimiter(defaultConcurrency)

	switch {
	case folderEmpty || current < desired:
		// We need more VMs — create the difference
		diff := desired - current
		if folderEmpty {
			diff = desired
		}
		log.Info("scaling up fleet", "current", current, "desired", desired, "creating", diff)

		for i := 0; i < int(diff); i++ {
			vmName := fmt.Sprintf("%s-vm-%s", fleet.Name, generateName())
			destPath := fleetPath + "/" + folderName
			log.Info("cloning VM", "name", vmName, "template", fleet.Spec.Template)

			eg.Go(func() error {
				lim.acquire()
				defer lim.release()
				if err := cloneVM(egCtx, r.Finder, fleet.Spec.Template, vmName, destPath, r.ResourcePool, fleet.Spec); err != nil {
					return errors.Wrapf(err, "failed to clone VM %q", vmName)
				}
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			msg := "one or more VM clones failed during scale up"
			log.Error(err, msg)
			return r.setStatus(ctx, fleet, vmv1alpha1.PendingFleetPhase, msg, err, &current, desired)
		}

		log.Info("scale up complete", "desired", desired)
		return r.setStatus(ctx, fleet, vmv1alpha1.RunningFleetPhase, successMsg, nil, &desired, desired)

	case current > desired:
		// We have too many VMs — delete the excess
		diff := current - desired
		log.Info("scaling down fleet", "current", current, "desired", desired, "deleting", diff)

		for i := 0; i < int(diff); i++ {
			vm := vms[i]
			log.Info("deleting VM", "name", vm.Name())

			eg.Go(func() error {
				lim.acquire()
				defer lim.release()
				if err := deleteVM(egCtx, vm); err != nil {
					return errors.Wrapf(err, "failed to delete VM %q", vm.Name())
				}
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			msg := "one or more VM deletions failed during scale down"
			log.Error(err, msg)
			return r.setStatus(ctx, fleet, vmv1alpha1.PendingFleetPhase, msg, err, &current, desired)
		}

		remaining := desired
		log.Info("scale down complete", "remaining", remaining)
		return r.setStatus(ctx, fleet, vmv1alpha1.RunningFleetPhase, successMsg, nil, &remaining, desired)

	default:
		// Replica count matches — check power state of each VM
		log.Info("replica count in sync, checking power state", "count", current)

		for i := range vms {
			vm := vms[i]
			on, err := isPoweredOn(ctx, vm)
			if err != nil {
				msg := fmt.Sprintf("could not get power state for VM %q", vm.Name())
				log.Error(err, msg)
				return r.setStatus(ctx, fleet, vmv1alpha1.PendingFleetPhase, msg, err, &current, desired)
			}

			if !on {
				log.Info("VM is powered off, powering on", "vm", vm.Name())
				eg.Go(func() error {
					lim.acquire()
					defer lim.release()
					task, err := vm.PowerOn(egCtx)
					if err != nil {
						return errors.Wrapf(err, "could not power on VM %q", vm.Name())
					}
					if err := task.Wait(egCtx); err != nil {
						return errors.Wrapf(err, "power on task failed for VM %q", vm.Name())
					}
					return nil
				})
			}
		}

		if err := eg.Wait(); err != nil {
			msg := "failed to power on one or more VMs"
			log.Error(err, msg)
			return r.setStatus(ctx, fleet, vmv1alpha1.PendingFleetPhase, msg, err, &current, desired)
		}

		return r.setStatus(ctx, fleet, vmv1alpha1.RunningFleetPhase, successMsg, nil, &current, desired)
	}
}

// handleDeletion cleans up vCenter resources when a VmFleet is deleted.
// It deletes all VMs in the fleet folder then deletes the folder itself.
// Only then does it remove the finalizer so Kubernetes can delete the CR.
func (r *VmFleetReconciler) handleDeletion(ctx context.Context, log logr.Logger, fleet *vmv1alpha1.VmFleet) (ctrl.Result, error) {
	if !containsString(fleet.Finalizers, fleetFinalizer) {
		// Finalizer already removed — nothing to do
		return ctrl.Result{}, nil
	}

	log.Info("VmFleet marked for deletion, cleaning up vCenter resources")
	folderName := fleetFolderName(fleet.Namespace, fleet.Name)
	var notFound *find.NotFoundError

	// Find the fleet folder
	folder, err := getFleet(ctx, r.Finder, folderName)
	if err != nil {
		if errors.As(err, &notFound) {
			// Folder already gone — skip straight to removing finalizer
			log.Info("fleet folder already gone, removing finalizer")
			return r.removeFinalizer(ctx, fleet)
		}
		return ctrl.Result{}, errors.Wrap(err, "failed to find fleet folder during deletion")
	}

	// Delete all VMs in the folder first
	vms, err := getReplicas(ctx, r.Finder, folderName)
	if err != nil && !errors.As(err, &notFound) {
		return ctrl.Result{}, errors.Wrap(err, "failed to list VMs during deletion")
	}

	if len(vms) > 0 {
		log.Info("deleting all VMs in fleet", "count", len(vms))
		eg, egCtx := errgroup.WithContext(ctx)
		lim := newLimiter(defaultConcurrency)

		for i := range vms {
			vm := vms[i]
			log.Info("deleting VM", "name", vm.Name())
			eg.Go(func() error {
				lim.acquire()
				defer lim.release()
				return deleteVM(egCtx, vm)
			})
		}

		if err := eg.Wait(); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to delete all VMs during fleet deletion")
		}
	}

	// Delete the fleet folder
	log.Info("deleting fleet folder", "folder", folder.InventoryPath)
	if err := deleteFleetFolder(ctx, folder); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to delete fleet folder")
	}

	log.Info("vCenter cleanup complete, removing finalizer")
	return r.removeFinalizer(ctx, fleet)
}

// removeFinalizer removes the fleet finalizer from the CR and updates it.
// Once the finalizer is gone Kubernetes will delete the CR object.
func (r *VmFleetReconciler) removeFinalizer(ctx context.Context, fleet *vmv1alpha1.VmFleet) (ctrl.Result, error) {
	fleet.Finalizers = removeString(fleet.Finalizers, fleetFinalizer)
	if err := r.Update(ctx, fleet); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to remove finalizer")
	}
	return ctrl.Result{}, nil
}

// setStatus updates the VmFleet status in Kubernetes and returns the
// appropriate ctrl.Result based on the phase.
func (r *VmFleetReconciler) setStatus(
	ctx context.Context,
	fleet *vmv1alpha1.VmFleet,
	phase vmv1alpha1.FleetPhase,
	msg string,
	err error,
	current *int32,
	desired int32,
) (ctrl.Result, error) {
	// Enrich the message with the error detail if present
	if err != nil {
		msg = fmt.Sprintf("%s: %s", msg, err.Error())
	}

	fleet.Status = vmv1alpha1.VmFleetStatus{
		Phase:           phase,
		CurrentReplicas: current,
		DesiredReplicas: desired,
		LastMessage:     msg,
	}

	if updateErr := r.Status().Update(ctx, fleet); updateErr != nil {
		return ctrl.Result{}, errors.Wrap(updateErr, "failed to update VmFleet status")
	}

	// On error or pending — requeue after a delay
	if phase == vmv1alpha1.ErrorFleetPhase || phase == vmv1alpha1.PendingFleetPhase {
		return ctrl.Result{RequeueAfter: defaultRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler into the controller manager.
func (r *VmFleetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vmv1alpha1.VmFleet{}).
		Named("vmfleet").
		Complete(r)
}

// ── Helper functions ───────────────────────────────────────────────────────

// containsString returns true if slice contains s.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// removeString returns a new slice with s removed.
func removeString(slice []string, s string) []string {
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}