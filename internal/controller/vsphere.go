package controller

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"github.com/pkg/errors"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"

	vmv1alpha1 "vmfleet/operator/api/v1alpha1"
)

const (
	// fleetPath is the base folder in vCenter where all fleets live
	fleetPath = "/DC0/vm/vm-fleet-operator"

	// alreadyDeletedErr is returned by vCenter when destroying something twice
	alreadyDeletedErr = "has already been deleted or has not been completely created"

	// defaultConcurrency is how many vCenter operations run in parallel
	defaultConcurrency = 3

	// nameLength is how many random characters to append to VM names
	nameLength = 8
)

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

// generateName returns a random lowercase string of length nameLength.
// Used to give each cloned VM a unique name suffix.
func generateName() string {
	b := make([]rune, nameLength)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// fleetFolderName builds the vCenter folder name for a fleet.
// Format: "<namespace>-<name>" e.g. "default-my-fleet"
func fleetFolderName(namespace, name string) string {
	return fmt.Sprintf("%s-%s", namespace, name)
}

// getFleet retrieves the vCenter folder for a fleet.
// Returns a NotFoundError if the folder does not exist yet.
func getFleet(ctx context.Context, finder *find.Finder, folderName string) (*object.Folder, error) {
	path := fleetPath + "/" + folderName
	f, err := finder.Folder(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find fleet folder %q", path)
	}
	return f, nil
}

// createFleet creates the vCenter folder for a fleet.
// The parent folder (fleetPath) must already exist.
func createFleet(ctx context.Context, finder *find.Finder, folderName string) (*object.Folder, error) {
	parent, err := finder.Folder(ctx, fleetPath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find base folder %q", fleetPath)
	}

	folder, err := parent.CreateFolder(ctx, folderName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create fleet folder %q", folderName)
	}

	return folder, nil
}

// getReplicas returns all VMs inside a fleet folder.
// Returns a NotFoundError if the folder has no VMs yet.
func getReplicas(ctx context.Context, finder *find.Finder, folderName string) ([]*object.VirtualMachine, error) {
	path := fleetPath + "/" + folderName + "/*"
	vms, err := finder.VirtualMachineList(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not list VMs in fleet %q", folderName)
	}
	return vms, nil
}

// cloneVM creates a new VM by cloning from a template.
// The new VM is placed in the destination folder with the given name.
// CPU, Memory and DiskGB come from the VmFleet spec.
func cloneVM(
	ctx context.Context,
	finder *find.Finder,
	templateName string,
	vmName string,
	destinationPath string,
	pool *object.ResourcePool,
	spec vmv1alpha1.VmFleetSpec,
) error {
	// find the template VM in vCenter
	tmpl, err := finder.VirtualMachine(ctx, templateName)
	if err != nil {
		return errors.Wrapf(err, "could not find template %q", templateName)
	}

	// find the destination folder
	folder, err := finder.Folder(ctx, destinationPath)
	if err != nil {
		return errors.Wrapf(err, "could not find destination folder %q", destinationPath)
	}

	// build the resource pool reference
	rpRef := pool.Reference()

	// build the clone spec — this is what vCenter uses to configure the new VM
	cloneSpec := types.VirtualMachineCloneSpec{
		Location: types.VirtualMachineRelocateSpec{
			Pool: &rpRef,
		},
		Config: &types.VirtualMachineConfigSpec{
			NumCPUs:  spec.CPU,
			MemoryMB: int64(1024 * spec.Memory),
			// Note: DiskGB would normally be set via disk reconfigure
			// after clone — vCenter does not support it directly in CloneSpec
			// We store it in the spec for future implementation
			Annotation: fmt.Sprintf("diskGB=%d", spec.DiskGB),
		},
		PowerOn: true,
	}

	// initiate the clone task
	task, err := tmpl.Clone(ctx, folder, vmName, cloneSpec)
	if err != nil {
		return errors.Wrapf(err, "could not start clone task for %q", vmName)
	}

	// wait for the task to complete
	if err := task.Wait(ctx); err != nil {
		return errors.Wrapf(err, "clone task failed for %q", vmName)
	}

	return nil
}

// isPoweredOn returns true if the VM is in the poweredOn state.
func isPoweredOn(ctx context.Context, vm *object.VirtualMachine) (bool, error) {
	state, err := vm.PowerState(ctx)
	if err != nil {
		return false, errors.Wrapf(err, "could not get power state for VM %q", vm.Name())
	}
	return state == types.VirtualMachinePowerStatePoweredOn, nil
}

// deleteVM powers off and destroys a VM.
// Power off errors are ignored — the VM may already be off.
func deleteVM(ctx context.Context, vm *object.VirtualMachine) error {
	// attempt power off — ignore errors (VM may already be off)
	if task, err := vm.PowerOff(ctx); err == nil {
		_ = task.Wait(ctx)
	}

	// destroy the VM
	task, err := vm.Destroy(ctx)
	if err != nil {
		if strings.Contains(err.Error(), alreadyDeletedErr) {
			return nil
		}
		return errors.Wrapf(err, "could not destroy VM %q", vm.InventoryPath)
	}

	if err := task.Wait(ctx); err != nil {
		return errors.Wrapf(err, "destroy task failed for VM %q", vm.InventoryPath)
	}

	return nil
}

// deleteFleetFolder destroys the fleet folder in vCenter.
// Called after all VMs in the folder have been deleted.
func deleteFleetFolder(ctx context.Context, folder *object.Folder) error {
	task, err := folder.Destroy(ctx)
	if err != nil {
		if strings.Contains(err.Error(), alreadyDeletedErr) {
			return nil
		}
		return errors.Wrapf(err, "could not destroy fleet folder %q", folder.InventoryPath)
	}

	if err := task.Wait(ctx); err != nil {
		return errors.Wrapf(err, "fleet folder destroy task failed for %q", folder.InventoryPath)
	}

	return nil
}

// limiter is a simple concurrency limiter using a buffered channel.
// It ensures we never run more than N operations against vCenter at once.
type limiter struct {
	ch chan struct{}
}

func newLimiter(n int) *limiter {
	return &limiter{ch: make(chan struct{}, n)}
}

// acquire blocks until a slot is available.
func (l *limiter) acquire() {
	l.ch <- struct{}{}
}

// release frees a slot.
func (l *limiter) release() {
	<-l.ch
}