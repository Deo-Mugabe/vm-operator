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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/pkg/errors"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/vim25/soap"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	vmv1alpha1 "vmfleet/operator/api/v1alpha1"
	"vmfleet/operator/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(vmv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	// ── Flags

	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var insecure bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"The address the metrics endpoint binds to. Use 0 to disable.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election. Ensures only one active controller manager.")
	flag.BoolVar(&insecure, "insecure", false,
		"Skip vCenter TLS certificate validation. Use for self-signed certs.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// ── vCenter connection ─────────────────────────────────────────────────
	// We read vCenter credentials from environment variables.
	// This keeps secrets out of the command line and config files.

	vcHost := os.Getenv("VC_HOST")
	vcUser := os.Getenv("VC_USER")
	vcPass := os.Getenv("VC_PASS")

	if vcHost == "" || vcUser == "" || vcPass == "" {
		setupLog.Error(
			errors.New("missing vCenter credentials"),
			"VC_HOST, VC_USER and VC_PASS environment variables must all be set",
		)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vc, err := newVCenterClient(ctx, vcHost, vcUser, vcPass, insecure)
	if err != nil {
		setupLog.Error(err, "failed to connect to vCenter")
		os.Exit(1)
	}
	setupLog.Info("connected to vCenter", "host", vcHost)

	// ── vCenter inventory setup ────────────────────────────────────────────
	// The Finder navigates the vCenter inventory tree by path.
	// We set a default datacenter so paths like "/DC0/vm/..." work.

	finder := find.NewFinder(vc.Client)

	dc, err := finder.DefaultDatacenter(ctx)
	if err != nil {
		setupLog.Error(err, "could not find default datacenter in vCenter")
		os.Exit(1)
	}
	finder.SetDatacenter(dc)
	setupLog.Info("using datacenter", "datacenter", dc.Name())

	// The ResourcePool is where new VMs are placed when cloned.
	// We use the explicit path we know vcsim exposes.
	rp, err := finder.ResourcePool(ctx, "/DC0/host/DC0_H0/Resources")
	if err != nil {
		setupLog.Error(err, "could not find resource pool")
		os.Exit(1)
	}
	setupLog.Info("using resource pool", "pool", rp.InventoryPath)

	// ── Manager ────────────────────────────────────────────────────────────
	// The manager owns the reconcile loop, metrics server, and health checks.
	// It reads kubeconfig from the KUBECONFIG env var or ~/.kube/config.

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "vmfleet.vm.operator.io",
	})
	if err != nil {
		setupLog.Error(err, "failed to create manager")
		os.Exit(1)
	}

	// ── Wire the reconciler ────────────────────────────────────────────────
	// We inject all dependencies into the reconciler here.
	// This is called dependency injection — the reconciler never creates
	// its own vCenter client, it receives one from main.

	if err = (&controller.VmFleetReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		VC:           vc,
		Finder:       finder,
		ResourcePool: rp,
		Log:          ctrl.Log.WithName("controllers").WithName("VmFleet"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "failed to create VmFleet controller")
		os.Exit(1)
	}

	// ── Health checks ──────────────────────────────────────────────────────
	// Kubernetes uses these endpoints to know if the operator is alive.
	// /healthz — is the process running?
	// /readyz  — is it ready to handle requests?

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "failed to set up ready check")
		os.Exit(1)
	}

	// ── Start ──────────────────────────────────────────────────────────────
	// mgr.Start blocks until Ctrl+C or SIGTERM.
	// ctrl.SetupSignalHandler returns a context that gets cancelled on signal.

	setupLog.Info("starting VmFleet operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "failed to run manager")
		os.Exit(1)
	}
}
// newVCenterClient creates and returns an authenticated govmomi client.
// It parses the URL, attaches credentials, and connects to vCenter.
func newVCenterClient(ctx context.Context, host, user, pass string, insecure bool) (*govmomi.Client, error) {
	// soap.ParseURL understands vCenter URL formats
	u, err := soap.ParseURL(host)
	if err != nil || u == nil {
		return nil, fmt.Errorf("could not parse vCenter URL %q: %w", host, err)
	}

	// attach credentials to the URL
	u.User = url.UserPassword(user, pass)

	// disable TLS verification for self-signed certs (vcsim, lab environments)
	if insecure {
		http := u.Scheme == "https"
		_ = http
	}

	client, err := govmomi.NewClient(ctx, u, insecure)
	if err != nil {
		return nil, fmt.Errorf("could not connect to vCenter at %q: %w", host, err)
	}

	return client, nil
}

// tlsConfig returns a TLS config with optional insecure mode.
// Kept here for reference — govmomi.NewClient handles this internally
// when insecure=true is passed.
func tlsConfig(insecure bool) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: insecure, //nolint:gosec
	}
}
