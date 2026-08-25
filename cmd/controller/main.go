// Command controller runs the Workload controller.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/controller"
)

func main() {
	var coordinatorImage string
	flag.StringVar(&coordinatorImage, "coordinator-image", os.Getenv("STARK8S_COORDINATOR_IMAGE"), "image used for per-workload coordinators")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
	if err != nil {
		ctrl.Log.Error(err, "manager")
		os.Exit(1)
	}
	r := &controller.Reconciler{Client: mgr.GetClient(), CoordinatorImage: coordinatorImage, ControllerNamespace: controller.Namespace()}
	if err := r.SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "setup")
		os.Exit(1)
	}
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "run")
		os.Exit(1)
	}
}
