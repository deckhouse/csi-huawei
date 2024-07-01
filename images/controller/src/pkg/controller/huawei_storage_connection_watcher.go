/*
Copyright 2024 Flant JSC

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
	v1alpha1 "d8-controller/api/storage.deckhouse.io/v1alpha1"
	v1huawei "d8-controller/api/xuanwu/v1"
	"d8-controller/pkg/config"
	"d8-controller/pkg/internal"
	"d8-controller/pkg/logger"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	// This value used as a name for the controller AND the value for managed-by label.
	HuaweiStorageConnectionCtrlName                = "d8-huawei-storage-connection-controller"
	HuaweiStorageConnectionControllerFinalizerName = "storage.deckhouse.io/huawei-storage-connection-controller"
	HuaweiCSIConfigMapDataKey                      = "csi.json"
)

var (
	HuaweiStorageConnectionManagedLabelSelector = labels.Set(map[string]string{
		internal.StorageManagedLabelKey: HuaweiStorageConnectionCtrlName,
	})
)

func RunHuaweiStorageConnectionWatcherController(
	mgr manager.Manager,
	cfg config.Options,
	log logger.Logger,
) (controller.Controller, error) {
	cl := mgr.GetClient()

	c, err := controller.New(HuaweiStorageConnectionCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Info(fmt.Sprintf("[HuaweiStorageConnectionReconciler] starts Reconcile for the HuaweiStorageConnection %q", request.Name))
			HuaweiStorageConnection := &v1alpha1.HuaweiStorageConnection{}
			err := cl.Get(ctx, request.NamespacedName, HuaweiStorageConnection)
			if err != nil && !k8serr.IsNotFound(err) {
				log.Error(err, fmt.Sprintf("[HuaweiStorageConnectionReconciler] unable to get HuaweiStorageConnection, name: %s", request.Name))
				return reconcile.Result{}, err
			}

			if HuaweiStorageConnection.Name == "" {
				log.Info(fmt.Sprintf("[HuaweiStorageConnectionReconciler] seems like the HuaweiStorageConnection for the request %s was deleted. Reconcile retrying will stop.", request.Name))
				return reconcile.Result{}, nil
			}

			shouldRequeue, msg, err := RunHuaweiStorageConnectionEventReconcile(ctx, cl, log, HuaweiStorageConnection, cfg.ControllerNamespace)
			log.Info(fmt.Sprintf("[HuaweiStorageConnectionReconciler] CeohClusterConnection %s has been reconciled with message: %s", HuaweiStorageConnection.Name, msg))
			phase := v1alpha1.PhaseCreated
			if err != nil {
				log.Error(err, fmt.Sprintf("[HuaweiStorageConnectionReconciler] an error occured while reconciles the HuaweiStorageConnection, name: %s", HuaweiStorageConnection.Name))
				phase = v1alpha1.PhaseFailed
			}

			if msg != "" {
				log.Debug(fmt.Sprintf("[HuaweiStorageConnectionReconciler] update the HuaweiStorageConnection %s with the phase %s and message: %s", HuaweiStorageConnection.Name, phase, msg))
				upErr := updateHuaweiStorageConnectionPhase(ctx, cl, HuaweiStorageConnection, phase, msg)
				if upErr != nil {
					log.Error(upErr, fmt.Sprintf("[HuaweiStorageConnectionReconciler] unable to update the HuaweiStorageConnection %s: %s", HuaweiStorageConnection.Name, upErr.Error()))
					shouldRequeue = true
				}
			}

			if shouldRequeue {
				log.Warning(fmt.Sprintf("[HuaweiStorageConnectionReconciler] Reconciler will requeue the request, name: %s", request.Name))
				return reconcile.Result{
					RequeueAfter: cfg.RequeueStorageClassInterval * time.Second,
				}, nil
			}

			log.Info(fmt.Sprintf("[HuaweiStorageConnectionReconciler] ends Reconcile for the HuaweiStorageConnection %q", request.Name))
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error(err, "[RunHuaweiStorageConnectionWatcherController] unable to create controller")
		return nil, err
	}

	err = c.Watch(
		source.Kind(mgr.GetCache(), &v1alpha1.HuaweiStorageConnection{},
			handler.TypedFuncs[*v1alpha1.HuaweiStorageConnection]{
				CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[*v1alpha1.HuaweiStorageConnection], q workqueue.RateLimitingInterface) {
					log.Info(fmt.Sprintf("[CreateFunc] get event for HuaweiStorageConnection %q. Add to the queue", e.Object.GetName()))
					request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
					q.Add(request)
				},
				UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[*v1alpha1.HuaweiStorageConnection], q workqueue.RateLimitingInterface) {
					log.Info(fmt.Sprintf("[UpdateFunc] get event for HuaweiStorageConnection %q. Check if it should be reconciled", e.ObjectNew.GetName()))

					oldHuaweiStorageConnection := e.ObjectOld
					newHuaweiStorageConnection := e.ObjectNew

					if reflect.DeepEqual(oldHuaweiStorageConnection.Spec, newHuaweiStorageConnection.Spec) && newHuaweiStorageConnection.DeletionTimestamp == nil {
						log.Info(fmt.Sprintf("[UpdateFunc] an update event for the HuaweiStorageConnection %s has no Spec field updates. It will not be reconciled", newHuaweiStorageConnection.Name))
						return
					}

					log.Info(fmt.Sprintf("[UpdateFunc] the HuaweiStorageConnection %q will be reconciled. Add to the queue", newHuaweiStorageConnection.Name))
					request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: newHuaweiStorageConnection.Namespace, Name: newHuaweiStorageConnection.Name}}
					q.Add(request)
				},
			},
		),
	)

	if err != nil {
		log.Error(err, "[RunHuaweiStorageConnectionWatcherController] unable to watch the events")
		return nil, err
	}

	return c, nil
}

func RunHuaweiStorageConnectionEventReconcile(ctx context.Context, cl client.Client, log logger.Logger, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] starts reconciliataion of HuaweiStorageConnection, name: %s", huaweiStorageConnection.Name))

	configMapName := huaweiStorageConnection.Name
	configMapList := &corev1.ConfigMapList{}
	err = cl.List(ctx, configMapList, client.InNamespace(controllerNamespace))
	if err != nil {
		err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] unable to list ConfigMaps in namespace %s: %w", controllerNamespace, err)
		return true, err.Error(), err
	}

	secretName := huaweiStorageConnection.Name
	secretList := &corev1.SecretList{}
	err = cl.List(ctx, secretList, client.InNamespace(controllerNamespace))
	if err != nil {
		log.Error(err, "[RunHuaweiStorageConnectionEventReconcile] unable to list Secrets")
		return true, err.Error(), err
	}

	storageBackendClaimName := huaweiStorageConnection.Name
	storageBackendClaimList := &v1huawei.StorageBackendClaimList{}
	err = cl.List(ctx, storageBackendClaimList, client.InNamespace(controllerNamespace))
	if err != nil {
		log.Error(err, "[RunHuaweiStorageConnectionEventReconcile] unable to list StorageBackendClaims")
		return true, err.Error(), err
	}

	if huaweiStorageConnection.DeletionTimestamp != nil {
		log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] HuaweiStorageConnection %s is being deleted", huaweiStorageConnection.Name))

		shouldRequeue, msg, err = reconcileConfigMapDeleteFunc(ctx, cl, log, configMapList, huaweiStorageConnection, configMapName)
		if err != nil || shouldRequeue {
			return shouldRequeue, msg, err
		}

		shouldRequeue, msg, err = reconcileSecretDeleteFunc(ctx, cl, log, secretList, huaweiStorageConnection, secretName)
		if err != nil || shouldRequeue {
			return shouldRequeue, msg, err
		}

		shouldRequeue, msg, err = reconcileStorageBackendClaimDeleteFunc(ctx, cl, log, storageBackendClaimList, huaweiStorageConnection, storageBackendClaimName)
		if err != nil || shouldRequeue {
			return shouldRequeue, msg, err
		}

		_, err = removeFinalizerIfExists(ctx, cl, huaweiStorageConnection, HuaweiStorageConnectionControllerFinalizerName)
		if err != nil {
			err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] unable to remove finalizer from the HuaweiStorageConnection %s: %w", huaweiStorageConnection.Name, err)
			return true, err.Error(), err
		}

		log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] successfully removed finalizer from the HuaweiStorageConnection %s", huaweiStorageConnection.Name))
		log.Info(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] successfully reconciled the deletion of HuaweiStorageConnection %s", huaweiStorageConnection.Name))

		return false, "", nil
	}

	valid, msg := validateHuaweiStorageConnectionSpec(huaweiStorageConnection)
	if !valid {
		err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] HuaweiStorageConnection %s has invalid spec: %s", huaweiStorageConnection.Name, msg)
		return false, msg, err
	}
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] HuaweiStorageConnection %s has valid spec", huaweiStorageConnection.Name))

	added, err := addFinalizerIfNotExists(ctx, cl, huaweiStorageConnection, HuaweiStorageConnectionControllerFinalizerName)
	if err != nil {
		err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] unable to add a finalizer %s to the HuaweiStorageConnection %s: %w", HuaweiStorageConnectionControllerFinalizerName, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] finalizer %s was added to the HuaweiStorageConnection %s: %t", HuaweiStorageConnectionControllerFinalizerName, huaweiStorageConnection.Name, added))

	// Reconcile ConfigMap
	reconcileTypeForConfigMap, err := IdentifyReconcileFuncForConfigMap(log, configMapList, huaweiStorageConnection, configMapName)
	if err != nil {
		err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] error occurred while identifying the reconcile function for HuaweiStorageConnection %s on ConfigMap %s: %w", huaweiStorageConnection.Name, configMapName, err)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] successfully identified the reconcile type for HuaweiStorageConnection %s to be performed on ConfigMap %s: %s", huaweiStorageConnection.Name, configMapName, reconcileTypeForConfigMap))
	switch reconcileTypeForConfigMap {
	case internal.CreateReconcile:
		shouldRequeue, msg, err = reconcileConfigMapCreateFunc(ctx, cl, log, huaweiStorageConnection, controllerNamespace, configMapName)
	case internal.UpdateReconcile:
		shouldRequeue, msg, err = reconcileConfigMapUpdateFunc(ctx, cl, log, configMapList, huaweiStorageConnection, configMapName)
	default:
		log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] no reconcile action required for HuaweiStorageConnection %s on ConfigMap %s. No changes will be made.", huaweiStorageConnection.Name, configMapName))
		msg = "Successfully reconciled"
	}
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] completed reconcile operation for HuaweiStorageConnection %s on ConfigMap %s.", huaweiStorageConnection.Name, configMapName))

	if err != nil || shouldRequeue {
		return shouldRequeue, msg, err
	}

	// Reconcile Secret
	reconcileTypeForSecret, err := IdentifyReconcileFuncForSecret(log, secretList, huaweiStorageConnection, secretName)
	if err != nil {
		err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] error occurred while identifying the reconcile function for HuaweiStorageConnection %s on Secret %s: %w", huaweiStorageConnection.Name, secretName, err)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] successfully identified the reconcile type for HuaweiStorageConnection %s to be performed on Secret %s: %s", huaweiStorageConnection.Name, secretName, reconcileTypeForSecret))
	switch reconcileTypeForSecret {
	case internal.CreateReconcile:
		shouldRequeue, msg, err = reconcileSecretCreateFunc(ctx, cl, log, huaweiStorageConnection, controllerNamespace, secretName)
	case internal.UpdateReconcile:
		shouldRequeue, msg, err = reconcileSecretUpdateFunc(ctx, cl, log, secretList, huaweiStorageConnection, controllerNamespace, secretName)
	default:
		log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] no reconcile action required for HuaweiStorageConnection %s on Secret %s. No changes will be made.", huaweiStorageConnection.Name, secretName))
		msg = "Successfully reconciled"
	}
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] completed reconcile operation for HuaweiStorageConnection %s on Secret %s.", huaweiStorageConnection.Name, secretName))

	if err != nil || shouldRequeue {
		return shouldRequeue, msg, err
	}

	// Reconcile StorageBackendClaim
	reconcileTypeForStorageBackendClaim, err := IdentifyReconcileFuncForStorageBackendClaim(log, storageBackendClaimList, huaweiStorageConnection, storageBackendClaimName, configMapName, secretName)
	if err != nil {
		err = fmt.Errorf("[RunHuaweiStorageConnectionEventReconcile] error occurred while identifying the reconcile function for HuaweiStorageConnection %s on StorageBackendClaim %s: %w", huaweiStorageConnection.Name, storageBackendClaimName, err)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] successfully identified the reconcile type for HuaweiStorageConnection %s to be performed on StorageBackendClaim %s: %s", huaweiStorageConnection.Name, storageBackendClaimName, reconcileTypeForStorageBackendClaim))
	switch reconcileTypeForStorageBackendClaim {
	case internal.CreateReconcile:
		shouldRequeue, msg, err = reconcileStorageBackendClaimCreateFunc(ctx, cl, log, huaweiStorageConnection, controllerNamespace, storageBackendClaimName, configMapName, secretName)
	case internal.UpdateReconcile:
		shouldRequeue, msg, err = reconcileStorageBackendClaimUpdateFunc(ctx, cl, log, storageBackendClaimList, huaweiStorageConnection, storageBackendClaimName, configMapName, secretName)
	default:
		log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] no reconcile action required for HuaweiStorageConnection %s on StorageBackendClaim %s. No changes will be made.", huaweiStorageConnection.Name, storageBackendClaimName))
		msg = "Successfully reconciled"
	}
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] completed reconcile operation for HuaweiStorageConnection %s on StorageBackendClaim %s.", huaweiStorageConnection.Name, storageBackendClaimName))

	if err != nil || shouldRequeue {
		return shouldRequeue, msg, err
	}

	// Finish
	log.Debug(fmt.Sprintf("[RunHuaweiStorageConnectionEventReconcile] finish all reconciliations for HuaweiStorageConnection %q.", huaweiStorageConnection.Name))
	return false, msg, nil
}
