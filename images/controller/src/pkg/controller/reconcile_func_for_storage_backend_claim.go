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
	"d8-controller/pkg/internal"
	"d8-controller/pkg/logger"
	"fmt"
	"reflect"
	"slices"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	StorageBackendClaimManagedLabelSelector = HuaweiStorageConnectionManagedLabelSelector
	StorageBackendClaimFinalizerName        = HuaweiStorageConnectionControllerFinalizerName
)

func IdentifyReconcileFuncForStorageBackendClaim(log logger.Logger, storageBackendClaimList *v1huawei.StorageBackendClaimList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, storageBackendClaimName, configMapName, secretName string) (reconcileType string, err error) {
	if shouldReconcileStorageBackendClaimByCreateFunc(storageBackendClaimList, huaweiStorageConnection, storageBackendClaimName) {
		return internal.CreateReconcile, nil
	}

	should, err := shouldReconcileStorageBackendClaimByUpdateFunc(log, storageBackendClaimList, huaweiStorageConnection, storageBackendClaimName, configMapName, secretName)
	if err != nil {
		return "", err
	}
	if should {
		return internal.UpdateReconcile, nil
	}

	return "", nil
}

func shouldReconcileStorageBackendClaimByCreateFunc(storageBackendClaimList *v1huawei.StorageBackendClaimList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, storageBackendClaimName string) bool {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return false
	}

	for _, sbc := range storageBackendClaimList.Items {
		if sbc.Name == storageBackendClaimName {
			return false
		}
	}

	return true
}

func shouldReconcileStorageBackendClaimByUpdateFunc(log logger.Logger, storageBackendClaimList *v1huawei.StorageBackendClaimList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, storageBackendClaimName, configMapName, secretName string) (bool, error) {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return false, nil
	}

	for _, oldStorageBackendClaim := range storageBackendClaimList.Items {
		if oldStorageBackendClaim.Name == storageBackendClaimName {
			newStorageBackendClaim := getUpdatedStorageBackendClaim(&oldStorageBackendClaim, huaweiStorageConnection, configMapName, secretName)
			equal := areStorageBackendClaimsEqual(oldStorageBackendClaim, *newStorageBackendClaim)

			log.Trace(fmt.Sprintf("[shouldReconcileStorageBackendClaimByUpdateFunc] old StorageBackendClaim: %+v for HuaweiStorageConnection %s", oldStorageBackendClaim, huaweiStorageConnection.Name))
			log.Trace(fmt.Sprintf("[shouldReconcileStorageBackendClaimByUpdateFunc] new StorageBackendClaim: %+v for HuaweiStorageConnection %s", newStorageBackendClaim, huaweiStorageConnection.Name))
			log.Trace(fmt.Sprintf("[shouldReconcileStorageBackendClaimByUpdateFunc] are StorageBackendClaims equal: %t", equal))

			if !equal {
				log.Debug(fmt.Sprintf("[shouldReconcileStorageBackendClaimByUpdateFunc] a storageBackendClaim %s should be updated", storageBackendClaimName))
				return true, nil
			}

			return false, nil
		}
	}

	err := fmt.Errorf("[shouldReconcileStorageBackendClaimByUpdateFunc] a storageBackendClaim %s not found in the list: %+v. It should be created", storageBackendClaimName, storageBackendClaimList.Items)
	return false, err
}

func reconcileStorageBackendClaimCreateFunc(ctx context.Context, cl client.Client, log logger.Logger, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, storageBackendClaimName, configMapName, secretName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileStorageBackendClaimCreateFunc] starts reconciliation of StorageBackendClaim %s for HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))

	newStorageBackendClaim := getNewStorageBackendClaim(huaweiStorageConnection, controllerNamespace, storageBackendClaimName, configMapName, secretName)
	log.Debug(fmt.Sprintf("[reconcileStorageBackendClaimCreateFunc] successfully configurated StorageBackendClaim %s for the HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))
	log.Trace(fmt.Sprintf("[reconcileStorageBackendClaimCreateFunc] storageBackendClaim: %+v", newStorageBackendClaim))

	err = cl.Create(ctx, newStorageBackendClaim)
	if err != nil {
		err = fmt.Errorf("[reconcileStorageBackendClaimCreateFunc] unable to create a StorageBackendClaim %s for HuaweiStorageConnection %s: %w", newStorageBackendClaim.Name, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	return false, "Successfully created", nil
}

func reconcileStorageBackendClaimUpdateFunc(ctx context.Context, cl client.Client, log logger.Logger, storageBackendClaimList *v1huawei.StorageBackendClaimList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, storageBackendClaimName, configMapName, secretName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileStorageBackendClaimUpdateFunc] starts reconciliation of StorageBackendClaim %s for HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))

	var oldStorageBackendClaim *v1huawei.StorageBackendClaim
	for _, sbc := range storageBackendClaimList.Items {
		if sbc.Name == storageBackendClaimName {
			oldStorageBackendClaim = &sbc
			break
		}
	}

	if oldStorageBackendClaim == nil {
		err := fmt.Errorf("[reconcileStorageBackendClaimUpdateFunc] unable to find a StorageBackendClaim %s for the HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[reconcileStorageBackendClaimUpdateFunc] StorageBackendClaim %s was found for the HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))

	updatedStorageBackendClaim := getUpdatedStorageBackendClaim(oldStorageBackendClaim, huaweiStorageConnection, configMapName, secretName)
	log.Debug(fmt.Sprintf("[reconcileStorageBackendClaimUpdateFunc] successfully configurated new StorageBackendClaim %s for the HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))
	log.Trace(fmt.Sprintf("[reconcileStorageBackendClaimUpdateFunc] old StorageBackendClaim: %+v", oldStorageBackendClaim))
	log.Trace(fmt.Sprintf("[reconcileStorageBackendClaimUpdateFunc] updated StorageBackendClaim: %+v", updatedStorageBackendClaim))

	err = cl.Update(ctx, updatedStorageBackendClaim)
	if err != nil {
		err = fmt.Errorf("[reconcileStorageBackendClaimUpdateFunc] unable to update the StorageBackendClaim %s for HuaweiStorageConnection %s: %w", updatedStorageBackendClaim.Name, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	log.Info(fmt.Sprintf("[reconcileStorageBackendClaimUpdateFunc] successfully updated the StorageBackendClaim %s for the HuaweiStorageConnection %s", updatedStorageBackendClaim.Name, huaweiStorageConnection.Name))

	return false, "Successfully updated", nil
}

func reconcileStorageBackendClaimDeleteFunc(ctx context.Context, cl client.Client, log logger.Logger, storageBackendClaimList *v1huawei.StorageBackendClaimList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, storageBackendClaimName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileStorageBackendClaimDeleteFunc] starts reconciliation of StorageBackendClaim %s for HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))

	var storageBackendClaim *v1huawei.StorageBackendClaim
	for _, sbc := range storageBackendClaimList.Items {
		if sbc.Name == storageBackendClaimName {
			storageBackendClaim = &sbc
			break
		}
	}

	if storageBackendClaim == nil {
		log.Info(fmt.Sprintf("[reconcileStorageBackendClaimDeleteFunc] no StorageBackendClaim with name %s found for the HuaweiStorageConnection %s. No action required.", storageBackendClaimName, huaweiStorageConnection.Name))
	}

	if storageBackendClaim != nil {
		log.Info(fmt.Sprintf("[reconcileStorageBackendClaimDeleteFunc] successfully found a StorageBackendClaim %s for the HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))

		err := deleteResource(ctx, cl, storageBackendClaim, StorageBackendClaimFinalizerName)
		if err != nil {
			err = fmt.Errorf("[reconcileStorageBackendClaimDeleteFunc] unable to delete the StorageBackendClaim %s for HuaweiStorageConnection %s: %w", storageBackendClaim.Name, huaweiStorageConnection.Name, err)
			return true, err.Error(), err
		}

		log.Info(fmt.Sprintf("[reconcileStorageBackendClaimDeleteFunc] successfully deleted the StorageBackendClaim %s for the HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))
	}

	log.Info(fmt.Sprintf("[reconcileStorageBackendClaimDeleteFunc] ends reconciliation of StorageBackendClaim %s for HuaweiStorageConnection %s", storageBackendClaimName, huaweiStorageConnection.Name))

	return false, "", nil
}

func getNewStorageBackendClaim(huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, storageBackendClaimName, configMapName, secretName string) *v1huawei.StorageBackendClaim {
	storageBackendClaim := &v1huawei.StorageBackendClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       storageBackendClaimName,
			Namespace:  controllerNamespace,
			Finalizers: []string{StorageBackendClaimFinalizerName},
		},
		Spec: getStorageBackendClaimSpec(huaweiStorageConnection, controllerNamespace, configMapName, secretName),
	}

	storageBackendClaim.Labels = ensureLabel(storageBackendClaim.Labels, StorageBackendClaimManagedLabelSelector)

	return storageBackendClaim
}

func getUpdatedStorageBackendClaim(oldStorageBackendClaim *v1huawei.StorageBackendClaim, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, configMapName, secretName string) *v1huawei.StorageBackendClaim {
	newStorageBackendClaim := oldStorageBackendClaim.DeepCopy()
	newStorageBackendClaim.Spec = getStorageBackendClaimSpec(huaweiStorageConnection, oldStorageBackendClaim.Namespace, configMapName, secretName)
	newStorageBackendClaim.Labels = ensureLabel(newStorageBackendClaim.Labels, StorageBackendClaimManagedLabelSelector)
	newStorageBackendClaim.Finalizers = ensureFinalizer(newStorageBackendClaim.Finalizers, StorageBackendClaimFinalizerName)

	return newStorageBackendClaim
}

func areStorageBackendClaimsEqual(old, new v1huawei.StorageBackendClaim) bool {
	if reflect.DeepEqual(old.Spec, new.Spec) && labels.Set(new.Labels).AsSelector().Matches(StorageBackendClaimManagedLabelSelector) && slices.Contains(new.Finalizers, StorageBackendClaimFinalizerName) {
		return true
	}

	return false
}

func getStorageBackendClaimSpec(huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, configMapName, secretName string) v1huawei.StorageBackendClaimSpec {
	return v1huawei.StorageBackendClaimSpec{
		Provider:         internal.HuaweiCSIProvisioner,
		ConfigMapMeta:    fmt.Sprintf("%s/%s", controllerNamespace, configMapName),
		SecretMeta:       fmt.Sprintf("%s/%s", controllerNamespace, secretName),
		MaxClientThreads: strconv.Itoa(huaweiStorageConnection.Spec.MaxClientThreads),
		UseCert:          false,
	}
}
