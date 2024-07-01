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
	"d8-controller/pkg/internal"
	"d8-controller/pkg/logger"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	ConfigMapManagedLabelSelector = HuaweiStorageConnectionManagedLabelSelector
	ConfigMapFinalizerName        = HuaweiStorageConnectionControllerFinalizerName
)

func IdentifyReconcileFuncForConfigMap(log logger.Logger, configMapList *corev1.ConfigMapList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, configMapName string) (reconcileType string, err error) {
	if shouldReconcileConfigMapByCreateFunc(configMapList, huaweiStorageConnection, configMapName) {
		return internal.CreateReconcile, nil
	}

	should, err := shouldReconcileConfigMapByUpdateFunc(log, configMapList, huaweiStorageConnection, configMapName)
	if err != nil {
		return "", err
	}
	if should {
		return internal.UpdateReconcile, nil
	}

	return "", nil
}

func shouldReconcileConfigMapByCreateFunc(configMapList *corev1.ConfigMapList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, configMapName string) bool {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return false
	}

	for _, cm := range configMapList.Items {
		if cm.Name == configMapName {
			if cm.Data[HuaweiCSIConfigMapDataKey] == "" {
				return true
			}

			return false
		}
	}

	return true
}

func shouldReconcileConfigMapByUpdateFunc(log logger.Logger, configMapList *corev1.ConfigMapList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, configMapName string) (bool, error) {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return false, nil
	}

	for _, oldConfigMap := range configMapList.Items {
		if oldConfigMap.Name == configMapName {
			newConfigMap, err := getUpdatedConfigMap(&oldConfigMap, huaweiStorageConnection)
			if err != nil {
				return false, fmt.Errorf("[shouldReconcileConfigMapByUpdateFunc] unable to configure a new ConfigMap %s for the HuaweiStorageConnection %s: %w", configMapName, huaweiStorageConnection.Name, err)
			}

			equal := areConfigMapsEqual(oldConfigMap, *newConfigMap)
			log.Trace(fmt.Sprintf("[shouldReconcileConfigMapByUpdateFunc] old ConfigMap: %+v for HuaweiStorageConnection %s", oldConfigMap, huaweiStorageConnection.Name))
			log.Trace(fmt.Sprintf("[shouldReconcileConfigMapByUpdateFunc] new ConfigMap: %+v for HuaweiStorageConnection %s", newConfigMap, huaweiStorageConnection.Name))
			log.Trace(fmt.Sprintf("[shouldReconcileConfigMapByUpdateFunc] are ConfigMaps equal: %t", equal))

			if !equal {
				log.Debug(fmt.Sprintf("[shouldReconcileConfigMapByUpdateFunc] a configMap %s should be updated", configMapName))
				return true, nil
			}

			return false, nil
		}
	}

	err := fmt.Errorf("[shouldReconcileConfigMapByUpdateFunc] a configMap %s not found in the list: %+v. It should be created", configMapName, configMapList.Items)
	return false, err
}

// func getCSIConfigFromConfigMap(configMap corev1.ConfigMap) ([]v1alpha1.HuaweiCSIConfigMapData, error) {
// 	jsonData, ok := configMap.Data[HuaweiCSIConfigMapDataKey]
// 	if !ok {
// 		return nil, fmt.Errorf("[getCSIConfigFromConfigMap] %s key not found in the ConfigMap %s", HuaweiCSIConfigMapDataKey, configMap.Name)
// 	}

// 	var csiConfig []v1alpha1.HuaweiCSIConfigMapData
// 	err := json.Unmarshal([]byte(jsonData), &csiConfig)
// 	if err != nil {
// 		return nil, fmt.Errorf("[getCSIConfigFromConfigMap] unable to unmarshal data from the ConfigMap %s: %w", configMap.Name, err)
// 	}

// 	return csiConfig, nil
// }

func configureCSIConfig(huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace string) v1alpha1.HuaweiCSIConfigMapData {

	parameters := v1alpha1.BackendParameters{
		Protocol: v1alpha1.HuaweiStorageProtocolMapping[huaweiStorageConnection.Spec.Protocol],
	}

	if len(huaweiStorageConnection.Spec.Portals) != 0 {
		parameters.Portals = huaweiStorageConnection.Spec.Portals
	}

	csiConfig := v1alpha1.HuaweiCSIConfigMapData{
		Backends: v1alpha1.Backend{
			Name:             huaweiStorageConnection.Name,
			Namespace:        controllerNamespace,
			Storage:          v1alpha1.HuaweiStorageTypeMapping[huaweiStorageConnection.Spec.StorageType],
			URLs:             huaweiStorageConnection.Spec.URLs,
			Pools:            huaweiStorageConnection.Spec.Pools,
			MaxClientThreads: strconv.Itoa(huaweiStorageConnection.Spec.MaxClientThreads),
			Provisioner:      internal.HuaweiCSIProvisioner,
			Parameters:       parameters,
		},
	}

	return csiConfig
}

func reconcileConfigMapCreateFunc(ctx context.Context, cl client.Client, log logger.Logger, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, configMapName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileConfigMapCreateFunc] starts reconciliation of ConfigMap %s for HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))

	newCSIConfig := configureCSIConfig(huaweiStorageConnection, controllerNamespace)
	newConfigMap, err := getNewConfigMap(newCSIConfig, controllerNamespace, configMapName)
	if err != nil {
		err = fmt.Errorf("[reconcileConfigMapCreateFunc] unable to configure a ConfigMap resource %s for HuaweiStorageConnection %s: %w", configMapName, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[reconcileConfigMapCreateFunc] successfully configurated ConfigMap %s for the HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))
	log.Trace(fmt.Sprintf("[reconcileConfigMapCreateFunc] configMap: %+v", newConfigMap))

	err = cl.Create(ctx, newConfigMap)
	if err != nil {
		err = fmt.Errorf("[reconcileConfigMapCreateFunc] unable to create a ConfigMap %s for HuaweiStorageConnection %s: %w", newConfigMap.Name, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	return false, "Successfully created", nil
}

func reconcileConfigMapUpdateFunc(ctx context.Context, cl client.Client, log logger.Logger, configMapList *corev1.ConfigMapList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, configMapName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileConfigMapUpdateFunc] starts reconciliation of ConfigMap %s for HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))

	var oldConfigMap *corev1.ConfigMap
	for _, cm := range configMapList.Items {
		if cm.Name == configMapName {
			oldConfigMap = &cm
			break
		}
	}

	if oldConfigMap == nil {
		err := fmt.Errorf("[reconcileConfigMapUpdateFunc] unable to find a ConfigMap %s for the HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[reconcileConfigMapUpdateFunc] ConfigMap %s was found for the HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))

	updatedConfigMap, err := getUpdatedConfigMap(oldConfigMap, huaweiStorageConnection)
	if err != nil {
		err = fmt.Errorf("[reconcileConfigMapUpdateFunc] unable to configure a new ConfigMap %s for the HuaweiStorageConnection %s: %w", configMapName, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}
	log.Debug(fmt.Sprintf("[reconcileConfigMapUpdateFunc] successfully configurated new ConfigMap %s for the HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))
	log.Trace(fmt.Sprintf("[reconcileConfigMapUpdateFunc] old ConfigMap: %+v", oldConfigMap))
	log.Trace(fmt.Sprintf("[reconcileConfigMapUpdateFunc] updated ConfigMap: %+v", updatedConfigMap))

	err = cl.Update(ctx, updatedConfigMap)
	if err != nil {
		err = fmt.Errorf("[reconcileConfigMapUpdateFunc] unable to update the ConfigMap %s for HuaweiStorageConnection %s: %w", updatedConfigMap.Name, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	log.Info(fmt.Sprintf("[reconcileConfigMapUpdateFunc] successfully updated the ConfigMap %s for the HuaweiStorageConnection %s", updatedConfigMap.Name, huaweiStorageConnection.Name))

	return false, "Successfully updated", nil
}

func reconcileConfigMapDeleteFunc(ctx context.Context, cl client.Client, log logger.Logger, configMapList *corev1.ConfigMapList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, configMapName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileConfigMapDeleteFunc] starts reconciliation of ConfigMap %s for HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))

	var configMap *corev1.ConfigMap
	for _, cm := range configMapList.Items {
		if cm.Name == configMapName {
			configMap = &cm
			break
		}
	}

	if configMap == nil {
		log.Info(fmt.Sprintf("[reconcileConfigMapDeleteFunc] no ConfigMap with name %s found for the HuaweiStorageConnection %s. No action required.", configMapName, huaweiStorageConnection.Name))
	}

	if configMap != nil {
		log.Info(fmt.Sprintf("[reconcileConfigMapDeleteFunc] successfully found a ConfigMap %s for the HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))

		err := deleteResource(ctx, cl, configMap, ConfigMapFinalizerName)
		if err != nil {
			err = fmt.Errorf("[reconcileConfigMapDeleteFunc] unable to delete the ConfigMap %s for HuaweiStorageConnection %s: %w", configMap.Name, huaweiStorageConnection.Name, err)
			return true, err.Error(), err
		}

		log.Info(fmt.Sprintf("[reconcileConfigMapDeleteFunc] successfully deleted the ConfigMap %s for the HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))
	}

	log.Info(fmt.Sprintf("[reconcileConfigMapDeleteFunc] ends reconciliation of ConfigMap %s for HuaweiStorageConnection %s", configMapName, huaweiStorageConnection.Name))

	return false, "", nil
}

func getNewConfigMap(csiConfig v1alpha1.HuaweiCSIConfigMapData, controllerNamespace, configMapName string) (*corev1.ConfigMap, error) {
	jsonData, err := json.Marshal(csiConfig)
	if err != nil {
		return nil, fmt.Errorf("[getNewConfigMap] unable to marshal data: %w", err)
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:       configMapName,
			Namespace:  controllerNamespace,
			Finalizers: []string{ConfigMapFinalizerName},
		},
		Data: map[string]string{
			HuaweiCSIConfigMapDataKey: string(jsonData),
		},
	}

	configMap.Labels = ensureLabel(configMap.Labels, ConfigMapManagedLabelSelector)

	return configMap, nil
}

func getUpdatedConfigMap(oldConfigMap *corev1.ConfigMap, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection) (*corev1.ConfigMap, error) {
	newCSIConfig := configureCSIConfig(huaweiStorageConnection, oldConfigMap.Namespace)

	newJsonData, err := json.Marshal(newCSIConfig)
	if err != nil {
		err = fmt.Errorf("[getUpdatedConfigMap] unable to marshal data: %w", err)
		return nil, err
	}

	newConfigMap := oldConfigMap.DeepCopy()
	newConfigMap.Data[HuaweiCSIConfigMapDataKey] = string(newJsonData)
	newConfigMap.Labels = ensureLabel(newConfigMap.Labels, ConfigMapManagedLabelSelector)
	newConfigMap.Finalizers = ensureFinalizer(newConfigMap.Finalizers, ConfigMapFinalizerName)

	return newConfigMap, nil
}

func areConfigMapsEqual(old, new corev1.ConfigMap) bool {
	if old.Data[HuaweiCSIConfigMapDataKey] == new.Data[HuaweiCSIConfigMapDataKey] && labels.Set(old.Labels).AsSelector().Matches(ConfigMapManagedLabelSelector) && slices.Contains(old.Finalizers, ConfigMapFinalizerName) {
		return true
	}

	return false
}
