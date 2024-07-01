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
	"fmt"
	"reflect"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	SecretManagedLabelSelector = HuaweiStorageConnectionManagedLabelSelector
	SecretFinalizerName        = HuaweiStorageConnectionControllerFinalizerName
)

func IdentifyReconcileFuncForSecret(log logger.Logger, secretList *corev1.SecretList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, secretName string) (reconcileType string, err error) {
	if shouldReconcileSecretByCreateFunc(secretList, huaweiStorageConnection, secretName) {
		return internal.CreateReconcile, nil
	}

	should, err := shouldReconcileSecretByUpdateFunc(log, secretList, huaweiStorageConnection, secretName)
	if err != nil {
		return "", err
	}
	if should {
		return internal.UpdateReconcile, nil
	}

	return "", nil
}

func shouldReconcileSecretByCreateFunc(secretList *corev1.SecretList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, secretName string) bool {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return false
	}

	for _, s := range secretList.Items {
		if s.Name == secretName {
			return false
		}
	}

	return true
}

func shouldReconcileSecretByUpdateFunc(log logger.Logger, secretList *corev1.SecretList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, secretName string) (bool, error) {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return false, nil
	}

	for _, oldSecret := range secretList.Items {
		if oldSecret.Name == secretName {
			newSecret := getUpdatedSecret(&oldSecret, huaweiStorageConnection)
			equal := areSecretsEqual(&oldSecret, newSecret)

			log.Trace(fmt.Sprintf("[shouldReconcileSecretByUpdateFunc] old secret: %+v", oldSecret))
			log.Trace(fmt.Sprintf("[shouldReconcileSecretByUpdateFunc] new secret: %+v", newSecret))
			log.Trace(fmt.Sprintf("[shouldReconcileSecretByUpdateFunc] are secrets equal: %t", equal))

			if !equal {
				log.Debug(fmt.Sprintf("[shouldReconcileSecretByUpdateFunc] a secret %s should be updated", secretName))
				return true, nil
			}

			return false, nil
		}
	}
	err := fmt.Errorf("[shouldReconcileSecretByUpdateFunc] a secret %s not found in the list: %+v. It should be created", secretName, secretList.Items)
	return false, err
}

func getNewSecret(huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, secretName string) *corev1.Secret {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:       secretName,
			Namespace:  controllerNamespace,
			Finalizers: []string{SecretFinalizerName},
		},
		StringData: getSecretStringData(huaweiStorageConnection),
	}

	secret.Labels = ensureLabel(secret.Labels, SecretManagedLabelSelector)

	return secret
}

func getSecretStringData(huaweiStorageConnection *v1alpha1.HuaweiStorageConnection) map[string]string {
	return map[string]string{
		"user":     huaweiStorageConnection.Spec.Login,
		"password": huaweiStorageConnection.Spec.Password,
	}
}

func getUpdatedSecret(oldSecret *corev1.Secret, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection) *corev1.Secret {
	newSecret := oldSecret.DeepCopy()
	newSecret.StringData = getSecretStringData(huaweiStorageConnection)
	newSecret.Labels = ensureLabel(newSecret.Labels, SecretManagedLabelSelector)
	newSecret.Finalizers = ensureFinalizer(newSecret.Finalizers, SecretFinalizerName)

	return newSecret
}

func areSecretsEqual(old, new *corev1.Secret) bool {
	if reflect.DeepEqual(old.StringData, new.StringData) && labels.Set(new.Labels).AsSelector().Matches(SecretManagedLabelSelector) && slices.Contains(new.Finalizers, SecretFinalizerName) {
		return true
	}

	return false
}

func reconcileSecretCreateFunc(ctx context.Context, cl client.Client, log logger.Logger, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, secretName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileSecretCreateFunc] starts reconciliation of HuaweiStorageConnection %s for Secret %s", huaweiStorageConnection.Name, secretName))

	newSecret := getNewSecret(huaweiStorageConnection, controllerNamespace, secretName)
	log.Debug(fmt.Sprintf("[reconcileSecretCreateFunc] successfully configurated secret %s for the HuaweiStorageConnection %s", secretName, huaweiStorageConnection.Name))
	log.Trace(fmt.Sprintf("[reconcileSecretCreateFunc] secret: %+v", newSecret))

	err = cl.Create(ctx, newSecret)
	if err != nil {
		err = fmt.Errorf("[reconcileSecretCreateFunc] unable to create a Secret %s for HuaweiStorageConnection %s: %w", newSecret.Name, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	return false, "Successfully created", nil
}

func reconcileSecretUpdateFunc(ctx context.Context, cl client.Client, log logger.Logger, secretList *corev1.SecretList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, controllerNamespace, secretName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileSecretUpdateFunc] starts reconciliation of HuaweiStorageConnection %s for Secret %s", huaweiStorageConnection.Name, secretName))

	var oldSecret *corev1.Secret
	for _, s := range secretList.Items {
		if s.Name == secretName {
			oldSecret = &s
			break
		}
	}

	if oldSecret == nil {
		err := fmt.Errorf("[reconcileSecretUpdateFunc] unable to find a secret %s for the HuaweiStorageConnection %s", secretName, huaweiStorageConnection.Name)
		return true, err.Error(), err
	}

	log.Debug(fmt.Sprintf("[reconcileSecretUpdateFunc] secret %s was found for the HuaweiStorageConnection %s", secretName, huaweiStorageConnection.Name))

	updatedSecret := getUpdatedSecret(oldSecret, huaweiStorageConnection)
	log.Debug(fmt.Sprintf("[reconcileSecretUpdateFunc] successfully configurated new secret %s for the HuaweiStorageConnection %s", secretName, huaweiStorageConnection.Name))
	log.Trace(fmt.Sprintf("[reconcileSecretUpdateFunc] old secret: %+v", oldSecret))
	log.Trace(fmt.Sprintf("[reconcileSecretUpdateFunc] updated secret: %+v", updatedSecret))

	err = cl.Update(ctx, updatedSecret)
	if err != nil {
		err = fmt.Errorf("[reconcileSecretUpdateFunc] unable to update the Secret %s for HuaweiStorageConnection %s: %w", updatedSecret.Name, huaweiStorageConnection.Name, err)
		return true, err.Error(), err
	}

	log.Info(fmt.Sprintf("[reconcileSecretUpdateFunc] successfully updated the Secret %s for the HuaweiStorageConnection %s", updatedSecret.Name, huaweiStorageConnection.Name))

	return false, "Successfully updated", nil
}

func reconcileSecretDeleteFunc(ctx context.Context, cl client.Client, log logger.Logger, secretList *corev1.SecretList, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, secretName string) (shouldRequeue bool, msg string, err error) {
	log.Debug(fmt.Sprintf("[reconcileSecretDeleteFunc] starts reconciliation of Secret %s for HuaweiStorageConnection %s", secretName, huaweiStorageConnection.Name))

	var secret *corev1.Secret
	for _, s := range secretList.Items {
		if s.Name == secretName {
			secret = &s
			break
		}
	}

	if secret == nil {
		log.Info(fmt.Sprintf("[reconcileSecretDeleteFunc] no secret with name %s found for the HuaweiStorageConnection %s. No action required.", secretName, huaweiStorageConnection.Name))
	}

	if secret != nil {
		log.Info(fmt.Sprintf("[reconcileSecretDeleteFunc] successfully found a secret %s for the HuaweiStorageConnection %s", secretName, huaweiStorageConnection.Name))
		err = deleteResource(ctx, cl, secret, SecretFinalizerName)

		if err != nil {
			err = fmt.Errorf("[reconcileSecretDeleteFunc] unable to delete the Secret %s for the CephCluster %s: %w", secret.Name, huaweiStorageConnection.Name, err)
			return true, err.Error(), err
		}

		log.Info(fmt.Sprintf("[reconcileSecretDeleteFunc] successfully deleted the Secret %s for the HuaweiStorageConnection %s", secret.Name, huaweiStorageConnection.Name))
	}

	log.Info(fmt.Sprintf("[reconcileSecretDeleteFunc] ends reconciliation of HuaweiStorageConnection %s for Secret %s", huaweiStorageConnection.Name, secretName))

	return false, "", nil
}
