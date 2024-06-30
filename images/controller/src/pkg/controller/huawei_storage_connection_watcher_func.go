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
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func validateHuaweiStorageConnectionSpec(huaweiStorageConnection *v1alpha1.HuaweiStorageConnection) (bool, string) {
	if huaweiStorageConnection.DeletionTimestamp != nil {
		return true, ""
	}

	var (
		failedMsgBuilder strings.Builder
		validationPassed = true
	)

	failedMsgBuilder.WriteString("Validation of HuaweiStorageConnection failed: ")

	if huaweiStorageConnection.Spec.StorageType == "" {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.storageType field is empty; ")
	} else {
		if !slices.Contains(v1alpha1.HuaweiStorageConnectionStorageTypes, huaweiStorageConnection.Spec.StorageType) {
			validationPassed = false
			failedMsgBuilder.WriteString(fmt.Sprintf("the spec.storageType field has an invalid value %s; ", huaweiStorageConnection.Spec.StorageType))
		} else {
			storageType := v1alpha1.HuaweiStorageTypeMapping[huaweiStorageConnection.Spec.StorageType]
			if storageType == "" {
				validationPassed = false
				failedMsgBuilder.WriteString(fmt.Sprintf("cannot find a mapping for the spec.storageType field value %s, mapping: %+v; ", huaweiStorageConnection.Spec.StorageType, v1alpha1.HuaweiStorageTypeMapping))
			}
		}
	}

	if len(huaweiStorageConnection.Spec.Pools) == 0 {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.pools field is empty; ")
	}

	if len(huaweiStorageConnection.Spec.URLs) == 0 {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.urls field is empty; ")
	}

	if huaweiStorageConnection.Spec.Login == "" {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.login field is empty; ")
	}

	if huaweiStorageConnection.Spec.Password == "" {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.password field is empty; ")
	}

	if huaweiStorageConnection.Spec.Protocol == "" {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.protocol field is empty; ")
	} else {
		if !slices.Contains(v1alpha1.HuaweiStorageConnectionProtocols, huaweiStorageConnection.Spec.Protocol) {
			validationPassed = false
			failedMsgBuilder.WriteString(fmt.Sprintf("the spec.protocol field has an invalid value %s; ", huaweiStorageConnection.Spec.Protocol))
		} else {
			protocol := v1alpha1.HuaweiStorageProtocolMapping[huaweiStorageConnection.Spec.Protocol]
			if protocol == "" {
				validationPassed = false
				failedMsgBuilder.WriteString(fmt.Sprintf("cannot find a mapping for the spec.protocol field value %s, mapping: %+v; ", huaweiStorageConnection.Spec.Protocol, v1alpha1.HuaweiStorageProtocolMapping))
			}
		}
	}

	if huaweiStorageConnection.Spec.MaxClientThreads == 0 {
		validationPassed = false
		failedMsgBuilder.WriteString("the spec.maxClientThreads field is empty; ")
	}

	return validationPassed, failedMsgBuilder.String()
}

func updateHuaweiStorageConnectionPhase(ctx context.Context, cl client.Client, huaweiStorageConnection *v1alpha1.HuaweiStorageConnection, phase, reason string) error {
	if huaweiStorageConnection.Status == nil {
		huaweiStorageConnection.Status = &v1alpha1.HuaweiStorageConnectionStatus{}
	}
	huaweiStorageConnection.Status.Phase = phase
	huaweiStorageConnection.Status.Reason = reason

	err := cl.Status().Update(ctx, huaweiStorageConnection)
	if err != nil {
		return err
	}

	return nil
}
