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
	"fmt"
	"slices"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func shouldReconcileByDeleteFunc(obj metav1.Object) bool {
	if obj.GetDeletionTimestamp() != nil {
		return true
	}

	return false
}

func removeFinalizerIfExists(ctx context.Context, cl client.Client, obj metav1.Object, finalizerName string) (bool, error) {
	removed := false
	finalizers := obj.GetFinalizers()
	for i, f := range finalizers {
		if f == finalizerName {
			finalizers = append(finalizers[:i], finalizers[i+1:]...)
			removed = true
			break
		}
	}

	if removed {
		obj.SetFinalizers(finalizers)
		err := cl.Update(ctx, obj.(client.Object))
		if err != nil {
			return false, err
		}
	}

	return removed, nil
}

func addFinalizerIfNotExists(ctx context.Context, cl client.Client, obj metav1.Object, finalizerName string) (bool, error) {
	added := false
	finalizers := obj.GetFinalizers()
	if !slices.Contains(finalizers, finalizerName) {
		finalizers = append(finalizers, finalizerName)
		added = true
	}

	if added {
		obj.SetFinalizers(finalizers)
		err := cl.Update(ctx, obj.(client.Object))
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func ensureLabel(resourceLabels map[string]string, selector labels.Set) map[string]string {
	if !labels.Set(resourceLabels).AsSelector().Matches(selector) {
		if resourceLabels == nil {
			resourceLabels = make(map[string]string)
		}
		for key, value := range selector {
			resourceLabels[key] = value
		}
	}

	return resourceLabels
}

func ensureFinalizer(resourceFinalizers []string, finalizerName string) []string {
	if !slices.Contains(resourceFinalizers, finalizerName) {
		if resourceFinalizers == nil {
			resourceFinalizers = []string{}
		}
		resourceFinalizers = append(resourceFinalizers, finalizerName)
	}

	return resourceFinalizers
}

func deleteResource(ctx context.Context, cl client.Client, obj client.Object, finalizerName string) error {
	_, err := removeFinalizerIfExists(ctx, cl, obj, finalizerName)
	if err != nil {
		return err
	}

	if len(obj.GetFinalizers()) > 0 {
		return fmt.Errorf("[deleteResource] resource %s can't be deleted because it still has finalizers: %v", obj.GetName(), obj.GetFinalizers())
	}

	err = cl.Delete(ctx, obj)
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}

	return nil
}
