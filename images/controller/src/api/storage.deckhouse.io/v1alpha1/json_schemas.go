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

package v1alpha1

type HuaweiCSIConfigMapData struct {
	Backends Backend `json:"backends"`
}

type Backend struct {
	Name             string            `json:"name"`
	Namespace        string            `json:"namespace"`
	Storage          string            `json:"storage"`
	URLs             []string          `json:"urls"`
	Pools            []string          `json:"pools"`
	MaxClientThreads string            `json:"maxClientThreads"`
	Provisioner      string            `json:"provisioner"`
	Parameters       BackendParameters `json:"parameters"`
}

type BackendParameters struct {
	Protocol string   `json:"protocol"`
	Portals  []string `json:"portals,omitempty"`
}
