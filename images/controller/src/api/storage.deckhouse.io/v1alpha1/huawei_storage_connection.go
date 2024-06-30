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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	HuaweiStorageConnectionProtocolISCSI  = "ISCSI"
	HuaweiStorageConnectionProtocolFC     = "FC"
	HuaweiStorageConnectionProtocolROCE   = "ROCE"
	HuaweiStorageConnectionProtocolFCnvme = "FC-NVME"
	HuaweiStorageConnectionProtocolNFS    = "NFS"
	HuaweiStorageConnectionProtocolDPC    = "DPC"
	HuaweiStorageConnectionProtocolSCSI   = "SCSI"

	HuaweiStorageConnectionStorageTypeOceanStorSAN     = "OceanStorSAN"
	HuaweiStorageConnectionStorageTypeOceanStorNAS     = "OceanStorNAS"
	HuaweiStorageConnectionStorageTypeOceanStorDtree   = "OceanStorDtree"
	HuaweiStorageConnectionStorageTypeFusionStorageSAN = "FusionStorageSAN"
	HuaweiStorageConnectionStorageTypeFusionStorageNAS = "FusionStorageNAS"
)

var (
	HuaweiStorageConnectionProtocols = []string{
		HuaweiStorageConnectionProtocolISCSI,
		HuaweiStorageConnectionProtocolFC,
		HuaweiStorageConnectionProtocolROCE,
		HuaweiStorageConnectionProtocolFCnvme,
		HuaweiStorageConnectionProtocolDPC,
		HuaweiStorageConnectionProtocolSCSI,
	}

	HuaweiStorageProtocolMapping = map[string]string{
		HuaweiStorageConnectionProtocolISCSI:  "iscsi",
		HuaweiStorageConnectionProtocolFC:     "fc",
		HuaweiStorageConnectionProtocolROCE:   "roce",
		HuaweiStorageConnectionProtocolFCnvme: "fc-nvme",
		HuaweiStorageConnectionProtocolNFS:    "nfs",
		HuaweiStorageConnectionProtocolDPC:    "dpc",
		HuaweiStorageConnectionProtocolSCSI:   "scsi",
	}

	HuaweiStorageConnectionStorageTypes = []string{
		HuaweiStorageConnectionStorageTypeOceanStorSAN,
		HuaweiStorageConnectionStorageTypeOceanStorNAS,
		HuaweiStorageConnectionStorageTypeOceanStorDtree,
		HuaweiStorageConnectionStorageTypeFusionStorageSAN,
		HuaweiStorageConnectionStorageTypeFusionStorageNAS,
	}

	HuaweiStorageTypeMapping = map[string]string{
		HuaweiStorageConnectionStorageTypeOceanStorSAN:     "oceanstor-san",
		HuaweiStorageConnectionStorageTypeOceanStorNAS:     "oceanstor-nas",
		HuaweiStorageConnectionStorageTypeOceanStorDtree:   "oceanstor-dtree",
		HuaweiStorageConnectionStorageTypeFusionStorageSAN: "fusionstorage-san",
		HuaweiStorageConnectionStorageTypeFusionStorageNAS: "fusionstorage-nas",
	}
)

type HuaweiStorageConnection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              HuaweiStorageConnectionSpec    `json:"spec"`
	Status            *HuaweiStorageConnectionStatus `json:"status,omitempty"`
}

// HuaweiStorageConnectionList contains a list of empty block device
type HuaweiStorageConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []HuaweiStorageConnection `json:"items"`
}

type HuaweiStorageConnectionSpec struct {
	StorageType      string   `json:"storageType"`
	Pools            []string `json:"pools"`
	URLs             []string `json:"urls"`
	Login            string   `json:"login"`
	Password         string   `json:"password"`
	Protocol         string   `json:"protocol"`
	Portals          []string `json:"portals"`
	MaxClientThreads int      `json:"maxClientThreads"`
}

type HuaweiStorageConnectionStatus struct {
	Phase  string `json:"phase,omitempty"`
	Reason string `json:"reason,omitempty"`
}
