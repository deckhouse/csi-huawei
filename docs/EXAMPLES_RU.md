---
title: "Модуль csi-huawei: примеры"
---

## Пример описания `HuaweiStorageConnection`

```yaml
apiVersion: storage.deckhouse.io/v1alpha1
kind: HuaweiStorageConnection
metadata:
  name: hs-1
spec:
  storageType: OceanStorSAN
  pools:
    - storage-pool-1
    - storage-pool-2
  urls: 
    - 172.22.1.10
    - 172.22.1.11
  login: "user-1"
  password: "secret-password"
  protocol: ISCSI
  portals:
    - portal1
  maxClientThreads: 30
```

- Проверить создание объекта можно командой (Phase должен быть `Created`):

```shell
kubectl get huaweistorageconnection <имя cephclusterconnection>
```
