# Collector permissions

[Collector setup](README.md) · [Node agents](node-agents.md)

## Read access the collector needs


The cluster collector reads the cluster with its service account (`auth_type: serviceAccount`). With the collector contrib distribution, version 0.162.0, the `k8s_cluster` receiver and `k8s_objects` read the kinds below with `get`, `list` and `watch`. The role also grants `events` and four status and spec subresources, which the upstream receiver's example lists and the shipped configuration does not read; they are read-only, and you may drop them. The collector writes nothing to the cluster, so those are all the verbs it needs. A resource it may not read shows as `forbidden` lines in the collector's log and no entities for that kind in toposhift; the receiver's other kinds wait up to its ten-minute initial sync, then flow, and its log says `Timed out waiting for initial cache sync.` The example is ready to apply: replace `<COLLECTOR_NAMESPACE>` with the namespace the collector runs in, and run the collector's pods under the service account `toposhift-collector`.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: toposhift-collector
  namespace: <COLLECTOR_NAMESPACE>
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: toposhift-collector
rules:
  - apiGroups:
      - ""
    resources:
      - events
      - namespaces
      - namespaces/status
      - nodes
      - nodes/spec
      - pods
      - pods/status
      - replicationcontrollers
      - replicationcontrollers/status
      - resourcequotas
      - services
      - persistentvolumeclaims
      - persistentvolumes
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - apps
    resources:
      - daemonsets
      - deployments
      - replicasets
      - statefulsets
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - batch
    resources:
      - jobs
      - cronjobs
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - autoscaling
    resources:
      - horizontalpodautoscalers
    verbs:
      - get
      - list
      - watch
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: toposhift-collector
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: toposhift-collector
subjects:
  - kind: ServiceAccount
    name: toposhift-collector
    namespace: <COLLECTOR_NAMESPACE>
```

Node agents need only `get` on nodes; see [Node agents](node-agents.md#pod-requirements).
