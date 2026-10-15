package itbench

// The labels of the three invented scenarios.

// scenario1Truth: a pod fault on the payment pods. The chain goes up through the
// cart and the front, and ends on the front's pods.
const scenario1Truth = `
fault:
  - entity: {name: pay-pods, kind: Pod, group_id: pay-pods}
    condition: invented
    category: Change
    fault_mechanism: custom
groups:
  - id: pay-pods
    kind: Pod
    namespace: shop
    filter: ["shop-pay-.*"]
    root_cause: true
  - id: cart-svc
    kind: Service
    namespace: shop
    filter: ['shop-cart\b']
  - id: front-svc
    kind: Service
    namespace: shop
    filter: ['shop-front\b']
  - id: front-pods
    kind: Pod
    namespace: shop
    filter: ["shop-front-r1-.*"]
propagations:
  - {source: pay-pods, target: cart-svc}
  - {source: cart-svc, target: front-svc}
  - {source: front-svc, target: front-pods}
`

// scenario2Truth: a configuration fault, whose root is a ConfigMap. The seed moves
// to the cart service; the chain also names the payment service, which the cart
// calls.
const scenario2Truth = `
fault:
  - entity: {name: shop-flags, kind: ConfigMap}
    condition: invented
    category: Configuration Setting
    fault_mechanism: custom
groups:
  - id: flags
    kind: ConfigMap
    namespace: shop
    name: shop-flags
    root_cause: true
  - id: cart-svc
    kind: Service
    namespace: shop
    filter: ['shop-cart\b']
  - id: front-svc
    kind: Service
    namespace: shop
    filter: ['shop-front\b']
  - id: pay-svc
    kind: Service
    namespace: shop
    filter: ['shop-pay\b']
propagations:
  - {source: flags, target: cart-svc}
  - {source: cart-svc, target: front-svc}
  - {source: cart-svc, target: pay-svc}
`

// scenario3Truth: memory pressure on the cart pod, which shares a node with the
// front and the proxy.
const scenario3Truth = `
fault:
  - entity: {name: cart-pods, kind: Pod, group_id: cart-pods}
    condition: invented
    category: Resource
    fault_mechanism: memory_stress
groups:
  - id: cart-pods
    kind: Pod
    namespace: shop
    filter: ["shop-cart-r1-.*"]
    root_cause: true
  - id: front-pod
    kind: Pod
    namespace: shop
    filter: ["shop-front-r1-p1"]
  - id: proxy-pods
    kind: Pod
    namespace: shop
    filter: ["shop-front-proxy-.*"]
propagations:
  - {source: cart-pods, target: front-pod}
  - {source: front-pod, target: proxy-pods}
`
