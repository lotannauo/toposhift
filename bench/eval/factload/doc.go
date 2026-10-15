// Package factload writes the facts of one labelled scenario into a store, so
// that the queries can be scored against them.
//
// The product's service writers (internal/ingest/svcgraph and
// internal/ingest/spancount) read the metrics of the service graph and span count
// connectors. The dataset holds spans and none of those metrics, so this loader
// pairs the spans itself, as the service graph connector does, and gives its
// observations a TTL of its own, an evaluation parameter where the product's expiry
// is configuration. Feeding the product's writers with connector output computed
// from the spans is not done here.
//
// One scenario is two inputs, written to one empty store and never kept:
//
//   - Kubernetes objects, from one pull or a series of pulls. Pods, nodes and containers go through the
//     product's translator and nothing else: each object is translated, its
//     backdated records are resumed from the store with resume.Apply and
//     Translator.Resumed, and the records the translator and the resume step
//     leave are written as they are. This package builds no L2 record of its
//     own, and makes up no node from a pod's node name: a pod whose node was never
//     seen stays unplaced, and [Stats.Unplaced] lists it.
//   - Spans. Services, the instances that report them, the dependencies between
//     services and the bridge from an instance to its pod come from an evaluation
//     writer working on the spans, under the producer [ProducerTraces].
//
// # The pulls
//
// An objects file is one pull of the collector, or a series of pulls at a fixed
// interval. The translator replays them as the collector's pulls.
//
// For one pull (a zero [Scenario.PullInterval]) the translator is built with no
// pull interval, because one generation says nothing about an interval, and with an
// explicit TTL, [ObjectTTL], an evaluation parameter and not a property of the pull.
// An object is asserted from its creation (a first sighting is backdated to it)
// through the time it was observed, and lapses a TTL after that.
//
// For a series the translator is built with the interval and its own default
// lifetime, three intervals ([Lifetime]). An object listed in each pull has no gap
// between two of them, however the interval jitters, which a lifetime of the
// interval itself would not give: pulls a fraction of a second more than five
// minutes apart would lapse every object a fraction of a second before its next
// listing. An object missing from a pull lapses three intervals after the last
// pull that listed it.
//
// An object created or deleted between pulls, or before the first, is invisible
// until a pull lists it, and so is one created after the last. The claim that
// there are no late events covers the evaluation's own writer below, not this part:
// the translator backdates, and resume.Apply times those records.
//
// # The service writer
//
// Each record's event time is the earliest span start in its minute bucket (basis
// producer_event); the bucket only groups the spans, and the store assigns the
// ingest sequence. Records are written in event time order, so this writer has no
// late events. Every Observe has the TTL [ServiceTTL].
//
//   - A service is (service.namespace, service.name): one Observe in each minute
//     in which it emits a span.
//   - depends_on A to B pairs spans as the service graph connector does: a client
//     span of A with a child server span in B, or a producer span of A with a child
//     consumer span in B, the child named by its parent span id within one trace.
//     The second kind carries the payload [MessagingPayload]. One Observe in each
//     minute in which the pair occurs, at the earliest client span start. A call
//     from a service to itself writes nothing. A client span with no instrumented
//     child (a database, a cache) writes nothing and is counted. No node is made
//     for a peer name or a database attribute.
//   - A service instance is written only when the resource carries
//     service.instance.id, with instance_of to its service. An instance whose id
//     equals the resource's k8s.pod.uid has the payload [AddressPayload].
//   - The bridge part_of from an instance to a pod is written only from the
//     resource's k8s.pod.uid, never from a pod name or an address, and always has
//     the payload [AddressPayload]: in the files this package was written against
//     the collector put the pod uid on the span by matching an address, and the
//     SDK did not assert it.
//   - A span with no service.namespace is the service (none, name), as in the
//     product's writers, if the scenario has no span of that name with a namespace:
//     an ordinary service. If it has, the namespace-less span would be a second
//     identity of one service, and the two are never merged: the span is counted in
//     [Stats.NamespaceSplit] and written to nothing, no service, pair or instance.
//   - Nothing is written from host.name or k8s.node.name. Placement comes from the
//     objects only.
//
// The spans of a scenario may be a sample of its traces: a pair the sample
// missed is a missing dependency, and [Stats.Pairs] says how many were found.
package factload
