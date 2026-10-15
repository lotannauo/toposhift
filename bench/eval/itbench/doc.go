// Package itbench reads the labelled fault scenarios of ITBench-Lite (IBM Research,
// Apache-2.0) and turns each into a [blastscore.Case] and the facts to score it
// against. It is the dataset adapter: [blastscore] never parses a dataset, and
// package factload writes the facts.
//
// # Files
//
// A dataset directory holds one folder a scenario, Scenario-N. A scenario has
// ground_truth.yaml (the labels), alert files (an alerts folder, and for one
// scenario an alerts_in_alerting_state file beside the labels),
// k8s_objects_raw.tsv and otel_traces_raw.tsv. The two exports are tab-separated
// with a header line; a field that holds a tab, a line break or a quote is quoted
// with double quotes, and a map in a field is the repr of a Python dictionary. The
// objects export is a series of log records of the collector's objects receiver,
// the object as JSON in Body and the resource it was listed from in LogAttributes.
// It is one snapshot: every row was listed within a minute. The traces export has a
// row a span, with the resource attributes of the process in ResourceAttributes.
// The package reads files and nothing else, and copies nothing from them.
//
// # Two tables before any score
//
// The case table is fixed in two steps, each printed outside the repository and
// each approved before a score is made.
//
// Table A ([ReadTableA], [FormatTableA]) is read from the labels and the alerts
// only. For a scenario it gives the root of the chain (the one group marked
// root_cause), the seed rule, the class, the window and the groups the truth is
// made of. The class comes from the labels alone, so that a recall against the call
// graph is not circular: a case is co-residency if the category or the fault
// mechanism of a fault entry about the root is one of a few exact strings (see
// coResidencyCategories and coResidencyMechanisms), and call-graph otherwise. The
// window is [T0, T1): the labels carry no time of injection, so T0 is the earliest
// time an alert became active, a proxy, and T1 the time of the latest alerts_at
// file, or, when there is none, of the alerts_in_alerting_state file, also a proxy.
// A scenario with no such time has no window and is excluded.
//
// Table B ([TableBText], [PlanSeed], [ChainTruth]) maps those groups to services and
// pods using the Kubernetes objects only. A group names the objects of its kind in
// its namespace whose name is its exact name, or whole-name matches a filter it
// carries (a Go RE2 expression, as written). A group with a wildcard name and no
// filter, or a filter that does not compile, names nothing. An object maps to a
// service only through the app.kubernetes.io/component label of the pods it is, owns
// (by owner reference uid, down to the pods) or selects (by an exact selector); an
// object whose pods name no service or several maps to none. There is no prefix, no
// fuzzy match and no use of an object's name. The seed is:
//
//   - a Pod root: the service of the pod, and the pod, if exactly one pod of the
//     group is alive at T0; otherwise no seed pod, and the pod level cannot say;
//   - a Deployment, ReplicaSet, StatefulSet or Service root: its service, and every
//     pod it owns or selects that is alive at T0;
//   - a Node root: the node, scored at the pod level only;
//   - any other root: the first propagation target that maps to one service, by
//     these rules, marked moved; if none does, the case is excluded as an
//     unmappable root.
//
// A seed service is uninstrumented if its name appears in no span of any trace data
// read for the evaluation ([PooledServices]: every scenario's traces export that is
// present, and the names files of the probes, one service.name a line, sorted and
// unique, in the probe folder beside the data). The set is a union and only grows
// as data is read; it is frozen by its hash before any case is scored. Such a case is
// excluded before the load check. A seed
// that has spans somewhere but none in its own scenario's window fails the load check
// like any other.
//
// The truth is every group of the chain except the root and the seed's own: a set of
// services after mapping (a Deployment and its Pod that map to one service count
// once, and the seed's service is not truth), and the pods of the chain's groups of
// pods. A group that maps to nothing is counted as unmappable truth. Only the groups
// of pods are expanded into pods. A pod whose node is not in the snapshot is
// unplaced and counted, never given a node.
//
// A ground_truth.yaml that does not parse is listed as unreadable, and is never
// repaired. Every scenario of the dataset stays in the dataset's count: one whose
// labels cannot be read, or with no Kubernetes objects to map through, has no seed and
// is excluded and kept in the count; one with no traces export has no service in its
// store, so a service seed is absent, which is a load failure; a node seed is loaded
// from the objects alone, and the node's existence decides the load check.
//
// # Facts
//
// [RunScenario] reads a scenario, loads it into a store of its own with package
// factload, runs [blastscore.RunCase] on it, and discards the store. The service of
// a name is (the namespace its spans carry, the name): the spans' service.namespace,
// not the namespace of the Kubernetes objects, which is not the same string in
// the files this was written against. A name with no spans
// takes the namespace every instrumented service of the scenario shares, if there is
// one; otherwise it is unmappable. A name whose spans never carry a namespace is the
// service (none, name), an ordinary service. A span with no namespace whose name also
// has spans with one would be a second identity of one service: it is counted and
// written to nothing, and the name maps to the namespaced service.
//
// In the sample of traces this was written against (17 services), the pod uid was
// on every span and the instance id equalled it for 13 of the services, the collector
// having put both there by matching an address, which the product's writer would not
// assert; the other four (ad, cart, fraud-detection and quote, 1,359 of 10,000 spans)
// carry instance ids their own SDKs make. The facts built from the pod uid are
// labelled address-associated, an instance only where its id is the pod uid, and the
// strata that depend on them (W->pod and C->svc) are read with that in mind.
package itbench
