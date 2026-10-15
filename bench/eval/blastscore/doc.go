// Package blastscore scores the blast-radius queries against labelled fault
// cases, with the acceptance rule fixed before anything is measured.
//
// The package never parses a dataset. A loader turns each labelled scenario
// into a [Case] and a store that holds its facts; [RunCase] runs the product
// queries on that store and the baselines on the same facts, window and
// snapshot token; [Evaluate] pools the runs into strata, bootstraps their
// intervals and judges them by the rule in accept.go. It changes no product
// code, reads the store only (a store handed to the queries refuses every Write
// and Retain), and writes nothing derived back.
//
// # Predictors and baselines
//
//   - W is the blast-radius walk at its default policy; its hop is the walk's
//     depth. At service level it is the Service members of Impact(service). For
//     a node or host seed it is the k8s.pod members of Impact(seed).
//   - C is the pod co-residency of the seed service's pods (shared node only),
//     unioned over the seed pods, each pod keeping its longest overlap, at hop 1.
//   - C->svc is the other service of each pair the seed service is in, from
//     extract.Extract with an exploratory seed, at hop 1. A pair of two other
//     members is not about the seed.
//   - W+C ranks W's list and then C->svc's by hop, W first among equals.
//   - B_all is every entity of the level that existed in the window, less the
//     seed: its precision is the base rate. B_1hop is the services one depends_on
//     edge from the seed in either direction. B_union is reverse depends_on
//     reachability over every edge with a piece in the window, ignoring when. It
//     is a union-graph comparator, reported as a ceiling and never judged.
//   - W->pod is the projection of W's service members onto pods, an evaluation
//     rule and not a product rule: a pod is named where W's reach of a member, the
//     instance_of edge of one of its instances and that instance's part_of edge to
//     the pod are all live at once. An instance part of two pods at once, or of two
//     services at once, names neither over the overlap, and a claim whose other end
//     is quarantined is never followed but still counts as a claim. A pod takes the
//     hop of the nearest member that names it. It needs the instance and bridge
//     facts a loader writes. At pod level W+C is W->pod followed by C.
//
// A hit is an entity in both the list and the truth. Credit goes only to what
// the walk returns: a truth service reachable only callee-ward is a miss, and
// counted in the against_direction diagnostic.
//
// # Metrics
//
// For a case, a level and a predictor, L is the ranked, de-duplicated list with
// n entities, T the truth with the seed removed (plus the truth no rule could map
// or place, which is a miss), and h = |L and T|. Per case, P = h/n (undefined if
// n = 0) and R = h/|T| (undefined if |T| = 0). Pooled, micro P is the sum of h
// over the sum of n, micro R the sum of h over the sum of |T|, F1 the harmonic
// mean of the two micro values, and lift micro P over B_all's. At k in {1, 3, 5},
// P@k = hits among the first k over min(k, n), and R@k = those hits over |T|,
// each as the median and the mean over the cases where it is defined. By hop,
// the micro precision of the entities named at each hop 1 to 6 is listed with both
// counts. The interval of every pooled number is a 95 % percentile interval of a
// case bootstrap of 10,000 resamples with seed 20261015; every statistic of a
// stratum is taken on the same resamples, so a difference between predictors
// has an interval of its own, from paired draws.
//
// # The acceptance rule
//
// The primary judgement is W at service level over call-graph cases; every other
// judged stratum gets the same rule and only the primary one is a headline.
// A stratum is "cannot say" if it has fewer than 8 scorable cases; if more than
// 20 % of its truth is unmappable, unplaced or uninstrumented; if more than 25 %
// of the dataset's cases fail the load check (uninstrumented seeds are excluded
// before that count, and only two things fail it: the seed is absent from the
// store in the window, or the facts hold no depends_on piece in it; a quarantined
// seed, an unmappable root, a missing window, labels that cannot be read and missing
// Kubernetes objects are reported and are not load failures, and stay in the count); or, for a co-residency predictor, if fewer than 8 cases
// remain after the single-node rule (a cluster of one node leaves co-residency
// predicting everything, so C, C->svc and W+C are not scored there). Otherwise it
// is Good if micro recall is at least 0.80 with a 95 % lower bound of at least
// 0.65, lift at least 1.5 with a lower bound above 1.0, and F1(W) - F1(B_1hop) at
// least 0 with a lower bound of at least -0.10; Poor if recall is below 0.60,
// lift below 1.10, or the upper bound of F1(W) - F1(B_1hop) is below 0; and Mixed
// otherwise. A difference whose interval holds zero is "not resolved" and never a
// ranking. A pod-level stratum has no one-hop comparator, so it can be Poor or
// Mixed and can never be Good. The cut-points are exported constants with their
// date; see [AcceptanceDate]. They are dated the day the labels' schema was
// read, which is before any fact was loaded or any case scored, and that is what
// fixing them in advance protects.
//
// # Results contract
//
// [Report.JSON] holds pooled numbers, and numbers keyed by case id
// ([CaseIDPattern]), the diagnostic counters, the policy versions and two plain
// labels. It holds no entity or pod or node name, no uid, no label text, no
// timestamp and no row of a dataset: a case carries fingerprints that are never
// printed, and what a result keeps of time is the boolean "the pulls overlap the
// window" and, since schema 2, each case's trace coverage: the boolean "the traces
// export can cover the window" and the whole minutes it overlaps it, each null
// where it could not be computed.
//
// # What a loader reads: the ground-truth files
//
// This is the schema of the labelled scenarios' files as read from the fetched
// dataset (35 scenarios), recorded here because the dataset publishes none. The
// fixtures in testdata mirror it with invented names.
//
// A scenario has ground_truth.yaml and alert files. The YAML comes in two
// shapes with the same fields: flat, with the keys at the top level, and wrapped,
// with apiVersion "itbench.io/v1", kind "GroundTruth", metadata {name} and the
// keys under spec. Of the 35 files, 4 are wrapped, and 31 are flat, of which one
// is not valid YAML (a repeated mapping key in an alert entry), so 30 flat files
// are readable. A strict parser fails on the invalid one; a loader lists such a
// scenario as one it cannot read and does not repair it.
//
//	fault:                  # the labelled fault, one or more entries
//	  - entity: {name, kind, group_id}   # group_id is optional; name may be a group id or a wildcard
//	    condition: text
//	    category: text      # e.g. a configuration, resource or network class
//	    fault_mechanism: text
//	    changed: {element, from, to}     # optional, for a changed configuration object
//	alerts:                 # what fired
//	  - {id, group_id, metadata: {description}}   # or `text` in place of metadata
//	groups:                 # named sets of cluster objects
//	  - id: text
//	    kind: Pod|Service|Deployment|ReplicaSet|ConfigMap|HorizontalPodAutoscaler|Namespace|...
//	    name: text          # optional, an exact name
//	    namespace: text
//	    filter: [text]      # optional, regular expressions over object names; \b is used
//	    root_cause: true    # optional, marks the root of the chain
//	aliases: [[group id]]   # sets of group ids that stand for one thing
//	propagations:           # the labelled chain, group id to group id
//	  - {source, target, condition, effect}
//	recommended_actions: [...]   # not used
//
// Filters are Go RE2 expressions (a handful of `\b` word boundaries included), to
// be matched against the whole name; one of the 217 in the 34 readable files
// (229 in all) does not compile and is unmappable. Names in fault entities and groups are group ids
// and wildcards as often as object names. No file carries a time of injection, so
// the start of a window is always taken from the alerts.
//
// Alert files sit in alerts/ and are named alerts_at_<time>.json: a JSON array of
// Prometheus-style alerts {labels, annotations, state, activeAt, value}, one file
// a minute, whose latest names the end of the window and whose earliest activeAt
// is the proxy for its start. Of the 34 alerts/ folders, 33 also hold one
// alerts_in_alerting_state_<time>.json, an object {data: {alerts: [...]},
// status, timestamp}, and one folder holds none. The scenario with no alerts/
// folder has one such file at its root.
package blastscore
