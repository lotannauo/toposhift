package k8scluster_test

import "testing"

// The golden files hold the translation of the fixtures cut from the cluster
// receiver's entity events of the simulated cluster captures: one step per log
// record, with the instant the record carries, the records, pending edges,
// skips, notes and namespace sightings it produced. They are made in the default
// mode (the producer's clock trusted, no receipt time) with a collector that
// reports every 60 seconds, so a TTL is 3 minutes. They are read through by eye
// when written. Regenerate them only with -update and list the files in the
// commit.
//
// Provenance of the fixtures. Every record is a copy of a record of the third
// kwok capture (its run1.jsonl and run2.jsonl), with its resource and scope. Each line of a fixture is one
// batch of the capture reduced to the entity events listed, and the lines are in
// the capture's order. The numbers below are the fixture line, then the line of
// the capture it was cut from; each was checked against the UID in the record.
// The cluster's own names are given for reading: kwok-node-2 was deleted at
// capture line 921 of run1 and registered again under a new UID at 1054; the
// containers of the simulated cluster all have an empty container.id; run2 starts
// a new translator, so its first records of a UID, among them the teardown
// deletes, are about entities that translator has not seen.
//
// objects.jsonl is not translated here. It holds records of the Kubernetes
// objects receiver from the same capture (a pull of the nodes, a pull of the
// pods, and the first record of the pods and the node that the pulls do not
// hold), for the test that compares the UIDs the two translators write.
//
//	run1.jsonl
//	   1  <- capture line 3    node state 36ee1190 (kwok-node-0)
//	   2  <- capture line 5    node state f41e8e62 (kwok-node-2)
//	   3  <- capture line 7    node state 70f60f5f (kwok-node-4)
//	   4  <- capture line 8    deployment state 746d8f2a (default)
//	   5  <- capture line 10   service state cdf1decc (default)
//	   6  <- capture line 11   namespace state ecd00ad7 (default)
//	   7  <- capture line 14   namespace state fda3fdb1 (kube-system)
//	   8  <- capture line 15   replicaset state 4f98dd5e (default)
//	   9  <- capture line 16   pod state 9ab1d6d5 (pre-5984778487-262tj); container of 9ab1d6d5
//	  10  <- capture line 17   pod state 4a9d1695 (pre-5984778487-42s2c); container of 4a9d1695
//	  11  <- capture line 52   pod state c5ad95aa (pre-5984778487-wr85m); container of c5ad95aa
//	  12  <- capture line 58   node state 70f60f5f (kwok-node-4)
//	  13  <- capture line 70   node state f41e8e62 (kwok-node-2)
//	  14  <- capture line 86   pod state 9ab1d6d5 (pre-5984778487-262tj); container of 9ab1d6d5
//	  15  <- capture line 109  statefulset state bf011e05 (default)
//	  16  <- capture line 147  node state f41e8e62 (kwok-node-2)
//	  17  <- capture line 229  pod state a19305d2 (pre-5984778487-nnsrv)
//	  18  <- capture line 260  pod state a19305d2 (pre-5984778487-nnsrv)
//	  19  <- capture line 280  pod state a19305d2 (pre-5984778487-nnsrv); container of a19305d2
//	  20  <- capture line 389  node state f41e8e62 (kwok-node-2)
//	  21  <- capture line 499  pod delete a19305d2; container delete
//	  22  <- capture line 534  pod delete 9ab1d6d5; container delete
//	  23  <- capture line 624  node state f41e8e62 (kwok-node-2)
//	  24  <- capture line 667  node state f41e8e62 (kwok-node-2)
//	  25  <- capture line 739  node state f41e8e62 (kwok-node-2)
//	  26  <- capture line 875  node state f41e8e62 (kwok-node-2)
//	  27  <- capture line 921  node delete f41e8e62
//	  28  <- capture line 974  pod state c5ad95aa (pre-5984778487-wr85m); container of c5ad95aa
//	  29  <- capture line 975  pod delete c5ad95aa; container delete
//	  30  <- capture line 1054 node state 4edef4ef (kwok-node-2)
//	  31  <- capture line 1057 node state 4edef4ef (kwok-node-2)
//	run2.jsonl
//	   1  <- capture line 5    node state 36ee1190 (kwok-node-0)
//	   2  <- capture line 10   node state 4edef4ef (kwok-node-2)
//	   3  <- capture line 13   node state 70f60f5f (kwok-node-4)
//	   4  <- capture line 20   namespace state fda3fdb1 (kube-system)
//	   5  <- capture line 24   pod state 1f037c43 (pre-5984778487-6xxvb); container of 1f037c43
//	   6  <- capture line 58   node state 36ee1190 (kwok-node-0)
//	   7  <- capture line 61   node state 4edef4ef (kwok-node-2)
//	   8  <- capture line 63   node state 70f60f5f (kwok-node-4)
//	   9  <- capture line 117  node state 4edef4ef (kwok-node-2)
//	  10  <- capture line 120  node state 70f60f5f (kwok-node-4)
//	  11  <- capture line 122  node state 36ee1190 (kwok-node-0)
//	  12  <- capture line 162  statefulset delete bf011e05
//	  13  <- capture line 175  pod delete 16414d46; container delete
//	  14  <- capture line 181  deployment delete 746d8f2a
//	  15  <- capture line 182  replicaset delete 4f98dd5e
//	  16  <- capture line 211  pod delete 1f037c43; container delete
//	  17  <- capture line 398  node state 70f60f5f (kwok-node-4)
//	  18  <- capture line 400  node state 36ee1190 (kwok-node-0)
//	  19  <- capture line 402  node state 4edef4ef (kwok-node-2)
//	  20  <- capture line 412  node state 36ee1190 (kwok-node-0)
//	  21  <- capture line 414  node state 4edef4ef (kwok-node-2)
//	  22  <- capture line 416  node state 70f60f5f (kwok-node-4)
//	  23  <- capture line 426  node state 36ee1190 (kwok-node-0)
//	  24  <- capture line 428  node state 4edef4ef (kwok-node-2)
//	  25  <- capture line 430  node state 70f60f5f (kwok-node-4)
//	  26  <- capture line 436  node state 70f60f5f (kwok-node-4)
//	  27  <- capture line 438  node state 36ee1190 (kwok-node-0)
//	  28  <- capture line 440  node state 4edef4ef (kwok-node-2)
//	  29  <- capture line 447  node state 36ee1190 (kwok-node-0)
//	  30  <- capture line 449  node state 4edef4ef (kwok-node-2)
//	  31  <- capture line 451  node state 70f60f5f (kwok-node-4)
//	  32  <- capture line 454  node state 4edef4ef (kwok-node-2)
//	  33  <- capture line 460  node state 4edef4ef (kwok-node-2)
//	  34  <- capture line 463  node state 70f60f5f (kwok-node-4)
//	  35  <- capture line 465  node state 36ee1190 (kwok-node-0)
//	  36  <- capture line 466  node state 70f60f5f (kwok-node-4)
//	  37  <- capture line 472  node state 36ee1190 (kwok-node-0)
//	  38  <- capture line 480  node state 4edef4ef (kwok-node-2)
//	  39  <- capture line 484  node state 36ee1190 (kwok-node-0)
//	  40  <- capture line 487  node delete 36ee1190
//	  41  <- capture line 491  node delete 4edef4ef
//	  42  <- capture line 495  node delete 70f60f5f

func TestGolden(t *testing.T) {
	for _, name := range []string{"run1", "run2"} {
		t.Run(name, func(t *testing.T) {
			in := loadFixture(t, name+".jsonl")
			rs := translateAll(t, newTranslator(t), in, false)
			checkGolden(t, name+".golden.json", render(labels(t, in), in, rs))
		})
	}
}
