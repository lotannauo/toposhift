package activity_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

func Example() {
	resolver := identity.NewResolver(catalog.Default())
	pod, _ := resolver.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: "pod-1"}})
	node, _ := resolver.Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: "node-1"}})
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	records := []store.Record{
		{Layer: catalog.L2, Subject: store.EntitySubject(pod.Fingerprint()), Producer: "k8s", EventTime: at, Seq: 1, Kind: lifecycle.Observe},
		{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod.Fingerprint(), node.Fingerprint(), catalog.ScheduledOn), Producer: "k8s",
			EventTime: at, Seq: 2, Kind: lifecycle.Observe, TTL: time.Minute,
		},
		{Layer: catalog.L2, Subject: store.EntitySubject(pod.Fingerprint()), Producer: "k8s", EventTime: at.Add(time.Hour), Seq: 3, Kind: lifecycle.Delete},
	}

	// Write the records, in ascending Seq, to any io.Writer.
	var file bytes.Buffer
	w, err := activity.NewWriter(&file, activity.WriterOptions{})
	if err != nil {
		panic(err)
	}
	for _, r := range records {
		if err := w.Write(r); err != nil {
			panic(err)
		}
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	// Read them back from any io.ReaderAt, one at a time.
	r, err := activity.NewReader(bytes.NewReader(file.Bytes()), int64(file.Len()), activity.ReaderOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d records, seq %d to %d\n", r.Info().Records, r.Info().MinSeq, r.Info().MaxSeq)
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(rec.Seq, rec.Kind)
	}

	// Output:
	// 3 records, seq 1 to 3
	// 1 observe
	// 2 observe
	// 3 delete
}
