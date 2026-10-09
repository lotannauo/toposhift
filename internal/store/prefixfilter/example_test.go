package prefixfilter_test

import (
	"fmt"
	"slices"

	"github.com/lotannauo/toposhift/internal/store/prefixfilter"
)

func ExampleBuild() {
	known := [][]byte{
		[]byte("entity-a/layer-1/out"),
		[]byte("entity-a/layer-1/in"),
		[]byte("entity-b/layer-1/out"),
	}
	f, err := prefixfilter.Build(slices.Values(known))
	if err != nil {
		panic(err)
	}

	// A prefix that was built in is always reported present.
	fmt.Println(f.Contains([]byte("entity-a/layer-1/in")))
	// An absent prefix is reported absent except for about one in 256, and this
	// one is not among them, so the caller may skip the read.
	fmt.Println(f.Contains([]byte("entity-c/layer-1/out")))
	fmt.Println(f.Len())
	// Output:
	// true
	// false
	// 3
}
