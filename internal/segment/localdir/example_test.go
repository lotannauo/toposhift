package localdir_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
)

// A segment is written whole or not at all, never replaced, and listed in byte
// order of name. Its digest is how a writer settles a Put whose outcome it
// does not know.
func Example() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "segments-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(root)

	store, err := localdir.New(filepath.Join(root, "store"))
	if err != nil {
		log.Fatal(err)
	}

	// The planned names: activity/<first seq>-<last seq>-<sha256>.parquet.
	body := "records 1 to 1000"
	sum := sha256.Sum256([]byte(body))
	name := fmt.Sprintf("activity/%020d-%020d-%x.parquet", 1, 1000, sum)
	later := fmt.Sprintf("activity/%020d-%020d-%x.parquet", 1001, 2000, sha256.Sum256([]byte("records 1001 to 2000")))

	info, err := store.Put(ctx, name, strings.NewReader(body), int64(len(body)))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stored", info.Size, "bytes, digest matches:", info.SHA256 == sum)
	if _, err := store.Put(ctx, later, strings.NewReader("records 1001 to 2000"), -1); err != nil {
		log.Fatal(err)
	}

	// The result of a Put was lost: retry. A name that exists says the earlier
	// attempt may have succeeded; the digest settles it.
	_, err = store.Put(ctx, name, strings.NewReader(body), int64(len(body)))
	fmt.Println("retry is refused as existing:", errors.Is(err, segment.ErrExists))
	stat, err := store.Stat(ctx, name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("the stored segment is ours:", stat.SHA256 == sum)

	for info, err := range store.List(ctx, "activity/") {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(info.Name[:len("activity/")+20], info.Size)
	}

	rc, err := store.Open(ctx, name)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", data)

	// Deleting twice is fine.
	for range 2 {
		if err := store.Delete(ctx, name); err != nil {
			log.Fatal(err)
		}
	}
	_, err = store.Stat(ctx, name)
	fmt.Println("after Delete:", errors.Is(err, segment.ErrNotFound))

	// Output:
	// stored 17 bytes, digest matches: true
	// retry is refused as existing: true
	// the stored segment is ours: true
	// activity/00000000000000000001 17
	// activity/00000000000000001001 20
	// records 1 to 1000
	// after Delete: true
}
