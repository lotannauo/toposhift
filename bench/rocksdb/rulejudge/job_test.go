package rulejudge

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// The bench workflow writes the job file and the judge reads it. The file is the
// runner's object joined with the rule's; the keys of the rule's are the tags of this
// package's Job beyond the embedded runner.Job, in order, so a field added to either
// fails here. The runner's object is checked by the runner's own test.
func TestJobFieldsAreTheWorkflows(t *testing.T) {
	t.Parallel()

	b, err := os.ReadFile("../../../.github/workflows/bench.yml")
	if err != nil {
		t.Fatal(err)
	}
	objs := regexp.MustCompile(`binary_sha256: \$binary_sha256\} \+ (\{engine: \$engine,[^}]*\})`).FindAllStringSubmatch(string(b), -1)
	if len(objs) != 1 {
		t.Fatalf("%d objects of the rule's fields joined to the runner's in the workflow, want 1", len(objs))
	}
	var workflow []string
	for _, m := range regexp.MustCompile(`(\w+): \$\w+`).FindAllStringSubmatch(objs[0][1], -1) {
		workflow = append(workflow, m[1])
	}
	var tags []string
	typ := reflect.TypeFor[Job]()
	for i := range typ.NumField() {
		if f := typ.Field(i); !f.Anonymous {
			tags = append(tags, strings.Split(f.Tag.Get("json"), ",")[0])
		}
	}
	if !reflect.DeepEqual(workflow, tags) {
		t.Errorf("the workflow writes\n  %v\nand the judge reads\n  %v", workflow, tags)
	}
}
