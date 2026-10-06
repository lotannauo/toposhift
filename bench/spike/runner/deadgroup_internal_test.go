package runner

import "testing"

// A group of reads past the end is refused if any read of it finds an entity alive, not
// only if every one does: a single node whose refresh has not lapsed means the instant is
// too early for the plan to say what a dead entity costs.
func TestADeadGroupWithOneEntityAliveIsRefused(t *testing.T) {
	t.Parallel()

	if err := checkDeadGroup(GroupInfo{Group: "node exists hot-records alive", Age: AgeDead, Queries: 25}); err != nil {
		t.Errorf("a group in which every entity is dead: %v", err)
	}
	for _, alive := range []int{1, 12, 25} {
		if err := checkDeadGroup(GroupInfo{Group: "node exists hot-records alive", Age: AgeDead, Queries: 25, NonEmpty: alive}); err == nil {
			t.Errorf("%d of 25 reads found an entity alive and the group was accepted", alive)
		}
	}
}
