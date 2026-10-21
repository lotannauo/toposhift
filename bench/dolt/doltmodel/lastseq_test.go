package doltmodel_test

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// ahead is a model whose LastSeq rises before the batch, and the commit that holds it,
// is complete: the defect of a writer that publishes the token before its SQL commit and
// its version-control commit have returned. A read pinned to that token sees less than it
// names.
type ahead struct {
	*doltmodel.Model
	pending atomic.Uint64
}

func (a *ahead) Write(batch []engine.Record) error {
	if len(batch) > 0 {
		a.pending.Store(batch[len(batch)-1].Seq)
		runtime.Gosched()
		time.Sleep(300 * time.Microsecond)
	}
	return a.Model.Write(batch)
}

func (a *ahead) LastSeq() uint64 { return max(a.Model.LastSeq(), a.pending.Load()) }

// TestLastSeqRaisedBeforeTheCommitsReturnIsCaught runs the concurrent-read check on a model
// that does so. Whether a tear shows depends on how the goroutines interleave, which a busy
// machine changes; the check is built to show it almost every time, and a few attempts make
// a miss vanishingly unlikely without hiding a check that cannot see it at all.
func TestLastSeqRaisedBeforeTheCommitsReturnIsCaught(t *testing.T) {
	var err error
	for range 5 {
		if err = conformance.CheckConcurrentReads(&ahead{Model: doltmodel.New()}); errors.Is(err, conformance.ErrTorn) {
			return
		}
	}
	t.Fatalf("a model that raises LastSeq before the batch is visible: err = %v, want ErrTorn", err)
}
