package vmengine

import (
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
)

// Engine is the reads of the spike's plans answered by the server over HTTP: an
// engine.Engine whose Neighbors, NeighborsBatch and Alive are the rewritten instant
// queries (one term per producer, each through its own window, with nocache=1) and whose
// Window and reads as of an old token are refused with the typed error of the capability
// table. It is read only: the store is written by [Loader].
type Engine struct {
	*vmquery.Engine
	client *Client
}

// New returns the engine over a server. lastSeq is the sequence number of the last record
// the store was written from.
func New(c *Client, reg *vmmodel.Registry, lastSeq uint64) *Engine {
	return &Engine{Engine: vmquery.NewEngine(reg, c, lastSeq), client: c}
}

// Client is the server the engine reads.
func (e *Engine) Client() *Client { return e.client }
