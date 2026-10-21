package main

import (
	"github.com/lotannauo/toposhift/internal/mcp/apihttp"
	"github.com/lotannauo/toposhift/internal/mcp/mcphttp"
	"github.com/lotannauo/toposhift/internal/server"
)

// apiPrefix is where the routes of the temporal subgraph API are, for the lines
// that say they are served or off.
const apiPrefix = "/api/v1/subgraph/"

// apiRoutes are the routes of the temporal subgraph API, for a loopback listener
// only. They run on the source of reads, so they share the tools' slots, deadline,
// answer before a store is attached, and drain: no second attach, and no second
// drain.
func apiRoutes(reads *mcphttp.Server) []server.Route {
	h := apihttp.New(reads).Handler()
	var routes []server.Route
	for _, p := range apihttp.Paths() {
		routes = append(routes, server.Route{Pattern: p, Handler: h})
	}
	return routes
}
