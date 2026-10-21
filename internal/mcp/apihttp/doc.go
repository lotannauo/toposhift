// Package apihttp serves temporal subgraphs as JSON over HTTP, for a client on
// the same machine: one route for each of four questions, each answered with the
// wire format's JSON twin of the subgraph, labelled derived, with every entity
// named as the MCP tools name it and every endpoint the request did not name
// withheld and counted. It is the HTTP face of the subgraph methods of
// internal/mcp, and it imports no MCP SDK and no package that does.
//
// # Routes
//
// Each route takes POST with a JSON body of at most [MaxBodyBytes], sent as
// application/json, and answers 200 with one JSON object, [mcp.SubgraphAnswer]:
//
//	POST /api/v1/subgraph/scope            {"scope": [id, ...], "from", "to", "asof"?}
//	POST /api/v1/subgraph/co_residents     {"pod", "from", "to", "node_only"?, "asof"?}
//	POST /api/v1/subgraph/service_context  {"service", "from", "to", "node_only"?, "asof"?}
//	POST /api/v1/subgraph/blast_radius     {"entity", "from", "to", "depth"?, "asof"?}
//
// The fields, their limits and their sentences are those of the MCP tool that
// asks the same question; see the types the methods of [mcp.Source] take. The
// body is read and decoded before the call takes a slot, so a slow sender holds
// a goroutine and never a slot, and the slot is released before the first byte
// of the answer is written, so a slow reader never holds one either. A field the
// route does not take, or data after the object, is refused.
//
// # The source
//
// The routes run on the source an [mcp.Source] provider gives them, which the MCP
// handler of the same process also runs on, so both share its two slots, its
// deadline, its sentence before a store is attached and its drain. A call that
// finds no store gets 503 with that sentence, and a drained one 503 shutting
// down.
//
// # Where it may be reached
//
// The handler is meant for a loopback listener, and it does not rely on the
// listener alone. A request is refused with 403 when the connection's local
// address is not a loopback address, when the Host header is not localhost or a
// loopback address (a DNS rebinding), or when a browser says the request comes
// from another site. No CORS header is ever written, so a browser page can use
// the routes only from the origin that serves them. Every response says
// Cache-Control: no-store, X-Content-Type-Options: nosniff and
// Referrer-Policy: no-referrer. Loopback is not authentication: any process on
// the machine, and anything that reaches a forwarded loopback port, can call
// the routes. A caller that cannot promise a loopback listener leaves the
// handler unmounted.
//
// [API.Routes] is the same routes with the request checks and the headers and with
// none of the three protections above, for a caller that puts its own in front of
// it and so owns that decision.
//
// # Errors
//
// A refusal is a JSON object, {"error": {"code": ..., "message": ...}}, with the
// status its cause calls for (see [Handler]). The message is the sentence the
// MCP tools give for the same cause, or a fixed one that echoes nothing the
// client sent; it never carries the error the sentence was made from, which may
// hold ids the sentence withholds. A request whose client has gone away is
// answered with nothing.
package apihttp
