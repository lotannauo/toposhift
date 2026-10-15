// Package mcphttp serves the tools of internal/mcp to MCP clients over
// streamable HTTP. It is the only package of toposhift that imports the MCP
// SDK: the tools, their input and output types and every rule an answer follows
// live in internal/mcp, which knows nothing of the protocol, and this package
// registers them as they are.
//
// # What it serves
//
// A [Server] answers POST requests, one JSON-RPC message each, with a JSON
// body. It keeps no session, because every call carries the token it reads at
// (asof) and every answer names it. It offers each tool of the core and nothing
// else: no prompts, no resources, no subscriptions.
//
// # Where it may be reached
//
// The handler is meant for a loopback listener, and it does not rely on the
// listener alone. A request is refused with 403 when:
//
//   - the connection's local address is not a loopback address, whatever the
//     server was configured to listen on;
//   - the connection is on loopback but the Host header names another host (the
//     SDK's protection against DNS rebinding);
//   - a browser says the request comes from another site (the standard
//     library's cross-origin protection).
//
// A body above [MaxBodyBytes] is refused with 413, and a POST that is not JSON
// with 415. Loopback is not authentication: any process on the machine may
// call the tools. A caller that cannot promise a loopback listener leaves the
// handler unmounted.
//
// # Before the store is there, and after
//
// The handler can be mounted before the store opens. Until [Server.Attach],
// every tool that reads the store answers with the sentence given to
// [Server.Unavailable], and the tools that read none (describe_schema, and
// resolve_entity without an instant) answer as always. [Server.Drain] ends the
// service: it refuses new calls, ends the ones running, and returns when they
// have all returned. A caller that gets an error from it must not close the
// store.
package mcphttp
