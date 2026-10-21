package mcphttp

import (
	"context"
	"fmt"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	core "github.com/lotannauo/toposhift/internal/mcp"
)

// register offers every tool of the core, each with the input and output types
// of the [core.Source] method of the same name. It fails if the core has a tool
// this package does not know, or this package knows one the core lacks, so a new
// tool cannot be added to one side alone.
func (s *Server) register() error {
	toolsByName := map[string]func(core.Tool){
		"blast_radius": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.BlastRadiusInput, core.BlastRadiusOutput] { return c.BlastRadius })
		},
		"what_changed": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.WhatChangedInput, core.WhatChangedOutput] { return c.WhatChanged })
		},
		"co_residents": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.CoResidentsInput, core.CoResidentsOutput] { return c.CoResidents })
		},
		"service_context": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.ServiceContextInput, core.ServiceContextOutput] {
				return c.ServiceContext
			})
		},
		"neighbors_at": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.NeighborsAtInput, core.NeighborsAtOutput] { return c.NeighborsAt })
		},
		"entity_history": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.EntityHistoryInput, core.EntityHistoryOutput] { return c.EntityHistory })
		},
		"find_entities": func(t core.Tool) {
			add(s, t, always, func(c *core.Source) call[core.FindEntitiesInput, core.FindEntitiesOutput] { return c.FindEntities })
		},
		"resolve_entity": func(t core.Tool) {
			// Without an instant no store is read: an id is a hash of the attributes.
			addWithSchema(s, t, resolveEntitySchema, func(in core.ResolveEntityInput) bool { return in.At != "" },
				func(c *core.Source) call[core.ResolveEntityInput, core.ResolveEntityOutput] { return c.ResolveEntity })
		},
		"describe_schema": func(t core.Tool) {
			add(s, t, func(core.DescribeSchemaInput) bool { return false },
				func(c *core.Source) call[core.DescribeSchemaInput, core.DescribeSchemaOutput] {
					return c.DescribeSchema
				})
		},
	}
	seen := map[string]bool{}
	for _, t := range core.Tools() {
		reg, ok := toolsByName[t.Name]
		if !ok {
			return fmt.Errorf("mcphttp: the core has a tool %q that is not registered here", t.Name)
		}
		reg(t)
		seen[t.Name] = true
	}
	for name := range toolsByName {
		if !seen[name] {
			return fmt.Errorf("mcphttp: the tool %q is registered here and the core lacks it", name)
		}
	}
	return nil
}

// call is a tool of the core: a method of a [core.Source].
type call[In, Out any] func(context.Context, In) (Out, error)

func always[In any](In) bool { return true }

// add registers one tool. readsStore says, from the input, whether the call
// reads the store; a call that does gets the sentence of the gate while no store
// is attached. A failure of the tool is returned as it is: the core's error is
// the sentence the client reads, and the SDK carries it as a tool error, not a
// protocol error.
func add[In, Out any](s *Server, t core.Tool, readsStore func(In) bool, bind func(*core.Source) call[In, Out]) {
	addWithSchema(s, t, nil, readsStore, bind)
}

// addWithSchema is add with the input schema given, for an input type whose
// inferred schema is not the one its JSON takes. A nil schema is inferred.
func addWithSchema[In, Out any](s *Server, t core.Tool, schema any, readsStore func(In) bool, bind func(*core.Source) call[In, Out]) {
	off := false
	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        t.Name,
		Description: t.Description,
		InputSchema: schema,
		Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: &off},
	}, func(ctx context.Context, _ *sdk.CallToolRequest, in In) (*sdk.CallToolResult, Out, error) {
		var zero Out
		src, err := s.Source(readsStore(in))
		if err != nil {
			return nil, zero, err
		}
		out, err := bind(src)(ctx, in)
		if err != nil {
			return nil, zero, err
		}
		return nil, out, nil
	})
}

// resolveEntitySchema is the input schema of resolve_entity. It is written out
// because the type of its attributes, raw JSON values, is inferred as arrays of
// bytes, which no client would send. An attribute is a string, which is also how
// a time is written, or an integer. A test checks its properties against the
// fields of [core.ResolveEntityInput].
var resolveEntitySchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"type", "attributes"},
	"properties": map[string]any{
		"type": map[string]any{
			"type":        "string",
			"description": "The entity type, such as host or k8s.pod. describe_schema lists the types.",
		},
		"attributes": map[string]any{
			"type":                 "object",
			"description":          "Exactly the identifying attributes of the type, by name: a string as a string, an integer as an integer, a time as an RFC 3339 string. Nothing is searched, trimmed or case-folded.",
			"additionalProperties": map[string]any{"type": []string{"string", "integer"}},
		},
		"at": map[string]any{
			"type":        "string",
			"description": "An instant, RFC 3339 with a zone. With it the answer says whether the entity exists then.",
		},
		"asof": map[string]any{
			"type":        "string",
			"description": "A token from an earlier answer, to read the same world again. Only with at.",
		},
	},
}
