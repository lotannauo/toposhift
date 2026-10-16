package mcp

// Tool is a tool of the surface: its name and the sentences a client reads to
// choose it. A transport registers each tool with the input and output types of
// the [Source] method of the same name; every tool is read-only and idempotent.
type Tool struct {
	// Name is the name the client calls.
	Name string
	// Description tells a client what the tool answers and what to expect.
	Description string
	// Range says the tool reads a range of event time, and so runs only on a store
	// with range reads.
	Range bool
}

// Tools lists the tools in the order they are offered.
func Tools() []Tool {
	return []Tool{
		{
			Name: "blast_radius",
			Description: "Lists what a failing entity reaches over a time window, following the catalog's failure directions in reverse, with every path checked by interval intersection. " +
				"Each member says when it was reached, by which edges, and how each edge ended. The answer is derived and read at one token.",
			Range: true,
		},
		{
			Name: "what_changed",
			Description: "Diffs a scope of entities between two instants: what was created, deleted or changed, what came and went inside the window, and the source of every disappearance. " +
				"A change in a description is reported by key, and a key that is withheld is named. An entry naming an endpoint the scope does not name is withheld and counted. The answer is derived and read at one token.",
			Range: true,
		},
		{
			Name: "co_residents",
			Description: "Infers which pods shared a node, or a host through two nodes, with a pod over a time window. " +
				"A placement that cannot be told is listed as unattributed, never guessed. The answer is derived and read at one token.",
			Range: true,
		},
		{
			Name: "neighbors_at",
			Description: "Lists the edges of an entity at one instant, each only while both of its ends exist, with the entity at the other end of each. " +
				"An edge with a quarantined end is listed and marked. An edge to an endpoint the request did not name is withheld and counted. The answer is read at one token.",
		},
		{
			Name: "entity_history",
			Description: "Shows when an entity existed over a time window and why each span ended, with the stored records that say so as facts. " +
				"A payload is printed through a table of allowed keys, and a key that is withheld is named.",
		},
		{
			Name: "resolve_entity",
			Description: "Computes the id of an entity from exactly its identifying attributes, and with an instant says whether it exists then. " +
				"It never searches: a missing, extra or mistyped attribute is refused by name.",
		},
		{
			Name: "describe_schema",
			Description: "Describes the entity and relation types, how failure travels along each relation, how names and descriptions are read, and the words every answer uses. " +
				"It reads no store.",
		},
	}
}
