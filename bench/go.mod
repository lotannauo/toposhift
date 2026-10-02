module github.com/lotannauo/toposhift/bench

go 1.27.1

require (
	github.com/lotannauo/toposhift v0.0.0-00010101000000-000000000000
	pgregory.net/rapid v1.3.0
)

// The root module is developed alongside this one. This replace is why bench
// is a separate module: the root go.mod must never carry one, or
// `go install github.com/lotannauo/toposhift/cmd/toposhift@latest` would fail.
replace github.com/lotannauo/toposhift => ../
