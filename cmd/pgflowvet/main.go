// Command pgflowvet is the standalone entry point for the pgflow workflow
// determinism linter. Use it through go vet:
//
//	go vet -vettool=$(go build -o pgflowvet ./cmd/pgflowvet) ./...
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/sagnikc395/anchora/pgflow/pgflowvet"
)

func main() { singlechecker.Main(pgflowvet.Analyzer) }
