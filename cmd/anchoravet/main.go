// Command anchoravet is the standalone entry point for the anchora workflow
// determinism linter. Use it through go vet:
//
//	go vet -vettool=$(go build -o anchoravet ./cmd/anchoravet) ./...
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/sagnikc395/anchora/anchora/anchoravet"
)

func main() { singlechecker.Main(anchoravet.Analyzer) }
