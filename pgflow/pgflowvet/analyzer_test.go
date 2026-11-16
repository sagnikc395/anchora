package pgflowvet_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/sagnikc395/anchora/pgflow/pgflowvet"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), pgflowvet.Analyzer, "a")
}
