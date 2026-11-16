package anchoravet_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/sagnikc395/anchora/anchora/anchoravet"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), anchoravet.Analyzer, "a")
}
