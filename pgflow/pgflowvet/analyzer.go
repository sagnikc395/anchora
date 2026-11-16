// Package pgflowvet is a go/analysis linter that flags nondeterministic code
// inside workflow functions: code that would replay differently than it ran
// first. It catches the mistake before deploy instead of after a crash.
//
// A function whose first parameter is *pgflow.Context is treated as workflow
// code. Inside such functions (including nested function literals) it flags:
//
//   - time.Now, time.Since, time.Until, time.Sleep, time.After, time.Tick,
//     time.NewTimer, time.NewTicker
//   - any call into math/rand or math/rand/v2
//   - any call into net/http
//   - os.Getenv, os.LookupEnv, os.Environ, os.Open, os.Create, os.ReadFile,
//     os.WriteFile
//   - any call into github.com/google/uuid
//   - bare go statements
//
// Run it with:
//
//	go vet -vettool=$(go build -o pgflowvet ./cmd/pgflowvet) ./...
package pgflowvet

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// pgflowPath is the import path of the engine. Functions taking a
// *pgflow.Context from this package are workflow code.
const pgflowPath = "github.com/sagnikc395/anchora/pgflow"

const doc = `report nondeterministic code in pgflow workflow functions

Workflow functions must make the same sequence of engine calls on every
execution, because a recovered workflow replays its recorded history from the
top. This analyzer flags sources of nondeterminism inside functions whose
first parameter is *pgflow.Context: direct clock reads, randomness, network
and environment access, and goroutine spawns. Use the pgflow engine's
recorded replacements (pgflow.Now, pgflow.Random, pgflow.UUID, pgflow.Step)
instead.`

// Analyzer flags nondeterministic operations in workflow functions.
var Analyzer = &analysis.Analyzer{
	Name: "pgflowvet",
	Doc:  doc,
	Run:  run,
}

// forbidden lists the banned calls: "*" means every function of the package,
// otherwise the set of function names.
var forbidden = map[string]map[string]bool{
	"time": {
		"Now": true, "Since": true, "Until": true, "Sleep": true,
		"After": true, "AfterFunc": true, "Tick": true,
		"NewTimer": true, "NewTicker": true,
	},
	"math/rand":              {"*": true},
	"math/rand/v2":           {"*": true},
	"net/http":               {"*": true},
	"github.com/google/uuid": {"*": true},
	"os": {
		"Getenv": true, "LookupEnv": true, "Environ": true,
		"Open": true, "Create": true, "ReadFile": true, "WriteFile": true,
	},
}

func run(pass *analysis.Pass) (any, error) {
	for _, fn := range workflowFunctions(pass) {
		ast.Inspect(fnBody(fn), func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.GoStmt:
				pass.Report(analysis.Diagnostic{
					Pos: n.Pos(),
					Message: "bare goroutine in workflow code: engine calls are positional; " +
						"use pgflow.Go so the slot is reserved in program order",
				})
				return true
			case *ast.CallExpr:
				call, ok := n.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				ident, ok := call.X.(*ast.Ident)
				if !ok {
					return true // a method chain, not a package selector
				}
				pkgName, ok := pass.TypesInfo.ObjectOf(ident).(*types.PkgName)
				if !ok {
					return true
				}
				path := pkgName.Imported().Path()
				names, banned := forbidden[path]
				if !banned {
					return true
				}
				if names[call.Sel.Name] || names["*"] {
					pass.Report(analysis.Diagnostic{
						Pos: n.Pos(),
						Message: "nondeterministic call " + path + "." + call.Sel.Name +
							" in workflow code; use the recorded pgflow replacement or wrap it in a pgflow.Step",
					})
				}
				return true
			case *ast.FuncLit:
				// A step body may be nondeterministic: it runs under the
				// engine's retry, timeout, and checkpoint machinery.
				if isStepSignature(pass, n.Type) {
					return false
				}
				return true
			}
			return true
		})
	}
	return nil, nil
}

// workflowFunctions returns every function whose first parameter is
// *pgflow.Context: both top-level declarations and function literals.
func workflowFunctions(pass *analysis.Pass) []ast.Node {
	var found []ast.Node
	for _, file := range pass.Files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if isWorkflowSignature(pass, n.Type) {
					found = append(found, n)
				}
				return true
			case *ast.FuncLit:
				if isWorkflowSignature(pass, n.Type) {
					found = append(found, n)
				}
				return true
			}
			return true
		})
	}
	return found
}

func isWorkflowSignature(pass *analysis.Pass, sig *ast.FuncType) bool {
	if sig == nil || sig.Params == nil || len(sig.Params.List) == 0 {
		return false
	}
	star, ok := sig.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	pkg, ok := pass.TypesInfo.ObjectOf(ident).(*types.PkgName)
	if !ok {
		return false
	}
	return pkg.Imported().Path() == pgflowPath && sel.Sel.Name == "Context"
}

// isStepSignature reports whether a function literal is a step body, whose
// first parameter is pgflow.StepCtx. Step bodies are exempt.
func isStepSignature(pass *analysis.Pass, sig *ast.FuncType) bool {
	if sig == nil || sig.Params == nil || len(sig.Params.List) == 0 {
		return false
	}
	sel, ok := sig.Params.List[0].Type.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	pkg, ok := pass.TypesInfo.ObjectOf(ident).(*types.PkgName)
	if !ok {
		return false
	}
	return pkg.Imported().Path() == pgflowPath && sel.Sel.Name == "StepCtx"
}

// fnBody returns a function node's body, whether it is a declaration or a
// literal.
func fnBody(fn ast.Node) ast.Node {
	switch f := fn.(type) {
	case *ast.FuncDecl:
		return f.Body
	case *ast.FuncLit:
		return f.Body
	}
	return nil
}
