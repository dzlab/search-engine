package expression

import (
	"fmt"

	"github.com/expr-lang/expr"
)

// ComputedField represents the definition of a field whose value is
// derived from an expression.
// This struct should ideally be defined in a types package (e.g., ../types).
// For the purpose of this file, we define a minimal version.
type ComputedField struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
	// Add other fields as necessary, e.g., Type, Description
}

// EvaluateComputedField evaluates a given ComputedField's expression
// using the provided data map as context.
// It returns the computed value and an error if evaluation fails.
func EvaluateComputedField(field ComputedField, data map[string]interface{}) (interface{}, error) {
	// Compile the expression. This step checks for syntax errors.
	program, err := expr.Compile(field.Expression, expr.Env(data))
	if err != nil {
		return nil, fmt.Errorf("failed to compile expression for field '%s': %w", field.Name, err)
	}

	// Run the compiled expression with the provided data context.
	output, err := expr.Run(program, data)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression for field '%s': %w", field.Name, err)
	}

	return output, nil
}
