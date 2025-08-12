package expression

import (
	"fmt"
	"testing"
)

func TestEvaluateComputedField(t *testing.T) {
	tests := []struct {
		name        string
		field       ComputedField
		data        map[string]interface{}
		expected    interface{}
		expectError bool
	}{
		{
			name: "Simple Numeric Expression",
			field: ComputedField{
				Name:       "total",
				Expression: "price * quantity",
			},
			data: map[string]interface{}{
				"price":    10,
				"quantity": 5,
			},
			expected:    50,
			expectError: false,
		},
		{
			name: "Boolean Expression",
			field: ComputedField{
				Name:       "is_eligible",
				Expression: "age >= 18 and has_license",
			},
			data: map[string]interface{}{
				"age":         20,
				"has_license": true,
			},
			expected:    true,
			expectError: false,
		},
		{
			name: "String Concatenation",
			field: ComputedField{
				Name:       "full_name",
				Expression: "first_name + ' ' + last_name",
			},
			data: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
			},
			expected:    "John Doe",
			expectError: false,
		},
		{
			name: "Expression with Missing Variable",
			field: ComputedField{
				Name:       "missing_data",
				Expression: "value1 + value2",
			},
			data: map[string]interface{}{
				"value1": 10,
			},
			expected:    nil,  // expr-lang/expr returns nil for undefined variables in arithmetic ops
			expectError: true, // It errors if the operation requires all operands to be defined and non-nil.
		},
		{
			name: "Invalid Expression Syntax",
			field: ComputedField{
				Name:       "invalid_syntax",
				Expression: "10 + * 5",
			},
			data:        map[string]interface{}{},
			expected:    nil,
			expectError: true,
		},
		{
			name: "Return Original Value (Self-Reference)",
			field: ComputedField{
				Name:       "original_value",
				Expression: "data_point", // If 'data_point' is expected to be in the context
			},
			data: map[string]interface{}{
				"data_point": "some_value",
			},
			expected:    "some_value",
			expectError: false,
		},
		{
			name: "Empty Expression",
			field: ComputedField{
				Name:       "empty_expr",
				Expression: "",
			},
			data:        map[string]interface{}{},
			expected:    nil,  // Empty expression might compile to nil or an empty string, depends on expr lib behavior
			expectError: true, // expr-lang/expr considers empty string an invalid expression.
		},
		{
			name: "Function Call in Expression",
			field: ComputedField{
				Name:       "length_of_string",
				Expression: `len(text)`,
			},
			data: map[string]interface{}{
				"text": "hello",
			},
			expected:    5,
			expectError: false,
		},
		{
			name: "Complex Conditionals",
			field: ComputedField{
				Name:       "discounted_price",
				Expression: `if quantity > 10 then price * 0.9 else price`,
			},
			data: map[string]interface{}{
				"quantity": 12,
				"price":    100,
			},
			expected:    90.0,
			expectError: false,
		},
		{
			name: "Complex Conditionals - No Discount",
			field: ComputedField{
				Name:       "discounted_price",
				Expression: `if quantity > 10 then price * 0.9 else price`,
			},
			data: map[string]interface{}{
				"quantity": 5,
				"price":    100,
			},
			expected:    100,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvaluateComputedField(tt.field, tt.data)

			if tt.expectError {
				if err == nil {
					t.Errorf("EvaluateComputedField() expected an error, but got none. Result: %v", got)
				}
			} else {
				if err != nil {
					t.Errorf("EvaluateComputedField() unexpected error: %v", err)
				}
				if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.expected) {
					t.Errorf("EvaluateComputedField() got = %v (%T), want %v (%T)", got, got, tt.expected, tt.expected)
				}
			}
		})
	}
}
