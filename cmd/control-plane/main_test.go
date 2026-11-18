package main

import "testing"

func TestLoadAuthTokensRequiresDistinctNonEmptyValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  map[string]string
		wantErr bool
	}{
		{
			name: "distinct tokens",
			values: map[string]string{
				"ORCHESTRATOR_NODE_TOKEN":     "node-secret",
				"ORCHESTRATOR_OPERATOR_TOKEN": "operator-secret",
			},
		},
		{
			name: "missing node token",
			values: map[string]string{
				"ORCHESTRATOR_OPERATOR_TOKEN": "operator-secret",
			},
			wantErr: true,
		},
		{
			name: "missing operator token",
			values: map[string]string{
				"ORCHESTRATOR_NODE_TOKEN": "node-secret",
			},
			wantErr: true,
		},
		{
			name: "same token",
			values: map[string]string{
				"ORCHESTRATOR_NODE_TOKEN":     "same-secret",
				"ORCHESTRATOR_OPERATOR_TOKEN": "same-secret",
			},
			wantErr: true,
		},
		{
			name: "tokens equal after trimming",
			values: map[string]string{
				"ORCHESTRATOR_NODE_TOKEN":     " shared-secret ",
				"ORCHESTRATOR_OPERATOR_TOKEN": "shared-secret",
			},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeToken, operatorToken, err := loadAuthTokens(func(name string) string {
				return tc.values[name]
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("loadAuthTokens() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && (nodeToken != "node-secret" || operatorToken != "operator-secret") {
				t.Fatalf("tokens = (%q, %q); want configured distinct tokens", nodeToken, operatorToken)
			}
		})
	}
}
