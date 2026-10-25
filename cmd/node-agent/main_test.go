package main

import (
	"testing"
)

func TestLoadAgentConfigSupportsMultipleInstances(t *testing.T) {
	values := map[string]string{
		"ORCHESTRATOR_NODE_ID":           "worker-02",
		"ORCHESTRATOR_LISTEN_ADDR":       ":9002",
		"ORCHESTRATOR_NODE_ADDRESS":      "127.0.0.1:9002",
		"ORCHESTRATOR_NODE_CPU_MILLIS":   "2000",
		"ORCHESTRATOR_NODE_MEMORY_MB":    "4096",
		"ORCHESTRATOR_CONTROL_PLANE_URL": "https://127.0.0.1:8080",
		"ORCHESTRATOR_NODE_TOKEN":        "test-token",
	}
	config, err := loadAgentConfig(func(name string) string {
		return values[name]
	})
	if err != nil {
		t.Fatalf("load node-agent configuration: %v", err)
	}
	if config.ID != "worker-02" ||
		config.ListenAddress != ":9002" ||
		config.Address != "127.0.0.1:9002" ||
		config.CPUCapacity != 2000 ||
		config.MemoryCapacity != 4096 {
		t.Fatalf("configuration = %+v; did not preserve worker-02 settings", config)
	}
}

func TestLoadAgentConfigKeepsSingleAgentDevelopmentDefaults(t *testing.T) {
	values := map[string]string{
		"ORCHESTRATOR_NODE_ADDRESS":      "127.0.0.1:9001",
		"ORCHESTRATOR_CONTROL_PLANE_URL": "https://127.0.0.1:8080",
		"ORCHESTRATOR_NODE_TOKEN":        "test-token",
	}
	config, err := loadAgentConfig(func(name string) string {
		return values[name]
	})
	if err != nil {
		t.Fatalf("load default node-agent configuration: %v", err)
	}
	if config.ID != "worker-01" ||
		config.ListenAddress != ":9001" ||
		config.CPUCapacity != 4000 ||
		config.MemoryCapacity != 8192 {
		t.Fatalf("default configuration = %+v", config)
	}
}

func TestLoadAgentConfigRequiresSecretsAndReachableAddress(t *testing.T) {
	base := map[string]string{
		"ORCHESTRATOR_NODE_ADDRESS":      "127.0.0.1:9001",
		"ORCHESTRATOR_CONTROL_PLANE_URL": "https://127.0.0.1:8080",
		"ORCHESTRATOR_NODE_TOKEN":        "test-token",
	}
	for _, tc := range []struct {
		name   string
		change func(map[string]string)
	}{
		{name: "token", change: func(values map[string]string) { values["ORCHESTRATOR_NODE_TOKEN"] = "" }},
		{name: "advertised wildcard", change: func(values map[string]string) { values["ORCHESTRATOR_NODE_ADDRESS"] = ":9001" }},
		{name: "advertised unspecified IP", change: func(values map[string]string) { values["ORCHESTRATOR_NODE_ADDRESS"] = "0.0.0.0:9001" }},
		{name: "zero CPU", change: func(values map[string]string) { values["ORCHESTRATOR_NODE_CPU_MILLIS"] = "0" }},
		{name: "non HTTPS control plane", change: func(values map[string]string) { values["ORCHESTRATOR_CONTROL_PLANE_URL"] = "http://127.0.0.1:8080" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := make(map[string]string, len(base))
			for key, value := range base {
				values[key] = value
			}
			tc.change(values)
			if _, err := loadAgentConfig(func(name string) string { return values[name] }); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}
