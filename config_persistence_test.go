package main

import (
	"path/filepath"
	"testing"
)

func TestSaveConfigFilePersistsAuthAndSources(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "trifle", "config.yaml")

	cfg := &cliConfig{
		Source: "project-main",
		Auth: &authConfig{
			URL:            "https://app.trifle.io",
			UserToken:      "trf_uat_test",
			Email:          "user@example.com",
			OrganizationID: "org-123",
			UserID:         "user-123",
		},
		Sources: map[string]sourceConfig{
			"project-main": {
				Driver:     "api",
				URL:        "https://app.trifle.io",
				Token:      "project-token",
				SourceType: "project",
				SourceID:   "proj-123",
			},
		},
	}

	if err := saveConfigFile(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	loaded, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if loaded.Source != "project-main" {
		t.Fatalf("unexpected active source: %s", loaded.Source)
	}
	if loaded.Auth == nil || loaded.Auth.UserToken != "trf_uat_test" {
		t.Fatalf("missing auth token: %#v", loaded.Auth)
	}
	source, ok := loaded.Sources["project-main"]
	if !ok {
		t.Fatalf("saved source missing")
	}
	if source.SourceType != "project" || source.SourceID != "proj-123" {
		t.Fatalf("saved source metadata mismatch: %#v", source)
	}
}
