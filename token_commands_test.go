package main

import "testing"

func TestSaveOrganizationTokenGlobally(t *testing.T) {
	t.Parallel()

	cfg := &cliConfig{}
	savedSource, err := saveOrganizationToken(
		cfg,
		"https://app.trifle.io/",
		" global-token ",
		"",
		"",
		"",
		true,
	)
	if err != nil {
		t.Fatalf("save organization token: %v", err)
	}
	if savedSource != "" {
		t.Fatalf("saved source = %q, want empty", savedSource)
	}
	if cfg.Auth == nil || cfg.Auth.UserToken != "global-token" {
		t.Fatalf("global token was not saved: %#v", cfg.Auth)
	}
	if cfg.Auth.URL != "https://app.trifle.io" {
		t.Fatalf("auth URL = %q", cfg.Auth.URL)
	}
}

func TestSaveOrganizationTokenCreatesSourceWithoutDuplicatingToken(t *testing.T) {
	t.Parallel()

	cfg := &cliConfig{
		Sources: map[string]sourceConfig{
			"production": {
				Token:   "legacy-source-token",
				Timeout: "45s",
			},
		},
	}
	savedSource, err := saveOrganizationToken(
		cfg,
		"https://app.trifle.io",
		"global-token",
		"production",
		"project",
		"project-123",
		true,
	)
	if err != nil {
		t.Fatalf("save organization token: %v", err)
	}
	if savedSource != "production" || cfg.Source != "production" {
		t.Fatalf("saved source = %q, active source = %q", savedSource, cfg.Source)
	}

	source := cfg.Sources["production"]
	if source.Token != "" {
		t.Fatalf("source token = %q, want global token only", source.Token)
	}
	if source.SourceID != "project-123" || source.SourceType != "project" {
		t.Fatalf("source metadata mismatch: %#v", source)
	}
	if source.Timeout != "45s" {
		t.Fatalf("unrelated source settings were not preserved: %#v", source)
	}
	if cfg.Auth == nil || cfg.Auth.UserToken != "global-token" {
		t.Fatalf("global token was not saved: %#v", cfg.Auth)
	}
}

func TestSaveOrganizationTokenRequiresSourceIDForNamedSource(t *testing.T) {
	t.Parallel()

	_, err := saveOrganizationToken(
		&cliConfig{},
		"https://app.trifle.io",
		"global-token",
		"production",
		"project",
		"",
		true,
	)
	if err == nil {
		t.Fatal("expected source ID validation error")
	}
}
