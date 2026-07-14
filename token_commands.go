package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/trifle-io/trifle-cli/internal/output"
)

func runToken(args []string) {
	if len(args) == 0 {
		tokenUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		tokenList(args[1:])
	case "create":
		tokenCreate(args[1:])
	case "delete":
		tokenDelete(args[1:])
	case "help", "-h", "--help":
		tokenUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown token command: %s\n", args[0])
		tokenUsage()
		os.Exit(1)
	}
}

func tokenList(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("token list", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "API token (or TRIFLE_USER_TOKEN / config)")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.BootstrapListTokens(context.Background(), &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func tokenCreate(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("token create", flag.ExitOnError)
	configPathFlag := addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "API token (or TRIFLE_USER_TOKEN / config)")
	name := fs.String("name", "CLI token", "Token name")
	wildcardRead := fs.Bool("wildcard-read", false, "Wildcard read permission")
	wildcardWrite := fs.Bool("wildcard-write", false, "Wildcard write permission")
	sourceType := fs.String("source-type", "", "Optional source type (database|project)")
	sourceID := fs.String("source-id", "", "Optional source ID grant")
	read := fs.Bool("read", true, "Source read permission when --source-id is set")
	write := fs.Bool("write", false, "Source write permission when --source-id is set")
	save := fs.Bool("save", false, "Save token into config source")
	sourceName := fs.String("source-name", "", "Config source name when saving")
	activate := fs.Bool("activate", true, "Set saved source as active")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"name": strings.TrimSpace(*name),
	}

	if *wildcardRead {
		payload["wildcard_read"] = true
	}
	if *wildcardWrite {
		payload["wildcard_write"] = true
	}

	if strings.TrimSpace(*sourceID) != "" {
		payload["source_id"] = strings.TrimSpace(*sourceID)
		payload["read"] = *read
		payload["write"] = *write
		if strings.TrimSpace(*sourceType) != "" {
			payload["source_type"] = strings.TrimSpace(*sourceType)
		}
	}

	var response map[string]any
	if err := client.BootstrapCreateToken(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if *save {
		cfg, path, err := loadConfigForWrite(*configPathFlag)
		if err != nil {
			exitError(err)
		}

		baseURL := normalizeURL(*url)
		tokenValue := nestedString(response, "data", "token", "value")
		resolvedSourceType := strings.TrimSpace(*sourceType)
		resolvedSourceID := strings.TrimSpace(*sourceID)

		if tokenValue == "" {
			exitError(errors.New("response did not include token value"))
		}
		if resolvedSourceID == "" {
			exitError(errors.New("--source-id is required when --save is enabled"))
		}
		if resolvedSourceType == "" {
			exitError(errors.New("--source-type is required when --save is enabled"))
		}

		chosenName := strings.TrimSpace(*sourceName)
		if chosenName == "" {
			chosenName = defaultSavedSourceName(resolvedSourceType, resolvedSourceID)
		}

		if cfg.Auth == nil {
			cfg.Auth = &authConfig{}
		}
		cfg.Auth.URL = baseURL
		cfg.Auth.UserToken = strings.TrimSpace(*userToken)

		cfg.Sources[chosenName] = sourceConfig{
			Driver:     "api",
			URL:        baseURL,
			Token:      tokenValue,
			SourceType: resolvedSourceType,
			SourceID:   resolvedSourceID,
		}

		if *activate {
			cfg.Source = chosenName
		}

		if err := saveConfigFile(path, cfg); err != nil {
			exitError(err)
		}

		attachConfigMeta(response, path)
		attachSavedSourceMeta(response, chosenName, cfg.Source)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func tokenDelete(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("token delete", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "API token (or TRIFLE_USER_TOKEN / config)")
	id := fs.String("id", "", "Token ID")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	if strings.TrimSpace(*id) == "" {
		exitError(errors.New("--id is required"))
	}

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.BootstrapDeleteToken(context.Background(), strings.TrimSpace(*id), &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func tokenUsage() {
	fmt.Println("trifle token <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  list    List organization tokens")
	fmt.Println("  create  Create organization token")
	fmt.Println("  delete  Delete organization token")
}
