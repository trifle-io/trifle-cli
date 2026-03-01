package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/trifle-io/trifle-cli/internal/api"
	"github.com/trifle-io/trifle-cli/internal/output"
)

func runAuth(args []string) {
	if len(args) == 0 {
		authUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "signup":
		authSignup(args[1:])
	case "login":
		authLogin(args[1:])
	case "me":
		authMe(args[1:])
	case "help", "-h", "--help":
		authUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown auth command: %s\n", args[0])
		authUsage()
		os.Exit(1)
	}
}

func runSource(args []string) {
	if len(args) == 0 {
		sourceUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		sourceList(args[1:])
	case "create":
		sourceCreate(args[1:])
	case "setup":
		sourceSetup(args[1:])
	case "token":
		sourceToken(args[1:])
	case "use":
		sourceUse(args[1:])
	case "help", "-h", "--help":
		sourceUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown source command: %s\n", args[0])
		sourceUsage()
		os.Exit(1)
	}
}

func authSignup(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("auth signup", flag.ExitOnError)
	configPathFlag := addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	email := fs.String("email", "", "Account email")
	password := fs.String("password", "", "Account password")
	name := fs.String("name", "", "Optional user name")
	orgName := fs.String("org-name", "", "Optional organization name to create")
	tokenName := fs.String("token-name", "CLI token", "User API token label")
	save := fs.Bool("save", true, "Save auth token to config")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	baseURL := normalizeURL(*url)
	if baseURL == "" {
		exitError(errors.New("missing base URL: set --url, TRIFLE_URL, or auth.url in config"))
	}
	if strings.TrimSpace(*email) == "" || strings.TrimSpace(*password) == "" {
		exitError(errors.New("--email and --password are required"))
	}

	client, err := api.New(baseURL, "", *timeout)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"email":      strings.TrimSpace(*email),
		"password":   *password,
		"token_name": strings.TrimSpace(*tokenName),
	}
	if strings.TrimSpace(*name) != "" {
		payload["name"] = strings.TrimSpace(*name)
	}
	if strings.TrimSpace(*orgName) != "" {
		payload["organization_name"] = strings.TrimSpace(*orgName)
	}

	var response map[string]any
	if err := client.BootstrapSignup(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if *save {
		cfg, path, err := loadConfigForWrite(*configPathFlag)
		if err != nil {
			exitError(err)
		}
		applyAuthFromResponse(cfg, baseURL, response, strings.TrimSpace(*email), "")
		if err := saveConfigFile(path, cfg); err != nil {
			exitError(err)
		}
		attachConfigMeta(response, path)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func authLogin(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("auth login", flag.ExitOnError)
	configPathFlag := addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	email := fs.String("email", "", "Account email")
	password := fs.String("password", "", "Account password")
	tokenName := fs.String("token-name", "CLI token", "User API token label")
	save := fs.Bool("save", true, "Save auth token to config")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	baseURL := normalizeURL(*url)
	if baseURL == "" {
		exitError(errors.New("missing base URL: set --url, TRIFLE_URL, or auth.url in config"))
	}
	if strings.TrimSpace(*email) == "" || strings.TrimSpace(*password) == "" {
		exitError(errors.New("--email and --password are required"))
	}

	client, err := api.New(baseURL, "", *timeout)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"email":      strings.TrimSpace(*email),
		"password":   *password,
		"token_name": strings.TrimSpace(*tokenName),
	}

	var response map[string]any
	if err := client.BootstrapLogin(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if *save {
		cfg, path, err := loadConfigForWrite(*configPathFlag)
		if err != nil {
			exitError(err)
		}
		applyAuthFromResponse(cfg, baseURL, response, strings.TrimSpace(*email), "")
		if err := saveConfigFile(path, cfg); err != nil {
			exitError(err)
		}
		attachConfigMeta(response, path)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func authMe(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("auth me", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "User API token (or TRIFLE_USER_TOKEN / config)")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	baseURL := normalizeURL(*url)
	if baseURL == "" {
		exitError(errors.New("missing base URL: set --url, TRIFLE_URL, or auth.url in config"))
	}
	token := strings.TrimSpace(*userToken)
	if token == "" {
		exitError(errors.New("missing user token: set --user-token, TRIFLE_USER_TOKEN, or auth.user_token in config"))
	}

	client, err := api.New(baseURL, token, *timeout)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.BootstrapMe(context.Background(), &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func sourceList(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("source list", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "User API token (or TRIFLE_USER_TOKEN / config)")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.BootstrapListSources(context.Background(), &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func sourceCreate(args []string) {
	if len(args) == 0 {
		sourceCreateUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "database":
		sourceCreateDatabase(args[1:])
	case "project":
		sourceCreateProject(args[1:])
	case "help", "-h", "--help":
		sourceCreateUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown source create command: %s\n", args[0])
		sourceCreateUsage()
		os.Exit(1)
	}
}

func sourceCreateDatabase(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("source create database", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "User API token (or TRIFLE_USER_TOKEN / config)")
	displayName := fs.String("display-name", "", "Database display name")
	driver := fs.String("driver", "", "Database driver (sqlite|postgres|mysql|redis|mongo)")
	host := fs.String("host", "", "Database host")
	port := fs.Int("port", 0, "Database port")
	username := fs.String("user", "", "Database username")
	password := fs.String("password", "", "Database password")
	database := fs.String("database", "", "Database name")
	filePath := fs.String("file-path", "", "SQLite file path")
	authDatabase := fs.String("auth-database", "", "Mongo auth database")
	timeZone := fs.String("timezone", "", "Source time zone")
	defaultTimeframe := fs.String("default-timeframe", "", "Default timeframe (for example 7d)")
	defaultGranularity := fs.String("default-granularity", "", "Default granularity")
	granularities := fs.String("granularities", "", "Comma-separated granularities")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	if strings.TrimSpace(*displayName) == "" {
		exitError(errors.New("--display-name is required"))
	}
	if strings.TrimSpace(*driver) == "" {
		exitError(errors.New("--driver is required"))
	}

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"display_name": strings.TrimSpace(*displayName),
		"driver":       strings.TrimSpace(*driver),
	}
	maybePutString(payload, "host", *host)
	if *port > 0 {
		payload["port"] = *port
	}
	maybePutString(payload, "username", *username)
	maybePutString(payload, "password", *password)
	maybePutString(payload, "database_name", *database)
	maybePutString(payload, "file_path", *filePath)
	maybePutString(payload, "auth_database", *authDatabase)
	maybePutString(payload, "time_zone", *timeZone)
	maybePutString(payload, "default_timeframe", *defaultTimeframe)
	maybePutString(payload, "default_granularity", *defaultGranularity)
	if list := parseGranularities(*granularities); list != nil {
		payload["granularities"] = list
	}

	var response map[string]any
	if err := client.BootstrapCreateDatabase(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func sourceCreateProject(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("source create project", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "User API token (or TRIFLE_USER_TOKEN / config)")
	name := fs.String("name", "", "Project name")
	clusterID := fs.String("project-cluster-id", "", "Project cluster ID")
	timeZone := fs.String("timezone", "", "Project time zone")
	beginningOfWeek := fs.Int("week-start", 0, "Numeric week start")
	granularities := fs.String("granularities", "", "Comma-separated granularities")
	expireAfter := fs.Int("expire-after", 0, "Retention in seconds")
	defaultTimeframe := fs.String("default-timeframe", "", "Default timeframe (for example 7d)")
	defaultGranularity := fs.String("default-granularity", "", "Default granularity")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	if strings.TrimSpace(*name) == "" {
		exitError(errors.New("--name is required"))
	}

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"name": strings.TrimSpace(*name),
	}
	maybePutString(payload, "project_cluster_id", *clusterID)
	maybePutString(payload, "time_zone", *timeZone)
	if *beginningOfWeek != 0 {
		payload["beginning_of_week"] = *beginningOfWeek
	}
	if list := parseGranularities(*granularities); list != nil {
		payload["granularities"] = list
	}
	if *expireAfter > 0 {
		payload["expire_after"] = *expireAfter
	}
	maybePutString(payload, "default_timeframe", *defaultTimeframe)
	maybePutString(payload, "default_granularity", *defaultGranularity)

	var response map[string]any
	if err := client.BootstrapCreateProject(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func sourceSetup(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("source setup", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "User API token (or TRIFLE_USER_TOKEN / config)")
	id := fs.String("id", "", "Database source ID")
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
	if err := client.BootstrapSetupDatabase(context.Background(), strings.TrimSpace(*id), &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func sourceToken(args []string) {
	if len(args) == 0 {
		sourceTokenUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "create":
		sourceTokenCreate(args[1:])
	case "help", "-h", "--help":
		sourceTokenUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown source token command: %s\n", args[0])
		sourceTokenUsage()
		os.Exit(1)
	}
}

func sourceTokenCreate(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("source token create", flag.ExitOnError)
	configPathFlag := addConfigFlag(fs, rc.ConfigPath)
	url := fs.String("url", defaultBootstrapURL(rc), "Trifle base URL (or TRIFLE_URL / config)")
	userToken := fs.String("user-token", defaultUserToken(rc.Config), "User API token (or TRIFLE_USER_TOKEN / config)")
	sourceType := fs.String("source-type", "", "Source type (database|project)")
	sourceID := fs.String("source-id", "", "Source ID")
	name := fs.String("name", "CLI source token", "Token name")
	read := fs.Bool("read", true, "Read permission (project tokens)")
	write := fs.Bool("write", true, "Write permission (project tokens)")
	save := fs.Bool("save", true, "Save source token in config")
	sourceName := fs.String("source-name", "", "Config source name to save")
	activate := fs.Bool("activate", true, "Set saved source as active")
	timeout := fs.Duration("timeout", 30*time.Second, "HTTP timeout")
	fs.Parse(args)

	if strings.TrimSpace(*sourceType) == "" || strings.TrimSpace(*sourceID) == "" {
		exitError(errors.New("--source-type and --source-id are required"))
	}

	client, err := bootstrapClient(*url, *userToken, *timeout)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"source_type": strings.TrimSpace(*sourceType),
		"source_id":   strings.TrimSpace(*sourceID),
		"name":        strings.TrimSpace(*name),
		"read":        *read,
		"write":       *write,
	}

	var response map[string]any
	if err := client.BootstrapCreateSourceToken(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if *save {
		cfg, path, err := loadConfigForWrite(*configPathFlag)
		if err != nil {
			exitError(err)
		}

		baseURL := normalizeURL(*url)
		tokenValue := nestedString(response, "data", "token", "value")
		respSourceType := nestedString(response, "data", "source", "type")
		respSourceID := nestedString(response, "data", "source", "id")

		if tokenValue == "" || respSourceType == "" || respSourceID == "" {
			exitError(errors.New("response did not include source token details"))
		}

		chosenName := strings.TrimSpace(*sourceName)
		if chosenName == "" {
			chosenName = defaultSavedSourceName(respSourceType, respSourceID)
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
			SourceType: respSourceType,
			SourceID:   respSourceID,
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

func sourceUse(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("source use", flag.ExitOnError)
	configPathFlag := addConfigFlag(fs, rc.ConfigPath)
	name := fs.String("name", "", "Saved config source name")
	fs.Parse(args)

	if strings.TrimSpace(*name) == "" {
		exitError(errors.New("--name is required"))
	}

	cfg, path, err := loadConfigForWrite(*configPathFlag)
	if err != nil {
		exitError(err)
	}

	chosen := strings.TrimSpace(*name)
	if _, ok := cfg.Sources[chosen]; !ok {
		existing := make([]string, 0, len(cfg.Sources))
		for sourceName := range cfg.Sources {
			existing = append(existing, sourceName)
		}
		sort.Strings(existing)
		exitError(fmt.Errorf("unknown source %q in config (available: %s)", chosen, strings.Join(existing, ", ")))
	}

	cfg.Source = chosen
	if err := saveConfigFile(path, cfg); err != nil {
		exitError(err)
	}

	response := map[string]any{
		"data": map[string]any{
			"active_source": chosen,
		},
	}
	attachConfigMeta(response, path)
	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func bootstrapClient(rawURL, rawUserToken string, timeout time.Duration) (*api.Client, error) {
	baseURL := normalizeURL(rawURL)
	if baseURL == "" {
		return nil, errors.New("missing base URL: set --url, TRIFLE_URL, or auth.url in config")
	}
	userToken := strings.TrimSpace(rawUserToken)
	if userToken == "" {
		return nil, errors.New("missing user token: set --user-token, TRIFLE_USER_TOKEN, or auth.user_token in config")
	}
	return api.New(baseURL, userToken, timeout)
}

func defaultBootstrapURL(rc resolvedConfig) string {
	return pickString(
		os.Getenv("TRIFLE_URL"),
		pickString(authURLFromConfig(rc.Config), rc.Source.URL, ""),
		"",
	)
}

func defaultUserToken(cfg *cliConfig) string {
	return pickString(
		os.Getenv("TRIFLE_USER_TOKEN"),
		authUserTokenFromConfig(cfg),
		"",
	)
}

func authURLFromConfig(cfg *cliConfig) string {
	if cfg == nil || cfg.Auth == nil {
		return ""
	}
	return strings.TrimSpace(cfg.Auth.URL)
}

func authUserTokenFromConfig(cfg *cliConfig) string {
	if cfg == nil || cfg.Auth == nil {
		return ""
	}
	return strings.TrimSpace(cfg.Auth.UserToken)
}

func applyAuthFromResponse(cfg *cliConfig, baseURL string, response map[string]any, fallbackEmail, fallbackUserToken string) {
	if cfg.Auth == nil {
		cfg.Auth = &authConfig{}
	}

	cfg.Auth.URL = baseURL

	tokenValue := nestedString(response, "data", "token", "value")
	if tokenValue == "" {
		tokenValue = strings.TrimSpace(fallbackUserToken)
	}
	if tokenValue != "" {
		cfg.Auth.UserToken = tokenValue
	}

	email := nestedString(response, "data", "user", "email")
	if email == "" {
		email = strings.TrimSpace(fallbackEmail)
	}
	if email != "" {
		cfg.Auth.Email = email
	}

	if userID := nestedString(response, "data", "user", "id"); userID != "" {
		cfg.Auth.UserID = userID
	}
	if organizationID := nestedString(response, "data", "organization", "id"); organizationID != "" {
		cfg.Auth.OrganizationID = organizationID
	}
}

func nestedString(payload map[string]any, path ...string) string {
	if len(path) == 0 {
		return ""
	}
	current := any(payload)
	for _, key := range path {
		asMap, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		next, ok := asMap[key]
		if !ok {
			return ""
		}
		current = next
	}
	value, ok := current.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func defaultSavedSourceName(sourceType, sourceID string) string {
	sourceType = strings.TrimSpace(sourceType)
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" {
		sourceType = "api"
	}
	shortID := sourceID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	shortID = strings.TrimSpace(shortID)
	if shortID == "" {
		shortID = "source"
	}
	return sourceType + "-" + shortID
}

func maybePutString(payload map[string]any, key, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	payload[key] = strings.TrimSpace(value)
}

func attachConfigMeta(payload map[string]any, configPath string) {
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return
	}
	data["config_path"] = configPath
}

func attachSavedSourceMeta(payload map[string]any, sourceName, activeSource string) {
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return
	}
	data["saved_source"] = sourceName
	data["active_source"] = activeSource
}

func normalizeURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	return strings.TrimRight(raw, "/")
}

func authUsage() {
	fmt.Println("trifle auth <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  signup  Create an account and issue a user API token")
	fmt.Println("  login   Log in with email/password and issue a user API token")
	fmt.Println("  me      Show authenticated user context")
}

func sourceUsage() {
	fmt.Println("trifle source <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  list       List available project/database sources")
	fmt.Println("  create     Create a source (database/project)")
	fmt.Println("  setup      Run database source setup")
	fmt.Println("  token      Manage source tokens")
	fmt.Println("  use        Set active saved source in config")
}

func sourceCreateUsage() {
	fmt.Println("trifle source create <database|project> [options]")
}

func sourceTokenUsage() {
	fmt.Println("trifle source token <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  create  Create a source token and optionally save it in config")
}
