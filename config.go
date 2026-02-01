package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type cliConfig struct {
	Source  string
	Sources map[string]sourceConfig
}

type sourceConfig struct {
	Driver          string            `yaml:"driver"`
	URL             string            `yaml:"url"`
	Token           string            `yaml:"token"`
	Timeout         string            `yaml:"timeout"`
	DB              string            `yaml:"db"`
	DSN             string            `yaml:"dsn"`
	Host            string            `yaml:"host"`
	Port            string            `yaml:"port"`
	User            string            `yaml:"user"`
	Password        string            `yaml:"password"`
	Database        string            `yaml:"database"`
	Table           string            `yaml:"table"`
	Collection      string            `yaml:"collection"`
	Prefix          string            `yaml:"prefix"`
	Joined          string            `yaml:"joined"`
	Separator       string            `yaml:"separator"`
	TimeZone        string            `yaml:"timezone"`
	WeekStart       string            `yaml:"week_start"`
	Granularities   configStringSlice `yaml:"granularities"`
	BufferMode      string            `yaml:"buffer_mode"`
	BufferDrivers   configStringSlice `yaml:"buffer_drivers"`
	BufferSize      int               `yaml:"buffer_size"`
	BufferDuration  string            `yaml:"buffer_duration"`
	BufferAggregate *bool             `yaml:"buffer_aggregate"`
	BufferAsync     *bool             `yaml:"buffer_async"`

	TimeoutDuration time.Duration `yaml:"-"`
	TimeoutSet      bool          `yaml:"-"`
}

type configStringSlice []string

func (c *cliConfig) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind == 0 {
		return nil
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("config must be a mapping")
	}

	c.Sources = map[string]sourceConfig{}

	var legacyDriver string
	var legacyAPI sourceConfig
	var legacySQLite sourceConfig
	var legacyAPISet bool
	var legacySQLiteSet bool

	for i := 0; i < len(value.Content); i += 2 {
		keyNode := value.Content[i]
		valNode := value.Content[i+1]
		key := strings.TrimSpace(keyNode.Value)
		if key == "" {
			continue
		}

		switch key {
		case "source":
			var srcName string
			if err := valNode.Decode(&srcName); err != nil {
				return err
			}
			c.Source = strings.TrimSpace(srcName)
		case "sources":
			var sources map[string]sourceConfig
			if err := valNode.Decode(&sources); err != nil {
				return err
			}
			for name, src := range sources {
				if strings.TrimSpace(name) == "" {
					continue
				}
				c.Sources[name] = src
			}
		case "driver":
			if valNode.Kind == yaml.ScalarNode {
				if err := valNode.Decode(&legacyDriver); err != nil {
					return err
				}
				legacyDriver = strings.TrimSpace(legacyDriver)
				continue
			}
			var src sourceConfig
			if err := valNode.Decode(&src); err != nil {
				return err
			}
			c.Sources[key] = src
		case "api":
			if valNode.Kind == yaml.MappingNode {
				if err := valNode.Decode(&legacyAPI); err != nil {
					return err
				}
				legacyAPI.Driver = "api"
				legacyAPISet = true
				continue
			}
			var src sourceConfig
			if err := valNode.Decode(&src); err != nil {
				return err
			}
			c.Sources[key] = src
		case "sqlite":
			if valNode.Kind == yaml.MappingNode {
				if err := valNode.Decode(&legacySQLite); err != nil {
					return err
				}
				legacySQLite.Driver = "sqlite"
				legacySQLiteSet = true
				continue
			}
			var src sourceConfig
			if err := valNode.Decode(&src); err != nil {
				return err
			}
			c.Sources[key] = src
		default:
			var src sourceConfig
			if err := valNode.Decode(&src); err != nil {
				return err
			}
			c.Sources[key] = src
		}
	}

	if legacyAPISet {
		if _, exists := c.Sources["api"]; !exists {
			c.Sources["api"] = legacyAPI
		}
	}
	if legacySQLiteSet {
		if _, exists := c.Sources["sqlite"]; !exists {
			c.Sources["sqlite"] = legacySQLite
		}
	}
	if c.Source == "" && legacyDriver != "" {
		c.Source = legacyDriver
	}

	return c.normalize()
}

func (c *cliConfig) normalize() error {
	if c == nil {
		return nil
	}
	for name, src := range c.Sources {
		if err := src.normalize(); err != nil {
			return fmt.Errorf("source %s: %w", name, err)
		}
		c.Sources[name] = src
	}
	return nil
}

func (s *sourceConfig) normalize() error {
	if s == nil {
		return nil
	}
	if strings.TrimSpace(s.Timeout) == "" {
		return nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(s.Timeout))
	if err != nil {
		return fmt.Errorf("invalid timeout: %w", err)
	}
	s.TimeoutDuration = parsed
	s.TimeoutSet = true
	return nil
}

func (s *configStringSlice) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	if value.Kind == 0 {
		return nil
	}

	switch value.Kind {
	case yaml.ScalarNode:
		var raw string
		if err := value.Decode(&raw); err != nil {
			return err
		}
		*s = normalizeStringList(strings.Split(raw, ","))
		return nil
	case yaml.SequenceNode:
		var raw []string
		if err := value.Decode(&raw); err != nil {
			return err
		}
		*s = normalizeStringList(raw)
		return nil
	default:
		return fmt.Errorf("granularities must be a string or list")
	}
}

func (s configStringSlice) Joined() string {
	if len(s) == 0 {
		return ""
	}
	return strings.Join([]string(s), ",")
}

func normalizeStringList(values []string) []string {
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		cleaned = append(cleaned, trimmed)
	}
	if len(cleaned) == 0 {
		return nil
	}
	return cleaned
}

func pickString(envValue, cfgValue, defaultValue string) string {
	if strings.TrimSpace(envValue) != "" {
		return envValue
	}
	if strings.TrimSpace(cfgValue) != "" {
		return cfgValue
	}
	return defaultValue
}

func addConfigFlag(fs *flag.FlagSet, defaultPath string) *string {
	help := "Config file path (YAML)"
	return fs.String("config", defaultPath, help)
}

func addSourceFlag(fs *flag.FlagSet, defaultSource string) *string {
	help := "Source name (or TRIFLE_SOURCE / config)"
	return fs.String("source", defaultSource, help)
}

func resolveConfig(args []string) (*cliConfig, string, error) {
	path, explicit, err := findConfigPath(args)
	if err != nil {
		return nil, "", err
	}
	if !explicit {
		if envPath := strings.TrimSpace(os.Getenv("TRIFLE_CONFIG")); envPath != "" {
			path = envPath
			explicit = true
		}
	}

	defaultPath, err := defaultConfigPath()
	if err == nil && strings.TrimSpace(path) == "" {
		path = defaultPath
	}

	if strings.TrimSpace(path) == "" {
		return &cliConfig{}, "", nil
	}

	expanded, err := expandPath(path)
	if err != nil {
		return nil, path, err
	}
	path = filepath.Clean(expanded)

	cfg, err := loadConfigFile(path)
	if err != nil {
		if !explicit && errors.Is(err, os.ErrNotExist) {
			return &cliConfig{}, path, nil
		}
		return nil, path, err
	}

	return cfg, path, nil
}

func resolveSourceName(args []string, cfg *cliConfig) (string, bool, error) {
	name, explicit, err := findSourceName(args)
	if err != nil {
		return "", false, err
	}
	if !explicit {
		if envName := strings.TrimSpace(os.Getenv("TRIFLE_SOURCE")); envName != "" {
			name = envName
			explicit = true
		}
	}
	if strings.TrimSpace(name) == "" && cfg != nil {
		name = strings.TrimSpace(cfg.Source)
	}
	if strings.TrimSpace(name) == "" {
		name = "api"
	}
	return name, explicit, nil
}

func resolveSourceConfig(cfg *cliConfig, name string) (sourceConfig, error) {
	if cfg == nil {
		return sourceConfig{}, nil
	}
	if len(cfg.Sources) == 0 {
		return sourceConfig{}, nil
	}

	src, ok := cfg.Sources[name]
	if !ok {
		return sourceConfig{}, fmt.Errorf("unknown source %q in config", name)
	}
	return src, nil
}

func findConfigPath(args []string) (string, bool, error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if arg == "--config" {
			if i+1 >= len(args) {
				return "", true, fmt.Errorf("--config requires a value")
			}
			value := strings.TrimSpace(args[i+1])
			if value == "" {
				return "", true, fmt.Errorf("--config requires a value")
			}
			return value, true, nil
		}
		if strings.HasPrefix(arg, "--config=") {
			value := strings.TrimSpace(strings.TrimPrefix(arg, "--config="))
			if value == "" {
				return "", true, fmt.Errorf("--config requires a value")
			}
			return value, true, nil
		}
	}
	return "", false, nil
}

func findSourceName(args []string) (string, bool, error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if arg == "--source" {
			if i+1 >= len(args) {
				return "", true, fmt.Errorf("--source requires a value")
			}
			value := strings.TrimSpace(args[i+1])
			if value == "" {
				return "", true, fmt.Errorf("--source requires a value")
			}
			return value, true, nil
		}
		if strings.HasPrefix(arg, "--source=") {
			value := strings.TrimSpace(strings.TrimPrefix(arg, "--source="))
			if value == "" {
				return "", true, fmt.Errorf("--source requires a value")
			}
			return value, true, nil
		}
	}
	return "", false, nil
}

func defaultConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "trifle", "config.yaml"), nil
}

func expandPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	if path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return home, nil
	}
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, path[2:]), nil
	}
	return path, nil
}

func loadConfigFile(path string) (*cliConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return &cliConfig{}, nil
	}

	var cfg cliConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

type resolvedConfig struct {
	Config     *cliConfig
	SourceName string
	Source     sourceConfig
	ConfigPath string
}

func resolveCommandConfig(args []string) (resolvedConfig, error) {
	cfg, configPath, err := resolveConfig(args)
	if err != nil {
		return resolvedConfig{}, err
	}

	sourceName, _, err := resolveSourceName(args, cfg)
	if err != nil {
		return resolvedConfig{}, err
	}

	sourceCfg, err := resolveSourceConfig(cfg, sourceName)
	if err != nil {
		return resolvedConfig{}, err
	}

	return resolvedConfig{
		Config:     cfg,
		SourceName: sourceName,
		Source:     sourceCfg,
		ConfigPath: configPath,
	}, nil
}
