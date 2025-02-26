package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	pathname          string
	DefaultRepository string                      `yaml:"default-repo"`
	Repositories      map[string]RepositoryConfig `yaml:"repositories"`
	Remotes           map[string]RemoteConfig     `yaml:"remotes"`
}

type RepositoryConfig map[string]string
type RemoteConfig map[string]string

func LoadOrCreate(configFile string) (*Config, error) {
	f, err := os.Open(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			cfg := &Config{
				pathname:     configFile,
				Repositories: make(map[string]RepositoryConfig),
				Remotes:      make(map[string]RemoteConfig),
			}
			return cfg, cfg.Save()
		}
		return nil, fmt.Errorf("error reading config file: %T", err)
	}
	defer f.Close()
	var config Config
	if err := yaml.NewDecoder(f).Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	config.pathname = configFile
	if config.Repositories == nil {
		config.Repositories = make(map[string]RepositoryConfig)
	}
	if config.Remotes == nil {
		config.Remotes = make(map[string]RemoteConfig)
	}
	return &config, nil
}

func (c *Config) Render(w io.Writer) error {
	return yaml.NewEncoder(w).Encode(c)
}

func (c *Config) Save() error {
	dir := filepath.Dir(c.pathname)
	tmpFile, err := os.CreateTemp(dir, "config.*.yaml")
	if err != nil {
		return err
	}

	err = c.Render(tmpFile)
	tmpFile.Close()
	if err != nil {
		os.Remove(tmpFile.Name())
		return err
	}
	return os.Rename(tmpFile.Name(), c.pathname)
}

func (c *Config) HasRepository(name string) bool {
	_, ok := c.Repositories[name]
	return ok
}

func (c *Config) GetRepository(name string) (map[string]string, bool) {
	kv, ok := c.Repositories[name]
	return kv, ok
}

func (c *Config) HasRemote(name string) bool {
	_, ok := c.Remotes[name]
	return ok
}

func (c *Config) GetRemote(name string) (map[string]string, bool) {
	kv, ok := c.Remotes[name]
	return kv, ok
}
