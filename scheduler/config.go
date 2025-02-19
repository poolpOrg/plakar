package scheduler

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Configuration struct {
	Agent AgentConfig `yaml:"agent"`
}

type RepositoryConfig struct {
	Name       string `yaml:"name"`
	URL        string `yaml:"url"`
	Passphrase string `yaml:"passphrase",omitempty`
}

type AgentConfig struct {
	Alerting AlertingConfig  `yaml:"alerting"`
	Cleanup  []CleanupConfig `yaml:"cleanup"`
	TaskSets []TaskSet       `yaml:"tasks"`
}

type AlertingConfig struct {
	Email []EmailConfig `yaml:"email"`
}

type EmailConfig struct {
	Name       string     `yaml:"name"`
	Sender     string     `yaml:"sender"`
	Recipients []string   `yaml:"recipients"`
	Smtp       SmtpConfig `yaml:"smtp"`
}

type SmtpConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type TaskSet struct {
	Name       string           `yaml:"name"`
	Repository RepositoryConfig `yaml:"repository"`
	Cleanup    *CleanupConfig   `yaml:"cleanup,omitempty"`
	Backup     *BackupConfig    `yaml:"backup,omitempty"`
	Check      []CheckConfig    `yaml:"check,omitempty"`
	Restore    []RestoreConfig  `yaml:"restore,omitempty"`
	Sync       []SyncConfig     `yaml:"sync,omitempty"`
}

type BackupConfig struct {
	Description string   `yaml:"description"`
	Name        string   `yaml:"name"`
	Tags        []string `yaml:"tags,omitempty"`
	Path        string   `yaml:"path"`
	Interval    string   `yaml:"interval"`
	Check       bool     `yaml:"check"`
	Retention   string   `yaml:"retention"`
}

type CheckConfig struct {
	Path     string `yaml:"path,omitempty"`
	Since    string `yaml:"since,omitempty"`
	Before   string `yaml:"before,omitempty"`
	Interval string `yaml:"interval"`
	Latest   bool   `yaml:"latest"`
}

type RestoreConfig struct {
	Path     string `yaml:"path"`
	Target   string `yaml:"target"`
	Interval string `yaml:"interval"`
}

type SyncConfig struct {
	Peer      string `yaml:"peer"`
	Direction string `yaml:"direction"`
	Interval  string `yaml:"interval"`
}

type CleanupConfig struct {
	Interval   string           `yaml:"interval"`
	Retention  string           `yaml:"retention"`
	Repository RepositoryConfig `yaml:"repository"`
}

func NewConfiguration() *Configuration {
	return &Configuration{}
}

func DefaultConfiguration() *Configuration {
	return &Configuration{}
}

// ParseConfig parses the YAML file into the Config struct.
func ParseConfigFile(filename string) (*Configuration, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Configuration
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
