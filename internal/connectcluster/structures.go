// Package connectcluster owns Kafka Connect cluster configuration.
package connectcluster

type AuthConfiguration struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
}

// ConnectClusterAPIConfiguration contains endpoint settings for one named cluster.
// The name is the key in the cluster configuration file.
type ConnectClusterAPIConfiguration struct {
	Host       string            `json:"host" yaml:"host"`
	Port       string            `json:"port" yaml:"port"`
	HTTPS      bool              `json:"https" yaml:"https"`
	AuthConfig AuthConfiguration `json:"authConfig" yaml:"authConfig"`
}

func DefaultConfiguration() ConnectClusterAPIConfiguration {
	return ConnectClusterAPIConfiguration{Host: "localhost", Port: "8083"}
}
