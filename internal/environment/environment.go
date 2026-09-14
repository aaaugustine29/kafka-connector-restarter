package environment

import (
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"
)

type CommunicationConfiguration struct {
	RequestTimeout time.Duration
}

type AuthConfiguration struct {
	Enabled  bool
	Username string
	Password string
}

type PollingBehavior struct {
	Interval           time.Duration
	RestartFailedTasks bool
}

type ConnectConfiguration struct {
	Host       string
	Port       string
	URL        string
	HTTPS      bool
	AuthConfig AuthConfiguration
}

type Configuration struct {
	PollingBehavior     PollingBehavior
	CommunicationConfig CommunicationConfiguration
	ConnectConfig       ConnectConfiguration
}

func LoadConfig() Configuration {
	return Configuration{
		PollingBehavior:     LoadPollingBehavior(),
		CommunicationConfig: LoadCommunicationConfiguration(),
		ConnectConfig:       LoadConnectConfiguration(),
	}
}

func LoadPollingBehavior() PollingBehavior {
	return PollingBehavior{
		Interval:           LoadPollingInterval(),
		RestartFailedTasks: LoadRestartFailedTasks(),
	}
}

func LoadPollingInterval() time.Duration {
	pollInterval := os.Getenv(PollingIntervalEnv)
	if pollInterval == "" {
		log.Println("No value of", PollingIntervalEnv, "set; using default of", DefaultPollingInterval)
		return DefaultPollingInterval
	}

	pollIntervalMS, err := strconv.Atoi(pollInterval)
	if err != nil || pollIntervalMS <= 0 {
		log.Println("Invalid", PollingIntervalEnv, "value of ", pollInterval, "; using default of", DefaultPollingInterval)
		return DefaultPollingInterval
	}

	pollingInterval := time.Duration(pollIntervalMS) * time.Millisecond
	log.Println("Using", PollingIntervalEnv, "of", pollingInterval)
	return pollingInterval
}

func LoadRestartFailedTasks() bool {
	restartFailedTasksString := os.Getenv(PollingRestartFailedTasks)
	if restartFailedTasksString == "" {
		log.Println("No value of", PollingRestartFailedTasks, "set; using default of", DefaultRestartFailedTasks)
		return DefaultRestartFailedTasks
	}

	restartFailedTasks, err := strconv.ParseBool(restartFailedTasksString)
	if err != nil {
		log.Println("Invalid", PollingRestartFailedTasks, "value of", restartFailedTasksString, "; using default of", DefaultRestartFailedTasks)
		return DefaultRestartFailedTasks
	}

	log.Println("Using", PollingRestartFailedTasks, "of", restartFailedTasks)
	return restartFailedTasks
}

func LoadConnectConfiguration() ConnectConfiguration {
	host := os.Getenv(ConnectHostEnv)
	if host == "" {
		log.Println("No value of", ConnectHostEnv, "set; using default of", DefaultConnectHost)
		host = DefaultConnectHost
	}

	port := os.Getenv(ConnectPortEnv)
	if port == "" {
		log.Println("No value of", ConnectPortEnv, "set; using default of", DefaultConnectPort)
		port = DefaultConnectPort
	}

	https := LoadHTTPS()
	authConfig := LoadAuthConfiguration()
	if authConfig.Enabled && !https {
		log.Println("Basic authentication requires", ConnectSecureHTTP, "to be true. Authentication disabled.")
		authConfig = AuthConfiguration{
			Enabled:  DefaultConnectBasicAuthEnabled,
			Username: "",
			Password: "",
		}
	}
	scheme := "http"
	if https {
		scheme = "https"
	}

	return ConnectConfiguration{
		Host:  host,
		Port:  port,
		HTTPS: https,
		URL: (&url.URL{
			Scheme: scheme,
			Host:   net.JoinHostPort(host, port),
		}).String(),
		AuthConfig: authConfig,
	}
}

func LoadHTTPS() bool {
	httpsValue := os.Getenv(ConnectSecureHTTP)
	if httpsValue == "" {
		log.Println("No value of", ConnectSecureHTTP, "set; using default of", DefaultConnectSecureHTTP)
		return DefaultConnectSecureHTTP
	}

	https, err := strconv.ParseBool(httpsValue)
	if err != nil {
		log.Println("Invalid", ConnectSecureHTTP, "value of", httpsValue, "; using default of", DefaultConnectSecureHTTP)
		return DefaultConnectSecureHTTP
	}

	log.Println("Using", ConnectSecureHTTP, "of", https)
	return https
}

func LoadCommunicationConfiguration() CommunicationConfiguration {
	requestTimeoutString := os.Getenv(HTTPRequestTimeout)
	var requestTimeoutInt int64
	var requestTimeout time.Duration
	var err error

	if requestTimeoutString == "" {
		log.Println("No value of", requestTimeoutString, "set; using default of", DefaultHTTPRequestTimeout)
		requestTimeout = DefaultHTTPRequestTimeout
	} else {
		requestTimeoutInt, err = strconv.ParseInt(requestTimeoutString, 10, 64)
		if err != nil || requestTimeoutInt <= 0 {
			log.Println("Invalid", HTTPRequestTimeout, "value", requestTimeoutString, "; using default of", DefaultHTTPRequestTimeout)
			requestTimeout = DefaultHTTPRequestTimeout
		} else {
			requestTimeout = time.Duration(requestTimeoutInt) * time.Millisecond
		}
	}

	return CommunicationConfiguration{
		RequestTimeout: requestTimeout,
	}
}

func LoadAuthConfiguration() AuthConfiguration {
	authEnabledString := os.Getenv(ConnectBasicAuthEnabledEnv)
	if authEnabledString == "" {
		log.Println("Authentication not configured, defaulting to", DefaultConnectBasicAuthEnabled)
		return AuthConfiguration{
			Enabled:  DefaultConnectBasicAuthEnabled,
			Username: "",
			Password: "",
		}
	}
	authEnabled, err := strconv.ParseBool(authEnabledString)
	if err != nil || authEnabled == false {
		if err != nil {
			log.Println("Error parsing", ConnectBasicAuthEnabledEnv, ", defaulting to", DefaultConnectBasicAuthEnabled)
		} else {
			log.Println("Authentication disabled")
		}
		return AuthConfiguration{
			Enabled:  DefaultConnectBasicAuthEnabled,
			Username: "",
			Password: "",
		}
	} else {
		authUsername := os.Getenv(ConnectBasicAuthUsernameEnv)
		authPassword := os.Getenv(ConnectBasicAuthPasswordEnv)
		if authUsername == "" || authPassword == "" {
			log.Println("Authentication username or password is blank. Authentication disabled.")
			return AuthConfiguration{
				Enabled:  DefaultConnectBasicAuthEnabled,
				Username: "",
				Password: "",
			}
		} else {
			return AuthConfiguration{
				Enabled:  true,
				Username: authUsername,
				Password: authPassword,
			}
		}
	}
}
