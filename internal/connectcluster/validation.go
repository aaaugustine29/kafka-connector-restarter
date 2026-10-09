package connectcluster

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

func ValidateConfiguration(configuration ConnectClusterAPIConfiguration) error {
	if strings.TrimSpace(configuration.Host) == "" {
		return fmt.Errorf("host: Connect host must not be empty")
	}
	if !validConnectHost(configuration.Host) {
		return fmt.Errorf("host: Connect host must be a hostname or unbracketed IP address without a scheme, port, path, or whitespace")
	}
	port, err := strconv.Atoi(configuration.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("port: Connect port must be between 1 and 65535")
	}
	if configuration.AuthConfig.Enabled {
		if !configuration.HTTPS {
			return fmt.Errorf("https: Basic Auth requires HTTPS")
		}
		if strings.TrimSpace(configuration.AuthConfig.Username) == "" {
			return fmt.Errorf("authConfig.username: Basic Auth requires a username and password")
		}
		if configuration.AuthConfig.Password == "" {
			return fmt.Errorf("authConfig.password: Basic Auth requires a username and password")
		}
	}
	return nil
}

func validConnectHost(host string) bool {
	if strings.ContainsAny(host, "/\\?#@[]") || strings.ContainsFunc(host, func(character rune) bool {
		return unicode.IsSpace(character) || unicode.IsControl(character)
	}) {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}
	if len(strings.TrimSuffix(host, ".")) > 253 {
		return false
	}
	for label := range strings.SplitSeq(strings.TrimSuffix(host, "."), ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
				(character >= '0' && character <= '9') || character == '-') {
				return false
			}
		}
	}
	return true
}
