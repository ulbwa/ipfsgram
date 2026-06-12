package main

import (
	"strings"
	"testing"
)

func TestValidateConfigKey(t *testing.T) {
	for _, key := range []string{"car_max_size", "bot_api_url", "channel_warn_threshold"} {
		if err := validateConfigKey(key); err != nil {
			t.Errorf("validateConfigKey(%q) = %v, want nil", key, err)
		}
	}

	err := validateConfigKey("mtproto_enabled")
	if err == nil {
		t.Fatal("mtproto_enabled must not be editable via config set")
	}
	for _, key := range knownConfigKeys() {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q should list valid key %q", err, key)
		}
	}
}

func TestValidateConfigValue(t *testing.T) {
	tests := []struct {
		key, value string
		ok         bool
	}{
		{"car_max_size", "15728640", true},
		{"car_max_size", "1", true},
		{"car_max_size", "0", false},
		{"car_max_size", "-5", false},
		{"car_max_size", "abc", false},
		{"car_max_size", "1.5", false},

		{"bot_api_url", "https://api.telegram.org", true},
		{"bot_api_url", "http://localhost:8081", true},
		{"bot_api_url", "ftp://example.com", false},
		{"bot_api_url", "not a url", false},
		{"bot_api_url", "https://", false},

		{"channel_warn_threshold", "0", true},
		{"channel_warn_threshold", "0.9", true},
		{"channel_warn_threshold", "1", true},
		{"channel_warn_threshold", "1.1", false},
		{"channel_warn_threshold", "-0.1", false},
		{"channel_warn_threshold", "x", false},

		{"unknown_key", "value", false},
	}
	for _, tt := range tests {
		err := validateConfigValue(tt.key, tt.value)
		if (err == nil) != tt.ok {
			t.Errorf("validateConfigValue(%q, %q) = %v, want ok=%t", tt.key, tt.value, err, tt.ok)
		}
	}
}
