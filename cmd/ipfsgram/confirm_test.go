package main

import (
	"bytes"
	"strings"
	"testing"
)

// withIO substitutes the package stdin/stdout for the duration of the test.
func withIO(t *testing.T, input string) *bytes.Buffer {
	t.Helper()
	out := &bytes.Buffer{}
	oldIn, oldOut := stdin, stdout
	stdin, stdout = strings.NewReader(input), out
	t.Cleanup(func() { stdin, stdout = oldIn, oldOut })
	return out
}

func TestConfirm(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"y\n", true},
		{"Y\n", true},
		{"yes\n", true},
		{"YES\n", true},
		{"  y  \n", true},
		{"y\r\n", true},
		{"n\n", false},
		{"no\n", false},
		{"\n", false},
		{"", false}, // EOF
		{"yessir\n", false},
	}
	for _, tt := range tests {
		out := withIO(t, tt.input)
		if got := Confirm("Continue?"); got != tt.want {
			t.Errorf("Confirm with input %q = %t, want %t", tt.input, got, tt.want)
		}
		if !strings.Contains(out.String(), "Continue? [y/N]: ") {
			t.Errorf("prompt not printed, got %q", out.String())
		}
	}
}

func TestConfirmConsumesSingleLine(t *testing.T) {
	withIO(t, "y\nn\n")
	if !Confirm("first") {
		t.Fatal("first Confirm should be yes")
	}
	if Confirm("second") {
		t.Fatal("second Confirm should be no")
	}
}

func TestPromptInt(t *testing.T) {
	withIO(t, "12345\n")
	v, err := promptInt("api_id")
	if err != nil || v != 12345 {
		t.Fatalf("promptInt = %d, %v; want 12345, nil", v, err)
	}

	withIO(t, "abc\n")
	if _, err := promptInt("api_id"); err == nil {
		t.Fatal("promptInt should fail on non-numeric input")
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
	}
	for _, tt := range tests {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
