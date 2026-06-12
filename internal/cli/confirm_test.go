package cli

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
		{"", false},        // EOF
		{"да\n", false},    // only y/yes count
		{"yessir\n", false},
	}
	for _, tt := range tests {
		out := withIO(t, tt.input)
		if got := Confirm("Продолжить?"); got != tt.want {
			t.Errorf("Confirm with input %q = %t, want %t", tt.input, got, tt.want)
		}
		if !strings.Contains(out.String(), "Продолжить? [y/N]: ") {
			t.Errorf("prompt not printed, got %q", out.String())
		}
	}
}

func TestConfirmConsumesSingleLine(t *testing.T) {
	withIO(t, "y\nn\n")
	if !Confirm("первый") {
		t.Fatal("first Confirm should be yes")
	}
	if Confirm("второй") {
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
