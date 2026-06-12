package cli

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// stdin/stdout are package-level so unit tests can substitute buffers.
var (
	stdin  io.Reader = os.Stdin
	stdout io.Writer = os.Stdout
)

// Confirm prints the prompt followed by " [y/N]: " and reads one line from
// stdin. It returns true only for "y"/"yes" (case-insensitive).
func Confirm(prompt string) bool {
	fmt.Fprintf(stdout, "%s [y/N]: ", prompt)
	line, err := readLine(stdin)
	if err != nil && line == "" {
		return false
	}
	return isYes(line)
}

// isYes reports whether the answer means "yes".
func isYes(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "y", "yes":
		return true
	}
	return false
}

// promptString prints the prompt and reads one non-empty line.
func promptString(prompt string) (string, error) {
	fmt.Fprintf(stdout, "%s: ", prompt)
	line, err := readLine(stdin)
	if err != nil && line == "" {
		return "", fmt.Errorf("чтение ввода: %w", err)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("пустой ввод")
	}
	return line, nil
}

// promptInt prints the prompt and reads one line parsed as int.
func promptInt(prompt string) (int, error) {
	s, err := promptString(prompt)
	if err != nil {
		return 0, err
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("ожидалось число, получено %q", s)
	}
	return v, nil
}

// readLine reads bytes one at a time until '\n' or EOF. Byte-at-a-time
// reading avoids buffering ahead of the line, so interleaved prompts each
// see their own input line.
func readLine(r io.Reader) (string, error) {
	var b strings.Builder
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if buf[0] == '\n' {
				return strings.TrimSuffix(b.String(), "\r"), nil
			}
			b.WriteByte(buf[0])
		}
		if err != nil {
			return b.String(), err
		}
	}
}
