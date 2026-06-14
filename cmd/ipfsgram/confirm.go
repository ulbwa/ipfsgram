package main

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
		return "", fmt.Errorf("read input: %w", err)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return "", fmt.Errorf("empty input")
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
		return 0, fmt.Errorf("expected a number, got %q", s)
	}
	return v, nil
}

// readLine reads bytes one at a time until '\n' or EOF. Byte-at-a-time reading
// avoids buffering ahead of the line, so interleaved prompts each see their own
// input line.
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

// humanBytes formats a byte count with binary prefixes.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), suffixes[exp])
}
