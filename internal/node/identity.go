// identity.go — persistence of the daemon's ed25519 libp2p identity key:
// loaded from the data dir when present, generated and written with 0600
// permissions on first start.

package node

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/rs/zerolog/log"
)

// loadOrCreateIdentity loads the ed25519 libp2p identity from path, creating it
// with 0600 permissions on first start.
func loadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		priv, err := crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return nil, fmt.Errorf("node: parse identity key %s: %w", path, err)
		}
		return priv, nil
	case errors.Is(err, fs.ErrNotExist):
		// fall through to creation
	default:
		return nil, fmt.Errorf("node: read identity key %s: %w", path, err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("node: generate identity key: %w", err)
	}
	raw, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("node: marshal identity key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("node: create data dir: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, fmt.Errorf("node: write identity key %s: %w", path, err)
	}
	log.Info().Str("path", path).Msg("generated new libp2p identity")
	return priv, nil
}
