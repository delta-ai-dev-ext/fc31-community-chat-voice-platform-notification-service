// Package auth verifies identity_service access tokens locally with identity's
// public keys (JWKS endpoint or static PEM key). No network round trip happens
// per request once the key set is cached.
package auth

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// ErrUnknownKey is returned when no public key matches the token's `kid`.
var ErrUnknownKey = errors.New("auth: unknown signing key")

// KeyProvider resolves the RSA public key used to sign a token.
type KeyProvider interface {
	// KeyFor returns the key matching kid. An empty kid selects the only
	// configured key when the provider holds exactly one.
	KeyFor(ctx context.Context, kid string) (*rsa.PublicKey, error)
}

// StaticKeyProvider verifies tokens with one or more PEM-encoded public keys.
type StaticKeyProvider struct {
	keys map[string]*rsa.PublicKey
	only *rsa.PublicKey
}

// ParsePublicKeysPEM parses one or more PEM blocks holding RSA public keys.
func ParsePublicKeysPEM(data []byte) ([]*rsa.PublicKey, error) {
	var keys []*rsa.PublicKey
	rest := data
	for {
		block, remainder := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = remainder
		switch block.Type {
		case "PUBLIC KEY":
			parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("auth: parse PUBLIC KEY block: %w", err)
			}
			rsaKey, ok := parsed.(*rsa.PublicKey)
			if !ok {
				return nil, fmt.Errorf("auth: PUBLIC KEY block is not an RSA key")
			}
			keys = append(keys, rsaKey)
		case "RSA PUBLIC KEY":
			rsaKey, err := x509.ParsePKCS1PublicKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("auth: parse RSA PUBLIC KEY block: %w", err)
			}
			keys = append(keys, rsaKey)
		default:
			return nil, fmt.Errorf("auth: unsupported PEM block %q", block.Type)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("auth: no public key found in PEM data")
	}
	return keys, nil
}

// NewStaticKeyProvider builds a KeyProvider from PEM-encoded RSA public keys.
// The key id map is empty, so tokens must omit `kid` (single-key setups).
func NewStaticKeyProvider(pemData []byte) (*StaticKeyProvider, error) {
	keys, err := ParsePublicKeysPEM(pemData)
	if err != nil {
		return nil, err
	}
	provider := &StaticKeyProvider{keys: make(map[string]*rsa.PublicKey, len(keys))}
	if len(keys) == 1 {
		provider.only = keys[0]
	}
	return provider, nil
}

// KeyFor implements KeyProvider.
func (p *StaticKeyProvider) KeyFor(_ context.Context, kid string) (*rsa.PublicKey, error) {
	if kid != "" {
		if key, ok := p.keys[kid]; ok {
			return key, nil
		}
		return nil, ErrUnknownKey
	}
	if p.only != nil {
		return p.only, nil
	}
	return nil, ErrUnknownKey
}

// JWKSProvider fetches and caches identity's JSON Web Key Set.
type JWKSProvider struct {
	url        string
	client     *http.Client
	ttl        time.Duration
	minRefresh time.Duration
	now        func() time.Time

	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

// NewJWKSProvider builds a caching JWKS KeyProvider. A nil client uses a
// client with a 5 second timeout.
func NewJWKSProvider(url string, client *http.Client) *JWKSProvider {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &JWKSProvider{
		url:        url,
		client:     client,
		ttl:        10 * time.Minute,
		minRefresh: time.Second,
		now:        func() time.Time { return time.Now().UTC() },
	}
}

// KeyFor implements KeyProvider, refreshing the key set when the cache expired
// or the requested kid is unknown.
func (p *JWKSProvider) KeyFor(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	p.mu.RLock()
	key, ok := p.keys[kid]
	stale := p.now().Sub(p.fetchedAt) >= p.ttl
	p.mu.RUnlock()
	if ok && !stale {
		return key, nil
	}

	if err := p.refresh(ctx, !stale); err != nil {
		if ok {
			// Serve the stale key rather than failing every request while the
			// JWKS endpoint is unavailable.
			return key, nil
		}
		return nil, err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if key, ok := p.keys[kid]; ok {
		return key, nil
	}
	return nil, ErrUnknownKey
}

// refresh pulls the JWKS document. Unless forced, refreshes that arrive within
// minRefresh of the previous fetch are skipped.
func (p *JWKSProvider) refresh(ctx context.Context, force bool) error {
	p.mu.Lock()
	if !force && p.now().Sub(p.fetchedAt) < p.minRefresh {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return fmt.Errorf("auth: build JWKS request: %w", err)
	}
	response, err := p.client.Do(request)
	if err != nil {
		return fmt.Errorf("auth: fetch JWKS: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("auth: fetch JWKS: unexpected status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("auth: read JWKS: %w", err)
	}
	keys, err := ParseJWKS(body)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.keys = keys
	p.fetchedAt = p.now()
	p.mu.Unlock()
	return nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}

// ParseJWKS converts a JWKS document into RSA public keys by key id.
func ParseJWKS(data []byte) (map[string]*rsa.PublicKey, error) {
	var document jwksDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("auth: parse JWKS: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, entry := range document.Keys {
		if entry.Kty != "RSA" {
			continue
		}
		if entry.Use != "" && entry.Use != "sig" {
			continue
		}
		if entry.Alg != "" && entry.Alg != "RS256" {
			continue
		}
		key, err := rsaKeyFromJWK(entry)
		if err != nil {
			return nil, err
		}
		keys[entry.Kid] = key
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("auth: JWKS contains no usable RSA signing key")
	}
	return keys, nil
}

func rsaKeyFromJWK(entry jwk) (*rsa.PublicKey, error) {
	modulus, err := base64URLToInt(entry.N)
	if err != nil {
		return nil, fmt.Errorf("auth: JWKS modulus: %w", err)
	}
	exponent, err := base64URLToInt(entry.E)
	if err != nil {
		return nil, fmt.Errorf("auth: JWKS exponent: %w", err)
	}
	if modulus.BitLen() < 1024 {
		return nil, fmt.Errorf("auth: JWKS modulus is too small")
	}
	if !exponent.IsInt64() || exponent.Int64() > int64(^uint32(0)) || exponent.Int64() < 3 {
		return nil, fmt.Errorf("auth: JWKS exponent is invalid")
	}
	return &rsa.PublicKey{N: modulus, E: int(exponent.Int64())}, nil
}

func base64URLToInt(value string) (*big.Int, error) {
	if value == "" {
		return nil, fmt.Errorf("empty value")
	}
	decoded, err := jwtBase64URLDecode(value)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(decoded), nil
}
