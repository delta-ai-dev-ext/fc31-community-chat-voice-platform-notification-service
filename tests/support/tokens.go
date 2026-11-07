// Package support holds test fixtures shared by the unit and integration
// tests: RSA key material and access-token signing helpers.
package support

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

// NewRSAKey generates a fresh 2048-bit RSA key.
func NewRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

// PEMPublicKey returns the PKIX PEM encoding of key's public part.
func PEMPublicKey(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

// SignToken signs claims with key, adding the usual identity claims.
func SignToken(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	require.NoError(t, err)
	return signed
}

// ValidClaims returns claims for an access token of userID that expires in one
// hour.
func ValidClaims(userID string) jwt.MapClaims {
	now := time.Now().UTC()
	return jwt.MapClaims{
		"sub":            userID,
		"sid":            "session-1",
		"email_verified": true,
		"iss":            "identity_service",
		"aud":            "platform",
		"iat":            now.Unix(),
		"exp":            now.Add(time.Hour).Unix(),
	}
}

// JWKSDocument renders a JWKS document for key with the given kid.
func JWKSDocument(t *testing.T, kid string, key *rsa.PrivateKey) []byte {
	t.Helper()
	document := map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA",
			"kid": kid,
			"use": "sig",
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
		}},
	}
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	return encoded
}
