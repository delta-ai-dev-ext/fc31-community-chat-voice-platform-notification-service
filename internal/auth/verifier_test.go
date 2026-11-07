package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"notification_service/internal/auth"
	"notification_service/tests/support"
)

func newVerifier(t *testing.T, keyPEM []byte) *auth.Verifier {
	t.Helper()
	provider, err := auth.NewStaticKeyProvider(keyPEM)
	require.NoError(t, err)
	return auth.NewVerifier(provider, "identity_service", "platform")
}

func TestVerifyValidToken(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)
	verifier := newVerifier(t, support.PEMPublicKey(t, key))

	raw := support.SignToken(t, key, support.ValidClaims("user-1"))
	claims, err := verifier.Verify(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "user-1", claims.UserID)
	require.Equal(t, "session-1", claims.SessionID)
	require.True(t, claims.EmailVerified)
	require.Equal(t, "identity_service", claims.Issuer)
	require.Equal(t, []string{"platform"}, claims.Audience)
	require.False(t, claims.ExpiresAt.IsZero())
}

func TestVerifyRejectsInvalidTokens(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)
	otherKey := support.NewRSAKey(t)
	verifier := newVerifier(t, support.PEMPublicKey(t, key))

	now := time.Now().UTC()
	hs256 := jwt.NewWithClaims(jwt.SigningMethodHS256, support.ValidClaims("user-1"))
	hsToken, err := hs256.SignedString([]byte("0123456789abcdef0123456789abcdef"))
	require.NoError(t, err)

	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, support.ValidClaims("user-1"))
	noneToken, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	tests := []struct {
		name   string
		token  string
		detail string
	}{
		{name: "empty token", token: "", detail: "empty"},
		{name: "garbage", token: "not-a-jwt", detail: "invalid"},
		{name: "expired", token: support.SignToken(t, key, jwt.MapClaims{
			"sub": "user-1", "iss": "identity_service", "aud": "platform",
			"iat": now.Add(-2 * time.Hour).Unix(), "exp": now.Add(-time.Hour).Unix(),
		}), detail: "expired"},
		{name: "no expiry", token: support.SignToken(t, key, jwt.MapClaims{
			"sub": "user-1", "iss": "identity_service", "aud": "platform", "iat": now.Unix(),
		}), detail: "exp"},
		{name: "wrong signing key", token: support.SignToken(t, otherKey, support.ValidClaims("user-1")), detail: "signature"},
		{name: "hs256 rejected", token: hsToken, detail: "algorithm"},
		{name: "alg none rejected", token: noneToken, detail: "algorithm"},
		{name: "wrong issuer", token: support.SignToken(t, key, jwt.MapClaims{
			"sub": "user-1", "iss": "someone_else", "aud": "platform", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		}), detail: "issuer"},
		{name: "wrong audience", token: support.SignToken(t, key, jwt.MapClaims{
			"sub": "user-1", "iss": "identity_service", "aud": "other", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		}), detail: "audience"},
		{name: "missing subject", token: support.SignToken(t, key, jwt.MapClaims{
			"iss": "identity_service", "aud": "platform", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		}), detail: "sub"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			claims, err := verifier.Verify(context.Background(), test.token)
			require.Error(t, err)
			require.True(t, errors.Is(err, auth.ErrInvalidToken), "expected ErrInvalidToken, got %v", err)
			require.Empty(t, claims)
		})
	}
}

func TestVerifyWithoutIssuerAndAudienceChecks(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)
	provider, err := auth.NewStaticKeyProvider(support.PEMPublicKey(t, key))
	require.NoError(t, err)

	verifier := auth.NewVerifier(provider, "", "")
	claims, err := verifier.Verify(context.Background(), support.SignToken(t, key, jwt.MapClaims{
		"sub": "user-2", "iss": "anything", "aud": "anything",
		"iat": time.Now().UTC().Unix(), "exp": time.Now().UTC().Add(time.Hour).Unix(),
	}))
	require.NoError(t, err)
	require.Equal(t, "user-2", claims.UserID)
}

func TestSetClock(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)
	verifier := newVerifier(t, support.PEMPublicKey(t, key))

	frozen := time.Now().UTC().Add(48 * time.Hour)
	verifier.SetClock(func() time.Time { return frozen })

	claims, err := verifier.Verify(context.Background(), support.SignToken(t, key, support.ValidClaims("user-1")))
	require.Error(t, err, "token must be expired once the injected clock moved past exp")
	require.Empty(t, claims)
}

func TestBearerToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		header  string
		want    string
		wantErr bool
	}{
		{name: "valid", header: "Bearer abc.def.ghi", want: "abc.def.ghi"},
		{name: "lowercase scheme", header: "bearer abc.def.ghi", want: "abc.def.ghi"},
		{name: "empty", header: "", wantErr: true},
		{name: "wrong scheme", header: "Basic abc", wantErr: true},
		{name: "no token", header: "Bearer ", wantErr: true},
		{name: "missing scheme", header: "abc.def.ghi", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			token, err := auth.BearerToken(test.header)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, token)
		})
	}
}

func TestStaticKeyProvider(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)
	provider, err := auth.NewStaticKeyProvider(support.PEMPublicKey(t, key))
	require.NoError(t, err)

	resolved, err := provider.KeyFor(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, 0, key.PublicKey.N.Cmp(resolved.N))

	_, err = provider.KeyFor(context.Background(), "unknown-kid")
	require.ErrorIs(t, err, auth.ErrUnknownKey)

	_, err = auth.NewStaticKeyProvider([]byte("not pem"))
	require.Error(t, err)

	certPEM := []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
	_, err = auth.NewStaticKeyProvider(certPEM)
	require.Error(t, err)
}

func TestParsePublicKeysPEMErrors(t *testing.T) {
	t.Parallel()
	_, err := auth.ParsePublicKeysPEM([]byte("-----BEGIN PUBLIC KEY-----\nZm9v\n-----END PUBLIC KEY-----\n"))
	require.Error(t, err)

	_, err = auth.ParsePublicKeysPEM([]byte("-----BEGIN RSA PUBLIC KEY-----\nZm9v\n-----END RSA PUBLIC KEY-----\n"))
	require.Error(t, err)
}

func TestJWKSProvider(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)
	document := support.JWKSDocument(t, "kid-1", key)

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(document)
	}))
	t.Cleanup(server.Close)

	provider := auth.NewJWKSProvider(server.URL, server.Client())
	resolved, err := provider.KeyFor(context.Background(), "kid-1")
	require.NoError(t, err)
	require.Equal(t, 0, key.PublicKey.N.Cmp(resolved.N))

	// Cached: no second fetch.
	_, err = provider.KeyFor(context.Background(), "kid-1")
	require.NoError(t, err)
	require.Equal(t, 1, requests)

	// Unknown kid triggers a refresh and then fails.
	_, err = provider.KeyFor(context.Background(), "kid-2")
	require.ErrorIs(t, err, auth.ErrUnknownKey)
	require.Equal(t, 2, requests)
}

func TestJWKSProviderFailsOnBadResponse(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	provider := auth.NewJWKSProvider(server.URL, server.Client())
	_, err := provider.KeyFor(context.Background(), "kid-1")
	require.Error(t, err)
}

func TestParseJWKS(t *testing.T) {
	t.Parallel()
	key := support.NewRSAKey(t)

	keys, err := auth.ParseJWKS(support.JWKSDocument(t, "kid-1", key))
	require.NoError(t, err)
	require.Contains(t, keys, "kid-1")

	_, err = auth.ParseJWKS([]byte("{"))
	require.Error(t, err)

	_, err = auth.ParseJWKS([]byte(`{"keys":[{"kty":"EC","kid":"x"}]}`))
	require.Error(t, err)

	_, err = auth.ParseJWKS([]byte(`{"keys":[{"kty":"RSA","kid":"x","use":"enc"}]}`))
	require.Error(t, err)
}
