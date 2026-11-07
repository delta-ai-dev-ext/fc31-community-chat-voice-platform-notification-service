package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrInvalidToken is returned (wrapped) for every unusable access token.
var ErrInvalidToken = errors.New("auth: invalid token")

// SigningAlgorithm is the only accepted JWS algorithm. identity_service signs
// access tokens with RS256.
const SigningAlgorithm = "RS256"

// clockLeeway tolerates small clock skew between replicas.
const clockLeeway = 30 * time.Second

// Claims are the verified access-token claims this service relies on.
type Claims struct {
	UserID        string
	SessionID     string
	EmailVerified bool
	Issuer        string
	Audience      []string
	ExpiresAt     time.Time
	IssuedAt      time.Time
}

// Verifier validates access tokens locally against identity's public keys.
type Verifier struct {
	keys     KeyProvider
	issuer   string
	audience string
	now      func() time.Time
}

// NewVerifier builds a Verifier. issuer and audience are enforced only when
// non-empty.
func NewVerifier(keys KeyProvider, issuer, audience string) *Verifier {
	return &Verifier{
		keys:     keys,
		issuer:   issuer,
		audience: audience,
		now:      func() time.Time { return time.Now().UTC() },
	}
}

// SetClock overrides the clock used for exp/iat/nbf checks (tests).
func (v *Verifier) SetClock(now func() time.Time) { v.now = now }

type accessTokenClaims struct {
	SessionID     string `json:"sid"`
	EmailVerified bool   `json:"email_verified"`
	jwt.RegisteredClaims
}

// Verify parses and validates rawToken, returning its claims.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Claims, error) {
	if strings.TrimSpace(rawToken) == "" {
		return Claims{}, fmt.Errorf("%w: empty token", ErrInvalidToken)
	}

	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{SigningAlgorithm}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(clockLeeway),
		jwt.WithTimeFunc(func() time.Time { return v.now() }),
	}
	if v.issuer != "" {
		options = append(options, jwt.WithIssuer(v.issuer))
	}
	if v.audience != "" {
		options = append(options, jwt.WithAudience(v.audience))
	}

	claims := &accessTokenClaims{}
	parser := jwt.NewParser(options...)
	token, err := parser.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		kid, _ := token.Header["kid"].(string)
		key, keyErr := v.keys.KeyFor(ctx, kid)
		if keyErr != nil {
			return nil, keyErr
		}
		return key, nil
	})
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !token.Valid {
		return Claims{}, fmt.Errorf("%w: token is not valid", ErrInvalidToken)
	}
	if claims.Subject == "" {
		return Claims{}, fmt.Errorf("%w: sub claim is required", ErrInvalidToken)
	}

	verified := Claims{
		UserID:        claims.Subject,
		SessionID:     claims.SessionID,
		EmailVerified: claims.EmailVerified,
		Issuer:        claims.Issuer,
		Audience:      []string(claims.Audience),
	}
	if claims.ExpiresAt != nil {
		verified.ExpiresAt = claims.ExpiresAt.Time
	}
	if claims.IssuedAt != nil {
		verified.IssuedAt = claims.IssuedAt.Time
	}
	return verified, nil
}

// BearerToken extracts the token from an Authorization header value.
func BearerToken(header string) (string, error) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("%w: authorization header must be a bearer token", ErrInvalidToken)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("%w: empty bearer token", ErrInvalidToken)
	}
	return token, nil
}

func jwtBase64URLDecode(value string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("invalid base64url value: %w", err)
	}
	return decoded, nil
}
