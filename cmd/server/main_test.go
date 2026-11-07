package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/auth"
	"notification_service/internal/config"
	"notification_service/internal/models"
	"notification_service/tests/support"
)

func TestNewLoggerEmitsDocumentedFields(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := newLogger("production", &out)
	logger.Info("hello")
	require.NoError(t, logger.Sync())

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 1)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &entry))
	require.Equal(t, serviceName, entry["service_name"])
	require.Equal(t, "info", entry["severity"])
	require.Equal(t, "hello", entry["message"])
	require.Contains(t, entry, "timestamp")
}

func TestNewLoggerLevelFollowsAppEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		appEnv       string
		debugEnabled bool
	}{
		{name: "development", appEnv: "development", debugEnabled: true},
		{name: "development is case-insensitive", appEnv: "Development", debugEnabled: true},
		{name: "production", appEnv: "production", debugEnabled: false},
		{name: "staging", appEnv: "staging", debugEnabled: false},
		{name: "unset", appEnv: "", debugEnabled: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			logger := newLogger(tc.appEnv, &out)
			require.Equal(t, tc.debugEnabled, logger.Check(zap.DebugLevel, "debug") != nil)
			require.NotNil(t, logger.Check(zap.InfoLevel, "info"), "info logging is always enabled")
		})
	}
}

func TestConfigureGin(t *testing.T) {
	configureGin("production")
	require.Equal(t, gin.ReleaseMode, gin.Mode())

	configureGin("development")
	require.Equal(t, gin.DebugMode, gin.Mode())
}

func TestNewKeyProviderPrefersStaticPEM(t *testing.T) {
	t.Parallel()

	key := support.NewRSAKey(t)
	keyProvider, err := newKeyProvider(config.Config{
		IdentityJWTPublicKey: string(support.PEMPublicKey(t, key)),
		IdentityJWKSURL:      "https://identity.example/.well-known/jwks.json",
	})
	require.NoError(t, err)
	require.IsType(t, &auth.StaticKeyProvider{}, keyProvider)

	resolved, err := keyProvider.KeyFor(context.Background(), "")
	require.NoError(t, err)
	require.True(t, key.PublicKey.Equal(resolved))
}

func TestNewKeyProviderFallsBackToJWKS(t *testing.T) {
	t.Parallel()

	keyProvider, err := newKeyProvider(config.Config{
		IdentityJWKSURL: "https://identity.example/.well-known/jwks.json",
	})
	require.NoError(t, err)
	require.IsType(t, &auth.JWKSProvider{}, keyProvider)
}

func TestNewKeyProviderRejectsMissingSource(t *testing.T) {
	t.Parallel()

	_, err := newKeyProvider(config.Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "IDENTITY_JWKS_URL or IDENTITY_JWT_PUBLIC_KEY is required")
}

func TestNewKeyProviderRejectsUnparsablePEM(t *testing.T) {
	t.Parallel()

	_, err := newKeyProvider(config.Config{IdentityJWTPublicKey: "not a pem document"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "IDENTITY_JWT_PUBLIC_KEY")
}

// fakeMetadata records the topics passed to RefreshMetadata.
type fakeMetadata struct {
	topics []string
	err    error
}

func (f *fakeMetadata) RefreshMetadata(topics ...string) error {
	f.topics = topics
	return f.err
}

func TestKafkaReadyCheckHealthy(t *testing.T) {
	t.Parallel()

	client := &fakeMetadata{}
	check := kafkaReadyCheck(client, models.ConsumedTopics)

	require.Equal(t, "kafka", check.Name)
	require.NoError(t, check.Ping(context.Background()))
	require.Equal(t, models.ConsumedTopics, client.topics)
}

func TestKafkaReadyCheckReportsBrokerFailure(t *testing.T) {
	t.Parallel()

	check := kafkaReadyCheck(&fakeMetadata{err: errors.New("no brokers available")}, models.ConsumedTopics)

	err := check.Ping(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no brokers available")
}

func TestKafkaReadyCheckHonoursCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := &fakeMetadata{}
	err := kafkaReadyCheck(client, models.ConsumedTopics).Ping(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, client.topics, "a cancelled probe must not talk to the brokers")
}
