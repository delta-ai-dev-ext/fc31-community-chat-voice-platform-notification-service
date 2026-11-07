package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification_service/internal/config"
)

func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":            "postgres://notification:secret@localhost:5432/notification_service?sslmode=disable",
		"KAFKA_BROKERS":           "broker-1:9092, broker-2:9092",
		"KAFKA_CONSUMER_GROUP":    "notification_service",
		"RESEND_API_KEY":          "re_test_key",
		"MAIL_FROM_ADDRESS":       "notifications@example.com",
		"IDENTITY_JWT_PUBLIC_KEY": "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	}
}

func loadFrom(env map[string]string) (config.Config, error) {
	return config.Load(func(key string) string { return env[key] })
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := loadFrom(validEnv())
	require.NoError(t, err)

	require.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, cfg.KafkaBrokers)
	require.Equal(t, config.DefaultNotificationEventsTopic, cfg.NotificationEventsTopic)
	require.Equal(t, config.DefaultMentionThrottleWindow, cfg.MentionEmailThrottleWindow)
	require.Equal(t, config.DefaultHTTPPort, cfg.HTTPPort)
	require.Equal(t, config.DefaultShutdownTimeout, cfg.ShutdownTimeout)
	require.Equal(t, ":8080", cfg.Addr())
	require.True(t, cfg.RunMigrations)
}

func TestLoadOverrides(t *testing.T) {
	t.Parallel()
	env := validEnv()
	env["MENTION_EMAIL_THROTTLE_WINDOW"] = "5m"
	env["HTTP_PORT"] = "9000"
	env["SHUTDOWN_TIMEOUT"] = "25s"
	env["NOTIFICATION_EVENTS_TOPIC"] = "custom_notifications"
	env["RUN_MIGRATIONS"] = "false"
	env["IDENTITY_JWKS_URL"] = "https://identity.internal/.well-known/jwks.json"
	env["JWT_ISSUER"] = "identity_service"
	env["JWT_AUDIENCE"] = "platform"

	cfg, err := loadFrom(env)
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, cfg.MentionEmailThrottleWindow)
	require.Equal(t, 9000, cfg.HTTPPort)
	require.Equal(t, 25*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, "custom_notifications", cfg.NotificationEventsTopic)
	require.False(t, cfg.RunMigrations)
	require.Equal(t, "identity_service", cfg.JWTIssuer)
	require.Equal(t, "platform", cfg.JWTAudience)
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(env map[string]string)
		wantVar string
	}{
		{name: "missing database url", mutate: func(env map[string]string) { delete(env, "DATABASE_URL") }, wantVar: "DATABASE_URL"},
		{name: "database url is not a url", mutate: func(env map[string]string) { env["DATABASE_URL"] = "localhost" }, wantVar: "DATABASE_URL"},
		{name: "missing brokers", mutate: func(env map[string]string) { delete(env, "KAFKA_BROKERS") }, wantVar: "KAFKA_BROKERS"},
		{name: "empty broker entry", mutate: func(env map[string]string) { env["KAFKA_BROKERS"] = " , " }, wantVar: "KAFKA_BROKERS"},
		{name: "missing consumer group", mutate: func(env map[string]string) { delete(env, "KAFKA_CONSUMER_GROUP") }, wantVar: "KAFKA_CONSUMER_GROUP"},
		{name: "missing resend key", mutate: func(env map[string]string) { delete(env, "RESEND_API_KEY") }, wantVar: "RESEND_API_KEY"},
		{name: "bad mail from", mutate: func(env map[string]string) { env["MAIL_FROM_ADDRESS"] = "noreply" }, wantVar: "MAIL_FROM_ADDRESS"},
		{name: "bad throttle window", mutate: func(env map[string]string) { env["MENTION_EMAIL_THROTTLE_WINDOW"] = "soon" }, wantVar: "MENTION_EMAIL_THROTTLE_WINDOW"},
		{name: "zero throttle window", mutate: func(env map[string]string) { env["MENTION_EMAIL_THROTTLE_WINDOW"] = "0s" }, wantVar: "MENTION_EMAIL_THROTTLE_WINDOW"},
		{name: "bad http port", mutate: func(env map[string]string) { env["HTTP_PORT"] = "70000" }, wantVar: "HTTP_PORT"},
		{name: "non numeric http port", mutate: func(env map[string]string) { env["HTTP_PORT"] = "http" }, wantVar: "HTTP_PORT"},
		{name: "bad shutdown timeout", mutate: func(env map[string]string) { env["SHUTDOWN_TIMEOUT"] = "later" }, wantVar: "SHUTDOWN_TIMEOUT"},
		{name: "no jwt keys", mutate: func(env map[string]string) { delete(env, "IDENTITY_JWT_PUBLIC_KEY") }, wantVar: "IDENTITY_JWKS_URL"},
		{name: "jwks url not http", mutate: func(env map[string]string) { env["IDENTITY_JWKS_URL"] = "file:///keys" }, wantVar: "IDENTITY_JWKS_URL"},
		{name: "bad run migrations", mutate: func(env map[string]string) { env["RUN_MIGRATIONS"] = "sometimes" }, wantVar: "RUN_MIGRATIONS"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			env := validEnv()
			test.mutate(env)
			_, err := loadFrom(env)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.wantVar)
		})
	}
}

func TestValidateRejectsZeroShutdownTimeout(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		DatabaseURL:                "postgres://localhost/db",
		KafkaBrokers:               []string{"localhost:9092"},
		KafkaConsumerGroup:         "group",
		NotificationEventsTopic:    "topic",
		ResendAPIKey:               "key",
		MailFromAddress:            "a@b.co",
		MentionEmailThrottleWindow: time.Minute,
		HTTPPort:                   8080,
		IdentityJWKSURL:            "https://identity.internal/jwks",
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "SHUTDOWN_TIMEOUT"))
}
