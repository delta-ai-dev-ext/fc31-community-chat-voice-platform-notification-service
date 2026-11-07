// Package config loads and validates the service configuration from the
// process environment (12-factor, docs/engineering_guide.md §6).
package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Defaults applied when an optional variable is absent.
const (
	DefaultNotificationEventsTopic = "notification_events"
	DefaultMentionThrottleWindow   = 15 * time.Minute
	DefaultHTTPPort                = 8080
	DefaultShutdownTimeout         = 20 * time.Second
)

// Config is the fully-validated runtime configuration of notification_service.
type Config struct {
	// DatabaseURL is the PostgreSQL connection string.
	DatabaseURL string
	// KafkaBrokers is the Kafka bootstrap broker list.
	KafkaBrokers []string
	// KafkaConsumerGroup is the shared consumer-group id of every consumed topic.
	KafkaConsumerGroup string
	// NotificationEventsTopic is the produced topic for EVT-011.
	NotificationEventsTopic string
	// ResendAPIKey is the Resend secret; it is never logged.
	ResendAPIKey string
	// MailFromAddress is the verified sender identity of notification emails.
	MailFromAddress string
	// MentionEmailThrottleWindow is the per-recipient mention email window.
	MentionEmailThrottleWindow time.Duration
	// HTTPPort is the listen port of the REST API and the probes.
	HTTPPort int
	// ShutdownTimeout bounds the graceful drain.
	ShutdownTimeout time.Duration

	// IdentityJWKSURL is the JWKS endpoint of identity_service (optional when a
	// static public key is configured).
	IdentityJWKSURL string
	// IdentityJWTPublicKey is a PEM-encoded RSA public key (optional when a
	// JWKS endpoint is configured).
	IdentityJWTPublicKey string
	// JWTIssuer is the expected `iss` claim; empty disables the check.
	JWTIssuer string
	// JWTAudience is the expected `aud` claim; empty disables the check.
	JWTAudience string
	// RunMigrations enables the startup migration run.
	RunMigrations bool
}

// Validate checks the invariants of the configuration.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	parsed, err := url.Parse(c.DatabaseURL)
	if err != nil || parsed.Scheme == "" {
		return fmt.Errorf("DATABASE_URL must be a connection URL")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	for _, broker := range c.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("KAFKA_BROKERS contains an empty broker")
		}
	}
	if strings.TrimSpace(c.KafkaConsumerGroup) == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP is required")
	}
	if strings.TrimSpace(c.NotificationEventsTopic) == "" {
		return fmt.Errorf("NOTIFICATION_EVENTS_TOPIC is required")
	}
	if strings.TrimSpace(c.ResendAPIKey) == "" {
		return fmt.Errorf("RESEND_API_KEY is required")
	}
	if !strings.Contains(c.MailFromAddress, "@") {
		return fmt.Errorf("MAIL_FROM_ADDRESS must be an email address")
	}
	if c.MentionEmailThrottleWindow <= 0 {
		return fmt.Errorf("MENTION_EMAIL_THROTTLE_WINDOW must be positive")
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("HTTP_PORT must be between 1 and 65535")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be positive")
	}
	if strings.TrimSpace(c.IdentityJWKSURL) == "" && strings.TrimSpace(c.IdentityJWTPublicKey) == "" {
		return fmt.Errorf("IDENTITY_JWKS_URL or IDENTITY_JWT_PUBLIC_KEY is required")
	}
	if strings.TrimSpace(c.IdentityJWKSURL) != "" {
		parsed, err := url.Parse(c.IdentityJWKSURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("IDENTITY_JWKS_URL must be an http(s) URL")
		}
	}
	return nil
}

// Load reads the configuration through getenv and validates it.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:                strings.TrimSpace(getenv("DATABASE_URL")),
		KafkaBrokers:               splitList(getenv("KAFKA_BROKERS")),
		KafkaConsumerGroup:         strings.TrimSpace(getenv("KAFKA_CONSUMER_GROUP")),
		NotificationEventsTopic:    defaultString(getenv("NOTIFICATION_EVENTS_TOPIC"), DefaultNotificationEventsTopic),
		ResendAPIKey:               strings.TrimSpace(getenv("RESEND_API_KEY")),
		MailFromAddress:            strings.TrimSpace(getenv("MAIL_FROM_ADDRESS")),
		MentionEmailThrottleWindow: DefaultMentionThrottleWindow,
		HTTPPort:                   DefaultHTTPPort,
		ShutdownTimeout:            DefaultShutdownTimeout,
		IdentityJWKSURL:            strings.TrimSpace(getenv("IDENTITY_JWKS_URL")),
		IdentityJWTPublicKey:       strings.TrimSpace(getenv("IDENTITY_JWT_PUBLIC_KEY")),
		JWTIssuer:                  strings.TrimSpace(getenv("JWT_ISSUER")),
		JWTAudience:                strings.TrimSpace(getenv("JWT_AUDIENCE")),
		RunMigrations:              true,
	}

	if raw := strings.TrimSpace(getenv("MENTION_EMAIL_THROTTLE_WINDOW")); raw != "" {
		window, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("MENTION_EMAIL_THROTTLE_WINDOW: %w", err)
		}
		cfg.MentionEmailThrottleWindow = window
	}
	if raw := strings.TrimSpace(getenv("HTTP_PORT")); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("HTTP_PORT: %w", err)
		}
		cfg.HTTPPort = port
	}
	if raw := strings.TrimSpace(getenv("SHUTDOWN_TIMEOUT")); raw != "" {
		timeout, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err)
		}
		cfg.ShutdownTimeout = timeout
	}
	if raw := strings.TrimSpace(getenv("RUN_MIGRATIONS")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("RUN_MIGRATIONS: %w", err)
		}
		cfg.RunMigrations = enabled
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Addr is the listen address of the HTTP server.
func (c Config) Addr() string { return fmt.Sprintf(":%d", c.HTTPPort) }

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func defaultString(value, fallback string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return fallback
}
