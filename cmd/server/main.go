// Command server is the entrypoint of notification_service: it loads and
// validates the configuration, wires the composition root (PostgreSQL
// repositories, Kafka consumer group and producer, outbox publisher, Resend
// adapter, HTTP API and periodic maintenance loops) and serves traffic until
// SIGINT or SIGTERM triggers the documented graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"notification_service/internal/auth"
	"notification_service/internal/config"
	"notification_service/internal/event"
	"notification_service/internal/handler"
	"notification_service/internal/health"
	"notification_service/internal/metrics"
	"notification_service/internal/models"
	"notification_service/internal/provider"
	"notification_service/internal/repository"
	"notification_service/internal/service"
)

const (
	// serviceName is emitted as the `service_name` field of every log line.
	serviceName = "notification_service"
	// lagRefreshInterval is how often kafka_consumer_lag is refreshed.
	lagRefreshInterval = 15 * time.Second
	// startupPingTimeout bounds the informational Postgres ping at startup.
	startupPingTimeout = 10 * time.Second
	// httpReadHeaderTimeout bounds the header read of an inbound request.
	httpReadHeaderTimeout = 10 * time.Second
)

func main() {
	appEnv := strings.TrimSpace(os.Getenv("APP_ENV"))

	logger := newLogger(appEnv, os.Stdout)
	defer func() { _ = logger.Sync() }()

	if err := run(context.Background(), appEnv, logger); err != nil {
		logger.Error("service stopped with an error",
			zap.String("operation", "run"),
			zap.Error(err),
		)
		_ = logger.Sync()
		os.Exit(1)
	}
}

// run wires every dependency of the service and blocks until a shutdown signal
// arrives or the HTTP server fails. It returns a non-nil error only for startup
// failures and unexpected server failures; a complete graceful shutdown returns
// nil (exit code 0).
func run(ctx context.Context, appEnv string, logger *zap.Logger) error {
	loadLocalEnv(appEnv, logger)

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	configureGin(appEnv)
	observed := metrics.New()

	if cfg.RunMigrations {
		logger.Info("applying embedded schema migrations", zap.String("operation", "migrate"))
		if err := repository.Migrate(cfg.DatabaseURL); err != nil {
			return err
		}
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	// A cold database must not keep the process from serving: /readyz reports the
	// outage until PostgreSQL comes back.
	pingCtx, cancelPing := context.WithTimeout(ctx, startupPingTimeout)
	pingErr := pool.Ping(pingCtx)
	cancelPing()
	if pingErr != nil {
		logger.Warn("postgres is not reachable at startup; readiness reports it until it recovers",
			zap.String("operation", "open_postgres_pool"),
			zap.Error(pingErr),
		)
	}

	notifications := repository.NewNotificationStore(pool)
	preferences := repository.NewPreferenceStore(pool)
	recipients := repository.NewRecipientStore(pool)
	deliveries := repository.NewEmailDeliveryStore(pool)
	windows := repository.NewMentionWindowStore(pool)
	processed := repository.NewProcessedEventStore(pool)
	outbox := repository.NewOutboxStore(pool)

	producer, err := event.NewSaramaProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer closeQuietly(logger, "kafka producer", producer.Close)

	publisher := event.NewOutboxPublisher(event.OutboxPublisherOptions{
		Outbox:        outbox,
		Notifications: notifications,
		Producer:      producer,
		Topic:         cfg.NotificationEventsTopic,
		Logger:        logger,
	})

	sender := provider.NewResendSender(cfg.ResendAPIKey, cfg.MailFromAddress, observed)

	ingest := service.NewIngestService(service.IngestDeps{
		Transactor:            repository.NewPoolTransactor(pool),
		Notifications:         notifications,
		Preferences:           preferences,
		Recipients:            recipients,
		Deliveries:            deliveries,
		Windows:               windows,
		Processed:             processed,
		Outbox:                outbox,
		Publisher:             publisher,
		Sender:                sender,
		Observer:              observed,
		Logger:                logger,
		MailFrom:              cfg.MailFromAddress,
		MentionThrottleWindow: cfg.MentionEmailThrottleWindow,
		MaxEmailAttempts:      service.DefaultMaxEmailAttempts,
		Backoff:               service.DefaultRetryBackoff,
	})

	inbox := service.NewInboxService(notifications, observed, nil)
	preferenceService := service.NewPreferenceService(preferences, nil)

	keyProvider, err := newKeyProvider(cfg)
	if err != nil {
		return err
	}
	verifier := auth.NewVerifier(keyProvider, cfg.JWTIssuer, cfg.JWTAudience)

	brokers, err := newKafkaClients(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer closeQuietly(logger, "kafka client", brokers.Close)

	readiness := health.New(
		health.Check{Name: "postgres", Ping: pool.Ping},
		kafkaReadyCheck(brokers, models.ConsumedTopics),
	)

	lagSource := event.NewSaramaLagSource(brokers.client, brokers.admin, cfg.KafkaConsumerGroup, models.ConsumedTopics)
	lagRefresher := event.NewLagRefresher(lagSource, observed, cfg.KafkaConsumerGroup, models.ConsumedTopics, logger)

	maintenance := event.NewMaintenance(event.MaintenanceOptions{
		Outbox:     publisher,
		Emails:     ingest,
		Processed:  processed,
		Logger:     logger,
		EmailBatch: service.DefaultRetryBatch,
	})

	consumer, err := event.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, models.ConsumedTopics, ingest, logger)
	if err != nil {
		return err
	}
	defer closeQuietly(logger, "kafka consumer group", consumer.Close)

	api := handler.New(handler.Options{
		Inbox:             inbox,
		Preferences:       preferenceService,
		Verifier:          verifier,
		Readiness:         readiness,
		Logger:            logger,
		Metrics:           observed,
		PrometheusHandler: observed.Handler(),
		ServiceName:       serviceName,
	})
	server := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           api.Router(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()

	// Consumer fetch loops, the outbox/email/retention jobs and the lag reporter
	// all belong to runCtx and must be stopped before their dependencies close.
	var background sync.WaitGroup
	background.Add(3)
	go func() {
		defer background.Done()
		if err := consumer.Run(runCtx); err != nil {
			logger.Error("consumer loop stopped",
				zap.String("operation", "consume_events"),
				zap.Error(err),
			)
		}
	}()
	go func() {
		defer background.Done()
		maintenance.Run(runCtx)
	}()
	go func() {
		defer background.Done()
		lagRefresher.Run(runCtx, lagRefreshInterval)
	}()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening",
			zap.String("operation", "serve_http"),
			zap.String("addr", cfg.Addr()),
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("http server: %w", err)
		}
	}()

	signalCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()

	var runErr error
	select {
	case <-signalCtx.Done():
		logger.Info("shutdown signal received", zap.String("operation", "shutdown"))
	case runErr = <-serverErr:
		logger.Error("http server failed; shutting down",
			zap.String("operation", "shutdown"),
			zap.Error(runErr),
		)
	}

	// 1. Fail readiness first so the load balancer stops routing to this replica.
	readiness.BeginShutdown()

	// 2. Stop the HTTP intake and the consumer fetch loops. Offsets of completed
	// messages are already committed; in-flight messages are redelivered.
	stopRun()
	background.Wait()

	// 3. Drain in-flight HTTP requests within the configured budget.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	if err := server.Shutdown(drainCtx); err != nil {
		logger.Warn("http server did not drain within SHUTDOWN_TIMEOUT",
			zap.String("operation", "shutdown"),
			zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
			zap.Error(err),
		)
	}
	cancelDrain()

	// 4. The deferred closes now release the consumer group, the Kafka
	// connections and the PostgreSQL pool (in that order).
	logger.Info("shutdown complete", zap.String("operation", "shutdown"))
	return runErr
}

// loadLocalEnv loads a local .env file outside production. A missing file is not
// an error: production reads the platform environment only (engineering guide §6).
func loadLocalEnv(appEnv string, logger *zap.Logger) {
	if strings.EqualFold(appEnv, "production") {
		logger.Debug("skipping .env loading", zap.String("operation", "load_config"))
		return
	}
	if err := godotenv.Load(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Debug("no local .env file found", zap.String("operation", "load_config"))
			return
		}
		logger.Warn("local .env file could not be loaded",
			zap.String("operation", "load_config"),
			zap.Error(err),
		)
	}
}

// newLogger builds the structured JSON logger of the service. Every line carries
// `timestamp`, `severity` and `service_name`; development logs at debug level
// and every other environment logs at info level (engineering guide §7).
func newLogger(appEnv string, out io.Writer) *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "severity",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	level := zap.InfoLevel
	if strings.EqualFold(appEnv, "development") {
		level = zap.DebugLevel
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(out), level)
	return zap.New(core).With(zap.String("service_name", serviceName))
}

// configureGin selects the gin mode: debug in development, release everywhere
// else so the framework does not print per-request debug output in production.
func configureGin(appEnv string) {
	if strings.EqualFold(appEnv, "development") {
		gin.SetMode(gin.DebugMode)
		return
	}
	gin.SetMode(gin.ReleaseMode)
}

// newKeyProvider builds the identity_service key provider: a static PEM public
// key when one is configured, otherwise the caching JWKS client.
func newKeyProvider(cfg config.Config) (auth.KeyProvider, error) {
	if pemData := strings.TrimSpace(cfg.IdentityJWTPublicKey); pemData != "" {
		staticKeys, err := auth.NewStaticKeyProvider([]byte(pemData))
		if err != nil {
			return nil, fmt.Errorf("load IDENTITY_JWT_PUBLIC_KEY: %w", err)
		}
		return staticKeys, nil
	}
	if jwksURL := strings.TrimSpace(cfg.IdentityJWKSURL); jwksURL != "" {
		return auth.NewJWKSProvider(jwksURL, nil), nil
	}
	return nil, errors.New("IDENTITY_JWKS_URL or IDENTITY_JWT_PUBLIC_KEY is required")
}

// metadataRefresher refreshes broker metadata; sarama.Client satisfies it.
type metadataRefresher interface {
	RefreshMetadata(topics ...string) error
}

// kafkaReadyCheck reports broker reachability for GET /readyz. Refreshing the
// metadata of the consumed topics proves the brokers answer without consuming or
// committing anything.
func kafkaReadyCheck(client metadataRefresher, topics []string) health.Check {
	return health.Check{
		Name: "kafka",
		Ping: func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := client.RefreshMetadata(topics...); err != nil {
				return fmt.Errorf("refresh broker metadata: %w", err)
			}
			return nil
		},
	}
}

// kafkaClients bundles the sarama client and cluster admin shared by the broker
// readiness check and the consumer-lag reporter.
type kafkaClients struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
}

// newKafkaClients connects the shared metadata client of the service.
func newKafkaClients(brokers []string) (*kafkaClients, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0

	client, err := sarama.NewClient(brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("event: create kafka client: %w", err)
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("event: create kafka cluster admin: %w", err)
	}
	return &kafkaClients{client: client, admin: admin}, nil
}

// RefreshMetadata implements metadataRefresher.
func (c *kafkaClients) RefreshMetadata(topics ...string) error {
	return c.client.RefreshMetadata(topics...)
}

// Close releases the broker connections. The sarama cluster admin owns the
// client and closes it too, so the client is never closed twice.
func (c *kafkaClients) Close() error { return c.admin.Close() }

// closeQuietly releases a resource, logging instead of failing on close errors:
// a close failure during shutdown must not change the exit code.
func closeQuietly(logger *zap.Logger, resource string, close func() error) {
	if err := close(); err != nil {
		logger.Warn("closing resource failed",
			zap.String("operation", "shutdown"),
			zap.String("resource", resource),
			zap.Error(err),
		)
	}
}
