package repository_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"notification_service/internal/models"
	"notification_service/internal/repository"
)

// fakeRow is a pgx.Row returning fixed values.
type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("fakeRow: column count mismatch")
	}
	for i := range dest {
		if err := assign(dest[i], r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

// fakeRows is a pgx.Rows over a slice of rows.
type fakeRows struct {
	rows   [][]any
	index  int
	err    error
	closed bool
}

func newFakeRows(rows ...[]any) *fakeRows { return &fakeRows{rows: rows, index: -1} }

func (r *fakeRows) Close() { r.closed = true }
func (r *fakeRows) Err() error {
	return r.err
}
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT 1") }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool {
	if r.index+1 >= len(r.rows) {
		return false
	}
	r.index++
	return true
}
func (r *fakeRows) Scan(dest ...any) error {
	if r.index < 0 || r.index >= len(r.rows) {
		return errors.New("fakeRows: scan before Next")
	}
	if len(dest) != len(r.rows[r.index]) {
		return errors.New("fakeRows: column count mismatch")
	}
	for i := range dest {
		if err := assign(dest[i], r.rows[r.index][i]); err != nil {
			return err
		}
	}
	return nil
}
func (r *fakeRows) Values() ([]any, error) { return r.rows[r.index], nil }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map   { return nil }

// assign copies value into the scan destination dest.
func assign(dest any, value any) error {
	target := reflect.ValueOf(dest)
	if target.Kind() != reflect.Ptr || target.IsNil() {
		return fmt.Errorf("assign: destination %T is not a non-nil pointer", dest)
	}
	slot := target.Elem()
	if value == nil {
		slot.Set(reflect.Zero(slot.Type()))
		return nil
	}
	source := reflect.ValueOf(value)
	switch {
	case source.Type().AssignableTo(slot.Type()):
		slot.Set(source)
		return nil
	case source.Type().ConvertibleTo(slot.Type()):
		slot.Set(source.Convert(slot.Type()))
		return nil
	default:
		return fmt.Errorf("assign: cannot assign %T to %s", value, slot.Type())
	}
}

// fakeQuerier records the SQL it receives and replays canned responses.
type fakeQuerier struct {
	mu       sync.Mutex
	queries  []string
	rowSQL   string
	row      pgx.Row
	rows     pgx.Rows
	rowsErr  error
	execSQL  string
	execArgs []any
	tag      pgconn.CommandTag
	execErr  error
}

func (q *fakeQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queries = append(q.queries, sql)
	q.execArgs = args
	if q.rowsErr != nil {
		return nil, q.rowsErr
	}
	if q.rows == nil {
		return newFakeRows(), nil
	}
	return q.rows, nil
}

func (q *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queries = append(q.queries, sql)
	q.rowSQL = sql
	q.execArgs = args
	if q.row == nil {
		return fakeRow{err: pgx.ErrNoRows}
	}
	return q.row
}

func (q *fakeQuerier) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queries = append(q.queries, sql)
	q.execSQL = sql
	q.execArgs = args
	if q.execErr != nil {
		return pgconn.CommandTag{}, q.execErr
	}
	if q.tag.String() == "" {
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	return q.tag, nil
}

func (q *fakeQuerier) lastSQL() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queries) == 0 {
		return ""
	}
	return q.queries[len(q.queries)-1]
}

var _ repository.Querier = (*fakeQuerier)(nil)

func TestRecipientUpsertAppliesOnlyNewerVersions(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	projection := models.RecipientProjection{
		UserID: "11111111-1111-4111-8111-111111111111", Email: "a@example.com", Username: "alice",
		DisplayName: "Alice", EmailVerified: true, Status: models.AccountActive, AccountVersion: 7,
	}

	t.Run("newer version is applied", func(t *testing.T) {
		t.Parallel()
		db := &fakeQuerier{row: fakeRow{values: []any{projection.UserID}}}
		applied, err := repository.NewRecipientStore(db).Upsert(context.Background(), projection, now)
		require.NoError(t, err)
		require.True(t, applied)
		require.Contains(t, db.lastSQL(), "recipient_projections.account_version < EXCLUDED.account_version",
			"the staleness rule is enforced by the upsert predicate")
		require.Equal(t, []any{
			projection.UserID, projection.Email, projection.Username, projection.DisplayName,
			true, "active", 7, now,
		}, db.execArgs)
	})

	t.Run("stale version is ignored", func(t *testing.T) {
		t.Parallel()
		db := &fakeQuerier{row: fakeRow{err: pgx.ErrNoRows}}
		applied, err := repository.NewRecipientStore(db).Upsert(context.Background(), projection, now)
		require.NoError(t, err)
		require.False(t, applied, "no row updated means the snapshot was stale")
	})

	t.Run("database failure", func(t *testing.T) {
		t.Parallel()
		db := &fakeQuerier{row: fakeRow{err: errors.New("boom")}}
		_, err := repository.NewRecipientStore(db).Upsert(context.Background(), projection, now)
		require.Error(t, err)
	})
}

func TestRecipientGet(t *testing.T) {
	t.Parallel()
	updated := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	db := &fakeQuerier{row: fakeRow{values: []any{
		"11111111-1111-4111-8111-111111111111", "a@example.com", "alice", "Alice", true, "deactivated", 3, updated,
	}}}

	projection, err := repository.NewRecipientStore(db).Get(context.Background(), "11111111-1111-4111-8111-111111111111")
	require.NoError(t, err)
	require.Equal(t, models.AccountDeactivated, projection.Status)
	require.Equal(t, 3, projection.AccountVersion)
	require.True(t, projection.UpdatedAt.Equal(updated))

	missing := &fakeQuerier{}
	_, err = repository.NewRecipientStore(missing).Get(context.Background(), "unknown")
	require.ErrorIs(t, err, repository.ErrNotFound)
}

func TestNotificationInsertGetAndList(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	columns := []any{"n1", "u1", "mention", "title", "summary", "/channels/c/messages/m", createdAt, nil}
	db := &fakeQuerier{row: fakeRow{values: columns}}
	store := repository.NewNotificationStore(db)

	notification, err := store.Insert(context.Background(), models.NewNotification{
		RecipientUserID: "u1", Kind: models.KindMention, Title: "title", Summary: "summary", Link: "/channels/c/messages/m",
	}, createdAt)
	require.NoError(t, err)
	require.Equal(t, "n1", notification.ID)
	require.Equal(t, models.KindMention, notification.Kind)
	require.Nil(t, notification.ReadAt)
	require.Contains(t, db.lastSQL(), "INSERT INTO notifications")

	got, err := store.Get(context.Background(), "u1", "n1")
	require.NoError(t, err)
	require.Equal(t, "n1", got.ID)
	require.Contains(t, db.lastSQL(), "WHERE recipient_user_id = $1 AND id = $2")

	_, err = repository.NewNotificationStore(&fakeQuerier{}).Get(context.Background(), "u1", "missing")
	require.ErrorIs(t, err, repository.ErrNotFound)

	byID, err := store.GetByID(context.Background(), "n1")
	require.NoError(t, err)
	require.Equal(t, "n1", byID.ID)

	readAt := createdAt.Add(time.Hour)
	listDB := &fakeQuerier{rows: newFakeRows(columns, []any{"n2", "u1", "appeal_decided", "t", "s", "/l", createdAt, &readAt})}
	listStore := repository.NewNotificationStore(listDB)
	page, err := listStore.List(context.Background(), models.ListOptions{
		RecipientUserID: "u1", UnreadOnly: true, Limit: 5,
		Cursor: &models.Cursor{CreatedAt: createdAt, ID: "n1"},
	})
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.NotNil(t, page[1].ReadAt)
	sql := listDB.lastSQL()
	require.Contains(t, sql, "ORDER BY created_at DESC, id DESC")
	require.Contains(t, sql, "(created_at, id) < ($3::timestamptz, $4::uuid)")
	args := listDB.execArgs
	require.Equal(t, "u1", args[0])
	require.Equal(t, true, args[1])
	require.NotNil(t, args[2])
	require.Equal(t, 5, args[4])

	listErrDB := &fakeQuerier{rowsErr: errors.New("boom")}
	_, err = repository.NewNotificationStore(listErrDB).List(context.Background(), models.ListOptions{Limit: 2})
	require.Error(t, err)
}

func TestNotificationUnreadCount(t *testing.T) {
	t.Parallel()
	db := &fakeQuerier{row: fakeRow{values: []any{4}}}
	count, err := repository.NewNotificationStore(db).UnreadCount(context.Background(), "u1")
	require.NoError(t, err)
	require.Equal(t, 4, count)
	require.Contains(t, db.lastSQL(), "read_at IS NULL")

	_, err = repository.NewNotificationStore(&fakeQuerier{row: fakeRow{err: errors.New("boom")}}).
		UnreadCount(context.Background(), "u1")
	require.Error(t, err)
}

func TestNotificationMarkRead(t *testing.T) {
	t.Parallel()
	readAt := time.Date(2026, 2, 1, 11, 0, 0, 0, time.UTC)

	t.Run("unread notification is marked", func(t *testing.T) {
		t.Parallel()
		db := &fakeQuerier{row: fakeRow{values: []any{"mention"}}}
		outcome, err := repository.NewNotificationStore(db).MarkRead(context.Background(), "u1", "n1", readAt)
		require.NoError(t, err)
		require.True(t, outcome.Changed)
		require.Equal(t, models.KindMention, outcome.Kind)
		require.Contains(t, db.lastSQL(), "WHERE id = $1 AND recipient_user_id = $2 AND read_at IS NULL")
	})

	t.Run("already read notification is idempotent", func(t *testing.T) {
		t.Parallel()
		db := &fakeQuerier{}
		db.row = nil // first QueryRow returns ErrNoRows
		outcome, err := repository.NewNotificationStore(&twoStepQuerier{
			first:  fakeRow{err: pgx.ErrNoRows},
			second: fakeRow{values: []any{"appeal_decided"}},
		}).MarkRead(context.Background(), "u1", "n1", readAt)
		require.NoError(t, err)
		require.False(t, outcome.Changed)
		require.Equal(t, models.KindAppealDecided, outcome.Kind)
	})

	t.Run("foreign or missing notification", func(t *testing.T) {
		t.Parallel()
		_, err := repository.NewNotificationStore(&twoStepQuerier{
			first:  fakeRow{err: pgx.ErrNoRows},
			second: fakeRow{err: pgx.ErrNoRows},
		}).MarkRead(context.Background(), "u1", "n1", readAt)
		require.ErrorIs(t, err, repository.ErrNotFound)
	})

	t.Run("database failure", func(t *testing.T) {
		t.Parallel()
		_, err := repository.NewNotificationStore(&fakeQuerier{row: fakeRow{err: errors.New("boom")}}).
			MarkRead(context.Background(), "u1", "n1", readAt)
		require.Error(t, err)
		require.False(t, errors.Is(err, repository.ErrNotFound))
	})
}

// twoStepQuerier returns first on the first QueryRow call and second afterwards.
type twoStepQuerier struct {
	mu     sync.Mutex
	calls  int
	first  pgx.Row
	second pgx.Row
}

func (q *twoStepQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return newFakeRows(), nil
}

func (q *twoStepQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	if q.calls == 1 {
		return q.first
	}
	return q.second
}

func (q *twoStepQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 0"), nil
}

func TestNotificationMarkAllRead(t *testing.T) {
	t.Parallel()
	db := &fakeQuerier{rows: newFakeRows([]any{"mention"}, []any{"mention"}, []any{"moderation_notice"})}
	counts, err := repository.NewNotificationStore(db).MarkAllRead(context.Background(), "u1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, map[models.Kind]int{models.KindMention: 2, models.KindModerationNotice: 1}, counts)

	_, err = repository.NewNotificationStore(&fakeQuerier{rowsErr: errors.New("boom")}).
		MarkAllRead(context.Background(), "u1", time.Now().UTC())
	require.Error(t, err)
}

func TestPreferenceStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	prefs := models.DefaultPreferences()
	prefs.Mention.Email = false

	db := &fakeQuerier{row: fakeRow{values: []any{true, false, true, true, true, true, true, true, true, true}}}
	stored, err := repository.NewPreferenceStore(db).Get(context.Background(), "u1")
	require.NoError(t, err)
	require.False(t, stored.Mention.Email)
	require.True(t, stored.Mention.InApp)

	_, err = repository.NewPreferenceStore(&fakeQuerier{}).Get(context.Background(), "u1")
	require.ErrorIs(t, err, repository.ErrNotFound)

	upsertDB := &fakeQuerier{}
	saved, err := repository.NewPreferenceStore(upsertDB).Upsert(context.Background(), "u1", prefs, now)
	require.NoError(t, err)
	require.Equal(t, prefs, saved)
	require.Contains(t, upsertDB.lastSQL(), "ON CONFLICT (user_id) DO UPDATE")
	require.Equal(t, 12, len(upsertDB.execArgs))

	_, err = repository.NewPreferenceStore(&fakeQuerier{execErr: errors.New("boom")}).
		Upsert(context.Background(), "u1", prefs, now)
	require.Error(t, err)
}

func TestEmailDeliveryStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	messageID := "resend-1"
	columns := []any{"d1", "n1", "mention", "u1", "sent", &messageID, 1, nil, nil, now, now}

	db := &fakeQuerier{row: fakeRow{values: columns}}
	inserted, err := repository.NewEmailDeliveryStore(db).Insert(context.Background(), models.EmailDelivery{
		NotificationID: "n1", Kind: models.KindMention, RecipientUserID: "u1", Status: models.DeliveryPending, CreatedAt: now,
	})
	require.NoError(t, err)
	require.Equal(t, models.DeliverySent, inserted.Status)
	require.Equal(t, 1, inserted.Attempts)
	require.Contains(t, db.lastSQL(), "INSERT INTO email_deliveries")

	updateDB := &fakeQuerier{}
	require.NoError(t, repository.NewEmailDeliveryStore(updateDB).Update(context.Background(), inserted, now))
	require.Contains(t, updateDB.lastSQL(), "UPDATE email_deliveries SET")
	require.Error(t, repository.NewEmailDeliveryStore(&fakeQuerier{execErr: errors.New("boom")}).Update(context.Background(), inserted, now))

	dueDB := &fakeQuerier{rows: newFakeRows(columns)}
	due, err := repository.NewEmailDeliveryStore(dueDB).ListDue(context.Background(), now, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Contains(t, dueDB.lastSQL(), "next_attempt_at <= $1")

	_, err = repository.NewEmailDeliveryStore(&fakeQuerier{rowsErr: errors.New("boom")}).ListDue(context.Background(), now, 10)
	require.Error(t, err)

	_, err = repository.NewEmailDeliveryStore(&fakeQuerier{row: fakeRow{err: errors.New("boom")}}).
		Insert(context.Background(), models.EmailDelivery{})
	require.Error(t, err)

	_, err = repository.NewEmailDeliveryStore(&fakeQuerier{rows: newFakeRows([]any{"only-one-column"})}).ListDue(context.Background(), now, 10)
	require.Error(t, err)
}

func TestProcessedEventStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)

	inserted, err := repository.NewProcessedEventStore(&fakeQuerier{tag: pgconn.NewCommandTag("INSERT 0 1")}).
		Record(context.Background(), "e1", models.TopicSocialEvents, now)
	require.NoError(t, err)
	require.True(t, inserted)

	duplicate, err := repository.NewProcessedEventStore(&fakeQuerier{tag: pgconn.NewCommandTag("INSERT 0 0")}).
		Record(context.Background(), "e1", models.TopicSocialEvents, now)
	require.NoError(t, err)
	require.False(t, duplicate, "a conflicting insert is a duplicate delivery")

	_, err = repository.NewProcessedEventStore(&fakeQuerier{execErr: errors.New("boom")}).
		Record(context.Background(), "e1", models.TopicSocialEvents, now)
	require.Error(t, err)

	removed, err := repository.NewProcessedEventStore(&fakeQuerier{tag: pgconn.NewCommandTag("DELETE 12")}).
		Prune(context.Background(), now)
	require.NoError(t, err)
	require.Equal(t, int64(12), removed)

	_, err = repository.NewProcessedEventStore(&fakeQuerier{execErr: errors.New("boom")}).Prune(context.Background(), now)
	require.Error(t, err)
}

func TestMentionWindowStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)

	db := &fakeQuerier{tag: pgconn.NewCommandTag("INSERT 0 1")}
	claimed, err := repository.NewMentionWindowStore(db).TryClaim(context.Background(), "u1", now, 15*time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Contains(t, db.lastSQL(), "last_email_sent_at <= $3")
	require.Equal(t, now.Add(-15*time.Minute), db.execArgs[2], "the window boundary is computed from the injected clock")

	blocked, err := repository.NewMentionWindowStore(&fakeQuerier{tag: pgconn.NewCommandTag("INSERT 0 0")}).
		TryClaim(context.Background(), "u1", now, 15*time.Minute)
	require.NoError(t, err)
	require.False(t, blocked)

	_, err = repository.NewMentionWindowStore(&fakeQuerier{execErr: errors.New("boom")}).
		TryClaim(context.Background(), "u1", now, 15*time.Minute)
	require.Error(t, err)
}

func TestOutboxStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)

	enqueueDB := &fakeQuerier{row: fakeRow{values: []any{"o1"}}}
	outboxID, err := repository.NewOutboxStore(enqueueDB).Enqueue(context.Background(), "n1", now)
	require.NoError(t, err)
	require.Equal(t, "o1", outboxID)
	require.Contains(t, enqueueDB.lastSQL(), "INSERT INTO notification_outbox")

	_, err = repository.NewOutboxStore(&fakeQuerier{row: fakeRow{err: errors.New("boom")}}).Enqueue(context.Background(), "n1", now)
	require.Error(t, err)

	listDB := &fakeQuerier{rows: newFakeRows([]any{"o1", "n1", nil, now}, []any{"o2", "n2", nil, now})}
	entries, err := repository.NewOutboxStore(listDB).ListPending(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Nil(t, entries[0].PublishedAt)
	require.Contains(t, listDB.lastSQL(), "WHERE published_at IS NULL")

	_, err = repository.NewOutboxStore(&fakeQuerier{rowsErr: errors.New("boom")}).ListPending(context.Background(), 10)
	require.Error(t, err)

	markDB := &fakeQuerier{}
	require.NoError(t, repository.NewOutboxStore(markDB).MarkPublished(context.Background(), "o1", now))
	require.Contains(t, markDB.lastSQL(), "SET published_at = $2")
	require.Error(t, repository.NewOutboxStore(&fakeQuerier{execErr: errors.New("boom")}).MarkPublished(context.Background(), "o1", now))
}

func TestWithTxUsesTheTransactionFromContext(t *testing.T) {
	t.Parallel()
	pool := &fakeQuerier{row: fakeRow{values: []any{2}}}
	tx := &fakeQuerier{row: fakeRow{values: []any{9}}}

	ctx := repository.WithTx(context.Background(), tx)
	count, err := repository.NewNotificationStore(pool).UnreadCount(ctx, "u1")
	require.NoError(t, err)
	require.Equal(t, 9, count, "the repository must use the transaction carried by the context")
	require.NotEmpty(t, tx.lastSQL())
	require.Empty(t, pool.lastSQL())
}
