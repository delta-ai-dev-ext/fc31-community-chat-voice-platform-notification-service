package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"notification_service/internal/models"
	"notification_service/internal/repository"
	"notification_service/internal/service"
	"notification_service/tests/mocks"
)

// recordingObserver counts observer calls without failing on unexpected ones.
type recordingObserver struct {
	created   map[models.Kind]int
	read      map[models.Kind]int
	emails    map[string]int
	throttled map[models.Kind]int
	eventErrs int
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{
		created:   map[models.Kind]int{},
		read:      map[models.Kind]int{},
		emails:    map[string]int{},
		throttled: map[models.Kind]int{},
	}
}

func (o *recordingObserver) NotificationCreated(kind models.Kind) { o.created[kind]++ }

func (o *recordingObserver) NotificationRead(kind models.Kind, n int) { o.read[kind] += n }

func (o *recordingObserver) EmailResult(kind models.Kind, result models.DeliveryStatus) {
	o.emails[string(kind)+"/"+string(result)]++
}

func (o *recordingObserver) EmailThrottled(kind models.Kind) { o.throttled[kind]++ }

func (o *recordingObserver) EventError(string, string) { o.eventErrs++ }

func fixedClock(at time.Time) service.Clock {
	return func() time.Time { return at }
}

func notification(id string, kind models.Kind, createdAt time.Time, readAt *time.Time) models.Notification {
	return models.Notification{
		ID:              id,
		RecipientUserID: "user-1",
		Kind:            kind,
		Title:           "title-" + id,
		Summary:         "summary",
		Link:            "/link/" + id,
		CreatedAt:       createdAt,
		ReadAt:          readAt,
	}
}

func TestInboxListDefaultsAndUnreadCount(t *testing.T) {
	t.Parallel()
	repo := &mocks.NotificationRepository{}
	inbox := service.NewInboxService(repo, newRecordingObserver(), fixedClock(time.Now().UTC()))
	now := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)

	repo.On("List", mock.Anything, mock.MatchedBy(func(opts models.ListOptions) bool {
		return opts.RecipientUserID == "user-1" && !opts.UnreadOnly && opts.Limit == models.DefaultPageLimit+1 && opts.Cursor == nil
	})).Return([]models.Notification{notification("n1", models.KindMention, now, nil)}, nil).Once()
	repo.On("UnreadCount", mock.Anything, "user-1").Return(4, nil).Once()

	page, err := inbox.List(context.Background(), "user-1", false, 0, "")
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Nil(t, page.NextCursor)
	require.Equal(t, 4, page.UnreadCount, "unread count is independent of the returned page")
}

func TestInboxListPagination(t *testing.T) {
	t.Parallel()
	repo := &mocks.NotificationRepository{}
	inbox := service.NewInboxService(repo, newRecordingObserver(), fixedClock(time.Now().UTC()))

	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	rows := []models.Notification{
		notification("n3", models.KindMention, base.Add(2*time.Minute), nil),
		notification("n2", models.KindMention, base.Add(time.Minute), nil),
		notification("n1", models.KindMention, base, nil),
	}

	t.Run("next page exists", func(t *testing.T) {
		repo.On("List", mock.Anything, mock.MatchedBy(func(opts models.ListOptions) bool {
			return opts.Limit == 3 && opts.UnreadOnly
		})).Return(rows, nil).Once()
		repo.On("UnreadCount", mock.Anything, "user-1").Return(3, nil).Once()

		page, err := inbox.List(context.Background(), "user-1", true, 2, "")
		require.NoError(t, err)
		require.Len(t, page.Items, 2)
		require.NotNil(t, page.NextCursor)

		cursor, err := models.ParseCursor(*page.NextCursor)
		require.NoError(t, err)
		require.Equal(t, "n2", cursor.ID)
		require.True(t, cursor.CreatedAt.Equal(rows[1].CreatedAt))
		require.Equal(t, 3, page.UnreadCount)
	})

	t.Run("exact page has no cursor", func(t *testing.T) {
		repo.On("List", mock.Anything, mock.Anything).Return(rows[:2], nil).Once()
		repo.On("UnreadCount", mock.Anything, "user-1").Return(0, nil).Once()

		page, err := inbox.List(context.Background(), "user-1", false, 2, "")
		require.NoError(t, err)
		require.Len(t, page.Items, 2)
		require.Nil(t, page.NextCursor)
	})

	t.Run("empty inbox", func(t *testing.T) {
		repo.On("List", mock.Anything, mock.Anything).Return([]models.Notification(nil), nil).Once()
		repo.On("UnreadCount", mock.Anything, "user-1").Return(0, nil).Once()

		page, err := inbox.List(context.Background(), "user-1", false, 0, "")
		require.NoError(t, err)
		require.Empty(t, page.Items)
		require.Equal(t, 0, page.UnreadCount)

		encoded, err := json.Marshal(page)
		require.NoError(t, err)
		require.JSONEq(t, `{"items":[],"next_cursor":null,"unread_count":0}`, string(encoded))
	})
}

func TestInboxListCursorPassthrough(t *testing.T) {
	t.Parallel()
	repo := &mocks.NotificationRepository{}
	inbox := service.NewInboxService(repo, newRecordingObserver(), fixedClock(time.Now().UTC()))

	position := models.Cursor{CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), ID: "n7"}
	repo.On("List", mock.Anything, mock.MatchedBy(func(opts models.ListOptions) bool {
		return opts.Cursor != nil && opts.Cursor.ID == "n7" && opts.Cursor.CreatedAt.Equal(position.CreatedAt)
	})).Return([]models.Notification{}, nil).Once()
	repo.On("UnreadCount", mock.Anything, "user-1").Return(0, nil).Once()

	_, err := inbox.List(context.Background(), "user-1", false, 10, position.Encode())
	require.NoError(t, err)
	repo.AssertNumberOfCalls(t, "List", 1)
}

func TestInboxListValidationErrors(t *testing.T) {
	t.Parallel()
	repo := &mocks.NotificationRepository{}
	inbox := service.NewInboxService(repo, newRecordingObserver(), fixedClock(time.Now().UTC()))

	tests := []struct {
		name     string
		limit    int
		cursor   string
		wantCode string
	}{
		{name: "limit too large", limit: 101, wantCode: models.CodeInvalidLimit},
		{name: "limit negative", limit: -3, wantCode: models.CodeInvalidLimit},
		{name: "bad cursor", limit: 0, cursor: "!!!not-base64", wantCode: models.CodeInvalidCursor},
		{name: "cursor without id", limit: 0, cursor: "MTIzOg", wantCode: models.CodeInvalidCursor},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := inbox.List(context.Background(), "user-1", false, test.limit, test.cursor)
			require.Error(t, err)
			var apiErr *models.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, test.wantCode, apiErr.Code)
			require.Equal(t, 400, apiErr.Status)
		})
	}
	repo.AssertNotCalled(t, "List", mock.Anything, mock.Anything)
}

func TestInboxListRepositoryError(t *testing.T) {
	t.Parallel()
	repo := &mocks.NotificationRepository{}
	inbox := service.NewInboxService(repo, nil, nil)
	repo.On("List", mock.Anything, mock.Anything).Return(([]models.Notification)(nil), errors.New("db down")).Once()

	_, err := inbox.List(context.Background(), "user-1", false, 5, "")
	require.Error(t, err)
	var apiErr *models.APIError
	require.False(t, errors.As(err, &apiErr), "infrastructure errors must not become 4xx API errors")
}

func TestInboxListUnreadCountError(t *testing.T) {
	t.Parallel()
	repo := &mocks.NotificationRepository{}
	inbox := service.NewInboxService(repo, nil, nil)
	repo.On("List", mock.Anything, mock.Anything).Return([]models.Notification{}, nil).Once()
	repo.On("UnreadCount", mock.Anything, "user-1").Return(0, errors.New("db down")).Once()

	_, err := inbox.List(context.Background(), "user-1", false, 5, "")
	require.Error(t, err)
}

func TestInboxMarkRead(t *testing.T) {
	t.Parallel()
	notificationID := "22222222-2222-4222-8222-222222222222"
	now := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)

	t.Run("marks unread notification", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		observer := newRecordingObserver()
		inbox := service.NewInboxService(repo, observer, fixedClock(now))
		repo.On("MarkRead", mock.Anything, "user-1", notificationID, now).
			Return(repository.ReadOutcome{Kind: models.KindMention, Changed: true}, nil).Once()

		require.NoError(t, inbox.MarkRead(context.Background(), "user-1", notificationID))
		require.Equal(t, 1, observer.read[models.KindMention])
	})

	t.Run("already read is idempotent", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		observer := newRecordingObserver()
		inbox := service.NewInboxService(repo, observer, fixedClock(now))
		repo.On("MarkRead", mock.Anything, "user-1", notificationID, now).
			Return(repository.ReadOutcome{Kind: models.KindMention, Changed: false}, nil).Once()

		require.NoError(t, inbox.MarkRead(context.Background(), "user-1", notificationID))
		require.Empty(t, observer.read, "no read transition is counted for an already-read notification")
	})

	t.Run("missing or foreign notification", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		inbox := service.NewInboxService(repo, nil, fixedClock(now))
		repo.On("MarkRead", mock.Anything, "user-1", notificationID, now).
			Return(repository.ReadOutcome{}, repository.ErrNotFound).Once()

		err := inbox.MarkRead(context.Background(), "user-1", notificationID)
		require.ErrorIs(t, err, models.ErrNotificationNotFound)
		var apiErr *models.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, 404, apiErr.Status)
	})

	t.Run("malformed id", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		inbox := service.NewInboxService(repo, nil, fixedClock(now))

		err := inbox.MarkRead(context.Background(), "user-1", "not-a-uuid")
		var apiErr *models.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, models.CodeInvalidNotificationID, apiErr.Code)
		repo.AssertNotCalled(t, "MarkRead", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("repository failure", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		inbox := service.NewInboxService(repo, nil, fixedClock(now))
		repo.On("MarkRead", mock.Anything, "user-1", notificationID, now).
			Return(repository.ReadOutcome{}, errors.New("db down")).Once()

		err := inbox.MarkRead(context.Background(), "user-1", notificationID)
		require.Error(t, err)
		var apiErr *models.APIError
		require.False(t, errors.As(err, &apiErr))
	})
}

func TestInboxMarkAllRead(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)

	t.Run("counts transitions per kind", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		observer := newRecordingObserver()
		inbox := service.NewInboxService(repo, observer, fixedClock(now))
		repo.On("MarkAllRead", mock.Anything, "user-1", now).Return(map[models.Kind]int{
			models.KindMention:          2,
			models.KindModerationNotice: 1,
		}, nil).Once()

		require.NoError(t, inbox.MarkAllRead(context.Background(), "user-1"))
		require.Equal(t, 2, observer.read[models.KindMention])
		require.Equal(t, 1, observer.read[models.KindModerationNotice])
	})

	t.Run("nothing unread", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		observer := newRecordingObserver()
		inbox := service.NewInboxService(repo, observer, fixedClock(now))
		repo.On("MarkAllRead", mock.Anything, "user-1", now).Return(map[models.Kind]int{}, nil).Once()

		require.NoError(t, inbox.MarkAllRead(context.Background(), "user-1"))
		require.Empty(t, observer.read)
	})

	t.Run("repository failure", func(t *testing.T) {
		repo := &mocks.NotificationRepository{}
		inbox := service.NewInboxService(repo, nil, nil)
		repo.On("MarkAllRead", mock.Anything, "user-1", mock.Anything).Return((map[models.Kind]int)(nil), errors.New("db down")).Once()

		require.Error(t, inbox.MarkAllRead(context.Background(), "user-1"))
	})
}
