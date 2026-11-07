package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification_service/internal/models"
	"notification_service/internal/repository"
)

// stubRecipients is an in-memory RecipientRepository for mapping tests.
type stubRecipients struct {
	projections map[string]models.RecipientProjection
}

func (s stubRecipients) Upsert(context.Context, models.RecipientProjection, time.Time) (bool, error) {
	return false, nil
}

func (s stubRecipients) Get(_ context.Context, userID string) (models.RecipientProjection, error) {
	if projection, ok := s.projections[userID]; ok {
		return projection, nil
	}
	return models.RecipientProjection{}, repository.ErrNotFound
}

func newMappingService(projections map[string]models.RecipientProjection) *IngestService {
	return NewIngestService(IngestDeps{Recipients: stubRecipients{projections: projections}})
}

const (
	mappedAlice = "aaaaaaaa-1111-4111-8111-111111111111"
	mappedBob   = "bbbbbbbb-2222-4222-8222-222222222222"
)

func TestMapMemberSanctionChanged(t *testing.T) {
	t.Parallel()
	reason := "Spamming the general channel"
	expires := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		payload        models.MemberSanctionChanged
		wantTitle      string
		wantSummary    []string
		wantLinkSuffix string
	}{
		{
			name: "mute with reason expiry and appeal",
			payload: models.MemberSanctionChanged{
				SanctionID: "s1", CommunityID: "c1", TargetUserID: mappedAlice, Action: models.SanctionActionMute,
				Source: models.SanctionSourceModerator, Reason: &reason, ExpiresAt: &expires, Appealable: true,
			},
			wantTitle:      "You were muted",
			wantSummary:    []string{"Reason: Spamming the general channel.", "You can appeal this decision."},
			wantLinkSuffix: "/appeal",
		},
		{
			name: "automatic mute expiry",
			payload: models.MemberSanctionChanged{
				SanctionID: "s2", CommunityID: "c1", TargetUserID: mappedAlice, Action: models.SanctionActionUnmute,
				Source: models.SanctionSourceExpiry,
			},
			wantTitle:   "Your mute expired",
			wantSummary: []string{"The sanction expired automatically."},
		},
		{
			name: "kick",
			payload: models.MemberSanctionChanged{
				SanctionID: "s3", CommunityID: "c1", TargetUserID: mappedAlice, Action: models.SanctionActionKick,
				Source: models.SanctionSourceModerator,
			},
			wantTitle: "You were removed from a community",
		},
		{
			name: "ban",
			payload: models.MemberSanctionChanged{
				SanctionID: "s4", CommunityID: "c1", TargetUserID: mappedAlice, Action: models.SanctionActionBan,
				Source: models.SanctionSourceModerator, Appealable: true,
			},
			wantTitle:      "You were banned from a community",
			wantLinkSuffix: "/appeal",
		},
		{
			name: "unban from appeal",
			payload: models.MemberSanctionChanged{
				SanctionID: "s5", CommunityID: "c1", TargetUserID: mappedAlice, Action: models.SanctionActionUnban,
				Source: models.SanctionSourceAppeal,
			},
			wantTitle:   "You were unbanned",
			wantSummary: []string{"This action follows your appeal."},
		},
		{
			name: "unknown action",
			payload: models.MemberSanctionChanged{
				SanctionID: "s6", CommunityID: "c1", TargetUserID: mappedAlice, Action: "warn",
				Source: models.SanctionSourceModerator,
			},
			wantTitle: "Moderation notice",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			notifications := mapMemberSanctionChanged(test.payload)
			require.Len(t, notifications, 1)
			notification := notifications[0]
			require.Equal(t, models.KindModerationNotice, notification.Kind)
			require.Equal(t, mappedAlice, notification.RecipientUserID)
			require.Equal(t, test.wantTitle, notification.Title)
			for _, fragment := range test.wantSummary {
				require.Contains(t, notification.Summary, fragment)
			}
			require.True(t, strings.HasPrefix(notification.Link, "/moderation/sanctions/"))
			require.True(t, strings.HasSuffix(notification.Link, test.wantLinkSuffix))
		})
	}

	require.Empty(t, mapMemberSanctionChanged(models.MemberSanctionChanged{SanctionID: "s7"}), "no recipient, no notification")
}

func TestMapAppealStatusChanged(t *testing.T) {
	t.Parallel()
	note := "The evidence did not support the sanction."

	upheld := mapAppealStatusChanged(models.AppealStatusChanged{
		AppealID: "a1", AppellantUserID: mappedAlice, Status: models.AppealStatusUpheld, DecisionNote: &note,
	})
	require.Len(t, upheld, 1)
	require.Equal(t, models.KindAppealDecided, upheld[0].Kind)
	require.Equal(t, "Your appeal was reviewed: sanction upheld", upheld[0].Title)
	require.Contains(t, upheld[0].Summary, note)
	require.Equal(t, "/moderation/appeals/a1", upheld[0].Link)

	overturned := mapAppealStatusChanged(models.AppealStatusChanged{
		AppealID: "a2", AppellantUserID: mappedAlice, Status: models.AppealStatusOverturned,
	})
	require.Len(t, overturned, 1)
	require.Equal(t, "Your appeal was overturned", overturned[0].Title)
	require.NotContains(t, overturned[0].Summary, "Note:")

	require.Empty(t, mapAppealStatusChanged(models.AppealStatusChanged{Status: models.AppealStatusSubmitted}))
	require.Empty(t, mapAppealStatusChanged(models.AppealStatusChanged{Status: models.AppealStatusUpheld}))
}

func TestMapFriendRequestChanged(t *testing.T) {
	t.Parallel()
	svc := newMappingService(map[string]models.RecipientProjection{
		mappedAlice: {UserID: mappedAlice, DisplayName: "Alice", Username: "alice"},
		mappedBob:   {UserID: mappedBob, Username: "bob"},
	})

	pending := svc.mapFriendRequestChanged(context.Background(), models.FriendRequestChanged{
		SenderUserID: mappedAlice, RecipientUserID: mappedBob, Status: models.FriendRequestPending,
	})
	require.Len(t, pending, 1)
	require.Equal(t, models.KindFriendRequestReceived, pending[0].Kind)
	require.Equal(t, mappedBob, pending[0].RecipientUserID)
	require.Equal(t, "Alice sent you a friend request.", pending[0].Summary)
	require.Equal(t, "/friends/requests", pending[0].Link)

	accepted := svc.mapFriendRequestChanged(context.Background(), models.FriendRequestChanged{
		SenderUserID: mappedAlice, RecipientUserID: mappedBob, Status: models.FriendRequestAccepted,
	})
	require.Len(t, accepted, 1)
	require.Equal(t, models.KindFriendRequestAccepted, accepted[0].Kind)
	require.Equal(t, mappedAlice, accepted[0].RecipientUserID)
	require.Equal(t, "@bob accepted your friend request.", accepted[0].Summary)
	require.Equal(t, "/friends", accepted[0].Link)

	require.Empty(t, svc.mapFriendRequestChanged(context.Background(), models.FriendRequestChanged{
		Status: models.FriendRequestDeclined,
	}))
	require.Empty(t, svc.mapFriendRequestChanged(context.Background(), models.FriendRequestChanged{
		Status: models.FriendRequestCancelled,
	}))
}

func TestMapMessageChanged(t *testing.T) {
	t.Parallel()
	svc := newMappingService(map[string]models.RecipientProjection{
		mappedAlice: {UserID: mappedAlice, DisplayName: "Alice"},
	})
	content := "hey <@bob> look at this"

	channel := svc.mapMessageChanged(context.Background(), models.MessageChanged{
		MessageID:        "m1",
		Change:           models.MessageChangeCreated,
		ContainerType:    models.ContainerTypeChannel,
		ContainerID:      "c1",
		AuthorUserID:     mappedAlice,
		Content:          &content,
		MentionedUserIDs: []string{mappedBob, mappedAlice, mappedBob, ""},
	})
	require.Len(t, channel, 1, "one notification per mentioned user, excluding the author and duplicates")
	require.Equal(t, models.KindMention, channel[0].Kind)
	require.Equal(t, mappedBob, channel[0].RecipientUserID)
	require.Equal(t, "Alice mentioned you", channel[0].Title)
	require.Contains(t, channel[0].Summary, "in a channel")
	require.Contains(t, channel[0].Summary, content)
	require.Equal(t, "/channels/c1/messages/m1", channel[0].Link)

	conversation := svc.mapMessageChanged(context.Background(), models.MessageChanged{
		MessageID:        "m2",
		Change:           models.MessageChangeCreated,
		ContainerType:    models.ContainerTypeConversation,
		ContainerID:      "conv1",
		AuthorUserID:     mappedBob,
		MentionedUserIDs: []string{mappedAlice},
	})
	require.Len(t, conversation, 1)
	require.Equal(t, "/conversations/conv1/messages/m2", conversation[0].Link)
	require.Contains(t, conversation[0].Summary, "in a conversation")
	require.Equal(t, "A member mentioned you", conversation[0].Title, "unknown authors fall back to a neutral label")

	for _, change := range []string{models.MessageChangeEdited, models.MessageChangeDeleted, models.MessageChangeReactionsChanged} {
		require.Empty(t, svc.mapMessageChanged(context.Background(), models.MessageChanged{
			Change: change, AuthorUserID: mappedAlice, MentionedUserIDs: []string{mappedBob},
		}))
	}
	require.Empty(t, svc.mapMessageChanged(context.Background(), models.MessageChanged{
		Change: models.MessageChangeCreated, AuthorUserID: mappedAlice,
	}), "created without mentions is a no-op")
}

func TestMapMessageChangedTruncatesLongContent(t *testing.T) {
	t.Parallel()
	svc := newMappingService(nil)
	content := strings.Repeat("x", 2000)
	notifications := svc.mapMessageChanged(context.Background(), models.MessageChanged{
		MessageID: "m1", Change: models.MessageChangeCreated, ContainerType: models.ContainerTypeChannel,
		ContainerID: "c1", AuthorUserID: mappedAlice, Content: &content, MentionedUserIDs: []string{mappedBob},
	})
	require.Len(t, notifications, 1)
	require.LessOrEqual(t, len([]rune(notifications[0].Summary)), MaxSummaryLength)
	require.LessOrEqual(t, len([]rune(notifications[0].Title)), MaxTitleLength)
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	require.Equal(t, "abc", truncate("abc", 5))
	require.Equal(t, "abc", truncate("  abc  ", 5))
	require.Equal(t, "ab…", truncate("abcdef", 3))
	require.Equal(t, "a", truncate("abcdef", 1))
}
