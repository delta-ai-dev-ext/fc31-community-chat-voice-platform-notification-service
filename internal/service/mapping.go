package service

import (
	"context"
	"fmt"
	"strings"

	"notification_service/internal/models"
	"notification_service/internal/repository"
)

// displayNameFor resolves a human-readable name for userID from the local
// recipient projection. Missing projections fall back to a neutral label so the
// notification is always created.
func (s *IngestService) displayNameFor(ctx context.Context, userID string) string {
	if userID == "" {
		return "Someone"
	}
	projection, err := s.recipients.Get(ctx, userID)
	if err != nil {
		return "A member"
	}
	if name := strings.TrimSpace(projection.DisplayName); name != "" {
		return name
	}
	if username := strings.TrimSpace(projection.Username); username != "" {
		return "@" + username
	}
	return "A member"
}

// mapMemberSanctionChanged derives the moderation notice of EVT-004. Every
// action produces a notice for the sanctioned member, including automatic mute
// expiry; in-app and email moderation notices ignore the recipient's
// preferences.
func mapMemberSanctionChanged(payload models.MemberSanctionChanged) []models.NewNotification {
	if payload.TargetUserID == "" {
		return nil
	}

	summary := sanctionSummary(payload)
	link := "/moderation/sanctions/" + payload.SanctionID
	if payload.Appealable {
		link = "/moderation/sanctions/" + payload.SanctionID + "/appeal"
	}

	return []models.NewNotification{{
		RecipientUserID: payload.TargetUserID,
		Kind:            models.KindModerationNotice,
		Title:           truncate(sanctionTitle(payload.Action, payload.Source), MaxTitleLength),
		Summary:         truncate(summary, MaxSummaryLength),
		Link:            link,
	}}
}

func sanctionTitle(action, source string) string {
	if source == models.SanctionSourceExpiry {
		switch action {
		case models.SanctionActionUnmute:
			return "Your mute expired"
		case models.SanctionActionUnban:
			return "Your ban expired"
		}
	}
	switch action {
	case models.SanctionActionMute:
		return "You were muted"
	case models.SanctionActionUnmute:
		return "You were unmuted"
	case models.SanctionActionKick:
		return "You were removed from a community"
	case models.SanctionActionBan:
		return "You were banned from a community"
	case models.SanctionActionUnban:
		return "You were unbanned"
	default:
		return "Moderation notice"
	}
}

func sanctionSummary(payload models.MemberSanctionChanged) string {
	parts := []string{fmt.Sprintf("A moderator applied a %s action to your account.", payload.Action)}
	switch payload.Source {
	case models.SanctionSourceExpiry:
		parts = append(parts, "The sanction expired automatically.")
	case models.SanctionSourceAppeal:
		parts = append(parts, "This action follows your appeal.")
	}
	if payload.Reason != nil && strings.TrimSpace(*payload.Reason) != "" {
		parts = append(parts, "Reason: "+strings.TrimSpace(*payload.Reason)+".")
	}
	if payload.ExpiresAt != nil {
		parts = append(parts, "Ends at "+payload.ExpiresAt.UTC().Format("2006-01-02 15:04 UTC")+".")
	}
	if payload.Appealable {
		parts = append(parts, "You can appeal this decision.")
	}
	return strings.Join(parts, " ")
}

// mapAppealStatusChanged derives the appeal decision notice of EVT-006.
// `submitted` is a no-op.
func mapAppealStatusChanged(payload models.AppealStatusChanged) []models.NewNotification {
	if payload.AppellantUserID == "" {
		return nil
	}

	var title, decision string
	switch payload.Status {
	case models.AppealStatusUpheld:
		title = "Your appeal was reviewed: sanction upheld"
		decision = "The moderators upheld the original sanction."
	case models.AppealStatusOverturned:
		title = "Your appeal was overturned"
		decision = "The moderators overturned the original sanction."
	default:
		return nil
	}

	summary := decision
	if payload.DecisionNote != nil && strings.TrimSpace(*payload.DecisionNote) != "" {
		summary += " Note: " + strings.TrimSpace(*payload.DecisionNote)
	}

	return []models.NewNotification{{
		RecipientUserID: payload.AppellantUserID,
		Kind:            models.KindAppealDecided,
		Title:           truncate(title, MaxTitleLength),
		Summary:         truncate(summary, MaxSummaryLength),
		Link:            "/moderation/appeals/" + payload.AppealID,
	}}
}

// mapFriendRequestChanged derives the friend notifications of EVT-007.
func (s *IngestService) mapFriendRequestChanged(ctx context.Context, payload models.FriendRequestChanged) []models.NewNotification {
	switch payload.Status {
	case models.FriendRequestPending:
		if payload.RecipientUserID == "" {
			return nil
		}
		sender := s.displayNameFor(ctx, payload.SenderUserID)
		return []models.NewNotification{{
			RecipientUserID: payload.RecipientUserID,
			Kind:            models.KindFriendRequestReceived,
			Title:           truncate("New friend request", MaxTitleLength),
			Summary:         truncate(sender+" sent you a friend request.", MaxSummaryLength),
			Link:            "/friends/requests",
		}}
	case models.FriendRequestAccepted:
		if payload.SenderUserID == "" {
			return nil
		}
		accepter := s.displayNameFor(ctx, payload.RecipientUserID)
		return []models.NewNotification{{
			RecipientUserID: payload.SenderUserID,
			Kind:            models.KindFriendRequestAccepted,
			Title:           truncate("Friend request accepted", MaxTitleLength),
			Summary:         truncate(accepter+" accepted your friend request.", MaxSummaryLength),
			Link:            "/friends",
		}}
	default:
		return nil
	}
}

// mapMessageChanged derives one mention notification per mentioned user of
// EVT-009, excluding the author. Every other change is a no-op.
func (s *IngestService) mapMessageChanged(ctx context.Context, payload models.MessageChanged) []models.NewNotification {
	if payload.Change != models.MessageChangeCreated || len(payload.MentionedUserIDs) == 0 {
		return nil
	}

	link := messageLink(payload)
	author := s.displayNameFor(ctx, payload.AuthorUserID)
	snippet := ""
	if payload.Content != nil {
		snippet = strings.TrimSpace(*payload.Content)
	}
	where := "a conversation"
	if payload.ContainerType == models.ContainerTypeChannel {
		where = "a channel"
	}

	summary := fmt.Sprintf("%s mentioned you in %s.", author, where)
	if snippet != "" {
		summary += " \"" + snippet + "\""
	}
	title := author + " mentioned you"

	notifications := make([]models.NewNotification, 0, len(payload.MentionedUserIDs))
	seen := make(map[string]struct{}, len(payload.MentionedUserIDs))
	for _, mentioned := range payload.MentionedUserIDs {
		if mentioned == "" || mentioned == payload.AuthorUserID {
			continue
		}
		if _, duplicate := seen[mentioned]; duplicate {
			continue
		}
		seen[mentioned] = struct{}{}
		notifications = append(notifications, models.NewNotification{
			RecipientUserID: mentioned,
			Kind:            models.KindMention,
			Title:           truncate(title, MaxTitleLength),
			Summary:         truncate(summary, MaxSummaryLength),
			Link:            link,
		})
	}
	return notifications
}

func messageLink(payload models.MessageChanged) string {
	base := "/conversations/"
	if payload.ContainerType == models.ContainerTypeChannel {
		base = "/channels/"
	}
	return base + payload.ContainerID + "/messages/" + payload.MessageID
}

// loadPreferences returns the stored preferences of a recipient, falling back to
// the documented defaults when none were saved yet.
func (s *IngestService) loadPreferences(ctx context.Context, userID string) (models.Preferences, error) {
	prefs, err := s.preferences.Get(ctx, userID)
	if err != nil {
		if err == repository.ErrNotFound {
			return models.DefaultPreferences(), nil
		}
		return models.Preferences{}, fmt.Errorf("service: load preferences of %s: %w", userID, err)
	}
	return prefs, nil
}
