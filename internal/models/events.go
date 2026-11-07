package models

import "time"

// UserAccountChanged is the EVT-001 payload (topic identity_events).
type UserAccountChanged struct {
	UserID         string        `json:"user_id"`
	Change         string        `json:"change"`
	Email          string        `json:"email"`
	Username       string        `json:"username"`
	DisplayName    string        `json:"display_name"`
	EmailVerified  bool          `json:"email_verified"`
	Status         AccountStatus `json:"status"`
	AccountVersion int           `json:"account_version"`
}

// EVT-001 change values.
const (
	AccountChangeEmailVerified  = "email_verified"
	AccountChangeProfileUpdated = "profile_updated"
	AccountChangeEmailChanged   = "email_changed"
	AccountChangeDeactivated    = "deactivated"
)

// MemberSanctionChanged is the EVT-004 payload (topic moderation_events).
type MemberSanctionChanged struct {
	SanctionID   string     `json:"sanction_id"`
	CommunityID  string     `json:"community_id"`
	TargetUserID string     `json:"target_user_id"`
	ActorUserID  *string    `json:"actor_user_id"`
	Action       string     `json:"action"`
	Source       string     `json:"source"`
	Reason       *string    `json:"reason"`
	ExpiresAt    *time.Time `json:"expires_at"`
	Appealable   bool       `json:"appealable"`
}

// EVT-004 actions and sources.
const (
	SanctionActionMute   = "mute"
	SanctionActionUnmute = "unmute"
	SanctionActionKick   = "kick"
	SanctionActionBan    = "ban"
	SanctionActionUnban  = "unban"

	SanctionSourceModerator = "moderator"
	SanctionSourceExpiry    = "expiry"
	SanctionSourceAppeal    = "appeal"
)

// AppealStatusChanged is the EVT-006 payload (topic moderation_events).
type AppealStatusChanged struct {
	AppealID        string  `json:"appeal_id"`
	SanctionID      string  `json:"sanction_id"`
	CommunityID     string  `json:"community_id"`
	AppellantUserID string  `json:"appellant_user_id"`
	SanctionType    string  `json:"sanction_type"`
	Status          string  `json:"status"`
	ReviewerUserID  *string `json:"reviewer_user_id"`
	DecisionNote    *string `json:"decision_note"`
}

// EVT-006 appeal statuses.
const (
	AppealStatusSubmitted  = "submitted"
	AppealStatusUpheld     = "upheld"
	AppealStatusOverturned = "overturned"
)

// FriendRequestChanged is the EVT-007 payload (topic social_events).
type FriendRequestChanged struct {
	RequestID       string `json:"request_id"`
	SenderUserID    string `json:"sender_user_id"`
	RecipientUserID string `json:"recipient_user_id"`
	Status          string `json:"status"`
}

// EVT-007 statuses.
const (
	FriendRequestPending   = "pending"
	FriendRequestAccepted  = "accepted"
	FriendRequestDeclined  = "declined"
	FriendRequestCancelled = "cancelled"
)

// Attachment is one message attachment reference of EVT-009.
type Attachment struct {
	AttachmentID string    `json:"attachment_id"`
	ContentType  string    `json:"content_type"`
	SizeBytes    int64     `json:"size_bytes"`
	DownloadURL  string    `json:"download_url"`
	URLExpiresAt time.Time `json:"url_expires_at"`
}

// Reaction is one aggregated reaction of EVT-009.
type Reaction struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}

// MessageChanged is the EVT-009 payload (topic message_events).
type MessageChanged struct {
	MessageID        string       `json:"message_id"`
	Change           string       `json:"change"`
	ContainerType    string       `json:"container_type"`
	ContainerID      string       `json:"container_id"`
	CommunityID      *string      `json:"community_id"`
	AuthorUserID     string       `json:"author_user_id"`
	ThreadRootID     *string      `json:"thread_root_id"`
	Content          *string      `json:"content"`
	Attachments      []Attachment `json:"attachments"`
	MentionedUserIDs []string     `json:"mentioned_user_ids"`
	Reactions        []Reaction   `json:"reactions"`
	DeletionReason   *string      `json:"deletion_reason"`
	CreatedAt        time.Time    `json:"created_at"`
	EditedAt         *time.Time   `json:"edited_at"`
	MessageVersion   int          `json:"message_version"`
}

// EVT-009 change values and container types.
const (
	MessageChangeCreated          = "created"
	MessageChangeEdited           = "edited"
	MessageChangeDeleted          = "deleted"
	MessageChangeReactionsChanged = "reactions_changed"

	ContainerTypeChannel      = "channel"
	ContainerTypeConversation = "conversation"
)

// NotificationCreated is the EVT-011 payload produced on notification_events.
type NotificationCreated struct {
	NotificationID  string `json:"notification_id"`
	RecipientUserID string `json:"recipient_user_id"`
	Kind            Kind   `json:"kind"`
	Title           string `json:"title"`
	Summary         string `json:"summary"`
	Link            string `json:"link"`
	UnreadCount     int    `json:"unread_count"`
}
