package models

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Cursor is the opaque pagination position of the inbox listing: the
// (created_at, id) key of the last item of the previous page.
type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// ErrInvalidCursor is returned by ParseCursor for malformed or foreign cursors.
var ErrInvalidCursor = fmt.Errorf("invalid cursor")

// Encode serializes the cursor into its opaque URL-safe form.
func (c Cursor) Encode() string {
	raw := strconv.FormatInt(c.CreatedAt.UTC().UnixMicro(), 10) + ":" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// ParseCursor decodes an opaque cursor produced by Cursor.Encode.
func ParseCursor(value string) (Cursor, error) {
	if value == "" {
		return Cursor{}, fmt.Errorf("%w: empty", ErrInvalidCursor)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: not base64url", ErrInvalidCursor)
	}
	microsPart, id, ok := strings.Cut(string(decoded), ":")
	if !ok || id == "" {
		return Cursor{}, fmt.Errorf("%w: malformed payload", ErrInvalidCursor)
	}
	micros, err := strconv.ParseInt(microsPart, 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: malformed timestamp", ErrInvalidCursor)
	}
	return Cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: id}, nil
}
