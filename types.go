package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// ---------------------------------------------------------------------------
// Wire format structs — JSON field names match TDD §3.3 exactly.
// ---------------------------------------------------------------------------

// Wire protocol version (TDD §3.3). Absent v/minV on the wire = 1 (pre-version
// peers). Phase 1 is advisory only: the server stamps these on every state
// message but never enforces; enforcement (phase 2) arrives only with a real
// breaking change, by raising minClientVersion.
const (
	protocolVersion  = 1 // the protocol version this server speaks
	minClientVersion = 1 // lowest client protocol version the server serves
)

// StateMsg is sent by the server to the client immediately after WebSocket connect.
type StateMsg struct {
	Type                  string                `json:"type"` // "state"
	V                     int                   `json:"v"`    // = protocolVersion
	MinV                  int                   `json:"minV"` // = minClientVersion; a client below this refuses to sync
	CartID                string                `json:"cartID"`
	OwnerID               string                `json:"ownerID"`
	Items                 []ItemRow             `json:"items"`
	ActiveShoppingMembers []ActiveShoppingEntry `json:"activeShoppingMembers"`
	Members               []MemberInfo          `json:"members"`
	Tombstones            []TombstoneRow        `json:"tombstones"` // ADR-038: soft-deleted rows, so the client deletes on explicit tombstone rather than absence
}

// AckMsg acknowledges a processed delta back to the originating connection only
// (never broadcast). ADR-038 invariant B: the client removes a PendingWrite only
// on a matching ack.
type AckMsg struct {
	Type   string  `json:"type"` // "ack"
	CartID string  `json:"cartID"`
	ItemID string  `json:"itemID"`
	Ts     float64 `json:"ts"`
	Status string  `json:"status"` // "applied" | "discarded" | "rejected"
}

// TombstoneRow is a soft-deleted item surfaced in state messages (ADR-038 invariant A).
type TombstoneRow struct {
	ID        string  `json:"id"`
	DeletedAt float64 `json:"deletedAt"`
}

// DeltaMsg is the delta wire format (client→server and server→other clients).
// UserID is absent in the client→server direction; injected by the server for relay.
type DeltaMsg struct {
	Type     string  `json:"type"`        // "delta"
	V        int     `json:"v,omitempty"` // client protocol version; omitempty keeps a pre-version client's absent v absent on relay (absent = 1)
	CartID   string  `json:"cartID"`
	UserID   string  `json:"userID,omitempty"` // injected by server on relay
	DeviceID string  `json:"deviceID"`
	Ts       float64 `json:"ts"`
	Ops      []Op    `json:"ops"`
}

// Op is a single operation within a delta message.
type Op struct {
	Op     string         `json:"op"` // "add" | "update" | "delete"
	ID     string         `json:"id"` // item UUID
	Fields map[string]any `json:"fields,omitempty"`
	// Actor opts an op out of userID injection: "system" (systemActor) attributes
	// the write to the reserved system sentinel instead of the connection's userID
	// (RULE[sharing.system-attribution] — staleness auto-pause and other
	// device-derived household decisions are attributed to system, not a member).
	// Any other value is ignored and the connection's userID is used as normal.
	Actor string `json:"actor,omitempty"`
}

// systemActor is the reserved attribution sentinel for device-derived household
// writes (e.g. staleness auto-pause). It is never a real CloudKit User Record ID.
const systemActor = "system"

// MemberInfo describes a cart member as returned in state and REST responses.
type MemberInfo struct {
	UserID      string `json:"userID"`
	DisplayName string `json:"displayName"`
	Color       string `json:"color"`
	Tier        string `json:"tier"`
}

// ActiveShoppingEntry represents a member currently in ActiveShopping for a cart.
type ActiveShoppingEntry struct {
	UserID  string `json:"userID"`
	StoreID string `json:"storeID"`
}

// ItemRow is the item representation used in state messages and REST responses.
// Tombstoned items are never included.
type ItemRow struct {
	ID                string   `json:"id"`
	Title             string   `json:"title"`
	Quantity          int      `json:"quantity"`
	Checked           bool     `json:"checked"`
	AttributedTo      *string  `json:"attributedTo"`
	Ts                float64  `json:"ts"`
	Notes             *string  `json:"notes"`
	Specification     *string  `json:"specification"`
	UrgencyLevel      int      `json:"urgencyLevel"`
	GlobalID          *string  `json:"globalID"`
	FrequencyDays     *int     `json:"frequencyDays"`
	IsActive          bool     `json:"isActive"`
	PausedAt          *float64 `json:"pausedAt"`
	PauseReason       *string  `json:"pauseReason"`
	DeferredGuessDate *float64 `json:"deferredGuessDate"`
	// DeferredGuessAuthoredAt is the instant the authored anchor (DeferredGuessDate) was DECLARED
	// (ADR-053). It rides beside the anchor wherever the anchor rides so every device derives anchor
	// CONSUMPTION — "the authored when has been fulfilled by a later purchase" — from facts alone,
	// instead of syncing a write-time clear (the distributed-mutation disease ADR-050 diagnosed on Ts).
	// Same presence semantics as DeferredGuessDate: field-merged, and pairs the anchor's NSNull on a
	// clear. Absent-tolerant — an old client omits it and the receiver falls back to value comparison.
	DeferredGuessAuthoredAt *float64 `json:"deferredGuessAuthoredAt"`
	// PurchasedAt is the purchase FACT time carried on a checked-carrying op/row (ADR-050) —
	// distinct from Ts (the LWW ordering time). omitempty so a concept-less/unchecked/old row
	// omits it and the receiver falls back to Ts. State rows carry it so a reconnect snapshot,
	// not only a live delta, delivers the fact (closing the state-door hole).
	PurchasedAt *float64 `json:"purchasedAt,omitempty"`
	// PurchaseStoreBrand is the BRAND NAME of the shop that purchase happened at, riding the same
	// checked-carrying ops and rows as PurchasedAt. A store RECORD never crosses this relay — stores
	// are personal — so this string is the only thing a receiving device can honestly say about where
	// a household member shopped; resolving its own store for someone else's trip would invent a fact.
	// The relay stores and returns it verbatim and never interprets it. omitempty: an unchecked row, a
	// purchase with no store, and every row written before this column existed all omit the key, and an
	// absent key means "that purchase's store is unknown" — never "reuse the last one".
	PurchaseStoreBrand *string `json:"purchaseStoreBrand,omitempty"`
	// Label hints (ADR-051): the sender's own category/store labels, carried so a receiver can
	// fill its OWN blank fields once, as a backstop to receiver-side resolution — not synced state
	// (category/store stay personal). Additive & absent-tolerant like PurchasedAt: old rows yield
	// nil hints and today's behaviour; state rows carry them so joiners see them (the ADR-020
	// STATE-absence lesson made structural). omitempty keeps an old/hint-less row's keys off the wire.
	CategoryHintName  *string `json:"categoryHintName,omitempty"`
	CategoryHintIcon  *string `json:"categoryHintIcon,omitempty"`
	CategoryHintColor *string `json:"categoryHintColor,omitempty"`
	StoreHintBrand    *string `json:"storeHintBrand,omitempty"`
	// LastWantedAt is the instant a member last said they still want this item — resuming a paused
	// item, or marking it needed now. It is a separate fact from Ts (which is only the ordering
	// stamp for conflicting writes), and the app's staleness clock reads it: an item nobody has
	// wanted for a long time is eventually paused. Carried on state rows, not only live messages, so
	// a device that was offline when somebody re-confirmed the item does not pause it anyway.
	// omitempty: an item nobody has re-confirmed omits the key. An absent key means "unchanged" and
	// nothing else — a client must not fall back to Ts. Ts moves on any write, so reading a want from
	// it would invent one out of an unrelated edit, which is the whole failure this field exists to
	// avoid: an item somebody merely renamed would look like an item somebody asked for.
	LastWantedAt *float64 `json:"lastWantedAt,omitempty"`
	// QuietUntilFirstPurchase means "make no prediction about this item until it is next purchased" —
	// a member's explicit request for silence, set when they dismiss a suggestion for an item they buy
	// only occasionally. Stored so a member who was offline when it was set still learns the item is
	// quiet; without it here, their device would keep predicting for an item the household muted.
	//
	// The name says "first" and the meaning is the NEXT purchase. That is deliberate: the key is the
	// contract with apps already in the field, so the meaning widened and the key did not. Do not
	// rename this column to match the meaning — it would silently stop matching the wire.
	QuietUntilFirstPurchase bool `json:"quietUntilFirstPurchase"`
}

// The four messages below carried no cartID while a connection served exactly one
// cart and the receiver could infer it. A connection now carries every cart its
// member belongs to, so each states its own subject.

// MemberRemovedMsg is broadcast to all connected members when a member is revoked.
type MemberRemovedMsg struct {
	Type   string `json:"type"`   // "member_removed"
	CartID string `json:"cartID"` // the cart the member was removed from
	UserID string `json:"userID"` // the removed member's userID
}

// SecretRotatedMsg is sent to remaining connected members after a revoke.
type SecretRotatedMsg struct {
	Type   string `json:"type"`   // "secret_rotated"
	CartID string `json:"cartID"` // the cart whose invite secret changed
	Secret string `json:"secret"` // new invite secret
}

// OwnerTransferredMsg is broadcast to all members when ownership changes.
type OwnerTransferredMsg struct {
	Type        string `json:"type"`
	CartID      string `json:"cartID"` // the cart whose owner changed
	NewOwnerID  string `json:"newOwnerID"`
	DisplayName string `json:"displayName"`
}

// MemberSessionMsg covers both member_session_start and member_session_end.
// Differentiated by the "type" field. StoreID is omitted on session_end.
// CartID travels in BOTH directions here: the client names the cart it is
// shopping, and the relay names it again on the way out.
type MemberSessionMsg struct {
	Type    string `json:"type"` // "member_session_start" | "member_session_end"
	CartID  string `json:"cartID"`
	UserID  string `json:"userID"`
	StoreID string `json:"storeID,omitempty"` // present on start, absent on end
}

// ---------------------------------------------------------------------------
// Subscription — the client's opening move
// ---------------------------------------------------------------------------

// SubscribeMsg is the client→server subscription request. It is the first
// message on a connection (nothing is registered before it arrives) and may be
// sent again at any time to REPLACE the connection's subscription set, which is
// how a cart the person just joined or left reaches an already-open socket
// without a reconnect.
type SubscribeMsg struct {
	Type  string          `json:"type"`        // "subscribe"
	V     int             `json:"v,omitempty"` // client protocol version; absent = 1
	Carts []SubscribeCart `json:"carts"`
}

// SubscribeCart is one requested (cart, credential) pair. The key alone would be
// enough to resolve the cart; the cartID is sent alongside so a client that has
// mixed up its own storage is told so, instead of silently subscribing to a cart
// it did not mean.
type SubscribeCart struct {
	CartID string `json:"cartID"`
	SubKey string `json:"subKey"`
}

// SubscribeResultMsg answers a SubscribeMsg. A pair that fails to verify is
// reported here and the rest of the subscription proceeds — one revoked cart
// must not cost a person the sync of every other cart they are in.
type SubscribeResultMsg struct {
	Type     string            `json:"type"` // "subscribe_result"
	V        int               `json:"v"`
	MinV     int               `json:"minV"`
	Accepted []string          `json:"accepted"`
	Rejected []SubscribeReject `json:"rejected"`
}

// SubscribeReject explains one refused pair. The client treats a rejected cart
// the way it treats member_removed: it is no longer in that cart.
type SubscribeReject struct {
	CartID string `json:"cartID"`
	Reason string `json:"reason"`
}

// Rejection reasons. They distinguish "you are not in this cart any more" from
// "this request is malformed", because the client acts on the first and only
// logs the second.
const (
	rejectUnknownKey   = "unknown_key"   // no member row holds this key — never minted, or revoked/left since
	rejectCartMismatch = "cart_mismatch" // the key is valid but was minted for a different cart
	rejectMalformed    = "malformed"     // not a cart id shape, or an empty key
	rejectUserMismatch = "user_mismatch" // the key belongs to a different person than this connection's other keys
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// generateSecret returns a random invite token in the form
// "AEYO-XXXX-XXXX-XXXX-XXXX" where each X is an uppercase hex digit — 8 bytes,
// so 64 bits of entropy.
//
// 64 rather than 128 because this is a code a person reads aloud, types from a
// message, or scans off a QR, and every extra group makes that worse. What makes
// 64 enough is the rate limiter in front of it: /api/cart/join is capped per IP
// at a burst of 15 refilling 1/sec, so an online guessing attack gets nowhere
// near 2^64, and the secret is rotated on every revoke besides. A credential
// that is never read by a human has no such tradeoff — see generateSubKey,
// which takes the full 16 bytes.
func generateSecret() string {
	b := make([]byte, 8) // 8 bytes = 16 hex chars = 4 groups of 4
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is unrecoverable in a security context
		panic(fmt.Sprintf("generateSecret: crypto/rand failed: %v", err))
	}
	return fmt.Sprintf("AEYO-%04X-%04X-%04X-%04X",
		uint16(b[0])<<8|uint16(b[1]),
		uint16(b[2])<<8|uint16(b[3]),
		uint16(b[4])<<8|uint16(b[5]),
		uint16(b[6])<<8|uint16(b[7]),
	)
}

// generateSubKey returns a 128-bit random member subscription key as 32
// lowercase hex characters. Unlike the invite secret it is never read aloud,
// typed, or put in a link, so it carries no grouping and no prefix — it is only
// ever moved machine-to-machine.
func generateSubKey() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is unrecoverable in a security context
		panic(fmt.Sprintf("generateSubKey: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// newUUID returns a random UUID v4 string.
func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("newUUID: crypto/rand failed: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
