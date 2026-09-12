package main

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// The only WebSocket client is the iOS app, and a native URLSession handshake sends
	// no Origin header at all. A browser always sends one, so "no Origin" is the signal
	// to allow and anything else is a cross-site attempt.
	//
	// Still defence in depth, but for a different reason than it used to be. The old
	// argument was that a browser cannot set the X-User-ID header on a WebSocket
	// handshake, so a cross-site connection failed membership auth anyway. There is no
	// such header now and the credential arrives in the first message, which a browser
	// CAN send. What replaces the argument is that a browser has no way to obtain a
	// subKey: it is returned only to the member who created or joined the cart, over the
	// API, and is never displayed, linked, or broadcast. A cross-site page can open a
	// socket and will simply have nothing to subscribe with.
	//
	// So this check is less load-bearing than before, not more. If a browser client ever
	// ships, it becomes an explicit allowlist of Aeyo origins rather than an emptiness test.
	CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "" },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// maxBodyBytes caps a JSON request body. Every API payload is a handful of
// short fields (IDs, names, a color, a small events array); 1 MB is far above
// any legitimate body while preventing an unbounded read from exhausting memory.
// An oversized body makes Decode return an error → the caller's 400 path.
const maxBodyBytes = 1 << 20 // 1 MB

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	return json.NewDecoder(r.Body).Decode(dst)
}

// ---------------------------------------------------------------------------
// POST /api/cart/create
// ---------------------------------------------------------------------------

// ⚠️ create is the BOOTSTRAP EXCEPTION to "the actor is proved by their key", and
// it is an exception by construction rather than by oversight: this endpoint is
// where the first key for a cart comes from, so there is nothing yet to present.
// Every other cart-scoped handler resolves its actor through `authorizeCartActor`
// — do not "finish the job" here, or nobody can ever create a cart again.
//
// It is safe unproved because it creates a NEW room owned by the caller: the
// worst a forged userID achieves is minting a cart for somebody else's ID, which
// grants the forger nothing and touches no existing data.
//
// ⚠️ The caller MAY PROPOSE the room's id (`cartID`), and that clause is the one
// the proposal tests, so it is enforced here rather than assumed. A proposed id
// is refused if a room already occupies it, and refused again if any row keyed to
// it survives without a room (`CartIDResidue`). What create does with a proposed
// id is therefore exactly what it does with a minted one: fill an id that holds
// nothing. It never adopts, re-owns, or re-secrets an existing room.
//
// What the proposal buys an attacker, stated plainly because the README publishes
// it: every member of a cart knows its id, so once a room is DELETED an ex-member
// can re-create it with themselves as owner. They get an empty room and a fresh
// secret — no items, no members, and no way to draw the real members in, since
// those devices hold the old secret and their join is refused. The residual is a
// squat that costs that one dead cart its identifier, available only to someone
// who was already a member of it. Weigh any change here against that sentence.
//
// Why the proposal exists at all: a cart's uuid is immutable once shared
// (RULE[sharing.cart-uuid-immutable]), so a client repairing a cart whose room is
// gone must re-create it UNDER ITS OWN id — minting a new one would rewrite the
// key every pending write, invite link, and member device already uses.
func createCartHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID      string `json:"userID"`
			DisplayName string `json:"displayName"`
			Color       string `json:"color"`
			CartName    string `json:"cartName"`
			HexColor    string `json:"hexColor"`
			// Optional. Absent (the 1.7.6 client and every ordinary first share) →
			// minted below, exactly as before.
			CartID string `json:"cartID"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		cartID := body.CartID
		if cartID == "" {
			cartID = newUUID()
		} else {
			if !isValidCartID(cartID) {
				log.Printf("createCart: rejecting malformed proposed cartID from %s", clientIP(r))
				writeError(w, http.StatusBadRequest, "invalid cartID")
				return
			}
			exists, err := RoomExists(db, cartID)
			if err != nil {
				log.Printf("createCart: RoomExists: %v", err)
				writeError(w, http.StatusInternalServerError, "internal error")
				return
			}
			if exists {
				writeError(w, http.StatusConflict, "cart already exists")
				return
			}
			residue, err := CartIDResidue(db, cartID)
			if err != nil {
				log.Printf("createCart: CartIDResidue: %v", err)
				writeError(w, http.StatusInternalServerError, "internal error")
				return
			}
			if residue {
				// Unreachable while the cascade works, so treat a firing as the alarm it
				// is: refusing keeps the promise above true, and the log line is how we
				// find out the cascade stopped.
				log.Printf("createCart: REFUSING proposed cartID %s — rows survive with no room (cascade not firing?)", cartID)
				writeError(w, http.StatusConflict, "cart already exists")
				return
			}
		}

		if err := UpsertUser(db, body.UserID, body.DisplayName, body.Color); err != nil {
			log.Printf("createCart: UpsertUser: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		secret := generateSecret()

		if err := CreateRoom(db, cartID, body.UserID, secret, body.CartName, body.HexColor); err != nil {
			log.Printf("createCart: CreateRoom: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		subKey, err := AddMember(db, cartID, body.UserID)
		if err != nil {
			log.Printf("createCart: AddMember: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		// subKey goes to this member and to nobody else, ever: it is returned here
		// and in this member's own join response, and appears in no state message,
		// no broadcast, and no other member's response.
		writeJSON(w, http.StatusOK, map[string]string{
			"cartID": cartID,
			"secret": secret,
			"subKey": subKey,
		})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/join
// ---------------------------------------------------------------------------

// ⚠️ join is the second and last BOOTSTRAP EXCEPTION (see createCartHandler): it
// is the other place a key is minted, so the caller cannot hold one yet. What
// stands in for the key here is the **invite secret**, compared in constant time
// below — a credential the caller must have been given, which is exactly the
// property a self-asserted userID lacks.
func joinCartHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			CartID      string `json:"cartID"`
			Secret      string `json:"secret"`
			UserID      string `json:"userID"`
			DisplayName string `json:"displayName"`
			Color       string `json:"color"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" || body.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if !isValidCartID(body.CartID) {
			log.Printf("join: rejecting malformed cartID from %s", clientIP(r))
			writeError(w, http.StatusBadRequest, "invalid cartID")
			return
		}

		serverSecret, err := GetSecret(db, body.CartID)
		if err != nil {
			writeError(w, http.StatusForbidden, "cart not found")
			return
		}
		// Constant-time: the endpoint is rate-limited, so a remote timing attack on the
		// invite secret is not practical — but a secret comparison is the one place
		// where paying three lines to remove the question entirely is obviously right.
		if subtle.ConstantTimeCompare([]byte(serverSecret), []byte(body.Secret)) != 1 {
			writeError(w, http.StatusForbidden, "invalid secret")
			return
		}

		// Capacity check + AddMember in one transaction.
		//
		// What makes the check safe against two people joining a full cart at the same
		// instant is `db.SetMaxOpenConns(1)` in InitDB, NOT this transaction: with one
		// connection every writer serialises, so the count and the insert cannot
		// interleave with another request's. ⚠️ Raising MaxOpenConns re-opens that race
		// and this transaction will not close it — a plain BEGIN takes a DEFERRED lock,
		// which two readers can both hold before either writes.
		//
		// There used to be a `tx.Exec("BEGIN EXCLUSIVE")` here with its error discarded.
		// It never did anything — SQLite refuses a BEGIN inside an open transaction — so
		// it read as a lock that was held while none was, which is worse than saying
		// plainly which mechanism is load-bearing.
		tx, err := db.Begin()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		defer tx.Rollback()

		var memberCount int
		tx.QueryRow(`SELECT COUNT(*) FROM members WHERE cart_id = ?`, body.CartID).Scan(&memberCount)

		var hasPremiumOwner int
		tx.QueryRow(`
			SELECT EXISTS (
			    SELECT 1 FROM rooms r
			    JOIN users u ON r.owner_id = u.user_id
			    WHERE r.cart_id = ? AND u.tier = 'premium'
			)
		`, body.CartID).Scan(&hasPremiumOwner)

		// Check if user is already a member — re-join is always allowed.
		var alreadyMember int
		tx.QueryRow(`SELECT COUNT(*) FROM members WHERE cart_id=? AND user_id=?`,
			body.CartID, body.UserID).Scan(&alreadyMember)

		// premiumForAll (TestFlight) bypasses the per-tier capacity limit entirely.
		if !premiumForAll && alreadyMember == 0 && memberCount >= 2 && hasPremiumOwner == 0 {
			tx.Rollback()
			writeError(w, http.StatusForbidden, "cart is at capacity")
			return
		}

		// Mirrors AddMember: a re-join keeps the member's existing subscription key
		// (DO UPDATE touches joined_at only), so the other devices of a member who
		// re-joins are not locked out of a cart they never left.
		if _, err := tx.Exec(`
			INSERT INTO members (cart_id, user_id, joined_at, sub_key)
			VALUES (?, ?, strftime('%s','now'), ?)
			ON CONFLICT(cart_id, user_id) DO UPDATE SET joined_at = excluded.joined_at
		`, body.CartID, body.UserID, generateSubKey()); err != nil {
			log.Printf("joinCart: AddMember in tx: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		if err := tx.Commit(); err != nil {
			log.Printf("joinCart: commit: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		subKey, err := GetSubKey(db, body.CartID, body.UserID)
		if err != nil {
			log.Printf("joinCart: GetSubKey: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		if err := UpsertUser(db, body.UserID, body.DisplayName, body.Color); err != nil {
			log.Printf("joinCart: UpsertUser: %v", err)
		}

		// Notify existing connected members of the new joiner via a full state snapshot.
		// The joiner is not yet connected over WS (they open it after this HTTP call),
		// so this only reaches already-connected members.
		stateB, _ := json.Marshal(buildStateMsg(db, hub, body.CartID))
		hub.BroadcastToCart(body.CartID, stateB, "")

		members, _ := GetMembers(db, body.CartID)
		items, _ := GetItems(db, body.CartID)

		writeJSON(w, http.StatusOK, map[string]any{
			"cartID":  body.CartID,
			"subKey":  subKey,
			"members": members,
			"items":   items,
		})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/leave
// ---------------------------------------------------------------------------

func leaveCartHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			CartID     string `json:"cartID"`
			TransferTo string `json:"transferTo"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// The leaver is whoever holds the key — the body cannot name them. It used
		// to (`userID`, checked with IsMember), which made this "remove the member
		// named in the body" and let any member force any OTHER member out.
		_, leaverID, ok := authorizeCartActor(db, w, r, body.CartID)
		if !ok {
			return
		}

		ownerID, err := GetOwner(db, body.CartID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		transferred := false
		if leaverID == ownerID {
			// Determine new owner.
			newOwner := body.TransferTo

			// Validate transferTo is a member (other than the leaver).
			if newOwner != "" && newOwner != leaverID {
				isMem, _ := IsMember(db, body.CartID, newOwner)
				if !isMem {
					newOwner = ""
				}
			} else {
				newOwner = ""
			}

			if newOwner == "" {
				// Auto-assign to earliest-joined member that isn't the leaver.
				err = db.QueryRow(`
					SELECT user_id FROM members
					WHERE cart_id = ? AND user_id != ?
					ORDER BY joined_at ASC LIMIT 1
				`, body.CartID, leaverID).Scan(&newOwner)
				if err == sql.ErrNoRows {
					newOwner = ""
				}
			}

			if newOwner != "" {
				if err := TransferOwner(db, body.CartID, newOwner); err != nil {
					log.Printf("leaveCart: TransferOwner: %v", err)
				}
				// Get new owner's display name for the broadcast.
				var displayName string
				db.QueryRow(`SELECT COALESCE(display_name,'') FROM users WHERE user_id=?`, newOwner).Scan(&displayName)
				msg := OwnerTransferredMsg{
					Type:        "owner_transferred",
					CartID:      body.CartID,
					NewOwnerID:  newOwner,
					DisplayName: displayName,
				}
				b, _ := json.Marshal(msg)
				hub.BroadcastToCart(body.CartID, b, "")
				transferred = true
			}
		}

		if err := RemoveMember(db, body.CartID, leaverID); err != nil {
			log.Printf("leaveCart: RemoveMember: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		// The member's key is gone with their row; end the subscription on any
		// socket they still hold for their OTHER carts, which no longer closes
		// when this membership ends.
		hub.UnsubscribeUserFromCart(leaverID, body.CartID)

		removed := MemberRemovedMsg{Type: "member_removed", CartID: body.CartID, UserID: leaverID}
		rb, _ := json.Marshal(removed)
		// Exclude the leaver: they initiated the leave and self-reset client-side.
		// Broadcasting member_removed to them would fire a false "you've been removed" insight.
		hub.BroadcastToCartExcludeUser(body.CartID, rb, leaverID)

		// A tier is reported for the owner and nobody else, so an ownership change IS a
		// tier change and owes a fresh snapshot: owner_transferred carries no members, and
		// a client that only rewrites who is owner would leave the new owner holding the
		// empty tier they had as an ordinary member. Sent here rather than beside that
		// broadcast because the leaver's row is gone by this point — a snapshot taken
		// earlier would re-assert the member this call is removing.
		if transferred {
			stateB, _ := json.Marshal(buildStateMsg(db, hub, body.CartID))
			hub.BroadcastToCart(body.CartID, stateB, "")
		}

		count, _ := GetMemberCount(db, body.CartID)
		if count == 0 {
			if err := DeleteRoom(db, body.CartID); err != nil {
				log.Printf("leaveCart: DeleteRoom: %v", err)
			}
		}

		writeJSON(w, http.StatusOK, map[string]any{})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/revoke
// ---------------------------------------------------------------------------

func revokeHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			CartID       string `json:"cartID"`
			TargetUserID string `json:"targetUserID"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// The owner is proved by their key. `requestingUserID` used to be a body
		// field compared against `owner_id` — a claim checked against a fact, which
		// any member could satisfy by writing in the owner's ID (they all know it;
		// it rides every state message).
		if _, _, ok := authorizeCartOwner(db, w, r, body.CartID); !ok {
			return
		}

		if err := RemoveMember(db, body.CartID, body.TargetUserID); err != nil {
			log.Printf("revoke: RemoveMember: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		newSecret, err := RotateSecret(db, body.CartID)
		if err != nil {
			log.Printf("revoke: RotateSecret: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		// Broadcast member_removed to ALL connected members (including target) —
		// before the unsubscribe below, which is what stops the target hearing it.
		removedMsg := MemberRemovedMsg{Type: "member_removed", CartID: body.CartID, UserID: body.TargetUserID}
		removedB, _ := json.Marshal(removedMsg)
		hub.BroadcastToCart(body.CartID, removedB, "")

		// Broadcast secret_rotated to remaining members only (exclude target).
		rotatedMsg := SecretRotatedMsg{Type: "secret_rotated", CartID: body.CartID, Secret: newSecret}
		rotatedB, _ := json.Marshal(rotatedMsg)
		hub.BroadcastToCartExcludeUser(body.CartID, rotatedB, body.TargetUserID)

		// The revoke takes effect on the socket here, not in the target's client.
		// Their key died with their member row, but the connection they already
		// hold was verified before that and stays open for their other carts — so
		// this cart is cut from it server-side. Self-teardown on member_removed
		// remains the client's own housekeeping, no longer the enforcement.
		hub.UnsubscribeUserFromCart(body.TargetUserID, body.CartID)

		writeJSON(w, http.StatusOK, map[string]any{})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/{cartID}/invite
// ---------------------------------------------------------------------------

// inviteHandler lets the cart owner read or rotate the live invite secret. The
// secret is server-authoritative: a client's CloudKit-cached Cart.secret can be
// reverted by a sync conflict, so the owner refreshes (rotate:false) on every
// share-sheet open and can explicitly reset it (rotate:true) when a link leaks.
func inviteHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cartID := r.PathValue("cartID")
		if !isValidCartID(cartID) {
			log.Printf("invite: rejecting malformed cartID from %s", clientIP(r))
			writeError(w, http.StatusBadRequest, "invalid cartID")
			return
		}

		var body struct {
			Rotate bool `json:"rotate"`
		}
		if err := decodeBody(w, r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// Reading the invite secret is reading the credential that admits new
		// members, and rotating it locks out everyone holding the old link — so
		// this is the owner door too, and it used to accept the owner's ID as a
		// body field.
		if _, _, ok := authorizeCartOwner(db, w, r, cartID); !ok {
			return
		}

		if !body.Rotate {
			secret, err := GetSecret(db, cartID)
			if err != nil {
				writeError(w, http.StatusForbidden, "cart not found")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"secret": secret})
			return
		}

		newSecret, err := RotateSecret(db, cartID)
		if err != nil {
			log.Printf("invite: RotateSecret: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		// Reset rotates the secret out from under every outstanding link, so all
		// members (no exclude) get the new value to keep their cache authoritative.
		rotatedMsg := SecretRotatedMsg{Type: "secret_rotated", CartID: cartID, Secret: newSecret}
		rotatedB, _ := json.Marshal(rotatedMsg)
		hub.BroadcastToCart(cartID, rotatedB, "")

		writeJSON(w, http.StatusOK, map[string]any{"secret": newSecret})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/transfer
// ---------------------------------------------------------------------------

func transferHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			CartID     string `json:"cartID"`
			NewOwnerID string `json:"newOwnerID"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// Same swap as revoke: the actor is proved, the TARGET is still named in
		// the body and validated below. Without this, any member could hand a
		// cart's ownership to themselves by claiming to be its owner.
		if _, _, ok := authorizeCartOwner(db, w, r, body.CartID); !ok {
			return
		}

		isMem, err := IsMember(db, body.CartID, body.NewOwnerID)
		if err != nil || !isMem {
			writeError(w, http.StatusUnprocessableEntity, "new owner is not a member")
			return
		}

		if err := TransferOwner(db, body.CartID, body.NewOwnerID); err != nil {
			log.Printf("transfer: TransferOwner: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		var displayName string
		db.QueryRow(`SELECT COALESCE(display_name,'') FROM users WHERE user_id=?`, body.NewOwnerID).Scan(&displayName)

		msg := OwnerTransferredMsg{
			Type:        "owner_transferred",
			CartID:      body.CartID,
			NewOwnerID:  body.NewOwnerID,
			DisplayName: displayName,
		}
		b, _ := json.Marshal(msg)
		hub.BroadcastToCart(body.CartID, b, "")

		// A tier is reported for the owner and nobody else, so an ownership change IS a
		// tier change. owner_transferred carries no members, so without this the new owner
		// keeps the empty tier they held as an ordinary member until some other write
		// happens to produce a state message.
		stateB, _ := json.Marshal(buildStateMsg(db, hub, body.CartID))
		hub.BroadcastToCart(body.CartID, stateB, "")

		writeJSON(w, http.StatusOK, map[string]any{})
	}
}

// ---------------------------------------------------------------------------
// POST /api/user/tier
// ---------------------------------------------------------------------------

func updateTierHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID string `json:"userID"`
			Tier   string `json:"tier"` // "free" | "premium"
		}
		if err := decodeBody(w, r, &body); err != nil || body.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// NOTE: StoreKit 2 transaction validation required before production.
		// Trust-client is acceptable for TestFlight only.
		if body.Tier != "free" && body.Tier != "premium" {
			writeError(w, http.StatusBadRequest, "invalid tier")
			return
		}

		if _, err := db.Exec(`UPDATE users SET tier=? WHERE user_id=?`, body.Tier, body.UserID); err != nil {
			log.Printf("updateTier: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{})
	}
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ---------------------------------------------------------------------------
// buildStateMsg — shared state-snapshot helper
// ---------------------------------------------------------------------------

func buildStateMsg(db *sql.DB, hub *Hub, cartID string) StateMsg {
	ownerID, _ := GetOwner(db, cartID)
	members, _ := GetMembers(db, cartID)
	items, _ := GetItems(db, cartID)
	tombstones, _ := GetTombstones(db, cartID)
	activeShopping := hub.GetActiveShopping(cartID)

	if items == nil {
		items = []ItemRow{}
	}
	if members == nil {
		members = []MemberInfo{}
	}
	if tombstones == nil {
		tombstones = []TombstoneRow{}
	}
	if activeShopping == nil {
		activeShopping = []ActiveShoppingEntry{}
	}

	// premiumForAll (TestFlight): report the OWNER as premium so the client's optimistic frozen
	// check (CartShareSheet.isCartFrozen) clears too. That check reads the owner's row and no
	// other, so the owner's row is all there is to rewrite — rewriting every row would put back
	// exactly the broadcast GetMembers stops making. The join capacity check is bypassed
	// separately; this only rewrites the broadcast state, leaving the DB and the hasPremiumOwner
	// read untouched.
	if premiumForAll {
		for i := range members {
			if members[i].UserID == ownerID {
				members[i].Tier = "premium"
			}
		}
	}

	return StateMsg{
		Type:                  "state",
		V:                     protocolVersion,
		MinV:                  minClientVersion,
		CartID:                cartID,
		OwnerID:               ownerID,
		Items:                 items,
		Members:               members,
		ActiveShoppingMembers: activeShopping,
		Tombstones:            tombstones,
	}
}

// ---------------------------------------------------------------------------
// WS /ws
// ---------------------------------------------------------------------------

// verifySubscription resolves requested (cartID, subKey) pairs into the set this
// connection may subscribe to, plus the rejections to report back.
//
// It is the whole authentication of the socket. Identity is DERIVED from the
// keys rather than asserted in a header: the first pair that verifies fixes the
// connection's userID, and any later pair minted for a different person is
// refused. That is what stops one member of a cart — who already knows every
// co-member's userID, since it rides every state message and every check-off
// attribution — from opening a user-scoped socket as somebody else and reading
// every cart that person is in.
//
// Pass expectUserID = "" for a fresh connection, or the established userID for a
// re-subscribe.
func verifySubscription(db *sql.DB, req []SubscribeCart, expectUserID string) (userID string, accepted []string, rejected []SubscribeReject) {
	userID = expectUserID
	if len(req) > maxSubscribeCarts {
		req = req[:maxSubscribeCarts]
	}

	seen := make(map[string]bool, len(req))
	for _, want := range req {
		wantCart := strings.ToLower(want.CartID)
		if !isValidCartID(wantCart) || want.SubKey == "" {
			rejected = append(rejected, SubscribeReject{CartID: want.CartID, Reason: rejectMalformed})
			continue
		}

		cartID, keyUserID, ok, err := ResolveSubKey(db, want.SubKey)
		if err != nil {
			log.Printf("verifySubscription: ResolveSubKey: %v", err)
			rejected = append(rejected, SubscribeReject{CartID: wantCart, Reason: rejectUnknownKey})
			continue
		}
		if !ok {
			rejected = append(rejected, SubscribeReject{CartID: wantCart, Reason: rejectUnknownKey})
			continue
		}
		if !strings.EqualFold(cartID, wantCart) {
			rejected = append(rejected, SubscribeReject{CartID: wantCart, Reason: rejectCartMismatch})
			continue
		}
		if userID == "" {
			userID = keyUserID
		} else if keyUserID != userID {
			rejected = append(rejected, SubscribeReject{CartID: wantCart, Reason: rejectUserMismatch})
			continue
		}

		if !seen[cartID] {
			seen[cartID] = true
			accepted = append(accepted, cartID)
		}
	}
	return userID, accepted, rejected
}

// wsHandler serves the one user-scoped socket. There is no cart in the path and
// no credential in the handshake: the connection is upgraded first and proves
// itself with its first message, because what it is proving is membership of a
// SET of carts, which does not fit in a URL.
//
// Nothing is registered on the hub until that proof lands. A connection that
// sends no valid subscription is told why and closed, never left half-registered
// holding a hub slot it cannot use.
func wsHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("wsHandler: upgrade: %v", err)
			return
		}

		conn.SetReadLimit(maxWSMessageBytes)
		conn.SetReadDeadline(time.Now().Add(subscribeDeadline))
		_, rawMsg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("wsHandler: no subscribe message from %s: %v", clientIP(r), err)
			conn.Close()
			return
		}

		var sub SubscribeMsg
		if err := json.Unmarshal(rawMsg, &sub); err != nil || sub.Type != "subscribe" {
			closeUnsubscribed(conn, "expected a subscribe message", nil)
			return
		}

		userID, accepted, rejected := verifySubscription(db, sub.Carts, "")
		if len(accepted) == 0 {
			// Every pair failed. The rejections still go out — "you were removed
			// from this cart" is the common reason and the client acts on it.
			closeUnsubscribed(conn, "no cart subscription verified", rejected)
			return
		}

		client := &Client{
			userID:     userID,
			deviceID:   r.Header.Get("X-Device-ID"),
			conn:       conn,
			send:       make(chan []byte, 256),
			subscribed: make(map[string]bool),
		}

		hub.Register(client)
		hub.Subscribe(client, accepted)

		sendJSON(client, SubscribeResultMsg{
			Type:     "subscribe_result",
			V:        protocolVersion,
			MinV:     minClientVersion,
			Accepted: accepted,
			Rejected: rejected,
		})
		// One state snapshot per subscribed cart.
		for _, cartID := range accepted {
			sendJSON(client, buildStateMsg(db, hub, cartID))
		}

		go writePump(client)
		readPump(client, hub, db) // blocks; unregisters on return
	}
}

// closeUnsubscribed reports why a connection is being refused and closes it. The
// result message is written directly — the pumps never started, so there is no
// send channel to queue it on.
func closeUnsubscribed(conn *websocket.Conn, reason string, rejected []SubscribeReject) {
	b, err := json.Marshal(SubscribeResultMsg{
		Type:     "subscribe_result",
		V:        protocolVersion,
		MinV:     minClientVersion,
		Accepted: []string{},
		Rejected: rejected,
	})
	if err == nil {
		conn.SetWriteDeadline(time.Now().Add(writeDeadline))
		conn.WriteMessage(websocket.TextMessage, b)
	}
	conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.ClosePolicyViolation, reason))
	conn.Close()
}

// ---------------------------------------------------------------------------
// Consumption-reservoir authorization
// ---------------------------------------------------------------------------

// authorizeCartActor authenticates the ACTOR of a cart-scoped HTTP request with
// their subscription key (header `X-Sub-Key`), and returns the cart the key was
// minted for along with the member who holds it.
//
// **Naming who you are acting ON is fine; naming who you ARE is the bug.** Every
// handler below still takes its target from the body — the member being revoked,
// the new owner — because a target is a fact about the request, checkable against
// the room. The ACTOR is different: a self-asserted actor is a claim, and checking
// a claim against a fact (`ownerID == body.RequestingUserID`) only looks like
// authorization. Every member learns the owner's userID from every state message
// (`MemberInfo.userID`), so anyone in the cart could satisfy that comparison by
// writing the owner's ID into their own request.
//
// The cartID comes from the KEY, and `wantCartID` — the cart the request claims to
// be about, from its path or its body — is then checked to agree. The key is the
// thing that was actually proved, so deriving the cart from it means a caller can
// never reach a cart the key was not minted for, even if a later edit drops the
// comparison. Comparing anyway catches a client that has mixed up its own storage,
// which is worth a 403 rather than silently acting on some other cart the caller
// happens to be in.
//
// A key that resolves IS proof of membership — the row it lives on is the
// membership — so no separate `IsMember` lookup is needed for the actor.
func authorizeCartActor(db *sql.DB, w http.ResponseWriter, r *http.Request, wantCartID string) (cartID, userID string, ok bool) {
	subKey := r.Header.Get("X-Sub-Key")
	if subKey == "" {
		http.Error(w, "missing X-Sub-Key header", http.StatusUnauthorized)
		return "", "", false
	}

	keyCartID, keyUserID, resolved, err := ResolveSubKey(db, subKey)
	if err != nil {
		log.Printf("authorizeCartActor: ResolveSubKey: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return "", "", false
	}
	if !resolved {
		http.Error(w, "forbidden", http.StatusForbidden)
		return "", "", false
	}
	if !strings.EqualFold(keyCartID, wantCartID) {
		log.Printf("authorizeCartActor: key/request cart mismatch from %s", clientIP(r))
		http.Error(w, "forbidden", http.StatusForbidden)
		return "", "", false
	}
	return keyCartID, keyUserID, true
}

// authorizeCartOwner is authorizeCartActor plus "and you are the owner of it".
// The owner check compares the room's `owner_id` against the userID the KEY
// resolved to — never against one the caller supplied.
func authorizeCartOwner(db *sql.DB, w http.ResponseWriter, r *http.Request, wantCartID string) (cartID, userID string, ok bool) {
	cartID, userID, ok = authorizeCartActor(db, w, r, wantCartID)
	if !ok {
		return "", "", false
	}
	ownerID, err := GetOwner(db, cartID)
	if err != nil || ownerID != userID {
		writeError(w, http.StatusForbidden, "not the owner")
		return "", "", false
	}
	return cartID, userID, true
}

// ---------------------------------------------------------------------------
// GET /api/cart/{cartID}/history?globalID=… (§3.4 consumption reservoir)
// ---------------------------------------------------------------------------

func historyReadHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cartID, _, ok := authorizeCartActor(db, w, r, r.PathValue("cartID"))
		if !ok {
			return
		}

		globalID := r.URL.Query().Get("globalID")
		if globalID == "" {
			writeError(w, http.StatusBadRequest, "missing globalID")
			return
		}

		events, err := GetConsumptionEvents(db, cartID, globalID)
		if err != nil {
			log.Printf("historyReadHandler: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if events == nil {
			events = []float64{}
		}

		writeJSON(w, http.StatusOK, map[string]any{"events": events})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/{cartID}/history/backfill (§3.4 pre-share backfill)
// ---------------------------------------------------------------------------

func historyBackfillHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cartID, _, ok := authorizeCartActor(db, w, r, r.PathValue("cartID"))
		if !ok {
			return
		}

		var body struct {
			GlobalID string    `json:"globalID"`
			Events   []float64 `json:"events"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.GlobalID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if err := BackfillConsumptionEvents(db, cartID, body.GlobalID, body.Events); err != nil {
			log.Printf("historyBackfillHandler: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
