package main

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// The only WebSocket client is the iOS app, and a native URLSession handshake sends
	// no Origin header at all. A browser always sends one, so "no Origin" is the signal
	// to allow and anything else is a cross-site attempt.
	//
	// Defence in depth rather than the load-bearing check: a browser cannot set the
	// X-User-ID header on a WebSocket handshake, so a cross-site connection already
	// fails membership auth. If a browser client ever ships, this becomes an explicit
	// allowlist of Aeyo origins rather than an emptiness test.
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

func createCartHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID      string `json:"userID"`
			DisplayName string `json:"displayName"`
			Color       string `json:"color"`
			CartName    string `json:"cartName"`
			HexColor    string `json:"hexColor"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if err := UpsertUser(db, body.UserID, body.DisplayName, body.Color); err != nil {
			log.Printf("createCart: UpsertUser: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		cartID := newUUID()
		secret := generateSecret()

		if err := CreateRoom(db, cartID, body.UserID, secret, body.CartName, body.HexColor); err != nil {
			log.Printf("createCart: CreateRoom: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if err := AddMember(db, cartID, body.UserID); err != nil {
			log.Printf("createCart: AddMember: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"cartID": cartID,
			"secret": secret,
		})
	}
}

// ---------------------------------------------------------------------------
// POST /api/cart/join
// ---------------------------------------------------------------------------

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

		// Capacity check + AddMember in an EXCLUSIVE transaction.
		tx, err := db.Begin()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		defer tx.Rollback()

		if _, err := tx.Exec(`BEGIN EXCLUSIVE`); err != nil {
			// SQLite driver auto-starts; ignore duplicate BEGIN if driver handles it.
			_ = err
		}

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

		if _, err := tx.Exec(`
			INSERT OR REPLACE INTO members (cart_id, user_id, joined_at)
			VALUES (?, ?, strftime('%s','now'))
		`, body.CartID, body.UserID); err != nil {
			log.Printf("joinCart: AddMember in tx: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		if err := tx.Commit(); err != nil {
			log.Printf("joinCart: commit: %v", err)
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
			UserID     string `json:"userID"`
			TransferTo string `json:"transferTo"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" || body.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		ok, err := IsMember(db, body.CartID, body.UserID)
		if err != nil || !ok {
			writeError(w, http.StatusForbidden, "not a member")
			return
		}

		ownerID, err := GetOwner(db, body.CartID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		if body.UserID == ownerID {
			// Determine new owner.
			newOwner := body.TransferTo

			// Validate transferTo is a member (other than the leaver).
			if newOwner != "" && newOwner != body.UserID {
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
				`, body.CartID, body.UserID).Scan(&newOwner)
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
					NewOwnerID:  newOwner,
					DisplayName: displayName,
				}
				b, _ := json.Marshal(msg)
				hub.BroadcastToCart(body.CartID, b, "")
			}
		}

		if err := RemoveMember(db, body.CartID, body.UserID); err != nil {
			log.Printf("leaveCart: RemoveMember: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		removed := MemberRemovedMsg{Type: "member_removed", UserID: body.UserID}
		rb, _ := json.Marshal(removed)
		// Exclude the leaver: they initiated the leave and self-reset client-side.
		// Broadcasting member_removed to them would fire a false "you've been removed" insight.
		hub.BroadcastToCartExcludeUser(body.CartID, rb, body.UserID)

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
			CartID           string `json:"cartID"`
			TargetUserID     string `json:"targetUserID"`
			RequestingUserID string `json:"requestingUserID"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		ownerID, err := GetOwner(db, body.CartID)
		if err != nil || ownerID != body.RequestingUserID {
			writeError(w, http.StatusForbidden, "not the owner")
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

		// Broadcast member_removed to ALL connected members (including target).
		removedMsg := MemberRemovedMsg{Type: "member_removed", UserID: body.TargetUserID}
		removedB, _ := json.Marshal(removedMsg)
		hub.BroadcastToCart(body.CartID, removedB, "")

		// Broadcast secret_rotated to remaining members only (exclude target).
		rotatedMsg := SecretRotatedMsg{Type: "secret_rotated", Secret: newSecret}
		rotatedB, _ := json.Marshal(rotatedMsg)
		hub.BroadcastToCartExcludeUser(body.CartID, rotatedB, body.TargetUserID)

		// No forced disconnect: the target's client self-tears-down on member_removed.
		// The secret is already rotated so a misbehaving client cannot rejoin.

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
			UserID string `json:"userID"`
			Rotate bool   `json:"rotate"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		ownerID, err := GetOwner(db, cartID)
		if err != nil || ownerID != body.UserID {
			writeError(w, http.StatusForbidden, "not the owner")
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
		rotatedMsg := SecretRotatedMsg{Type: "secret_rotated", Secret: newSecret}
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
			CartID           string `json:"cartID"`
			NewOwnerID       string `json:"newOwnerID"`
			RequestingUserID string `json:"requestingUserID"`
		}
		if err := decodeBody(w, r, &body); err != nil || body.CartID == "" {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		ownerID, err := GetOwner(db, body.CartID)
		if err != nil || ownerID != body.RequestingUserID {
			writeError(w, http.StatusForbidden, "not the owner")
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
			NewOwnerID:  body.NewOwnerID,
			DisplayName: displayName,
		}
		b, _ := json.Marshal(msg)
		hub.BroadcastToCart(body.CartID, b, "")

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

	// premiumForAll (TestFlight): report every member as premium so the client's optimistic frozen
	// check (CartShareSheet.isCartFrozen) clears too. The join capacity check is bypassed separately;
	// this only rewrites the broadcast state, leaving the DB and the hasPremiumOwner read untouched.
	if premiumForAll {
		for i := range members {
			members[i].Tier = "premium"
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
// WS /ws/{cartID}
// ---------------------------------------------------------------------------

func wsHandler(db *sql.DB, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract cartID from path: /ws/{cartID}
		cartID := strings.TrimPrefix(r.URL.Path, "/ws/")
		if cartID == "" || strings.Contains(cartID, "/") {
			http.Error(w, "invalid cartID", http.StatusBadRequest)
			return
		}

		userID := r.Header.Get("X-User-ID")
		if userID == "" {
			http.Error(w, "missing X-User-ID header", http.StatusUnauthorized)
			return
		}

		ok, err := IsMember(db, cartID, userID)
		if err != nil || !ok {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("wsHandler: upgrade: %v", err)
			return
		}

		deviceID := r.Header.Get("X-Device-ID")

		client := &Client{
			cartID:   cartID,
			userID:   userID,
			deviceID: deviceID,
			conn:     conn,
			send:     make(chan []byte, 256),
		}

		hub.Register(client)

		// Send initial state snapshot.
		stateB, _ := json.Marshal(buildStateMsg(db, hub, cartID))
		client.send <- stateB

		go writePump(client)
		readPump(client, hub, db) // blocks; unregisters on return
	}
}

// ---------------------------------------------------------------------------
// GET /api/cart/{cartID}/history?globalID=… (§3.4 consumption reservoir)
// ---------------------------------------------------------------------------

func historyReadHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cartID := r.PathValue("cartID")

		userID := r.Header.Get("X-User-ID")
		if userID == "" {
			http.Error(w, "missing X-User-ID header", http.StatusUnauthorized)
			return
		}
		ok, err := IsMember(db, cartID, userID)
		if err != nil || !ok {
			http.Error(w, "forbidden", http.StatusForbidden)
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
		cartID := r.PathValue("cartID")

		userID := r.Header.Get("X-User-ID")
		if userID == "" {
			http.Error(w, "missing X-User-ID header", http.StatusUnauthorized)
			return
		}
		ok, err := IsMember(db, cartID, userID)
		if err != nil || !ok {
			http.Error(w, "forbidden", http.StatusForbidden)
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
