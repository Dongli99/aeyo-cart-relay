package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// The WebSocket origin policy is the one security-relevant behaviour that is decided
// by a header's ABSENCE, which makes it easy to break without noticing: a change that
// makes CheckOrigin permissive again looks harmless and passes every other test.
func TestCheckOriginAllowsOnlyOriginlessHandshakes(t *testing.T) {
	cases := []struct {
		name   string
		origin string
		want   bool
	}{
		{"native client sends no Origin", "", true},
		{"browser on another site", "https://evil.example", false},
		{"browser on our own site is still a browser", "https://aeyo.app", false},
		{"empty-ish but present", " ", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest("GET", "/ws", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := upgrader.CheckOrigin(r); got != tc.want {
				t.Errorf("CheckOrigin(Origin=%q) = %v, want %v", tc.origin, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Subscription verification — the socket's whole authentication
// ---------------------------------------------------------------------------

// subTestDB builds a DB with two real-shaped carts and two members, returning
// each member's subscription key. verifySubscription checks cart-id SHAPE, so
// these carts need newUUID ids rather than the "cart-a" fixtures elsewhere.
func subTestDB(t *testing.T) (db *sql.DB, cartA, cartB, keyAliceA, keyAliceB, keyBobA string) {
	t.Helper()
	db, err := InitDB(filepath.Join(t.TempDir(), "sub.db"))
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, u := range []string{"alice", "bob"} {
		if err := UpsertUser(db, u, u, "#fff"); err != nil {
			t.Fatalf("UpsertUser %s: %v", u, err)
		}
	}
	cartA, cartB = newUUID(), newUUID()
	if err := CreateRoom(db, cartA, "alice", "secret-a", "A", "#fff"); err != nil {
		t.Fatalf("CreateRoom A: %v", err)
	}
	if err := CreateRoom(db, cartB, "alice", "secret-b", "B", "#fff"); err != nil {
		t.Fatalf("CreateRoom B: %v", err)
	}
	keyAliceA, _ = AddMember(db, cartA, "alice")
	keyAliceB, _ = AddMember(db, cartB, "alice")
	keyBobA, _ = AddMember(db, cartA, "bob")
	return db, cartA, cartB, keyAliceA, keyAliceB, keyBobA
}

// Identity is derived from the keys, never asserted. Every member of a cart
// already knows every co-member's userID — it rides each state message and each
// check-off attribution — so a user-scoped socket that trusted a claimed userID
// would hand Bob every cart Alice is in. Bob's own key for a cart they share
// must not open Alice's connection.
func TestVerifySubscriptionDerivesIdentityFromTheKey(t *testing.T) {
	db, cartA, cartB, keyAliceA, keyAliceB, keyBobA := subTestDB(t)

	userID, accepted, rejected := verifySubscription(db,
		[]SubscribeCart{{CartID: cartA, SubKey: keyAliceA}, {CartID: cartB, SubKey: keyAliceB}}, "")
	if userID != "alice" {
		t.Errorf("derived userID = %q, want alice", userID)
	}
	if len(accepted) != 2 || len(rejected) != 0 {
		t.Errorf("accepted=%v rejected=%v, want both of alice's carts", accepted, rejected)
	}

	// Bob's key resolves to Bob, so it cannot ride along on Alice's connection.
	_, accepted, rejected = verifySubscription(db,
		[]SubscribeCart{{CartID: cartA, SubKey: keyAliceA}, {CartID: cartA, SubKey: keyBobA}}, "")
	if len(accepted) != 1 || accepted[0] != cartA {
		t.Errorf("accepted = %v, want only alice's cart-A", accepted)
	}
	if len(rejected) != 1 || rejected[0].Reason != rejectUserMismatch {
		t.Errorf("rejected = %v, want one user_mismatch", rejected)
	}

	// And a re-subscribe cannot change who the connection is.
	_, accepted, rejected = verifySubscription(db,
		[]SubscribeCart{{CartID: cartA, SubKey: keyBobA}}, "alice")
	if len(accepted) != 0 {
		t.Errorf("a re-subscribe accepted another person's cart: %v", accepted)
	}
	if len(rejected) != 1 || rejected[0].Reason != rejectUserMismatch {
		t.Errorf("rejected = %v, want one user_mismatch", rejected)
	}
}

// One bad pair costs that cart and nothing else. A member revoked from one cart
// must not lose the sync of every other cart they are in — which is precisely
// what failing the whole connection would do.
func TestVerifySubscriptionRejectsPerCart(t *testing.T) {
	db, cartA, cartB, keyAliceA, _, _ := subTestDB(t)

	revokedCart := cartB
	if err := RemoveMember(db, revokedCart, "alice"); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}

	cases := []struct {
		name   string
		cart   SubscribeCart
		reason string
	}{
		{"revoked membership", SubscribeCart{CartID: revokedCart, SubKey: generateSubKey()}, rejectUnknownKey},
		{"key minted for another cart", SubscribeCart{CartID: newUUID(), SubKey: keyAliceA}, rejectCartMismatch},
		{"malformed cart id", SubscribeCart{CartID: "not-a-cart", SubKey: keyAliceA}, rejectMalformed},
		{"empty key", SubscribeCart{CartID: newUUID(), SubKey: ""}, rejectMalformed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userID, accepted, rejected := verifySubscription(db,
				[]SubscribeCart{{CartID: cartA, SubKey: keyAliceA}, tc.cart}, "")
			if userID != "alice" {
				t.Errorf("userID = %q, want alice", userID)
			}
			if len(accepted) != 1 || accepted[0] != cartA {
				t.Errorf("accepted = %v, want the good cart to survive", accepted)
			}
			if len(rejected) != 1 || rejected[0].Reason != tc.reason {
				t.Errorf("rejected = %v, want one %s", rejected, tc.reason)
			}
		})
	}
}

// A request naming more carts than a person could plausibly be in is truncated
// rather than turned into an unbounded burst of key lookups.
func TestVerifySubscriptionBoundsRequestSize(t *testing.T) {
	db, cartA, _, keyAliceA, _, _ := subTestDB(t)

	req := make([]SubscribeCart, 0, maxSubscribeCarts+10)
	for i := 0; i < maxSubscribeCarts+10; i++ {
		req = append(req, SubscribeCart{CartID: newUUID(), SubKey: generateSubKey()})
	}
	req = append(req, SubscribeCart{CartID: cartA, SubKey: keyAliceA})

	_, accepted, rejected := verifySubscription(db, req, "")
	if len(accepted) != 0 {
		t.Errorf("accepted = %v, want nothing past the cap", accepted)
	}
	if len(rejected) != maxSubscribeCarts {
		t.Errorf("rejected %d pairs, want the cap of %d", len(rejected), maxSubscribeCarts)
	}
}

// The consumption reservoir is authenticated by the member's key, not by a
// self-asserted userID. It holds a household's purchase-timing history, and every
// member of a cart knows every co-member's userID — so a header-only guard let
// anyone holding a (userID, cartID) pair read that history or inject fabricated
// events into the household's cadence learning.
//
// The cart comes from the KEY; the path is checked to agree. A key minted for
// another cart must not reach this one even though it is perfectly valid.
func TestAuthorizeCartRequestTakesTheCartFromTheKey(t *testing.T) {
	db, cartA, cartB, keyAliceA, _, _ := subTestDB(t)

	cases := []struct {
		name     string
		path     string
		header   string
		wantOK   bool
		wantCode int
	}{
		{"member's own key", cartA, keyAliceA, true, http.StatusOK},
		{"no key at all", cartA, "", false, http.StatusUnauthorized},
		{"key never minted", cartA, generateSubKey(), false, http.StatusForbidden},
		{"valid key for another cart", cartB, keyAliceA, false, http.StatusForbidden},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/api/cart/"+tc.path+"/history", nil)
			r.SetPathValue("cartID", tc.path)
			if tc.header != "" {
				r.Header.Set("X-Sub-Key", tc.header)
			}
			w := httptest.NewRecorder()

			cartID, _, ok := authorizeCartActor(db, w, r, r.PathValue("cartID"))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (body %q)", ok, tc.wantOK, w.Body.String())
			}
			if !tc.wantOK {
				if w.Code != tc.wantCode {
					t.Errorf("status = %d, want %d", w.Code, tc.wantCode)
				}
				if cartID != "" {
					t.Errorf("a refused request yielded cartID %q", cartID)
				}
				return
			}
			if cartID != cartA {
				t.Errorf("cartID = %q, want %q", cartID, cartA)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Actor authorization on the membership-mutating handlers
// ---------------------------------------------------------------------------

// postJSON drives a handler with a JSON body, a subscription key, and the cartID
// set as a path value (harmless for the body-cart handlers, required for invite).
func postJSON(t *testing.T, h http.HandlerFunc, cartID, subKey string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	r := httptest.NewRequest("POST", "/api/cart/"+cartID, bytes.NewReader(raw))
	r.SetPathValue("cartID", cartID)
	if subKey != "" {
		r.Header.Set("X-Sub-Key", subKey)
	}
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

func isMemberNow(t *testing.T, db *sql.DB, cartID, userID string) bool {
	t.Helper()
	ok, err := IsMember(db, cartID, userID)
	if err != nil {
		t.Fatalf("IsMember: %v", err)
	}
	return ok
}

// Bob is a member of cart A. Alice owns it. Every member learns the owner's
// userID from every state message, so before this was a key check Bob could
// revoke Alice — or anyone — simply by writing her ID into `requestingUserID`.
// The assertion is the refusal; the owner's own success only proves the refusal
// is discriminating rather than a handler that rejects everything.
func TestRevokeRefusesAMemberClaimingToBeTheOwner(t *testing.T) {
	db, cartA, _, keyAliceA, _, keyBobA := subTestDB(t)
	h := revokeHandler(db, NewHub())

	w := postJSON(t, h, cartA, keyBobA, map[string]any{
		"cartID": cartA, "targetUserID": "alice", "requestingUserID": "alice",
	})
	if w.Code != http.StatusForbidden {
		t.Errorf("a member revoking the owner: status = %d, want 403 (body %q)", w.Code, w.Body.String())
	}
	if !isMemberNow(t, db, cartA, "alice") {
		t.Errorf("the owner was revoked by a non-owner member")
	}

	w = postJSON(t, h, cartA, keyAliceA, map[string]any{"cartID": cartA, "targetUserID": "bob"})
	if w.Code != http.StatusOK {
		t.Fatalf("the owner's own key must be accepted: status = %d (body %q)", w.Code, w.Body.String())
	}
	if isMemberNow(t, db, cartA, "bob") {
		t.Errorf("the owner's revoke did not remove the target")
	}
}

// Ownership seizure: Bob names himself the new owner and claims to be Alice.
func TestTransferRefusesAMemberClaimingToBeTheOwner(t *testing.T) {
	db, cartA, _, keyAliceA, _, keyBobA := subTestDB(t)
	h := transferHandler(db, NewHub())

	w := postJSON(t, h, cartA, keyBobA, map[string]any{
		"cartID": cartA, "newOwnerID": "bob", "requestingUserID": "alice",
	})
	if w.Code != http.StatusForbidden {
		t.Errorf("a member seizing ownership: status = %d, want 403 (body %q)", w.Code, w.Body.String())
	}
	if owner, _ := GetOwner(db, cartA); owner != "alice" {
		t.Errorf("ownership was seized: owner = %q, want alice", owner)
	}

	w = postJSON(t, h, cartA, keyAliceA, map[string]any{"cartID": cartA, "newOwnerID": "bob"})
	if w.Code != http.StatusOK {
		t.Fatalf("the owner's own key must be accepted: status = %d (body %q)", w.Code, w.Body.String())
	}
	if owner, _ := GetOwner(db, cartA); owner != "bob" {
		t.Errorf("the owner's transfer did not take: owner = %q, want bob", owner)
	}
}

// Leave removes the KEYHOLDER. The body used to name the leaver, which made this
// "remove whoever is named" — so Bob could force Alice out of her own cart.
func TestLeaveRemovesTheKeyholderNotTheBodysNominee(t *testing.T) {
	db, cartA, _, _, _, keyBobA := subTestDB(t)
	h := leaveCartHandler(db, NewHub())

	w := postJSON(t, h, cartA, keyBobA, map[string]any{"cartID": cartA, "userID": "alice"})
	if w.Code != http.StatusOK {
		t.Fatalf("bob leaving: status = %d, want 200 (body %q)", w.Code, w.Body.String())
	}
	if !isMemberNow(t, db, cartA, "alice") {
		t.Errorf("the body's nominee was removed — a member can force another member out")
	}
	if isMemberNow(t, db, cartA, "bob") {
		t.Errorf("the keyholder was not removed")
	}
}

// The invite secret admits new members, and rotating it locks out every
// outstanding link — so reading or rotating it is the owner's alone.
func TestInviteRefusesAMemberClaimingToBeTheOwner(t *testing.T) {
	db, cartA, _, keyAliceA, _, keyBobA := subTestDB(t)
	h := inviteHandler(db, NewHub())

	w := postJSON(t, h, cartA, keyBobA, map[string]any{"userID": "alice", "rotate": false})
	if w.Code != http.StatusForbidden {
		t.Errorf("a member reading the invite secret: status = %d, want 403 (body %q)", w.Code, w.Body.String())
	}

	w = postJSON(t, h, cartA, keyBobA, map[string]any{"userID": "alice", "rotate": true})
	if w.Code != http.StatusForbidden {
		t.Errorf("a member rotating the invite secret: status = %d, want 403 (body %q)", w.Code, w.Body.String())
	}
	if secret, _ := GetSecret(db, cartA); secret != "secret-a" {
		t.Errorf("a non-owner rotated the invite secret out from under everyone")
	}

	w = postJSON(t, h, cartA, keyAliceA, map[string]any{"rotate": false})
	if w.Code != http.StatusOK {
		t.Fatalf("the owner's own key must be accepted: status = %d (body %q)", w.Code, w.Body.String())
	}
}

// No key at all is refused everywhere, so a client that simply stopped sending
// the header cannot fall back to the old self-asserted path.
func TestMembershipHandlersRefuseAKeylessRequest(t *testing.T) {
	db, cartA, _, _, _, _ := subTestDB(t)
	hub := NewHub()

	handlers := map[string]http.HandlerFunc{
		"revoke":   revokeHandler(db, hub),
		"transfer": transferHandler(db, hub),
		"leave":    leaveCartHandler(db, hub),
		"invite":   inviteHandler(db, hub),
	}
	for name, h := range handlers {
		t.Run(name, func(t *testing.T) {
			w := postJSON(t, h, cartA, "", map[string]any{
				"cartID": cartA, "userID": "alice", "requestingUserID": "alice",
				"targetUserID": "bob", "newOwnerID": "bob",
			})
			if w.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401 (body %q)", w.Code, w.Body.String())
			}
		})
	}
}
