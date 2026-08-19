package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// newTestClient builds a Client usable by handleDelta without a real WebSocket:
// handleDelta only touches userID/deviceID, the subscription set, and the
// buffered send channel (the conn is used solely by the read/write pumps).
func newTestClient(userID, deviceID string) *Client {
	return &Client{
		userID:     userID,
		deviceID:   deviceID,
		send:       make(chan []byte, 16),
		subscribed: make(map[string]bool),
	}
}

// joinHub registers a client and subscribes it to cartIDs, the way wsHandler does.
func joinHub(h *Hub, c *Client, cartIDs ...string) *Client {
	h.Register(c)
	h.Subscribe(c, cartIDs)
	return c
}

// drainAck reads one message from a client's send channel and, if it is an ack,
// returns its status. Returns ("", false) if nothing is queued.
func drainAck(c *Client) (status string, ok bool) {
	select {
	case b := <-c.send:
		var ack AckMsg
		if err := json.Unmarshal(b, &ack); err == nil && ack.Type == "ack" {
			return ack.Status, true
		}
		return "", false
	default:
		return "", false
	}
}

// F6: a multi-op delta is rejected — not persisted, not relayed — and the
// connection survives (handleDelta returns normally, sending only a reject ack).
func TestHandleDeltaRejectsMultiOp(t *testing.T) {
	db := testDB(t)
	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	hub := NewHub()

	sender := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a")
	peer := joinHub(hub, newTestClient("user-a", "dev-2"), "cart-a")

	raw, _ := json.Marshal(DeltaMsg{
		Type: "delta", CartID: "cart-a", DeviceID: "dev-1", Ts: 1000,
		Ops: []Op{
			{Op: "add", ID: "item-1", Fields: map[string]any{"title": "A"}},
			{Op: "add", ID: "item-2", Fields: map[string]any{"title": "B"}},
		},
	})
	handleDelta(sender, hub, db, raw)

	if status, ok := drainAck(sender); !ok || status != "rejected" {
		t.Errorf("sender ack: status=%q ok=%v, want \"rejected\"", status, ok)
	}
	if _, found := itemTitle(t, db, "cart-a", "item-1"); found {
		t.Errorf("item-1 must not be persisted from a rejected multi-op delta")
	}
	if _, found := itemTitle(t, db, "cart-a", "item-2"); found {
		t.Errorf("item-2 must not be persisted from a rejected multi-op delta")
	}
	if _, ok := drainAck(peer); ok {
		t.Errorf("peer must receive nothing for a rejected multi-op delta")
	}
	if len(peer.send) != 0 {
		t.Errorf("peer received a relay for a rejected multi-op delta")
	}
}

// F6: a single-op delta behaves as before — persisted, relayed to peers,
// acked "applied" to the sender.
func TestHandleDeltaSingleOpAppliesAndRelays(t *testing.T) {
	db := testDB(t)
	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	hub := NewHub()

	sender := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a")
	peer := joinHub(hub, newTestClient("user-a", "dev-2"), "cart-a")

	raw, _ := json.Marshal(DeltaMsg{
		Type: "delta", CartID: "cart-a", DeviceID: "dev-1", Ts: 1000,
		Ops: []Op{{Op: "add", ID: "item-1", Fields: map[string]any{"title": "Milk"}}},
	})
	handleDelta(sender, hub, db, raw)

	if title, found := itemTitle(t, db, "cart-a", "item-1"); !found || title != "Milk" {
		t.Errorf("item-1 must be persisted: found=%v title=%q", found, title)
	}
	if status, ok := drainAck(sender); !ok || status != "applied" {
		t.Errorf("sender ack: status=%q ok=%v, want \"applied\"", status, ok)
	}
	// Peer (different deviceID) must receive exactly the relayed delta.
	select {
	case b := <-peer.send:
		var relayed DeltaMsg
		if err := json.Unmarshal(b, &relayed); err != nil || relayed.Type != "delta" {
			t.Errorf("peer relay decode: err=%v type=%q", err, relayed.Type)
		}
		if relayed.UserID != "user-a" {
			t.Errorf("relayed delta userID=%q, want injected \"user-a\"", relayed.UserID)
		}
	default:
		t.Errorf("peer must receive the relayed single-op delta")
	}
}

// ---------------------------------------------------------------------------
// Multiplexing: one connection, several carts
// ---------------------------------------------------------------------------

// The load-bearing property of a user-scoped socket: carts multiplexed over one
// connection stay separate. A delta naming cart B is persisted to cart B, is
// relayed only to clients subscribed to cart B, and leaves cart A untouched.
func TestMultiplexedDeltaStaysInItsCart(t *testing.T) {
	db := testDB(t)
	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember a: %v", err)
	}
	if _, err := AddMember(db, "cart-b", "user-a"); err != nil {
		t.Fatalf("AddMember b: %v", err)
	}
	hub := NewHub()

	// One connection carrying both carts, one peer in each.
	sender := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a", "cart-b")
	peerA := joinHub(hub, newTestClient("user-a", "dev-2"), "cart-a")
	peerB := joinHub(hub, newTestClient("user-a", "dev-3"), "cart-b")

	raw, _ := json.Marshal(DeltaMsg{
		Type: "delta", CartID: "cart-b", DeviceID: "dev-1", Ts: 1000,
		Ops: []Op{{Op: "add", ID: "item-1", Fields: map[string]any{"title": "Milk"}}},
	})
	handleDelta(sender, hub, db, raw)

	if title, found := itemTitle(t, db, "cart-b", "item-1"); !found || title != "Milk" {
		t.Errorf("cart-b must hold the item: found=%v title=%q", found, title)
	}
	if _, found := itemTitle(t, db, "cart-a", "item-1"); found {
		t.Errorf("cart-a must not receive a write addressed to cart-b")
	}
	if status, ok := drainAck(sender); !ok || status != "applied" {
		t.Errorf("sender ack: status=%q ok=%v, want \"applied\"", status, ok)
	}
	if len(peerA.send) != 0 {
		t.Errorf("a cart-a subscriber must not be relayed a cart-b delta")
	}
	if len(peerB.send) != 1 {
		t.Errorf("a cart-b subscriber must be relayed the delta (queued %d)", len(peerB.send))
	}
}

// The generalised connection-scope guard (RULE[sharing.acked-outbox] sub-rule 2,
// ADR-038 finding 3): a delta for a cart this connection never proved is
// rejected, not applied. The client keeps the write and re-routes it — which is
// exactly what makes widening the client's flush to a SET safe.
func TestDeltaForUnsubscribedCartIsRejected(t *testing.T) {
	db := testDB(t)
	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	hub := NewHub()

	sender := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a")

	raw, _ := json.Marshal(DeltaMsg{
		Type: "delta", CartID: "cart-b", DeviceID: "dev-1", Ts: 1000,
		Ops: []Op{{Op: "add", ID: "item-1", Fields: map[string]any{"title": "Milk"}}},
	})
	handleDelta(sender, hub, db, raw)

	if status, ok := drainAck(sender); !ok || status != "rejected" {
		t.Errorf("sender ack: status=%q ok=%v, want \"rejected\"", status, ok)
	}
	if _, found := itemTitle(t, db, "cart-b", "item-1"); found {
		t.Errorf("an unsubscribed cart must not be written")
	}
}

// A revoke or a leave ends the subscription server-side. The connection stays
// open for the person's other carts, so nothing else may end it: the socket
// outlives the membership now, and a client that ignored member_removed would
// otherwise keep reading and writing a cart it was thrown out of.
func TestUnsubscribeUserFromCartCutsOneCartOnly(t *testing.T) {
	db := testDB(t)
	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember a: %v", err)
	}
	if _, err := AddMember(db, "cart-b", "user-a"); err != nil {
		t.Fatalf("AddMember b: %v", err)
	}
	hub := NewHub()

	c := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a", "cart-b")

	if !hub.UnsubscribeUserFromCart("user-a", "cart-a") {
		t.Fatalf("UnsubscribeUserFromCart reported no subscription to drop")
	}
	if hub.IsSubscribed(c, "cart-a") {
		t.Errorf("cart-a subscription survived the revoke")
	}
	if !hub.IsSubscribed(c, "cart-b") {
		t.Errorf("cart-b subscription must be untouched by a cart-a revoke")
	}

	// The broadcast index must agree with the subscription set.
	hub.BroadcastToCart("cart-a", []byte(`{"type":"noop"}`), "")
	if len(c.send) != 0 {
		t.Errorf("a revoked cart still reaches the connection through the broadcast index")
	}
	hub.BroadcastToCart("cart-b", []byte(`{"type":"noop"}`), "")
	if len(c.send) != 1 {
		t.Errorf("cart-b broadcast did not reach the connection (queued %d)", len(c.send))
	}
}

// A dropped connection ends presence in EVERY cart it was shopping — one
// connection now carries several, and the member walks away from all of them at
// once. Each session_end names its own cart.
func TestUnregisterEndsSessionInEveryActiveCart(t *testing.T) {
	hub := NewHub()

	leaver := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a", "cart-b")
	watcher := joinHub(hub, newTestClient("user-b", "dev-2"), "cart-a", "cart-b")

	hub.SetActiveShopping("cart-a", "user-a", "store-1")
	hub.SetActiveShopping("cart-b", "user-a", "store-2")

	hub.Unregister(leaver)

	ended := map[string]bool{}
	for len(watcher.send) > 0 {
		var msg MemberSessionMsg
		if err := json.Unmarshal(<-watcher.send, &msg); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if msg.Type == "member_session_end" && msg.UserID == "user-a" {
			ended[msg.CartID] = true
		}
	}
	for _, cartID := range []string{"cart-a", "cart-b"} {
		if !ended[cartID] {
			t.Errorf("no member_session_end relayed for %s", cartID)
		}
		if len(hub.GetActiveShopping(cartID)) != 0 {
			t.Errorf("%s still lists the disconnected member as active", cartID)
		}
	}
}

// A re-subscribe replaces the set and reports only what is NEW, so adding one
// cart to an open connection does not re-deliver every other cart's state.
func TestSubscribeReportsOnlyNewCarts(t *testing.T) {
	hub := NewHub()
	c := joinHub(hub, newTestClient("user-a", "dev-1"), "cart-a")

	added := hub.Subscribe(c, []string{"cart-a", "cart-b"})
	if len(added) != 1 || added[0] != "cart-b" {
		t.Errorf("Subscribe added = %v, want [cart-b]", added)
	}
	if hub.Subscribe(c, []string{"cart-a", "cart-b"}) != nil {
		t.Errorf("an unchanged re-subscribe must add nothing")
	}

	// Dropping a cart deindexes it as well as unsetting it.
	hub.Subscribe(c, []string{"cart-b"})
	if hub.IsSubscribed(c, "cart-a") {
		t.Errorf("cart-a survived a replacing subscribe")
	}
	hub.BroadcastToCart("cart-a", []byte(`{"type":"noop"}`), "")
	if len(c.send) != 0 {
		t.Errorf("a dropped cart still reaches the connection through the broadcast index")
	}
}

// The per-connection message budget absorbs an initial-share burst instead of
// severing it. The old 30-per-60s ceiling disconnected any share of more than 30
// items mid-flush; the bucket now holds a burst far above that, and a message
// past it waits rather than costing the connection.
func TestMessageBudgetAbsorbsInitialShareBurst(t *testing.T) {
	c := newTestClient("user-a", "dev-1")
	c.tokens = wsBurstMsgs
	c.lastToken = time.Now()

	start := time.Now()
	for i := 0; i < wsBurstMsgs; i++ {
		c.awaitBudget()
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("a burst within capacity waited %v — it must not be throttled", elapsed)
	}
	if wsBurstMsgs <= 30 {
		t.Errorf("burst capacity %d is no better than the ceiling it replaced", wsBurstMsgs)
	}

	// Past the burst the connection is throttled, never dropped.
	start = time.Now()
	c.awaitBudget()
	if elapsed := time.Since(start); elapsed < time.Duration(float64(time.Second)/wsRefillPerSec)/2 {
		t.Errorf("a message past the burst waited %v — the budget is not being enforced", elapsed)
	}
}

// ---------------------------------------------------------------------------
// One writer per connection (R-400)
// ---------------------------------------------------------------------------

// socketWriteOwners is the allowlist: the functions permitted to write to a
// WebSocket connection, each with the reason it is allowed.
//
//   - writePump — THE owner. Once it is running it is the only goroutine that may
//     write, because gorilla/websocket permits exactly one concurrent writer and
//     traps a second one by design.
//   - closeUnsubscribed — runs BEFORE any pump exists (wsHandler refusing a
//     handshake), so there is no second writer to race. This is the distinction
//     that matters: the rule is not "only writePump touches the conn", it is
//     "once writePump is started, nothing else writes".
//
// Adding a name here is the point of the mechanism, not a way around it: it makes
// an author state which of those two cases their new write is.
var socketWriteOwners = map[string]string{
	"writePump":         "the owner — the one goroutine that writes a live connection",
	"closeUnsubscribed": "pre-pump: refuses a handshake before writePump is started",
}

// socketWriteMethods are the gorilla/websocket calls that write to the wire or
// arm a write. SetWriteDeadline is included deliberately — it mutates the write
// half's state, so calling it from a second goroutine is the same defect with a
// quieter symptom.
var socketWriteMethods = map[string]bool{
	"WriteMessage":         true,
	"WriteJSON":            true,
	"WriteControl":         true,
	"WritePreparedMessage": true,
	"NextWriter":           true,
	"SetWriteDeadline":     true,
}

// findSocketWrites reports every socket write in src as "function:method", using
// the enclosing top-level function's name. A receiver is treated as a connection
// when its expression mentions `conn`, which is how both files name it.
func findSocketWrites(t *testing.T, filename, src string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}

	var found []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !socketWriteMethods[sel.Sel.Name] {
				return true
			}
			var receiver bytes.Buffer
			if err := printer.Fprint(&receiver, fset, sel.X); err != nil {
				return true
			}
			if !strings.Contains(receiver.String(), "conn") {
				return true
			}
			found = append(found, fn.Name.Name+":"+sel.Sel.Name)
			return true
		})
	}
	return found
}

// Exactly one goroutine writes a live connection, and nothing in the code says so
// — which is how it broke. At 65c40d6 readPump's rate-limit branch wrote its own
// close frame while writePump owned the connection: a real "concurrent write to
// websocket connection" panic, recovered per-connection by net/http, so the
// service stayed up and nobody read it for twelve days. It went away as a SIDE
// EFFECT of 1ec1f66 deleting that whole branch (the budget now waits instead of
// disconnecting) — the commit message never mentions it — so nothing prevents the
// shape returning the next time somebody adds an early-return disconnect.
//
// The test is deliberately static rather than a -race case driving a disconnect.
// The disconnect path this bug lived on no longer exists, so a -race test would
// have to manufacture a second writer to have anything to detect — proving the
// race detector works, not that the code is right — and a race detector reports
// only interleavings it actually observes, so a green run would be silence
// mistaken for proof. Reading the source answers the real question directly: is
// there a socket write anywhere but its owner?
func TestOnlyTheWritePumpWritesTheConnection(t *testing.T) {
	for _, filename := range []string{"hub.go", "handlers.go"} {
		src, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("read %s: %v", filename, err)
		}
		writes := findSocketWrites(t, filename, string(src))
		if len(writes) == 0 {
			t.Errorf("%s: no socket writes found at all — the scan has lost its subject, "+
				"which makes a pass here meaningless", filename)
		}
		for _, w := range writes {
			fn := strings.SplitN(w, ":", 2)[0]
			if _, allowed := socketWriteOwners[fn]; !allowed {
				t.Errorf("%s: %s writes the connection, and only these may: %v. "+
					"Two goroutines on one socket is a panic gorilla/websocket raises by design, "+
					"and net/http recovers it per connection — so it will not crash, it will just "+
					"be wrong quietly. Queue it on c.send instead.",
					filename, w, keysOf(socketWriteOwners))
			}
		}
	}
}

// The scan's own canary. A checker whose healthy output is silence owes a second
// mechanism proving it can still see its subject: this feeds it the exact defect
// from 65c40d6 and fails if the scan shrugs.
func TestSocketWriteScanCatchesTheOriginalDefect(t *testing.T) {
	const regressionSource = `package main

func writePump(c *Client) {
	c.conn.WriteMessage(1, nil)
}

func readPump(c *Client) {
	for {
		if c.tokens < 1 {
			c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "rate limit exceeded"))
			return
		}
	}
}
`
	writes := findSocketWrites(t, "regression.go", regressionSource)

	var offenders []string
	for _, w := range writes {
		if fn := strings.SplitN(w, ":", 2)[0]; socketWriteOwners[fn] == "" {
			offenders = append(offenders, w)
		}
	}
	if len(offenders) != 1 || offenders[0] != "readPump:WriteMessage" {
		t.Errorf("the scan reported %v as offending; want exactly [readPump:WriteMessage] — "+
			"it can no longer see the defect it exists to catch", offenders)
	}
	if len(writes) != 2 {
		t.Errorf("the scan found %d writes in the fixture, want 2 — it must see the legal one too, "+
			"or it is passing by blindness rather than by correctness", len(writes))
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
