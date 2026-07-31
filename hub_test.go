package main

import (
	"encoding/json"
	"testing"
)

// newTestClient builds a Client usable by handleDelta without a real WebSocket:
// handleDelta only touches cartID/userID/deviceID and the buffered send channel
// (the conn is used solely by the read/write pumps).
func newTestClient(cartID, userID, deviceID string) *Client {
	return &Client{
		cartID:   cartID,
		userID:   userID,
		deviceID: deviceID,
		send:     make(chan []byte, 16),
	}
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
	if err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	hub := NewHub()

	sender := newTestClient("cart-a", "user-a", "dev-1")
	peer := newTestClient("cart-a", "user-a", "dev-2")
	hub.Register(sender)
	hub.Register(peer)

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
	if err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	hub := NewHub()

	sender := newTestClient("cart-a", "user-a", "dev-1")
	peer := newTestClient("cart-a", "user-a", "dev-2")
	hub.Register(sender)
	hub.Register(peer)

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
