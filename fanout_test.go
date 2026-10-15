package main

import (
	"database/sql"
	"encoding/json"
	"testing"
)

// R-596 / ADR-098: a delta is fanned out per member. The sender's own user never hears it on
// another device; a different member always does.

// fanoutRig is one cart with user-a on two devices (sender + sibling) and user-b on one.
type fanoutRig struct {
	db                     *sql.DB
	hub                    *Hub
	sender, sibling, other *Client
}

func newFanoutRig(t *testing.T) fanoutRig {
	t.Helper()
	db := testDB(t)
	for _, u := range []string{"user-a", "user-b"} {
		if _, err := AddMember(db, "cart-a", u); err != nil {
			t.Fatalf("AddMember %s: %v", u, err)
		}
	}
	hub := NewHub()
	return fanoutRig{
		db: db, hub: hub,
		sender:  joinHub(hub, newTestClient("user-a"), "cart-a"),
		sibling: joinHub(hub, newTestClient("user-a"), "cart-a"),
		other:   joinHub(hub, newTestClient("user-b"), "cart-a"),
	}
}

func (r fanoutRig) send(ts float64, op Op) {
	raw, _ := json.Marshal(DeltaMsg{Type: "delta", CartID: "cart-a", DeviceID: "dev-1", Ts: ts, Ops: []Op{op}})
	handleDelta(r.sender, r.hub, r.db, raw)
}

func (r fanoutRig) add(ts float64, id string) {
	r.send(ts, Op{Op: "add", ID: id, Fields: map[string]any{"title": "Milk"}})
	drainAll(r.sender)
	drainAll(r.sibling)
	drainAll(r.other)
}

func TestDeltaSkipsTheSendersOtherDevices(t *testing.T) {
	r := newFanoutRig(t)
	r.add(1000, "item-1")

	r.send(1010, Op{Op: "delete", ID: "item-1"})

	if len(r.other.send) != 1 {
		t.Errorf("other member got %d messages, want the relayed delta", len(r.other.send))
	}
	if n := len(r.sibling.send); n != 0 {
		t.Errorf("the sender's sibling device received %d messages, want 0", n)
	}
	if status, ok := drainAck(r.sender); !ok || status != "applied" {
		t.Errorf("sender ack = %q ok=%v, want applied", status, ok)
	}
}

func TestSiblingHearsNoOpKind(t *testing.T) {
	for _, kind := range []string{"add", "update", "delete"} {
		t.Run(kind, func(t *testing.T) {
			r := newFanoutRig(t)
			if kind != "add" {
				r.add(1000, "item-1")
			}
			op := Op{Op: kind, ID: "item-1", Fields: map[string]any{"title": "Oat milk"}}
			if kind == "delete" {
				op.Fields = nil
			}
			ts := 1010.0
			r.send(ts, op)
			if len(r.other.send) != 1 {
				t.Errorf("%s: other member got %d messages, want 1 (positive control)", kind, len(r.other.send))
			}
			if n := len(r.sibling.send); n != 0 {
				t.Errorf("%s: sibling received %d messages, want 0", kind, n)
			}
		})
	}
}

// The exclusion is the connection's user, not the op's actor: a system-attributed op sent on
// user-a's connection still must not reach user-a's sibling.
func TestSystemActorOpStillSkipsSendersAccount(t *testing.T) {
	r := newFanoutRig(t)
	r.send(1000, Op{Op: "add", ID: "item-1", Actor: systemActor, Fields: map[string]any{"title": "Milk"}})
	if len(r.other.send) != 1 {
		t.Errorf("other member got %d messages, want 1 (positive control)", len(r.other.send))
	}
	if n := len(r.sibling.send); n != 0 {
		t.Errorf("sibling received %d messages for a system-attributed op, want 0", n)
	}
}

func tombstoneDeletedAt(t *testing.T, db *sql.DB, cartID, id string) (float64, bool) {
	t.Helper()
	var at sql.NullFloat64
	var deleted int
	err := db.QueryRow(`SELECT deleted, deleted_at FROM items WHERE id=? AND cart_id=?`, id, cartID).Scan(&deleted, &at)
	if err != nil {
		return 0, false
	}
	return at.Float64, deleted == 1
}

func TestRepeatedDeleteIsDiscardedAndNotBroadcast(t *testing.T) {
	r := newFanoutRig(t)
	r.add(1000, "item-1")
	r.send(1010, Op{Op: "delete", ID: "item-1"})
	drainAll(r.sender)
	drainAll(r.other)
	first, ok := tombstoneDeletedAt(t, r.db, "cart-a", "item-1")
	if !ok {
		t.Fatal("first delete left no tombstone")
	}

	r.send(1020, Op{Op: "delete", ID: "item-1"})

	if status, ok := drainAck(r.sender); !ok || status != "discarded" {
		t.Errorf("second delete ack = %q ok=%v, want discarded", status, ok)
	}
	if n := len(r.other.send); n != 0 {
		t.Errorf("second delete was broadcast to the other member (%d messages)", n)
	}
	if at, _ := tombstoneDeletedAt(t, r.db, "cart-a", "item-1"); at != first {
		t.Errorf("deleted_at moved from %v to %v on a repeated delete", first, at)
	}
}

func TestDeleteOfUnseenItemTombstonesAndBlocksOlderAdd(t *testing.T) {
	r := newFanoutRig(t)
	r.send(1010, Op{Op: "delete", ID: "ghost"})

	if _, ok := tombstoneDeletedAt(t, r.db, "cart-a", "ghost"); !ok {
		t.Fatal("a delete of an unseen item left no tombstone")
	}
	if len(r.other.send) != 1 {
		t.Errorf("other member got %d messages, want 1", len(r.other.send))
	}
	drainAll(r.sender)

	r.send(1000, Op{Op: "add", ID: "ghost", Fields: map[string]any{"title": "Late"}})
	if status, ok := drainAck(r.sender); !ok || status != "discarded" {
		t.Errorf("older add ack = %q ok=%v, want discarded", status, ok)
	}
	if _, found := itemTitle(t, r.db, "cart-a", "ghost"); found {
		t.Error("an add older than the tombstone resurrected the item")
	}
}
