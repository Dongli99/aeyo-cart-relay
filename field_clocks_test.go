package main

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

// R-582 / ADR-096: a shared item's content is decided key by key. Each content key keeps its own clock in
// items.field_ts (a floor "*" for keys never written alone), a tie keeps the stored value, the row clock
// never goes down, and two shapes — an add and the whole-item snapshot — keep the row rule.

// apply runs one op the way handleDelta does and returns what the relay ruled, Rescued included.
func (r *relayRig) apply(device string, ts float64, op Op) OpOutcome {
	r.t.Helper()
	out, err := ApplyOp(r.db, relayCart, op.ID, op.Op, op.Fields, ts, device, "user-"+device)
	if err != nil {
		r.t.Fatalf("ApplyOp: %v", err)
	}
	return out
}

// col reads one column of an item row as text ("<nil>" for NULL).
func (r *relayRig) col(id, column string) string {
	r.t.Helper()
	var v sql.NullString
	if err := r.db.QueryRow(`SELECT CAST(`+column+` AS TEXT) FROM items WHERE cart_id=? AND id=?`, relayCart, id).
		Scan(&v); err != nil {
		r.t.Fatalf("read %s: %v", column, err)
	}
	if !v.Valid {
		return "<nil>"
	}
	return v.String
}

func (r *relayRig) clocks(id string) map[string]float64 {
	r.t.Helper()
	raw := r.col(id, "field_ts")
	var m map[string]float64
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		r.t.Fatalf("field_ts %q: %v", raw, err)
	}
	return m
}

func (r *relayRig) seedMilk() {
	r.t.Helper()
	if out := r.apply("a", 1000, add("milk", map[string]any{"title": "Milk", "quantity": 1.0, "notes": "plain"})); !out.Applied {
		r.t.Fatal("seed add discarded")
	}
}

// The R-582 case: B's offline note at 2000 reaches the relay after A's quantity at 3000. Both stand, the
// op is applied and relayed, and the relay asks for a state broadcast so the phones' per-item gates let
// the older edit in.
func TestR582OlderEditToAnotherFieldSurvives(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	r.apply("a", 3000, update("milk", map[string]any{"quantity": 2.0}))
	out := r.apply("b", 2000, update("milk", map[string]any{"notes": "2%"}))
	if !out.Applied || !out.Rescued || keys(out.Fields) != "notes" {
		t.Errorf("outcome = %+v, want applied, rescued, relaying notes", out)
	}
	expect(t, "notes", r.col("milk", "notes"), "2%")
	expect(t, "quantity", r.col("milk", "quantity"), "2")
	expect(t, "row ts", r.col("milk", "ts"), "3000.0")
	expect(t, "device", r.col("milk", "device_id"), "a")
	c := r.clocks("milk")
	if c["notes"] != 2000 || c["quantity"] != 3000 || c[fieldFloor] != 1000 {
		t.Errorf("clocks = %v", c)
	}

	t.Run("a plain win is no rescue", func(t *testing.T) {
		out := r.apply("b", 4000, update("milk", map[string]any{"notes": "skim"}))
		if !out.Applied || out.Rescued {
			t.Errorf("outcome = %+v, want applied and not rescued", out)
		}
	})
}

func TestSameKeyOlderEditLoses(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	r.apply("a", 3000, update("milk", map[string]any{"notes": "A"}))
	out := r.apply("b", 2000, update("milk", map[string]any{"notes": "B"}))
	if out.Applied || out.Rescued || out.Broadcast {
		t.Errorf("outcome = %+v, want discarded", out)
	}
	expect(t, "notes", r.col("milk", "notes"), "A")
}

// An op whose keys split — one wins, one loses — leaves the losing key's clock where it was. Stamped with the
// op's older ts, that key would lower its clock, and a still-older edit to it would then win.
func TestASplitOpLeavesTheLosingKeysClock(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	r.apply("a", 3000, update("milk", map[string]any{"quantity": 2.0}))
	out := r.apply("b", 2000, update("milk", map[string]any{"notes": "2%", "quantity": 5.0}))
	if !out.Applied || keys(out.Fields) != "notes" {
		t.Errorf("outcome = %+v, want applied, relaying notes only", out)
	}
	if c := r.clocks("milk"); c["quantity"] != 3000 || c["notes"] != 2000 {
		t.Errorf("clocks = %v, want quantity kept at 3000 and notes at 2000", c)
	}
	r.apply("c", 2500, update("milk", map[string]any{"quantity": 7.0}))
	expect(t, "quantity", r.col("milk", "quantity"), "2")
}

// A tie keeps the stored value whatever the device ids: the row's device tiebreak is not a key's.
func TestTieKeepsTheStoredValue(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	r.apply("z", 3000, update("milk", map[string]any{"notes": "first"}))
	out := r.apply("a", 3000, update("milk", map[string]any{"notes": "second"}))
	if out.Applied {
		t.Errorf("outcome = %+v, want discarded", out)
	}
	expect(t, "notes", r.col("milk", "notes"), "first")
}

// A row written before ADR-096 has no map. Its floor is its ts as it stood, stored by the first write
// BEFORE the row clock rises — so a key nobody touched still wins against an edit next to it.
func TestLegacyRowFloorIsTheTsAsItStood(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	if _, err := r.db.Exec(`UPDATE items SET field_ts=NULL WHERE cart_id=? AND id='milk'`, relayCart); err != nil {
		t.Fatal(err)
	}
	r.apply("a", 3000, update("milk", map[string]any{"quantity": 2.0}))
	c := r.clocks("milk")
	if c[fieldFloor] != 1000 || c["quantity"] != 3000 {
		t.Fatalf("first write stored clocks %v, want floor 1000 and quantity 3000", c)
	}
	out := r.apply("b", 2000, update("milk", map[string]any{"notes": "2%"}))
	if !out.Applied || !out.Rescued {
		t.Errorf("outcome = %+v, want the untouched key to win", out)
	}
	expect(t, "notes", r.col("milk", "notes"), "2%")
}

// The row clock never goes down, and a later push-back above it still wins every key.
func TestRowClockNeverGoesDown(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	r.apply("a", 3000, update("milk", map[string]any{"quantity": 2.0}))
	r.apply("b", 2000, update("milk", map[string]any{"notes": "2%"}))
	expect(t, "row ts", r.col("milk", "ts"), "3000.0")
	expect(t, "device", r.col("milk", "device_id"), "a")

	// B's state door pushes back its whole content at its local clock, 3500, above the row.
	out := r.apply("b", 3500, update("milk", map[string]any{"title": "Oat milk", "quantity": 3.0, "notes": "oat"}))
	if keys(out.Fields) != "notes,quantity,title" || out.Rescued {
		t.Errorf("push-back outcome = %+v, want every key, no rescue", out)
	}
	expect(t, "row ts", r.col("milk", "ts"), "3500.0")
	expect(t, "device", r.col("milk", "device_id"), "b")
}

// An add stamps the floor and nothing else; a re-add over a tombstone restarts every clock at its ts.
func TestAddStampsTheFloor(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	expect(t, "clocks after add", r.col("milk", "field_ts"), `{"*":1000}`)

	r.apply("a", 1500, update("milk", map[string]any{"notes": "x"}))
	r.apply("a", 2000, Op{Op: "delete", ID: "milk"})
	if out := r.apply("b", 3000, add("milk", map[string]any{"title": "Milk again"})); !out.Applied {
		t.Fatal("re-add discarded")
	}
	expect(t, "clocks after re-add", r.col("milk", "field_ts"), `{"*":3000}`)
	if out := r.apply("a", 2500, update("milk", map[string]any{"notes": "late"})); out.Applied {
		t.Errorf("an edit older than the re-add applied: %+v", out)
	}
}

// The whole-item snapshot keeps the row rule: losing the row, it writes no key, even one whose own clock
// is older than it; winning, it stamps exactly the keys it wrote.
func TestSnapshotKeepsTheRowRule(t *testing.T) {
	r := newRelayRig(t)
	r.seedMilk()
	r.apply("a", 3000, update("milk", map[string]any{"quantity": 2.0}))
	snap := map[string]any{"title": "Milk", "quantity": 1.0, "checked": false, "notes": "stale"}
	if out := r.apply("d", 2000, update("milk", snap)); out.Applied || out.Rescued {
		t.Errorf("a losing snapshot = %+v, want discarded", out)
	}
	expect(t, "notes", r.col("milk", "notes"), "plain")

	r.apply("d", 4000, update("milk", snap))
	c := r.clocks("milk")
	if c["title"] != 4000 || c["quantity"] != 4000 || c["notes"] != 4000 || c[fieldFloor] != 1000 {
		t.Errorf("clocks = %v, want the snapshot's keys at 4000 and the floor kept", c)
	}
	if _, ok := c["checked"]; ok {
		t.Errorf("a completion key got a content clock: %v", c)
	}
}

// The outbox merges a note into a pending check-off's row. Older than a quantity edit, the act lands, the
// note survives, and both reach peers.
func TestAppliedAckMergeKeepsItsNote(t *testing.T) {
	r := newRelayRig(t)
	r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1", "quantity": 1.0}))
	r.send("user-a", 3000, update("milk", map[string]any{"quantity": 2.0}))
	d := r.send("user-b", 2000, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0, "notes": "2%"}))
	expectDelivery(t, d, "applied", "checked,notes,purchasedAt")
	expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=- bit=0")
	expect(t, "notes", r.col("milk", "notes"), "2%")
	expect(t, "quantity", r.col("milk", "quantity"), "2")
}

// The explicit quiet flag is authoritative when ITS key won, whatever the row did.
func TestQuietFlagFollowsItsOwnKey(t *testing.T) {
	r := newRelayRig(t)
	r.send("user-a", 1000, add("sun", map[string]any{"title": "Sunscreen", "checked": true, "purchasedAt": 1000.0}))
	r.send("user-a", 3000, update("sun", map[string]any{"title": "SPF 50"}))
	r.send("user-b", 2000, update("sun", map[string]any{
		"checked": true, "purchasedAt": 2000.0, "quietUntilFirstPurchase": true}))
	if r.stateRow("sun")["quietUntilFirstPurchase"] != true {
		t.Error("the flag's key won, yet the derived clear overrode it")
	}
}

// The same rule on the want's path: a want that advances the register clears the flag unless the flag's
// own key won in the same op.
func TestQuietFlagBesideAWantFollowsItsOwnKey(t *testing.T) {
	r := newRelayRig(t)
	r.send("user-a", 1000, add("sun", map[string]any{"title": "Sunscreen"}))
	r.send("user-a", 3000, update("sun", map[string]any{"title": "SPF 50"}))
	r.send("user-b", 2000, update("sun", map[string]any{"lastWantedAt": 2000.0, "quietUntilFirstPurchase": true}))
	if r.stateRow("sun")["quietUntilFirstPurchase"] != true {
		t.Error("the flag's key won beside an advancing want, yet the derived clear overrode it")
	}
}

// An uncheck with no verdict follows the ROW comparison, even when a content key beside it wins.
func TestNoVerdictUncheckFollowsTheRow(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked()
	r.send("user-b", 3000, update("milk", map[string]any{"quantity": 2.0}))
	d := r.send("user-d", 2000, update("milk", map[string]any{"checked": false, "notes": "x"}))
	expectDelivery(t, d, "applied", "notes")
	expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")

	d = r.send("user-d", 4000, update("milk", map[string]any{"checked": false}))
	expectDelivery(t, d, "applied", "checked")
	expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=0")
}

func TestGuardsAreUnchanged(t *testing.T) {
	t.Run("an update to a missing row", func(t *testing.T) {
		r := newRelayRig(t)
		if out := r.apply("a", 1000, update("milk", map[string]any{"notes": "x"})); out.Applied {
			t.Errorf("applied: %+v", out)
		}
		var n int
		if err := r.db.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&n); err != nil || n != 0 {
			t.Errorf("rows = %d (%v), want 0", n, err)
		}
	})
	t.Run("an update to a tombstone, even below its clocks", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedMilk()
		r.apply("a", 2000, Op{Op: "delete", ID: "milk"})
		if out := r.apply("b", 3000, update("milk", map[string]any{"notes": "x"})); out.Applied {
			t.Errorf("applied: %+v", out)
		}
	})
	t.Run("an add older than its tombstone", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedMilk()
		r.apply("a", 2000, Op{Op: "delete", ID: "milk"})
		if out := r.apply("b", 1500, add("milk", map[string]any{"title": "Milk"})); out.Applied {
			t.Errorf("applied: %+v", out)
		}
		expect(t, "deleted", r.col("milk", "deleted"), "1")
	})
}

// ADR-096 §5, at the hub: a rescue reaches every connected member as the cart's state — the sender
// included — after the ack; a plain win sends no state at all.
func TestRescueBroadcastsStateToEveryMember(t *testing.T) {
	r := newRelayRig(t)
	sender := joinHub(r.hub, newTestClient("user-b"), relayCart)
	peer := joinHub(r.hub, newTestClient("user-a"), relayCart)
	deliver := func(c *Client, ts float64, op Op) {
		raw, _ := json.Marshal(DeltaMsg{Type: "delta", CartID: relayCart, DeviceID: "dev-" + c.userID, Ts: ts, Ops: []Op{op}})
		handleDelta(c, r.hub, r.db, raw)
	}
	// kinds lists the message types each client received, in order.
	kinds := func(c *Client) string {
		var out []string
		for _, b := range drainAll(c) {
			var m struct {
				Type string `json:"type"`
			}
			json.Unmarshal(b, &m)
			out = append(out, m.Type)
		}
		return strings.Join(out, ",")
	}
	clear := func() { kinds(sender); kinds(peer); kinds(r.observer) }

	deliver(peer, 1000, add("milk", map[string]any{"title": "Milk", "quantity": 1.0}))
	deliver(peer, 3000, update("milk", map[string]any{"quantity": 2.0}))
	clear()

	t.Run("a plain win sends no state", func(t *testing.T) {
		deliver(peer, 3500, update("milk", map[string]any{"title": "Oat milk"}))
		expect(t, "sender", kinds(peer), "ack")
		expect(t, "peer", kinds(sender), "delta")
		expect(t, "observer", kinds(r.observer), "delta")
	})

	t.Run("a rescue sends state to everyone, the sender included", func(t *testing.T) {
		deliver(sender, 2000, update("milk", map[string]any{"notes": "2%"}))
		expect(t, "sender", kinds(sender), "ack,state")
		expect(t, "peer", kinds(peer), "delta,state")
		expect(t, "observer", kinds(r.observer), "delta,state")
		row := r.stateRow("milk")
		if row["notes"] != "2%" || row["quantity"] != 2.0 {
			t.Errorf("state row = %v, want the rescued note beside the newer quantity", row)
		}
	})
}
