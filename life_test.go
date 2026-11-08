package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ADR-101: the relay names each life of a row. A life begins when an add inserts a row or revives a
// tombstoned one, and it has two values: a marker the relay mints, and the echo the add carried.

// lifeOf reads a row's (marker, echo) as stored, "-" for NULL.
func lifeOf(t *testing.T, db *sql.DB, cartID, id string) (marker, echo string) {
	t.Helper()
	var m, e sql.NullString
	if err := db.QueryRow(`SELECT marker, echo FROM items WHERE cart_id=? AND id=?`, cartID, id).Scan(&m, &e); err != nil {
		t.Fatalf("read life of %s: %v", id, err)
	}
	marker, echo = "-", "-"
	if m.Valid {
		marker = m.String
	}
	if e.Valid {
		echo = e.String
	}
	return marker, echo
}

// A database built with the schema from before ADR-101 gains the life columns at startup: every live row
// gets its own uppercase marker and no echo, a tombstone gets neither, and a second startup changes nothing.
func TestStartupMigrationGivesEveryLiveRowALife(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-life.db")
	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','')`); err != nil {
		t.Fatalf("seed room: %v", err)
	}
	for _, id := range []string{"live-1", "live-2", "live-3"} {
		if _, err := db.Exec(`INSERT INTO items (id, cart_id, title, ts) VALUES (?, 'cart-a', 'x', 1000)`, id); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	now := float64(time.Now().Unix()) // a recent tombstone, so the startup prune keeps it
	if _, err := db.Exec(`INSERT INTO items (id, cart_id, ts, deleted, deleted_at) VALUES ('gone', 'cart-a', ?, 1, ?)`, now, now); err != nil {
		t.Fatalf("seed tombstone: %v", err)
	}
	// Back to the schema as it stood before ADR-101.
	for _, s := range []string{
		`ALTER TABLE items DROP COLUMN marker`,
		`ALTER TABLE items DROP COLUMN echo`,
		`DROP TABLE ended_memberships`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	db.Close()

	markers := map[string]string{}
	for run := 1; run <= 2; run++ {
		db, err := InitDB(path)
		if err != nil {
			t.Fatalf("InitDB run %d: %v", run, err)
		}
		seen := map[string]bool{}
		for _, id := range []string{"live-1", "live-2", "live-3"} {
			m, e := lifeOf(t, db, "cart-a", id)
			if m == "-" || m != strings.ToUpper(m) || len(m) != 36 {
				t.Errorf("run %d: %s marker = %q, want an uppercase UUID", run, id, m)
			}
			if seen[m] {
				t.Errorf("run %d: %s shares marker %s", run, id, m)
			}
			seen[m] = true
			if e != "-" {
				t.Errorf("run %d: %s echo = %q, want none", run, id, e)
			}
			if run == 1 {
				markers[id] = m
			} else if markers[id] != m {
				t.Errorf("run 2 re-minted %s's marker: %s -> %s", id, markers[id], m)
			}
		}
		if m, e := lifeOf(t, db, "cart-a", "gone"); m != "-" || e != "-" {
			t.Errorf("run %d: tombstone life = (%s, %s), want none", run, m, e)
		}
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ended_memberships`).Scan(&n); err != nil {
			t.Errorf("run %d: ended_memberships: %v", run, err)
		}
		db.Close()
	}
}

// The ended-membership table hangs off nothing, so deleting the room cannot take it.
func TestEndedMembershipsOutliveTheRoom(t *testing.T) {
	db := testDB(t)
	if _, err := db.Exec(`INSERT INTO ended_memberships (sub_key, cart_id, user_id, ended_at, rows) VALUES ('k', 'cart-a', 'user-a', 1, '[]')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := DeleteRoom(db, "cart-a"); err != nil {
		t.Fatalf("DeleteRoom: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ended_memberships WHERE cart_id='cart-a'`).Scan(&n); err != nil || n != 1 {
		t.Errorf("ended_memberships after DeleteRoom: n=%d err=%v, want 1", n, err)
	}
}

// lifeRig applies ops to cart-a's item "MILK" the way handleDelta does.
type lifeRig struct {
	t  *testing.T
	db *sql.DB
}

func (r lifeRig) apply(ts float64, op Op) {
	r.t.Helper()
	op.ID = "MILK"
	if _, err := ApplyOp(r.db, "cart-a", op, ts, "dev-a", "user-a"); err != nil {
		r.t.Fatalf("ApplyOp %s: %v", op.Op, err)
	}
}

func (r lifeRig) life() (string, string) {
	r.t.Helper()
	return lifeOf(r.t, r.db, "cart-a", "MILK")
}

const (
	echo1 = "11111111-1111-4111-8111-111111111111"
	echo2 = "22222222-2222-4222-8222-222222222222"
)

// ADR-101 §1: a life begins only when an add inserts a row or revives a tombstoned one. Every other write
// leaves the life as it stands.
func TestALifeBeginsOnlyAtAnInsertOrARevival(t *testing.T) {
	milk := map[string]any{"title": "Milk"}

	t.Run("an insert mints a marker and keeps the add's echo", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "add", Fields: milk, Echo: echo1})
		m, e := r.life()
		if m == "-" || m != strings.ToUpper(m) || len(m) != 36 {
			t.Errorf("marker = %q, want an uppercase UUID", m)
		}
		expect(t, "echo", e, echo1)
	})

	t.Run("an insert with no echo stores none", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "add", Fields: milk})
		if m, e := r.life(); m == "-" || e != "-" {
			t.Errorf("life = (%s, %s), want a marker and no echo", m, e)
		}
	})

	t.Run("an add reviving a tombstone begins a new life", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "add", Fields: milk, Echo: echo1})
		first, _ := r.life()
		r.apply(2000, Op{Op: "delete"})
		r.apply(3000, Op{Op: "add", Fields: milk, Echo: echo2})
		m, e := r.life()
		if m == "-" || m == first {
			t.Errorf("revived marker = %q, want a new one (first life %s)", m, first)
		}
		expect(t, "echo", e, echo2)

		// A revival with no echo carries none forward from the life before.
		r.apply(4000, Op{Op: "delete"})
		r.apply(5000, Op{Op: "add", Fields: milk})
		if m2, e := r.life(); m2 == m || e != "-" {
			t.Errorf("second revival life = (%s, %s), want a new marker and no echo", m2, e)
		}
	})

	t.Run("an add that beats a live row keeps both values", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "add", Fields: milk, Echo: echo1})
		m0, _ := r.life()
		r.apply(2000, Op{Op: "add", Fields: map[string]any{"title": "Oat milk"}, Echo: echo2})
		m, e := r.life()
		expect(t, "marker", m, m0)
		expect(t, "echo", e, echo1)
		if title, _ := itemTitle(t, r.db, "cart-a", "MILK"); title != "Oat milk" {
			t.Fatalf("fixture: the later add did not win (title %q)", title)
		}
	})

	t.Run("an update and a delete never set either", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "add", Fields: milk, Echo: echo1})
		m0, _ := r.life()
		r.apply(2000, Op{Op: "update", Fields: map[string]any{"title": "Oat milk"}, Marker: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", Echo: echo2})
		m, e := r.life()
		expect(t, "marker after update", m, m0)
		expect(t, "echo after update", e, echo1)
		r.apply(3000, Op{Op: "delete"})
		m, e = r.life()
		expect(t, "marker on the tombstone", m, m0)
		expect(t, "echo on the tombstone", e, echo1)
	})

	t.Run("a delete over a missing row leaves a tombstone with no life", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "delete"})
		if m, e := r.life(); m != "-" || e != "-" {
			t.Errorf("life = (%s, %s), want none", m, e)
		}
	})

	t.Run("a row pruned and added again gets a new marker", func(t *testing.T) {
		r := lifeRig{t, testDB(t)}
		r.apply(1000, Op{Op: "add", Fields: milk, Echo: echo1})
		first, _ := r.life()
		r.apply(2000, Op{Op: "delete"})
		if _, err := r.db.Exec(`UPDATE items SET deleted_at = 1 WHERE id = 'MILK'`); err != nil {
			t.Fatal(err)
		}
		pruneExpired(r.db)
		if _, found := itemDeleted(t, r.db, "cart-a", "MILK"); found {
			t.Fatal("fixture: the tombstone was not pruned")
		}
		r.apply(3000, Op{Op: "add", Fields: milk, Echo: echo1})
		if m, _ := r.life(); m == "-" || m == first {
			t.Errorf("marker after prune and re-add = %q, want a new one (first life %s)", m, first)
		}
	})
}

// wireRig is cart-a with user-a sending and user-b observing, both through handleDelta, read back as raw
// JSON dictionaries: the way a client decodes, and the only reading that sees a key it did not expect.
type wireRig struct {
	t              *testing.T
	db             *sql.DB
	hub            *Hub
	sender, member *Client
}

func newWireRig(t *testing.T) *wireRig {
	t.Helper()
	db := testDB(t)
	hub := NewHub()
	return &wireRig{t: t, db: db, hub: hub,
		sender: joinHub(hub, newTestClient("user-a"), "cart-a"),
		member: joinHub(hub, newTestClient("user-b"), "cart-a"),
	}
}

func decodeAll(t *testing.T, c *Client) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, b := range drainAll(c) {
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
		out = append(out, m)
	}
	return out
}

// send delivers a raw delta and returns the sender's ack and what the member was relayed, if anything.
func (r *wireRig) send(delta DeltaMsg) (ack, relayed map[string]any) {
	r.t.Helper()
	delta.Type = "delta"
	if delta.CartID == "" {
		delta.CartID = "cart-a"
	}
	delta.DeviceID = "dev-a"
	raw, _ := json.Marshal(delta)
	handleDelta(r.sender, r.hub, r.db, raw)
	for _, m := range decodeAll(r.t, r.sender) {
		if m["type"] == "ack" {
			ack = m
		}
	}
	for _, m := range decodeAll(r.t, r.member) {
		if m["type"] == "delta" {
			relayed = m
		}
	}
	return ack, relayed
}

func (r *wireRig) op(ts float64, op Op) (ack, relayed map[string]any) {
	return r.send(DeltaMsg{Ts: ts, Ops: []Op{op}})
}

func obj(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("%q missing or not an object in %v", key, m)
	}
	return v
}

func absent(t *testing.T, m map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, ok := m[k]; ok {
			t.Errorf("%q present in %v, want absent", k, m)
		}
	}
}

// ADR-101 §1–§2: the ack carries the row as it stands after the op, for applied and discarded alike, and
// nothing for a rejected op; a relayed add carries the row's life at the op's level, never in fields.
func TestTheAckCarriesTheRowAndTheRelayCarriesTheLife(t *testing.T) {
	r := newWireRig(t)

	// An add that applies.
	ack, relayed := r.op(1000, Op{Op: "add", ID: "MILK", Fields: map[string]any{"title": "Milk"}, Echo: echo1})
	expect(t, "add ack", fmt.Sprint(ack["status"]), "applied")
	item := obj(t, ack, "item")
	marker, _ := lifeOf(t, r.db, "cart-a", "MILK")
	expect(t, "ack item marker", fmt.Sprint(item["marker"]), marker)
	expect(t, "ack item echo", fmt.Sprint(item["echo"]), echo1)
	expect(t, "ack item title", fmt.Sprint(item["title"]), "Milk")
	expect(t, "ack item ts", fmt.Sprint(item["ts"]), "1000")
	absent(t, ack, "tombstone")
	if relayed == nil {
		t.Fatal("the add was not relayed")
	}
	rop := relayed["ops"].([]any)[0].(map[string]any)
	expect(t, "relayed marker", fmt.Sprint(rop["marker"]), marker)
	expect(t, "relayed echo", fmt.Sprint(rop["echo"]), echo1)
	absent(t, rop["fields"].(map[string]any), "marker", "echo")

	// A losing add: older, another echo. Discarded, and the ack shows the row as it stands — the first life.
	ack, relayed = r.op(500, Op{Op: "add", ID: "MILK", Fields: map[string]any{"title": "Old milk"}, Echo: echo2})
	expect(t, "losing add ack", fmt.Sprint(ack["status"]), "discarded")
	item = obj(t, ack, "item")
	expect(t, "losing ack marker", fmt.Sprint(item["marker"]), marker)
	expect(t, "losing ack echo", fmt.Sprint(item["echo"]), echo1)
	expect(t, "losing ack title", fmt.Sprint(item["title"]), "Milk")
	if relayed != nil {
		t.Errorf("a losing add was relayed: %v", relayed)
	}

	// A refused op: two ops in one delta. Rejected, and no row.
	ack, _ = r.send(DeltaMsg{Ts: 2000, Ops: []Op{{Op: "update", ID: "MILK"}, {Op: "update", ID: "MILK"}}})
	expect(t, "refused ack", fmt.Sprint(ack["status"]), "rejected")
	absent(t, ack, "item", "tombstone")

	// A relayed update carries no life, whatever the client put beside its id.
	_, relayed = r.op(3000, Op{Op: "update", ID: "MILK", Fields: map[string]any{"title": "Oat milk"}, Marker: "X", Echo: "Y"})
	absent(t, relayed["ops"].([]any)[0].(map[string]any), "marker", "echo")

	// A delete: the ack carries the tombstone with the life it ended, and the update after it is discarded
	// with the same tombstone.
	ack, _ = r.op(4000, Op{Op: "delete", ID: "MILK"})
	expect(t, "delete ack", fmt.Sprint(ack["status"]), "applied")
	tomb := obj(t, ack, "tombstone")
	expect(t, "tombstone marker", fmt.Sprint(tomb["marker"]), marker)
	expect(t, "tombstone echo", fmt.Sprint(tomb["echo"]), echo1)
	absent(t, ack, "item")
	ack, _ = r.op(5000, Op{Op: "update", ID: "MILK", Fields: map[string]any{"title": "Ghost"}})
	expect(t, "update on tombstone ack", fmt.Sprint(ack["status"]), "discarded")
	expect(t, "update on tombstone marker", fmt.Sprint(obj(t, ack, "tombstone")["marker"]), marker)

	// The state carries the life on the tombstone too.
	b, _ := json.Marshal(buildStateMsg(r.db, "cart-a"))
	var state map[string]any
	json.Unmarshal(b, &state)
	st := state["tombstones"].([]any)[0].(map[string]any)
	expect(t, "state tombstone marker", fmt.Sprint(st["marker"]), marker)
	expect(t, "state tombstone echo", fmt.Sprint(st["echo"]), echo1)
}

// A state row carries its life.
func TestStateRowsCarryTheirLife(t *testing.T) {
	r := newRelayRig(t)
	if out := r.apply("a", 1000, Op{Op: "add", ID: "MILK", Fields: map[string]any{"title": "Milk"}, Echo: echo1}); !out.Applied {
		t.Fatal("add discarded")
	}
	marker, _ := lifeOf(t, r.db, relayCart, "MILK")
	row := r.stateRow("MILK")
	expect(t, "state marker", fmt.Sprint(row["marker"]), marker)
	expect(t, "state echo", fmt.Sprint(row["echo"]), echo1)
}

// relayedOp is the one op of a relayed delta.
func relayedOp(t *testing.T, relayed map[string]any) map[string]any {
	t.Helper()
	if relayed == nil {
		t.Fatal("nothing was relayed")
	}
	return relayed["ops"].([]any)[0].(map[string]any)
}

// ADR-101 §3: a delete that names a selector ends a life only if every selector it names matches the live
// life. Anything else changes nothing and is discarded, and it never inserts a tombstone over a missing row.
// A delete naming neither keeps today's rule exactly. A relayed delete carries the ended life.
func TestADeleteEndsOnlyTheLifeItNames(t *testing.T) {
	milk := map[string]any{"title": "Milk"}
	const wrong = "99999999-9999-4999-8999-999999999999"

	// live seeds a live milk row with echo1 and returns its marker.
	live := func(r *wireRig) string {
		r.op(1000, Op{Op: "add", ID: "MILK", Fields: milk, Echo: echo1})
		m, _ := lifeOf(r.t, r.db, "cart-a", "MILK")
		return m
	}
	ended := func(t *testing.T, r *wireRig, ack, relayed map[string]any, marker string) {
		t.Helper()
		expect(t, "ack", fmt.Sprint(ack["status"]), "applied")
		tomb := obj(t, ack, "tombstone")
		expect(t, "ack tombstone marker", fmt.Sprint(tomb["marker"]), marker)
		expect(t, "ack tombstone echo", fmt.Sprint(tomb["echo"]), echo1)
		rop := relayedOp(t, relayed)
		expect(t, "relayed marker", fmt.Sprint(rop["marker"]), marker)
		expect(t, "relayed echo", fmt.Sprint(rop["echo"]), echo1)
		if d, _ := itemDeleted(t, r.db, "cart-a", "MILK"); d != 1 {
			t.Error("the row is not a tombstone")
		}
	}
	untouched := func(t *testing.T, r *wireRig, ack, relayed map[string]any, marker string) {
		t.Helper()
		expect(t, "ack", fmt.Sprint(ack["status"]), "discarded")
		expect(t, "ack item marker", fmt.Sprint(obj(t, ack, "item")["marker"]), marker)
		if relayed != nil {
			t.Errorf("a discarded delete was relayed: %v", relayed)
		}
		if d, _ := itemDeleted(t, r.db, "cart-a", "MILK"); d != 0 {
			t.Error("the live row was deleted")
		}
	}

	t.Run("a marker that matches ends the life", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Marker: m})
		ended(t, r, ack, rel, m)
	})
	t.Run("a marker that does not match changes nothing", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Marker: wrong})
		untouched(t, r, ack, rel, m)
	})
	t.Run("an echo that matches ends the life, and the relay names the marker too", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Echo: echo1})
		ended(t, r, ack, rel, m)
	})
	t.Run("an echo that does not match changes nothing", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Echo: echo2})
		untouched(t, r, ack, rel, m)
	})
	t.Run("every named selector must match", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Marker: m, Echo: echo2})
		untouched(t, r, ack, rel, m)
		ack, rel = r.op(2100, Op{Op: "delete", ID: "MILK", Marker: m, Echo: echo1})
		ended(t, r, ack, rel, m)
	})
	t.Run("a selector naming a missing row is discarded and inserts nothing", func(t *testing.T) {
		r := newWireRig(t)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Marker: wrong})
		expect(t, "ack", fmt.Sprint(ack["status"]), "discarded")
		absent(t, ack, "item", "tombstone")
		if rel != nil {
			t.Errorf("relayed: %v", rel)
		}
		if _, found := itemDeleted(t, r.db, "cart-a", "MILK"); found {
			t.Error("a tombstone was inserted over the missing row")
		}
	})
	t.Run("a delete of a tombstoned row is refused, with or without a selector", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		r.op(2000, Op{Op: "delete", ID: "MILK"})
		at, _ := tombstoneDeletedAt(t, r.db, "cart-a", "MILK")
		for _, op := range []Op{{Op: "delete", ID: "MILK", Marker: m}, {Op: "delete", ID: "MILK"}} {
			ack, rel := r.op(3000, op)
			expect(t, "ack", fmt.Sprint(ack["status"]), "discarded")
			expect(t, "ack tombstone marker", fmt.Sprint(obj(t, ack, "tombstone")["marker"]), m)
			if rel != nil {
				t.Errorf("relayed: %v", rel)
			}
		}
		if at2, _ := tombstoneDeletedAt(t, r.db, "cart-a", "MILK"); at2 != at {
			t.Errorf("deletedAt moved %v -> %v", at, at2)
		}
	})
	t.Run("a delete naming neither keeps today's rule", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK"})
		ended(t, r, ack, rel, m)

		// Over a missing row it inserts a tombstone, which ended no life the relay knows of.
		ack, rel = r.op(2000, Op{Op: "delete", ID: "BREAD"})
		expect(t, "ack", fmt.Sprint(ack["status"]), "applied")
		tomb := obj(t, ack, "tombstone")
		expect(t, "tombstone id", fmt.Sprint(tomb["id"]), "BREAD")
		absent(t, tomb, "marker", "echo")
		absent(t, ack, "item")
		absent(t, relayedOp(t, rel), "marker", "echo")
		if d, _ := itemDeleted(t, r.db, "cart-a", "BREAD"); d != 1 {
			t.Error("no tombstone over the missing row")
		}
		// And it still blocks an older add, as today.
		ack, _ = r.op(1500, Op{Op: "add", ID: "BREAD", Fields: map[string]any{"title": "Bread"}})
		expect(t, "older add after the tombstone", fmt.Sprint(ack["status"]), "discarded")
	})
	t.Run("a relayed delete carries the ended life, whatever the client named", func(t *testing.T) {
		r := newWireRig(t)
		m := live(r)
		ack, rel := r.op(2000, Op{Op: "delete", ID: "MILK", Marker: m})
		ended(t, r, ack, rel, m) // the client named only the marker; the relay names the echo too
	})
}

// finalListRig is cart A (alice owns it, bob is a member) with Milk live, Bread live with echo1, and Eggs a
// tombstone, and a connection for each member.
type finalListRig struct {
	t                *testing.T
	db               *sql.DB
	hub              *Hub
	cartA            string
	keyAlice, keyBob string
	alice, bob       *Client
	breadMarker      string // BREAD's life, read at seed time: the room may be gone when the list is read
}

func newFinalListRig(t *testing.T) *finalListRig {
	t.Helper()
	db, cartA, _, keyAliceA, _, keyBobA := subTestDB(t)
	r := &finalListRig{t: t, db: db, hub: NewHub(), cartA: cartA, keyAlice: keyAliceA, keyBob: keyBobA}
	r.alice = joinHub(r.hub, newTestClient("alice"), cartA)
	r.bob = joinHub(r.hub, newTestClient("bob"), cartA)
	for _, o := range []struct {
		ts float64
		op Op
	}{
		{1000, Op{Op: "add", ID: "MILK", Fields: map[string]any{"title": "Milk"}}},
		{1100, Op{Op: "add", ID: "BREAD", Fields: map[string]any{"title": "Bread"}, Echo: echo1}},
		{1200, Op{Op: "add", ID: "EGGS", Fields: map[string]any{"title": "Eggs"}}},
		{1300, Op{Op: "delete", ID: "EGGS"}},
	} {
		if _, err := ApplyOp(db, cartA, o.op, o.ts, "dev", "alice"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	r.breadMarker, _ = lifeOf(t, db, cartA, "BREAD")
	return r
}

// jsonOf re-reads any value as the wire's dictionaries.
func jsonOf(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return m
}

// expectFinalList checks a finalList holds the cart's live rows only, each with its life and ts.
func (r *finalListRig) expectFinalList(where string, m map[string]any) {
	r.t.Helper()
	list, ok := m["finalList"].([]any)
	if !ok {
		r.t.Errorf("%s: finalList missing or not an array in %v", where, m)
		return
	}
	got := map[string]map[string]any{}
	for _, it := range list {
		row := it.(map[string]any)
		got[fmt.Sprint(row["id"])] = row
	}
	if len(got) != 2 || got["MILK"] == nil || got["BREAD"] == nil {
		r.t.Errorf("%s: finalList ids = %v, want MILK and BREAD only", where, keys(map[string]any{"MILK": got["MILK"], "BREAD": got["BREAD"], "n": len(got)}))
		return
	}
	expect(r.t, where+" BREAD marker", fmt.Sprint(got["BREAD"]["marker"]), r.breadMarker)
	expect(r.t, where+" BREAD echo", fmt.Sprint(got["BREAD"]["echo"]), echo1)
	expect(r.t, where+" BREAD ts", fmt.Sprint(got["BREAD"]["ts"]), "1100")
}

func messagesOfType(t *testing.T, c *Client, typ string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, m := range decodeAll(t, c) {
		if m["type"] == typ {
			out = append(out, m)
		}
	}
	return out
}

// refusal subscribes with a key and returns the one rejection, as the wire carries it.
func (r *finalListRig) refusal(key string) map[string]any {
	r.t.Helper()
	_, accepted, rejected := verifySubscription(r.db, []SubscribeCart{{CartID: r.cartA, SubKey: key}}, "")
	if len(accepted) != 0 || len(rejected) != 1 {
		r.t.Fatalf("accepted=%v rejected=%v, want one refusal", accepted, rejected)
	}
	return jsonOf(r.t, rejected[0])
}

// ADR-101 §4: a revoke hands the target its final list on member_removed, and the others' copy carries none.
func TestARevokeHandsTheTargetItsFinalList(t *testing.T) {
	r := newFinalListRig(t)
	w := postJSON(t, revokeHandler(r.db, r.hub), r.cartA, r.keyAlice, map[string]any{"cartID": r.cartA, "targetUserID": "bob"})
	if w.Code != 200 {
		t.Fatalf("revoke: %d %s", w.Code, w.Body.String())
	}
	bobs := messagesOfType(t, r.bob, "member_removed")
	if len(bobs) != 1 {
		t.Fatalf("bob got %d member_removed, want 1", len(bobs))
	}
	r.expectFinalList("bob's member_removed", bobs[0])
	alices := messagesOfType(t, r.alice, "member_removed")
	if len(alices) != 1 {
		t.Fatalf("alice got %d member_removed, want 1", len(alices))
	}
	absent(t, alices[0], "finalList")

	// Bob's other devices read it at their next subscription, under today's reason.
	rej := r.refusal(r.keyBob)
	expect(t, "reason", fmt.Sprint(rej["reason"]), "unknown_key")
	r.expectFinalList("bob's refusal", rej)
}

// A leave hands the leaver its final list on the response; the leaver still hears no member_removed, and
// the others' copy carries no list.
func TestALeaveHandsTheLeaverItsFinalList(t *testing.T) {
	r := newFinalListRig(t)
	w := postJSON(t, leaveCartHandler(r.db, r.hub), r.cartA, r.keyBob, map[string]any{"cartID": r.cartA})
	if w.Code != 200 {
		t.Fatalf("leave: %d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode leave response %q: %v", w.Body.String(), err)
	}
	r.expectFinalList("leave response", body)
	if n := len(messagesOfType(t, r.bob, "member_removed")); n != 0 {
		t.Errorf("the leaver heard %d member_removed, want 0", n)
	}
	alices := messagesOfType(t, r.alice, "member_removed")
	if len(alices) != 1 {
		t.Fatalf("alice got %d member_removed, want 1", len(alices))
	}
	absent(t, alices[0], "finalList")
}

// The last member's leave deletes the room, and the list survives it and is still served. After the prune a
// refusal carries no list. A v2-shaped client, which reads only cartID and reason, still sees unknown_key.
func TestTheFinalListOutlivesTheRoomForThirtyDays(t *testing.T) {
	r := newFinalListRig(t)
	for _, key := range []string{r.keyBob, r.keyAlice} {
		if w := postJSON(t, leaveCartHandler(r.db, r.hub), r.cartA, key, map[string]any{"cartID": r.cartA}); w.Code != 200 {
			t.Fatalf("leave: %d %s", w.Code, w.Body.String())
		}
	}
	if exists, _ := RoomExists(r.db, r.cartA); exists {
		t.Fatal("fixture: the last leave did not delete the room")
	}
	rej := r.refusal(r.keyAlice)
	r.expectFinalList("alice's refusal after the room is gone", rej)

	var v2 struct {
		CartID string `json:"cartID"`
		Reason string `json:"reason"`
	}
	b, _ := json.Marshal(rej)
	if err := json.Unmarshal(b, &v2); err != nil || v2.Reason != rejectUnknownKey || v2.CartID != r.cartA {
		t.Errorf("a v2 reading = %+v (err %v), want unknown_key for %s", v2, err, r.cartA)
	}

	if _, err := r.db.Exec(`UPDATE ended_memberships SET ended_at = ?`, float64(time.Now().Add(-31*24*time.Hour).Unix())); err != nil {
		t.Fatal(err)
	}
	pruneExpired(r.db)
	var n int
	r.db.QueryRow(`SELECT COUNT(*) FROM ended_memberships`).Scan(&n)
	if n != 0 {
		t.Errorf("%d ended memberships survived the prune", n)
	}
	rej = r.refusal(r.keyAlice)
	expect(t, "reason after the prune", fmt.Sprint(rej["reason"]), "unknown_key")
	absent(t, rej, "finalList")
}

// An ended membership of a cart with no live rows still has a list: present and empty, because its presence
// is what says the account was removed.
func TestAnEmptyFinalListIsStillPresent(t *testing.T) {
	db, cartA, _, _, _, keyBobA := subTestDB(t)
	w := postJSON(t, leaveCartHandler(db, NewHub()), cartA, keyBobA, map[string]any{"cartID": cartA})
	if w.Code != 200 {
		t.Fatalf("leave: %d", w.Code)
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"finalList":[]}` {
		t.Errorf("leave response = %s, want {\"finalList\":[]}", got)
	}
	_, _, rejected := verifySubscription(db, []SubscribeCart{{CartID: cartA, SubKey: keyBobA}}, "")
	b, _ := json.Marshal(rejected)
	if !strings.Contains(string(b), `"finalList":[]`) {
		t.Errorf("refusal = %s, want an empty finalList present", b)
	}
}

// ADR-101 §5: one case for every id the relay keys a row by. An op's id, an item's globalID, a reservoir
// endpoint's globalID, and an op's marker and echo are uppercased at receipt; cart ids stay lowercase.
func TestIDsAreUppercasedAtReceipt(t *testing.T) {
	const lower = "abcdef01-2345-4678-89ab-cdef01234567"
	upper := strings.ToUpper(lower)
	r := newWireRig(t)

	ack, rel := r.op(1000, Op{Op: "add", ID: lower, Fields: map[string]any{"title": "Milk", "globalID": lower}, Echo: lower})
	expect(t, "add ack", fmt.Sprint(ack["status"]), "applied")
	expect(t, "ack itemID", fmt.Sprint(ack["itemID"]), upper)
	expect(t, "ack cartID", fmt.Sprint(ack["cartID"]), "cart-a")
	item := obj(t, ack, "item")
	expect(t, "stored id", fmt.Sprint(item["id"]), upper)
	expect(t, "stored globalID", fmt.Sprint(item["globalID"]), upper)
	expect(t, "stored echo", fmt.Sprint(item["echo"]), upper)
	rop := relayedOp(t, rel)
	expect(t, "relayed id", fmt.Sprint(rop["id"]), upper)
	expect(t, "relayed globalID", fmt.Sprint(rop["fields"].(map[string]any)["globalID"]), upper)

	// The uppercase form names the same row, and so does the lowercase one.
	ack, _ = r.op(2000, Op{Op: "update", ID: upper, Fields: map[string]any{"title": "Oat milk"}})
	expect(t, "update by the uppercase id", fmt.Sprint(ack["status"]), "applied")
	ack, _ = r.op(3000, Op{Op: "update", ID: lower, Fields: map[string]any{"title": "Whole milk"}})
	expect(t, "update by the lowercase id", fmt.Sprint(ack["status"]), "applied")
	var n int
	r.db.QueryRow(`SELECT COUNT(*) FROM items WHERE cart_id='cart-a'`).Scan(&n)
	if n != 1 {
		t.Errorf("%d rows, want one", n)
	}

	// A delete's selectors in lowercase match the life.
	marker, _ := lifeOf(t, r.db, "cart-a", upper)
	ack, _ = r.op(4000, Op{Op: "delete", ID: lower, Marker: strings.ToLower(marker), Echo: lower})
	expect(t, "delete by lowercase selectors", fmt.Sprint(ack["status"]), "applied")

	// Both reservoir endpoints key on the uppercase spine.
	db, cartA, _, keyAliceA, _, _ := subTestDB(t)
	w := postJSON(t, historyBackfillHandler(db), cartA, keyAliceA, map[string]any{"globalID": lower, "events": []float64{1000}})
	if w.Code != 200 {
		t.Fatalf("backfill: %d %s", w.Code, w.Body.String())
	}
	if ev, _ := GetConsumptionEvents(db, cartA, upper); len(ev) != 1 {
		t.Errorf("backfill stored under %v, want the uppercase spine", ev)
	}
	req := httptest.NewRequest("GET", "/api/cart/"+cartA+"/history?globalID="+lower, nil)
	req.SetPathValue("cartID", cartA)
	req.Header.Set("X-Sub-Key", keyAliceA)
	rec := httptest.NewRecorder()
	historyReadHandler(db)(rec, req)
	if !strings.Contains(rec.Body.String(), "[1000]") {
		t.Errorf("history read by the lowercase spine = %s, want [1000]", rec.Body.String())
	}
}
