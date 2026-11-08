package main

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ADR-101 §7 (R-609): the relay records who began a row's life and who ended it. The value is the actor
// ApplyOp is given (handleDelta's), never anything the body names.

type authorRig struct {
	t  *testing.T
	db *sql.DB
}

func (r authorRig) apply(actor string, ts float64, op Op) {
	r.t.Helper()
	op.ID = "MILK"
	if _, err := ApplyOp(r.db, "cart-a", op, ts, "dev-a", actor); err != nil {
		r.t.Fatalf("ApplyOp %s: %v", op.Op, err)
	}
}

// authors reads (added_by, deleted_by) as stored, "-" for NULL.
func (r authorRig) authors() (string, string) {
	r.t.Helper()
	var a, d sql.NullString
	if err := r.db.QueryRow(`SELECT added_by, deleted_by FROM items WHERE cart_id='cart-a' AND id='MILK'`).Scan(&a, &d); err != nil {
		r.t.Fatalf("read authors: %v", err)
	}
	out := [2]string{"-", "-"}
	if a.Valid {
		out[0] = a.String
	}
	if d.Valid {
		out[1] = d.String
	}
	return out[0], out[1]
}

func (r authorRig) expect(what, added, deleted string) {
	r.t.Helper()
	a, d := r.authors()
	if a != added || d != deleted {
		r.t.Errorf("%s: (added_by, deleted_by) = (%s, %s), want (%s, %s)", what, a, d, added, deleted)
	}
}

func TestAuthorsFollowTheLife(t *testing.T) {
	milk := map[string]any{"title": "Milk"}
	oat := map[string]any{"title": "Oat milk"}

	t.Run("an insert records the adder and no deleter", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		r.expect("insert", "alice", "-")
	})

	t.Run("the system sentinel is recorded as itself", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply(systemActor, 1000, Op{Op: "add", Fields: milk})
		r.apply(systemActor, 2000, Op{Op: "delete"})
		r.expect("system", systemActor, systemActor)
	})

	t.Run("a delete records the deleter and keeps the adder", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		r.apply("bob", 2000, Op{Op: "delete"})
		r.expect("delete", "alice", "bob")
	})

	t.Run("a revival replaces the adder and clears the deleter", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		r.apply("bob", 2000, Op{Op: "delete"})
		r.apply("carol", 3000, Op{Op: "add", Fields: milk})
		r.expect("revival", "carol", "-")
	})

	t.Run("an add that beats a live row keeps the adder", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		r.apply("bob", 2000, Op{Op: "add", Fields: oat})
		if title, _ := itemTitle(t, r.db, "cart-a", "MILK"); title != "Oat milk" {
			t.Fatalf("fixture: the later add did not win (title %q)", title)
		}
		r.expect("add over live", "alice", "-")
	})

	t.Run("an update sets neither", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		r.apply("bob", 2000, Op{Op: "update", Fields: map[string]any{"title": "Skim"}})
		r.expect("update", "alice", "-")
		// A completion is an update's kind of write too.
		r.apply("bob", 3000, Op{Op: "update", Fields: map[string]any{"checked": true, "purchasedAt": 3000.0}})
		r.expect("check-off", "alice", "-")
	})

	t.Run("a delete that ends no life sets nothing", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk, Echo: echo1})
		// A selector naming another life.
		r.apply("bob", 2000, Op{Op: "delete", Marker: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"})
		r.expect("other life", "alice", "-")
		// A selector naming a life of a missing row inserts no tombstone.
		if _, err := ApplyOp(r.db, "cart-a", Op{ID: "NOPE", Op: "delete", Marker: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"}, 2500, "dev-a", "bob"); err != nil {
			t.Fatal(err)
		}
		var n int
		r.db.QueryRow(`SELECT COUNT(*) FROM items WHERE id='NOPE'`).Scan(&n)
		if n != 0 {
			t.Errorf("a named delete of a missing row inserted %d rows", n)
		}
		// A second delete of a tombstone changes nothing: the first deleter stays.
		r.apply("carol", 3000, Op{Op: "delete"})
		r.apply("dave", 4000, Op{Op: "delete"})
		r.expect("repeat delete", "alice", "carol")
	})

	t.Run("a delete of a missing row with no selector records its deleter and no adder", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("bob", 1000, Op{Op: "delete"})
		r.expect("tombstone over nothing", "-", "bob")
	})

	t.Run("a tombstone carries both authors on the wire", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		r.apply("bob", 2000, Op{Op: "delete"})
		_, tombs, err := GetCartRows(r.db, "cart-a")
		if err != nil || len(tombs) != 1 {
			t.Fatalf("tombstones = %v, %v", tombs, err)
		}
		m := jsonOf(t, tombs[0])
		expect(t, "addedBy", m["addedBy"].(string), "alice")
		expect(t, "deletedBy", m["deletedBy"].(string), "bob")
	})

	t.Run("a live row omits the keys when nothing was recorded", func(t *testing.T) {
		r := authorRig{t, testDB(t)}
		r.apply("alice", 1000, Op{Op: "add", Fields: milk})
		if _, err := r.db.Exec(`UPDATE items SET added_by = NULL`); err != nil {
			t.Fatal(err)
		}
		items, _, _ := GetCartRows(r.db, "cart-a")
		absent(t, jsonOf(t, items[0]), "addedBy", "deletedBy")
	})
}

func TestAFinalListRowsCarryTheirAdder(t *testing.T) {
	r := newFinalListRig(t)
	list, err := RemoveMember(r.db, r.cartA, "bob")
	if err != nil || len(list) == 0 {
		t.Fatalf("RemoveMember: %v %v", list, err)
	}
	for _, row := range list {
		expect(t, "addedBy of "+row.ID, row.AddedBy, "alice")
	}
	// And it survives the stored JSON round trip.
	got, err := FinalList(r.db, r.keyBob, r.cartA)
	if err != nil || len(got) != len(list) {
		t.Fatalf("FinalList: %v %v", got, err)
	}
	for _, row := range got {
		expect(t, "stored addedBy of "+row.ID, row.AddedBy, "alice")
	}
}

// ADR-101 §9: /join's response carries the cart's tombstones, built from the state's reads.
func TestJoinCarriesTheCartsTombstones(t *testing.T) {
	join := func(r *finalListRig) map[string]any {
		t.Helper()
		w := postJSON(t, joinCartHandler(r.db, r.hub), r.cartA, "", map[string]any{
			"cartID": r.cartA, "secret": "secret-a", "userID": "bob", "displayName": "B", "color": "#fff",
		})
		if w.Code != 200 {
			t.Fatalf("join: %d %s", w.Code, w.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}

	t.Run("a join after a delete lists the row as a tombstone only", func(t *testing.T) {
		r := newFinalListRig(t)
		eggsMarker, _ := lifeOf(t, r.db, r.cartA, "EGGS")
		body := join(r)
		tombs, _ := body["tombstones"].([]any)
		if len(tombs) != 1 {
			t.Fatalf("tombstones = %v, want one", body["tombstones"])
		}
		tomb := tombs[0].(map[string]any)
		expect(t, "id", tomb["id"].(string), "EGGS")
		expect(t, "marker", tomb["marker"].(string), eggsMarker)
		expect(t, "addedBy", tomb["addedBy"].(string), "alice")
		expect(t, "deletedBy", tomb["deletedBy"].(string), "alice")
		for _, it := range body["items"].([]any) {
			if it.(map[string]any)["id"] == "EGGS" {
				t.Errorf("EGGS is in items as well as tombstones")
			}
		}
	})

	t.Run("an echo rides the tombstone", func(t *testing.T) {
		r := newFinalListRig(t)
		if _, err := ApplyOp(r.db, r.cartA, Op{ID: "BREAD", Op: "delete"}, 1400, "dev", "alice"); err != nil {
			t.Fatal(err)
		}
		body := join(r)
		for _, tb := range body["tombstones"].([]any) {
			m := tb.(map[string]any)
			if m["id"] == "BREAD" {
				expect(t, "echo", m["echo"].(string), echo1)
				return
			}
		}
		t.Errorf("BREAD not in tombstones: %v", body["tombstones"])
	})

	t.Run("an empty cart answers an empty array, never null", func(t *testing.T) {
		db := testDB(t)
		hub := NewHub()
		w := httptest.NewRecorder()
		const cart = "11111111-2222-4333-8444-555555555555"
		if err := CreateRoom(db, cart, "user-a", "sec", "Empty", "#fff"); err != nil {
			t.Fatal(err)
		}
		raw := `{"cartID":"` + cart + `","secret":"sec","userID":"zed","displayName":"Z","color":"#fff"}`
		req := httptest.NewRequest("POST", "/api/join", strings.NewReader(raw))
		joinCartHandler(db, hub)(w, req)
		if w.Code != 200 {
			t.Fatalf("join: %d %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"tombstones":[]`) || !strings.Contains(w.Body.String(), `"items":[]`) {
			t.Errorf("body = %s, want empty arrays for items and tombstones", w.Body.String())
		}
	})
}

// The snapshot's reads share one transaction on the one connection. A helper that took db inside it would
// wait for that connection forever, so this fails by timeout rather than hanging the suite.
func TestTheSnapshotCannotDeadlock(t *testing.T) {
	db := testDB(t)
	if _, err := ApplyOp(db, "cart-a", Op{ID: "MILK", Op: "add", Fields: map[string]any{"title": "Milk"}}, 1000, "d", "alice"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buildStateMsg(db, "cart-a")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("buildStateMsg did not return: the snapshot is waiting for the one connection")
	}
}
