package main

import (
	"database/sql"
	"testing"
)

// A member's latest want is a monotone register, not LWW content: the max of what is stored and what an
// op carries, applied whenever the op passes the whole-op guards, and sent to peers only when it moved.
// Older clients re-assert an item's whole content, want included, and rely on receivers only moving
// forward.

func wantRig(t *testing.T, stored float64) *sql.DB {
	t.Helper()
	db := testDB(t)
	if _, err := ApplyOp(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Sunscreen", "lastWantedAt": stored}, 1000, "dev-1", "user-a"); err != nil {
		t.Fatalf("add: %v", err)
	}
	return db
}

func storedWant(t *testing.T, db *sql.DB) sql.NullFloat64 {
	t.Helper()
	var w sql.NullFloat64
	if err := db.QueryRow(`SELECT last_wanted_at FROM items WHERE cart_id='cart-a' AND id='item-1'`).Scan(&w); err != nil {
		t.Fatalf("read want: %v", err)
	}
	return w
}

func TestWantRegister(t *testing.T) {
	t.Run("a newer op carrying an older want leaves the register and is not relayed for it", func(t *testing.T) {
		db := wantRig(t, 5000)
		out, err := ApplyOp(db, "cart-a", "item-1", "update",
			map[string]any{"title": "Sunscreen SPF50", "lastWantedAt": 3000.0}, 2000, "dev-2", "user-b")
		if err != nil || !out.Applied {
			t.Fatalf("rename: out=%+v err=%v", out, err)
		}
		if w := storedWant(t, db); w.Float64 != 5000 {
			t.Errorf("stored want = %v, want 5000 (an older want dragged the register back)", w)
		}
		if _, sent := out.Fields["lastWantedAt"]; sent {
			t.Errorf("an older want was relayed: %v", out.Fields)
		}
		if out.Fields["title"] != "Sunscreen SPF50" {
			t.Errorf("the rename's content was not relayed: %v", out.Fields)
		}
	})

	t.Run("an older op carrying a later want advances the register and relays only the want", func(t *testing.T) {
		db := wantRig(t, 5000)
		if _, err := ApplyOp(db, "cart-a", "item-1", "update",
			map[string]any{"title": "Newer name"}, 3000, "dev-1", "user-a"); err != nil {
			t.Fatal(err)
		}
		out, err := ApplyOp(db, "cart-a", "item-1", "update",
			map[string]any{"title": "Stale name", "lastWantedAt": 6000.0}, 2000, "dev-2", "user-b")
		if err != nil || !out.Applied || !out.Broadcast {
			t.Fatalf("losing op with later want: out=%+v err=%v", out, err)
		}
		if w := storedWant(t, db); w.Float64 != 6000 {
			t.Errorf("stored want = %v, want 6000", w)
		}
		if len(out.Fields) != 1 || out.Fields["lastWantedAt"] != 6000.0 {
			t.Errorf("relayed %v, want only lastWantedAt 6000", out.Fields)
		}
		var title string
		_ = db.QueryRow(`SELECT title FROM items WHERE id='item-1'`).Scan(&title)
		if title != "Newer name" {
			t.Errorf("title = %q: the losing op's content rode in", title)
		}
	})

	t.Run("an equal want is a no-op and is not relayed", func(t *testing.T) {
		db := wantRig(t, 5000)
		out, err := ApplyOp(db, "cart-a", "item-1", "update",
			map[string]any{"lastWantedAt": 5000.0}, 2000, "dev-2", "user-b")
		if err != nil {
			t.Fatal(err)
		}
		if out.Broadcast || len(out.Fields) != 0 {
			t.Errorf("an equal want was relayed: %+v", out)
		}
		if w := storedWant(t, db); w.Float64 != 5000 {
			t.Errorf("stored want = %v, want 5000", w)
		}
	})

	t.Run("an update to a tombstoned row writes no want", func(t *testing.T) {
		db := wantRig(t, 5000)
		if _, err := ApplyOp(db, "cart-a", "item-1", "delete", nil, 2000, "dev-1", "user-a"); err != nil {
			t.Fatal(err)
		}
		out, err := ApplyOp(db, "cart-a", "item-1", "update",
			map[string]any{"lastWantedAt": 9000.0}, 3000, "dev-2", "user-b")
		if err != nil || out.Applied || out.Broadcast {
			t.Errorf("update over a tombstone: out=%+v err=%v", out, err)
		}
		if w := storedWant(t, db); w.Float64 != 5000 {
			t.Errorf("stored want = %v, want 5000", w)
		}
	})

	t.Run("an add that lost to a tombstone takes no want", func(t *testing.T) {
		db := testDB(t)
		if _, err := ApplyOp(db, "cart-a", "item-1", "delete", nil, 5000, "dev-1", "user-a"); err != nil {
			t.Fatal(err)
		}
		out, err := ApplyOp(db, "cart-a", "item-1", "add",
			map[string]any{"title": "Ghost", "lastWantedAt": 4000.0}, 2000, "dev-2", "user-b")
		if err != nil || out.Broadcast {
			t.Errorf("stale add: out=%+v err=%v", out, err)
		}
		if w := storedWant(t, db); w.Valid {
			t.Errorf("stored want = %v, want none", w)
		}
	})

	t.Run("a far-future or non-numeric want is no want", func(t *testing.T) {
		db := wantRig(t, 5000)
		for _, v := range []any{4e12, "soon", nil} {
			out, err := ApplyOp(db, "cart-a", "item-1", "update",
				map[string]any{"lastWantedAt": v}, 2000, "dev-2", "user-b")
			if err != nil || out.Broadcast {
				t.Errorf("%v: out=%+v err=%v", v, out, err)
			}
		}
		if w := storedWant(t, db); w.Float64 != 5000 {
			t.Errorf("stored want = %v, want 5000", w)
		}
	})
}
