package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// Regression tests for the 2026-07-10 re-share incident: item identity must be
// (cart_id, id). Under the legacy global-id PRIMARY KEY, an add for an item
// uuid already living in another room LWW-updated THAT room's row and acked
// "applied" — the new room stayed empty forever while every member's orphan
// self-heal re-uploaded the same items on each reconnect.

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := InitDB(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := UpsertUser(db, "user-a", "A", "#fff"); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	if err := CreateRoom(db, "cart-a", "user-a", "secret-a", "Cart A", "#fff"); err != nil {
		t.Fatalf("CreateRoom a: %v", err)
	}
	if err := CreateRoom(db, "cart-b", "user-a", "secret-b", "Cart B", "#fff"); err != nil {
		t.Fatalf("CreateRoom b: %v", err)
	}
	return db
}

func itemTitle(t *testing.T, db *sql.DB, cartID, itemID string) (title string, found bool) {
	t.Helper()
	err := db.QueryRow(
		`SELECT COALESCE(title,'') FROM items WHERE cart_id=? AND id=? AND deleted=0`,
		cartID, itemID,
	).Scan(&title)
	if err != nil {
		return "", false
	}
	return title, true
}

func itemDeleted(t *testing.T, db *sql.DB, cartID, itemID string) (deleted int, found bool) {
	t.Helper()
	err := db.QueryRow(
		`SELECT deleted FROM items WHERE cart_id=? AND id=?`, cartID, itemID,
	).Scan(&deleted)
	if err != nil {
		return 0, false
	}
	return deleted, true
}

func itemQuantity(t *testing.T, db *sql.DB, cartID, itemID string) int {
	t.Helper()
	var q int
	if err := db.QueryRow(
		`SELECT quantity FROM items WHERE cart_id=? AND id=?`, cartID, itemID,
	).Scan(&q); err != nil {
		t.Fatalf("itemQuantity: %v", err)
	}
	return q
}

// F7 defensive decode: a wrong-typed field from a misbehaving/future client is
// skipped (not applied) and must never panic the goroutine — the rest of the
// update still lands.
func TestApplyLWWWrongTypedFieldSkipped(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk", "quantity": float64(1)}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}

	// "quantity": "three" is a string where a number is required. The update must
	// apply the good field (title) and skip the bad one (quantity) without panic.
	ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"title": "Oat Milk", "quantity": "three"}, 2000, "dev-1", "user-a")
	if err != nil || !ok {
		t.Fatalf("update with wrong-typed field: ok=%v err=%v", ok, err)
	}

	if title, found := itemTitle(t, db, "cart-a", "item-1"); !found || title != "Oat Milk" {
		t.Errorf("title: found=%v title=%q, want \"Oat Milk\"", found, title)
	}
	if q := itemQuantity(t, db, "cart-a", "item-1"); q != 1 {
		t.Errorf("quantity: got %d, want 1 (wrong-typed update skipped)", q)
	}
}

// ADR-039 (a): an update op must NEVER resurrect a tombstoned row, even when its
// ts is newer than the tombstone — partial-field resurrection is the ghost bug.
func TestApplyLWWUpdateDoesNotResurrectTombstone(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "delete",
		map[string]any{}, 2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}

	// Update with a newer ts than the tombstone — must be skipped, not applied.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"title": "Ghost"}, 3000, "dev-1", "user-a"); err != nil || ok {
		t.Fatalf("update over tombstone: ok=%v err=%v, want ok=false", ok, err)
	}

	if deleted, found := itemDeleted(t, db, "cart-a", "item-1"); !found || deleted != 1 {
		t.Errorf("item must stay tombstoned: found=%v deleted=%d, want deleted=1", found, deleted)
	}
	if _, found := itemTitle(t, db, "cart-a", "item-1"); found {
		t.Errorf("tombstoned item must not surface as a live row")
	}
}

// ADR-039 (b): an add op with a newer ts MAY resurrect a tombstoned row — a
// deliberate re-add carrying full fields.
func TestApplyLWWAddResurrectsTombstoneWithFullFields(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk", "quantity": float64(1)}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "delete",
		map[string]any{}, 2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}

	// A stale add (older than the tombstone) must NOT resurrect — deletion wins.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Stale", "quantity": float64(9)}, 1500, "dev-1", "user-a"); err != nil || ok {
		t.Fatalf("stale add: ok=%v err=%v, want ok=false", ok, err)
	}
	if deleted, _ := itemDeleted(t, db, "cart-a", "item-1"); deleted != 1 {
		t.Errorf("stale add must not resurrect; deleted=%d, want 1", deleted)
	}

	// A newer add resurrects with its full fields.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Reborn Milk", "quantity": float64(3)}, 3000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("re-add: ok=%v err=%v", ok, err)
	}
	if title, found := itemTitle(t, db, "cart-a", "item-1"); !found || title != "Reborn Milk" {
		t.Errorf("resurrected title: found=%v title=%q, want \"Reborn Milk\"", found, title)
	}
	if q := itemQuantity(t, db, "cart-a", "item-1"); q != 3 {
		t.Errorf("resurrected quantity: got %d, want 3 (full fields)", q)
	}
}

// ADR-039 (c): pruneDeletedItems retention is unchanged — tombstones past 30d
// are removed, recent ones kept.
func TestPruneDeletedItemsRetention(t *testing.T) {
	db := testDB(t)

	old := float64(time.Now().Add(-40 * 24 * time.Hour).Unix())
	recent := float64(time.Now().Add(-1 * 24 * time.Hour).Unix())
	if _, err := db.Exec(`
		INSERT INTO items (id, cart_id, ts, deleted, deleted_at) VALUES ('old','cart-a',?,1,?)
	`, old, old); err != nil {
		t.Fatalf("insert old tombstone: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO items (id, cart_id, ts, deleted, deleted_at) VALUES ('recent','cart-a',?,1,?)
	`, recent, recent); err != nil {
		t.Fatalf("insert recent tombstone: %v", err)
	}

	pruneDeletedItems(db)

	if _, found := itemDeleted(t, db, "cart-a", "old"); found {
		t.Errorf("tombstone older than 30d must be pruned")
	}
	if deleted, found := itemDeleted(t, db, "cart-a", "recent"); !found || deleted != 1 {
		t.Errorf("recent tombstone must be retained: found=%v deleted=%d", found, deleted)
	}
}

// The incident shape: the same item uuid added to a second room must insert
// into that room — never redirect into the room that first owned the uuid.
func TestApplyLWWSameItemIDInTwoCarts(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk in A"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add to cart-a: ok=%v err=%v", ok, err)
	}
	if ok, err := ApplyLWW(db, "cart-b", "item-1", "add",
		map[string]any{"title": "Milk in B"}, 2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add to cart-b: ok=%v err=%v", ok, err)
	}

	if title, found := itemTitle(t, db, "cart-b", "item-1"); !found || title != "Milk in B" {
		t.Errorf("cart-b row: found=%v title=%q, want \"Milk in B\"", found, title)
	}
	if title, found := itemTitle(t, db, "cart-a", "item-1"); !found || title != "Milk in A" {
		t.Errorf("cart-a row clobbered: found=%v title=%q, want untouched \"Milk in A\"", found, title)
	}
}

// A delete in a room where the uuid has no row must insert that room's
// tombstone (under the global PK this INSERT conflicted with the other room's
// live row and errored).
func TestApplyLWWCrossCartDeleteInsertsTombstone(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk in A"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add to cart-a: ok=%v err=%v", ok, err)
	}
	if ok, err := ApplyLWW(db, "cart-b", "item-1", "delete",
		map[string]any{}, 2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("delete in cart-b: ok=%v err=%v", ok, err)
	}

	var deleted int
	if err := db.QueryRow(
		`SELECT deleted FROM items WHERE cart_id=? AND id=?`, "cart-b", "item-1",
	).Scan(&deleted); err != nil || deleted != 1 {
		t.Errorf("cart-b tombstone: err=%v deleted=%d, want 1", err, deleted)
	}
	if _, found := itemTitle(t, db, "cart-a", "item-1"); !found {
		t.Errorf("cart-a live row must survive a cart-b delete")
	}
}

// A legacy DB (global id PRIMARY KEY) is rebuilt onto (cart_id, id) with rows
// intact, and accepts the previously-impossible second-room row afterwards.
func TestMigrateItemsCompositeKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")

	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE rooms (
		    cart_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, secret TEXT NOT NULL,
		    created_at REAL NOT NULL, cart_name TEXT NOT NULL DEFAULT '', hex_color TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE items (
		    id TEXT PRIMARY KEY, cart_id TEXT NOT NULL, title TEXT,
		    quantity INTEGER DEFAULT 1, checked INTEGER DEFAULT 0, notes TEXT,
		    specification TEXT, urgency_level INTEGER DEFAULT 1, attributed_to TEXT,
		    ts REAL NOT NULL, device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
		    FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
		INSERT INTO rooms VALUES ('cart-b','user-a','s',1,'B','');
		INSERT INTO items (id, cart_id, title, ts) VALUES ('item-1','cart-a','Legacy Milk',1000);
	`); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}
	legacy.Close()

	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on legacy db: %v", err)
	}
	defer db.Close()

	if title, found := itemTitle(t, db, "cart-a", "item-1"); !found || title != "Legacy Milk" {
		t.Errorf("migrated row: found=%v title=%q, want \"Legacy Milk\"", found, title)
	}
	if ok, err := ApplyLWW(db, "cart-b", "item-1", "add",
		map[string]any{"title": "Milk in B"}, 2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("post-migration add to cart-b: ok=%v err=%v", ok, err)
	}
	if title, found := itemTitle(t, db, "cart-b", "item-1"); !found || title != "Milk in B" {
		t.Errorf("post-migration cart-b row: found=%v title=%q", found, title)
	}
}

// ADR-050 (a): a pre-ADR-050 items table (no purchased_at column) gains it on InitDB, and a checked op
// carrying the fact then round-trips through the state door (GetItems).
func TestPurchaseFactColumnMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prefact.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE rooms (
		    cart_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, secret TEXT NOT NULL,
		    created_at REAL NOT NULL, cart_name TEXT NOT NULL DEFAULT '', hex_color TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE items (
		    id TEXT NOT NULL, cart_id TEXT NOT NULL, title TEXT, quantity INTEGER DEFAULT 1,
		    checked INTEGER DEFAULT 0, notes TEXT, specification TEXT, urgency_level INTEGER DEFAULT 1,
		    global_id TEXT, frequency_days INTEGER, is_active INTEGER DEFAULT 1, paused_at REAL,
		    pause_reason TEXT, deferred_guess_date REAL, attributed_to TEXT, ts REAL NOT NULL,
		    device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
		    PRIMARY KEY (cart_id, id), FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
	`); err != nil {
		t.Fatalf("seed pre-fact schema: %v", err)
	}
	legacy.Close()

	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on pre-fact db: %v", err)
	}
	defer db.Close()

	cols, err := existingColumns(db, "items")
	if err != nil {
		t.Fatalf("existingColumns: %v", err)
	}
	if !cols["purchased_at"] {
		t.Fatal("migration did not add the purchased_at column")
	}

	pa := 1_700_000_000.0
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk", "checked": true, "globalID": "g1", "purchasedAt": pa},
		1_700_050_000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add carrying purchasedAt: ok=%v err=%v", ok, err)
	}
	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	if items[0].PurchasedAt == nil || *items[0].PurchasedAt != pa {
		t.Errorf("state row purchasedAt = %v, want %v (the fact must ride state rows)", items[0].PurchasedAt, pa)
	}
}

// ADR-053: a pre-ADR-053 items table (no deferred_guess_authored_at column) gains it on InitDB; a
// field-targeted anchor update then carries the anchor/authored-at PAIR, both persist, and the state
// door (GetItems) returns the pair — the fact every device needs to derive consumption without a synced
// clear. A NSNull-paired clear (both keys carrying null) wipes both columns together.
func TestAnchorAuthoredAtColumnMigrationAndFieldUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preauthored.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE rooms (
		    cart_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, secret TEXT NOT NULL,
		    created_at REAL NOT NULL, cart_name TEXT NOT NULL DEFAULT '', hex_color TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE items (
		    id TEXT NOT NULL, cart_id TEXT NOT NULL, title TEXT, quantity INTEGER DEFAULT 1,
		    checked INTEGER DEFAULT 0, notes TEXT, specification TEXT, urgency_level INTEGER DEFAULT 1,
		    global_id TEXT, frequency_days INTEGER, is_active INTEGER DEFAULT 1, paused_at REAL,
		    pause_reason TEXT, deferred_guess_date REAL, attributed_to TEXT, ts REAL NOT NULL,
		    device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
		    PRIMARY KEY (cart_id, id), FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
	`); err != nil {
		t.Fatalf("seed pre-authored schema: %v", err)
	}
	legacy.Close()

	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on pre-authored db: %v", err)
	}
	defer db.Close()

	cols, err := existingColumns(db, "items")
	if err != nil {
		t.Fatalf("existingColumns: %v", err)
	}
	if !cols["deferred_guess_authored_at"] {
		t.Fatal("migration did not add the deferred_guess_authored_at column")
	}

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Shampoo"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	// A defer authors the anchor: the anchor date AND its authorship instant ride together.
	anchor := 1_700_000_000.0
	authoredAt := 1_699_950_000.0
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"deferredGuessDate": anchor, "deferredGuessAuthoredAt": authoredAt},
		2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("anchor update: ok=%v err=%v", ok, err)
	}

	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	it := items[0]
	if it.DeferredGuessDate == nil || *it.DeferredGuessDate != anchor {
		t.Errorf("state row deferredGuessDate = %v, want %v", it.DeferredGuessDate, anchor)
	}
	if it.DeferredGuessAuthoredAt == nil || *it.DeferredGuessAuthoredAt != authoredAt {
		t.Errorf("state row deferredGuessAuthoredAt = %v, want %v (the pair must ride state rows)", it.DeferredGuessAuthoredAt, authoredAt)
	}

	// A NSNull-paired clear (user clears the authored anchor): both columns wipe together.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"deferredGuessDate": nil, "deferredGuessAuthoredAt": nil},
		3000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("anchor clear: ok=%v err=%v", ok, err)
	}
	items, err = GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems after clear: err=%v n=%d", err, len(items))
	}
	if items[0].DeferredGuessDate != nil || items[0].DeferredGuessAuthoredAt != nil {
		t.Errorf("paired clear left anchor=%v authoredAt=%v, want both nil",
			items[0].DeferredGuessDate, items[0].DeferredGuessAuthoredAt)
	}
}

// ADR-050 (b): a re-assert of a completion carrying the SAME purchase fact at a fresh (next-day) LWW ts
// mints NO second consumption row — the fact-keyed PK + INSERT OR IGNORE collapse it. Pre-fix the row ts
// (next day) would key a distinct PK and mint a phantom cross-day event.
func TestCaptureConsumptionReassertSameFactIsIdempotent(t *testing.T) {
	db := testDB(t)
	pa := 1_700_000_000.0 // the real purchase instant (day 1)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk", "checked": true, "globalID": "g1", "purchasedAt": pa},
		pa+3600, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("initial check-off: ok=%v err=%v", ok, err)
	}
	captureConsumption(db, "cart-a", "item-1", true)

	// A stale device re-asserts checked:true the NEXT DAY (fresh op ts) but carrying the SAME fact.
	nextDay := pa + 86_400 + 3600
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"checked": true, "purchasedAt": pa},
		nextDay, "dev-2", "user-a"); err != nil || !ok {
		t.Fatalf("stale re-assert: ok=%v err=%v", ok, err)
	}
	captureConsumption(db, "cart-a", "item-1", true)

	events, err := GetConsumptionEvents(db, "cart-a", "g1")
	if err != nil {
		t.Fatalf("GetConsumptionEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("re-assert of the same fact minted %d consumption rows, want 1 (%v)", len(events), events)
	}
	if events[0] != pa {
		t.Errorf("consumption stamped at %v, want the purchase fact %v (not the op ts)", events[0], pa)
	}
}

// ADR-051 (a): a pre-ADR-051 items table (no hint columns) gains them on InitDB; an add carrying the
// sender's label hints then round-trips through the state door (GetItems), and an old-style add that
// omits every hint key stays valid with nil hints (today's behaviour).
func TestLabelHintColumnsMigrationAndStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prehints.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE rooms (
		    cart_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, secret TEXT NOT NULL,
		    created_at REAL NOT NULL, cart_name TEXT NOT NULL DEFAULT '', hex_color TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE items (
		    id TEXT NOT NULL, cart_id TEXT NOT NULL, title TEXT, quantity INTEGER DEFAULT 1,
		    checked INTEGER DEFAULT 0, notes TEXT, specification TEXT, urgency_level INTEGER DEFAULT 1,
		    global_id TEXT, frequency_days INTEGER, is_active INTEGER DEFAULT 1, paused_at REAL,
		    pause_reason TEXT, deferred_guess_date REAL, purchased_at REAL, attributed_to TEXT, ts REAL NOT NULL,
		    device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
		    PRIMARY KEY (cart_id, id), FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
	`); err != nil {
		t.Fatalf("seed pre-hints schema: %v", err)
	}
	legacy.Close()

	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on pre-hints db: %v", err)
	}
	defer db.Close()

	cols, err := existingColumns(db, "items")
	if err != nil {
		t.Fatalf("existingColumns: %v", err)
	}
	for _, c := range []string{"category_hint_name", "category_hint_icon", "category_hint_color", "store_hint_brand"} {
		if !cols[c] {
			t.Fatalf("migration did not add the %s column", c)
		}
	}

	// Add carrying the sender's hints — they must ride the STATE row for joiners.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{
			"title":             "Kale",
			"categoryHintName":  "Produce",
			"categoryHintIcon":  "leaf",
			"categoryHintColor": "#4CAF50",
			"storeHintBrand":    "Costco",
		}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add carrying hints: ok=%v err=%v", ok, err)
	}
	// Old-style add omitting every hint key must stay valid with nil hints.
	if ok, err := ApplyLWW(db, "cart-a", "item-2", "add",
		map[string]any{"title": "Milk"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("old-style add: ok=%v err=%v", ok, err)
	}

	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 2 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	byID := map[string]ItemRow{}
	for _, it := range items {
		byID[it.ID] = it
	}

	hinted := byID["item-1"]
	if hinted.CategoryHintName == nil || *hinted.CategoryHintName != "Produce" {
		t.Errorf("state row categoryHintName = %v, want Produce", hinted.CategoryHintName)
	}
	if hinted.CategoryHintIcon == nil || *hinted.CategoryHintIcon != "leaf" {
		t.Errorf("state row categoryHintIcon = %v, want leaf", hinted.CategoryHintIcon)
	}
	if hinted.CategoryHintColor == nil || *hinted.CategoryHintColor != "#4CAF50" {
		t.Errorf("state row categoryHintColor = %v, want #4CAF50", hinted.CategoryHintColor)
	}
	if hinted.StoreHintBrand == nil || *hinted.StoreHintBrand != "Costco" {
		t.Errorf("state row storeHintBrand = %v, want Costco", hinted.StoreHintBrand)
	}

	plain := byID["item-2"]
	if plain.CategoryHintName != nil || plain.CategoryHintIcon != nil ||
		plain.CategoryHintColor != nil || plain.StoreHintBrand != nil {
		t.Errorf("hint-less add must yield nil hints, got name=%v icon=%v color=%v brand=%v",
			plain.CategoryHintName, plain.CategoryHintIcon, plain.CategoryHintColor, plain.StoreHintBrand)
	}
}

// ADR-051 (b): a member's manual relabel enqueues a field-targeted hint update; it lands on the row
// and rides subsequent state, without disturbing unrelated fields.
func TestLabelHintFieldUpdate(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Kale"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	// A manual category reassignment enqueues just the hint fields.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"categoryHintName": "Greens", "categoryHintIcon": "leaf", "categoryHintColor": "#4CAF50"},
		2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("hint update: ok=%v err=%v", ok, err)
	}

	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	it := items[0]
	if it.CategoryHintName == nil || *it.CategoryHintName != "Greens" {
		t.Errorf("categoryHintName = %v, want Greens", it.CategoryHintName)
	}
	if it.StoreHintBrand != nil {
		t.Errorf("storeHintBrand must stay nil (untouched by a category-only update), got %v", it.StoreHintBrand)
	}
	if it.Title != "Kale" {
		t.Errorf("title clobbered: got %q, want Kale", it.Title)
	}
}

// ADR-050 (c): an old client that omits purchasedAt falls back to the row ts — today's behaviour.
func TestCaptureConsumptionFallsBackToTs(t *testing.T) {
	db := testDB(t)
	ts := 1_700_000_000.0
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk", "checked": true, "globalID": "g1"},
		ts, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add without purchasedAt: ok=%v err=%v", ok, err)
	}
	captureConsumption(db, "cart-a", "item-1", true)

	events, err := GetConsumptionEvents(db, "cart-a", "g1")
	if err != nil {
		t.Fatalf("GetConsumptionEvents: %v", err)
	}
	if len(events) != 1 || events[0] != ts {
		t.Fatalf("absent purchasedAt: events=%v, want [%v] (fallback to ts)", events, ts)
	}
}

// ADR-052: a concept-less item (global_id NULL) accretes into the reservoir keyed on its
// cart-scoped item id, and the same spine is readable/retractable — the pre-052 skip is gone.
func TestCaptureConsumptionConceptLessKeysOnItemID(t *testing.T) {
	db := testDB(t)
	pa := 1_700_000_000.0
	// Concept-less: the add carries no globalID, so global_id stays NULL server-side.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "House Keeping", "checked": true, "purchasedAt": pa},
		pa+3600, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("concept-less check-off: ok=%v err=%v", ok, err)
	}
	captureConsumption(db, "cart-a", "item-1", true)

	// Nothing under a concept spine…
	if events, err := GetConsumptionEvents(db, "cart-a", "g1"); err != nil || len(events) != 0 {
		t.Fatalf("concept spine: events=%v err=%v, want none", events, err)
	}
	// …but the item-id spine holds the purchase fact.
	events, err := GetConsumptionEvents(db, "cart-a", "item-1")
	if err != nil {
		t.Fatalf("GetConsumptionEvents(item-1): %v", err)
	}
	if len(events) != 1 || events[0] != pa {
		t.Fatalf("concept-less capture: events=%v, want [%v] under item-id spine", events, pa)
	}
}
