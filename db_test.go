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

// R-384: a pre-R-384 items table (no purchase_store_brand column) gains it on InitDB, a checked op
// carrying the brand round-trips through the state door, and the present-keys-only rule holds in both
// directions — an unchecked update leaves the recorded brand standing, and a later checked op at a
// different shop replaces it.
//
// The last clause is the one worth a test rather than a comment: the brand names ONE purchase, so a
// value that outlived its purchase would put a shop nobody visited on the next trip — two plausible
// strings with nothing to tell them apart afterwards.
func TestPurchaseStoreBrandColumnMigrationAndRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prebrand.db")
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
		    pause_reason TEXT, deferred_guess_date REAL, purchased_at REAL, attributed_to TEXT,
		    ts REAL NOT NULL, device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
		    PRIMARY KEY (cart_id, id), FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
	`); err != nil {
		t.Fatalf("seed pre-brand schema: %v", err)
	}
	legacy.Close()

	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on pre-brand db: %v", err)
	}
	defer db.Close()

	cols, err := existingColumns(db, "items")
	if err != nil {
		t.Fatalf("existingColumns: %v", err)
	}
	if !cols["purchase_store_brand"] {
		t.Fatal("migration did not add the purchase_store_brand column")
	}

	// (a) A checked `add` carrying the brand round-trips through the state door.
	pa := 1_700_000_000.0
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk", "checked": true, "purchasedAt": pa, "purchaseStoreBrand": "FreshCo"},
		1_700_050_000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add carrying purchaseStoreBrand: ok=%v err=%v", ok, err)
	}
	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	if items[0].PurchaseStoreBrand == nil || *items[0].PurchaseStoreBrand != "FreshCo" {
		t.Errorf("state row purchaseStoreBrand = %v, want FreshCo (the brand must ride state rows)", items[0].PurchaseStoreBrand)
	}

	// (b) An unchecked update omits the key entirely — present-keys-only leaves the recorded brand alone.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"title": "Whole Milk"},
		1_700_060_000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("unchecked update: ok=%v err=%v", ok, err)
	}
	items, _ = GetItems(db, "cart-a")
	if items[0].PurchaseStoreBrand == nil || *items[0].PurchaseStoreBrand != "FreshCo" {
		t.Errorf("an absent key cleared the brand: %v — present-keys-only means unchanged", items[0].PurchaseStoreBrand)
	}

	// (c) A later purchase at a different shop replaces it. The brand names one purchase; it must not
	// accumulate or stick.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"checked": true, "purchasedAt": pa + 604_800, "purchaseStoreBrand": "Costco"},
		1_700_700_000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("second checked update: ok=%v err=%v", ok, err)
	}
	items, _ = GetItems(db, "cart-a")
	if items[0].PurchaseStoreBrand == nil || *items[0].PurchaseStoreBrand != "Costco" {
		t.Errorf("purchaseStoreBrand = %v, want Costco — the brand names THIS purchase", items[0].PurchaseStoreBrand)
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

// stateAttributedTo reads attributed_to the way a client does — through the state door (GetItems),
// where NULL surfaces as a nil pointer.
func stateAttributedTo(t *testing.T, db *sql.DB, cartID, itemID string) *string {
	t.Helper()
	items, err := GetItems(db, cartID)
	if err != nil {
		t.Fatalf("GetItems: %v", err)
	}
	for _, it := range items {
		if it.ID == itemID {
			return it.AttributedTo
		}
	}
	t.Fatalf("item %s not in state for cart %s", itemID, cartID)
	return nil
}

// R-468 (a) — the regression. attributed_to means "who checked this off", and an op that says
// nothing about completion must not move it. A rename by another member used to overwrite the
// checker, and the next state snapshot then named the wrong buyer (durable since ADR-079's
// PurchaseEvent.buyer). The two test suites — Swift receiver-door, Go LWW — meet nowhere, so
// this is the first thing that crosses the seam.
func TestAttributedToSurvivesAnEditByAnotherMember(t *testing.T) {
	db := testDB(t)

	// A checks the item off.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk"}, 1000, "dev-a", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"checked": true}, 2000, "dev-a", "user-a"); err != nil || !ok {
		t.Fatalf("check off: ok=%v err=%v", ok, err)
	}
	if got := stateAttributedTo(t, db, "cart-a", "item-1"); got == nil || *got != "user-a" {
		t.Fatalf("after check-off attributed_to = %v, want user-a", got)
	}

	// B renames it. This asserts nothing about who bought it.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"title": "Whole Milk"}, 3000, "dev-b", "user-b"); err != nil || !ok {
		t.Fatalf("rename: ok=%v err=%v", ok, err)
	}
	if got := stateAttributedTo(t, db, "cart-a", "item-1"); got == nil || *got != "user-a" {
		t.Errorf("a rename by user-b changed the completer to %v, want it untouched at user-a", got)
	}

	// A quantity change by B — same story.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"quantity": 2.0}, 4000, "dev-b", "user-b"); err != nil || !ok {
		t.Fatalf("quantity change: ok=%v err=%v", ok, err)
	}
	if got := stateAttributedTo(t, db, "cart-a", "item-1"); got == nil || *got != "user-a" {
		t.Errorf("a quantity change by user-b changed the completer to %v, want user-a", got)
	}
}

// R-468 (b) — the completer moves with, and only with, an op that asserts completion.
func TestAttributedToTracksTheCheckedAssertion(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk"}, 1000, "dev-a", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	if got := stateAttributedTo(t, db, "cart-a", "item-1"); got != nil {
		t.Fatalf("a fresh unchecked add has a completer %v, want none", got)
	}

	// checked:true — the asserting actor becomes the completer.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"checked": true}, 2000, "dev-b", "user-b"); err != nil || !ok {
		t.Fatalf("check off: ok=%v err=%v", ok, err)
	}
	if got := stateAttributedTo(t, db, "cart-a", "item-1"); got == nil || *got != "user-b" {
		t.Errorf("checked:true by user-b: completer = %v, want user-b", got)
	}

	// checked:false — the completion is retracted, so is the completer.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"checked": false}, 3000, "dev-a", "user-a"); err != nil || !ok {
		t.Fatalf("uncheck: ok=%v err=%v", ok, err)
	}
	if got := stateAttributedTo(t, db, "cart-a", "item-1"); got != nil {
		t.Errorf("checked:false: completer = %v, want none", got)
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

// The reconnect hole this pair of columns closes: a birth `add` op already carries
// quietUntilFirstPurchase today, and before these columns existed the relay dropped it on persist —
// so it reached a live-connected peer but was absent from the state snapshot a reconnecting peer
// reads. A pre-columns items table gains both columns on InitDB, and an add carrying the quiet flag
// then survives the state door. This test fails against the pre-change schema (no column to persist
// into, so GetItems returns the zero value).
func TestWantedAndQuietColumnMigrationAndBirthOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prewanted.db")
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
		    pause_reason TEXT, deferred_guess_date REAL, deferred_guess_authored_at REAL,
		    purchased_at REAL, category_hint_name TEXT, category_hint_icon TEXT,
		    category_hint_color TEXT, store_hint_brand TEXT, attributed_to TEXT, ts REAL NOT NULL,
		    device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
		    PRIMARY KEY (cart_id, id), FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
	`); err != nil {
		t.Fatalf("seed pre-wanted schema: %v", err)
	}
	legacy.Close()

	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on pre-wanted db: %v", err)
	}
	defer db.Close()

	cols, err := existingColumns(db, "items")
	if err != nil {
		t.Fatalf("existingColumns: %v", err)
	}
	for _, c := range []string{"last_wanted_at", "quiet_until_first_purchase"} {
		if !cols[c] {
			t.Fatalf("migration did not add the %s column", c)
		}
	}

	// The live bug: a quiet item is BORN quiet, on the add op. A peer that was offline at that moment
	// learns it only from the state snapshot.
	wanted := 1_700_000_000.0
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Sunscreen", "quietUntilFirstPurchase": true, "lastWantedAt": wanted},
		1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("quiet add: ok=%v err=%v", ok, err)
	}

	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	if !items[0].QuietUntilFirstPurchase {
		t.Error("state row quietUntilFirstPurchase = false, want true (a quiet birth must survive the state door)")
	}
	if items[0].LastWantedAt == nil || *items[0].LastWantedAt != wanted {
		t.Errorf("state row lastWantedAt = %v, want %v", items[0].LastWantedAt, wanted)
	}
}

// Both facts round-trip on a field-targeted `update` op at NON-DEFAULT values, through persistence and
// back out of the state door — and the quiet flag clears when the client sends false (which is what a
// purchase does). Asserting a non-default value is the point: `false` / nil would pass on a column that
// was never written, and reading the row directly separates "persisted" from "returned by GetItems", so
// dropping the column from either the insert/update list or the select list reddens a distinct line.
func TestWantedAndQuietFieldUpdateRoundTrip(t *testing.T) {
	db := testDB(t)

	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Shampoo"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	// A plain add sets neither fact.
	if items, err := GetItems(db, "cart-a"); err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	} else if items[0].LastWantedAt != nil || items[0].QuietUntilFirstPurchase {
		t.Errorf("plain add left lastWantedAt=%v quiet=%v, want nil/false",
			items[0].LastWantedAt, items[0].QuietUntilFirstPurchase)
	}

	// A member resumes the item and asks for silence until its next purchase.
	wanted := 1_700_123_456.0
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"lastWantedAt": wanted, "quietUntilFirstPurchase": true},
		2000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("wanted/quiet update: ok=%v err=%v", ok, err)
	}

	// Persisted in the row itself…
	var rowWanted sql.NullFloat64
	var rowQuiet int
	if err := db.QueryRow(
		`SELECT last_wanted_at, quiet_until_first_purchase FROM items WHERE cart_id=? AND id=?`,
		"cart-a", "item-1",
	).Scan(&rowWanted, &rowQuiet); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if !rowWanted.Valid || rowWanted.Float64 != wanted {
		t.Errorf("row last_wanted_at = %v, want %v", rowWanted, wanted)
	}
	if rowQuiet != 1 {
		t.Errorf("row quiet_until_first_purchase = %d, want 1", rowQuiet)
	}

	// …and returned through the state door.
	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	if items[0].LastWantedAt == nil || *items[0].LastWantedAt != wanted {
		t.Errorf("state row lastWantedAt = %v, want %v", items[0].LastWantedAt, wanted)
	}
	if !items[0].QuietUntilFirstPurchase {
		t.Error("state row quietUntilFirstPurchase = false, want true")
	}

	// An unrelated op must not clear either fact — present-keys-only.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"quantity": 3.0}, 3000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("unrelated update: ok=%v err=%v", ok, err)
	}
	items, err = GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems after unrelated update: err=%v n=%d", err, len(items))
	}
	if items[0].LastWantedAt == nil || *items[0].LastWantedAt != wanted || !items[0].QuietUntilFirstPurchase {
		t.Errorf("unrelated op disturbed the facts: lastWantedAt=%v quiet=%v",
			items[0].LastWantedAt, items[0].QuietUntilFirstPurchase)
	}

	// The purchase arrives: the client sends the flag false and the silence ends.
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "update",
		map[string]any{"checked": true, "quietUntilFirstPurchase": false},
		4000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("purchase update: ok=%v err=%v", ok, err)
	}
	items, err = GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems after purchase: err=%v n=%d", err, len(items))
	}
	if items[0].QuietUntilFirstPurchase {
		t.Error("quiet flag still set after the purchase cleared it")
	}
	if items[0].LastWantedAt == nil || *items[0].LastWantedAt != wanted {
		t.Errorf("purchase disturbed lastWantedAt = %v, want %v", items[0].LastWantedAt, wanted)
	}
}

// The composite-key rebuild path (legacy global-`id` PRIMARY KEY) copies rows by explicit column
// list. Both new columns must be in that list, or a rebuild silently drops facts that were already
// persisted. This is the rebuild-path counterpart to the migration test above.
func TestCompositeKeyRebuildCarriesWantedAndQuiet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacypk.db")
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
		    id TEXT PRIMARY KEY, cart_id TEXT NOT NULL, title TEXT, quantity INTEGER DEFAULT 1,
		    checked INTEGER DEFAULT 0, notes TEXT, specification TEXT, urgency_level INTEGER DEFAULT 1,
		    global_id TEXT, frequency_days INTEGER, is_active INTEGER DEFAULT 1, paused_at REAL,
		    pause_reason TEXT, deferred_guess_date REAL, deferred_guess_authored_at REAL,
		    purchased_at REAL, category_hint_name TEXT, category_hint_icon TEXT,
		    category_hint_color TEXT, store_hint_brand TEXT,
		    last_wanted_at REAL, quiet_until_first_purchase INTEGER DEFAULT 0,
		    attributed_to TEXT, ts REAL NOT NULL, device_id TEXT,
		    deleted INTEGER DEFAULT 0, deleted_at REAL
		);
		INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
		INSERT INTO items (id, cart_id, title, ts, last_wanted_at, quiet_until_first_purchase)
		VALUES ('item-1','cart-a','Shampoo',1000,1700999000,1);
	`); err != nil {
		t.Fatalf("seed legacy-pk schema: %v", err)
	}
	legacy.Close()

	// The row carries both facts BEFORE the rebuild. InitDB rebuilds the table onto the composite key
	// by copying rows through an explicit column list — if either new column is missing from that list,
	// the copy silently drops the fact and the assertions below redden.
	db, err := InitDB(path)
	if err != nil {
		t.Fatalf("InitDB on legacy-pk db: %v", err)
	}
	defer db.Close()

	pk, err := primaryKeyColumns(db, "items")
	if err != nil {
		t.Fatalf("primaryKeyColumns: %v", err)
	}
	if !pk["cart_id"] {
		t.Fatal("composite-key rebuild did not run")
	}
	cols, err := existingColumns(db, "items")
	if err != nil {
		t.Fatalf("existingColumns: %v", err)
	}
	for _, c := range []string{"last_wanted_at", "quiet_until_first_purchase"} {
		if !cols[c] {
			t.Fatalf("rebuilt table is missing the %s column", c)
		}
	}

	wanted := 1_700_999_000.0
	items, err := GetItems(db, "cart-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("GetItems: err=%v n=%d", err, len(items))
	}
	if items[0].LastWantedAt == nil || *items[0].LastWantedAt != wanted || !items[0].QuietUntilFirstPurchase {
		t.Errorf("rebuilt table lost the facts: lastWantedAt=%v quiet=%v",
			items[0].LastWantedAt, items[0].QuietUntilFirstPurchase)
	}
}

// ---------------------------------------------------------------------------
// Member subscription keys
// ---------------------------------------------------------------------------

// The lookup that authenticates a user-scoped socket. An unknown key must
// resolve to nothing at all — not to a cartID a caller could then subscribe to,
// and not to a userID a caller could then write under. A partial tuple here
// would be an authentication bypass wearing the shape of a lookup miss.
func TestResolveSubKeyUnknownKeyYieldsNothing(t *testing.T) {
	db := testDB(t)
	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}

	// The third is well-formed and simply never minted — the shape a real key has,
	// which is the case a length or charset check would wave through.
	for _, key := range []string{"", "not-a-key", generateSubKey()} {
		cartID, userID, ok, err := ResolveSubKey(db, key)
		if err != nil {
			t.Fatalf("ResolveSubKey(%q): %v", key, err)
		}
		if ok {
			t.Errorf("ResolveSubKey(%q) reported ok for a key never minted", key)
		}
		if cartID != "" || userID != "" {
			t.Errorf("ResolveSubKey(%q) leaked a partial tuple: cart=%q user=%q", key, cartID, userID)
		}
	}
}

// The key a member holds resolves to exactly the pair it was minted for.
func TestResolveSubKeyRoundTrip(t *testing.T) {
	db := testDB(t)
	key, err := AddMember(db, "cart-a", "user-a")
	if err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	if len(key) != 32 {
		t.Errorf("sub_key = %q, want 32 hex chars (128 bits)", key)
	}

	cartID, userID, ok, err := ResolveSubKey(db, key)
	if err != nil || !ok {
		t.Fatalf("ResolveSubKey: ok=%v err=%v", ok, err)
	}
	if cartID != "cart-a" || userID != "user-a" {
		t.Errorf("ResolveSubKey = (%q, %q), want (cart-a, user-a)", cartID, userID)
	}
}

// Two members of the same cart get different keys, and a member of two carts
// gets a different key per cart. The key is per membership, so revoking one
// membership can never invalidate another.
func TestSubKeysAreUniquePerMembership(t *testing.T) {
	db := testDB(t)
	if err := UpsertUser(db, "user-b", "B", "#000"); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}

	aInA, _ := AddMember(db, "cart-a", "user-a")
	bInA, _ := AddMember(db, "cart-a", "user-b")
	aInB, _ := AddMember(db, "cart-b", "user-a")

	for _, pair := range [][2]string{{aInA, bInA}, {aInA, aInB}, {bInA, aInB}} {
		if pair[0] == pair[1] {
			t.Errorf("two memberships share the key %q", pair[0])
		}
	}
}

// A re-join keeps the member's existing key. The key is per (cart, member) and
// travels to that member's other devices, which have no way to learn of a
// re-mint — so re-minting on an idempotent re-join would lock them out.
func TestAddMemberRejoinKeepsKey(t *testing.T) {
	db := testDB(t)
	first, err := AddMember(db, "cart-a", "user-a")
	if err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	second, err := AddMember(db, "cart-a", "user-a")
	if err != nil {
		t.Fatalf("AddMember re-join: %v", err)
	}
	if first != second {
		t.Errorf("re-join re-minted the key: %q → %q", first, second)
	}
}

// Removing a member retires their key in the same statement — there is no
// separate revocation list that could be forgotten. This is the whole
// revocation story for the socket credential.
func TestRemoveMemberRetiresSubKey(t *testing.T) {
	db := testDB(t)
	key, err := AddMember(db, "cart-a", "user-a")
	if err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	if err := RemoveMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}

	cartID, userID, ok, err := ResolveSubKey(db, key)
	if err != nil {
		t.Fatalf("ResolveSubKey: %v", err)
	}
	if ok || cartID != "" || userID != "" {
		t.Errorf("a removed member's key still resolves: ok=%v cart=%q user=%q", ok, cartID, userID)
	}
}

// ---------------------------------------------------------------------------
// Cascade
// ---------------------------------------------------------------------------

// Deleting a room takes its members, items and consumption events with it.
//
// The schema has said so since the beginning and nothing ever checked it, which
// was fine while the cascade was only housekeeping. It stopped being only that
// when create began accepting a client-PROPOSED cart id: what makes "create
// touches no existing data" true for a proposed id is that a dead room leaves
// nothing behind to adopt. The guarantee runs through `PRAGMA foreign_keys=ON`,
// a PER-CONNECTION setting that holds today only because InitDB pins the pool to
// one connection — see the ⚠️ at SetMaxOpenConns. This test is what fails if that
// changes, instead of the failure being a stranger reading somebody's cart.
func TestDeleteRoomCascadesToEveryDependentTable(t *testing.T) {
	db := testDB(t)

	if _, err := AddMember(db, "cart-a", "user-a"); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	if ok, err := ApplyLWW(db, "cart-a", "item-1", "add",
		map[string]any{"title": "Milk"}, 1000, "dev-1", "user-a"); err != nil || !ok {
		t.Fatalf("add item: ok=%v err=%v", ok, err)
	}
	if err := BackfillConsumptionEvents(db, "cart-a", "item-1", []float64{1000}); err != nil {
		t.Fatalf("BackfillConsumptionEvents: %v", err)
	}
	// A positive before the negative: the rows have to be there for their absence
	// afterwards to mean anything.
	if residue, err := CartIDResidue(db, "cart-a"); err != nil || !residue {
		t.Fatalf("fixture: CartIDResidue = %v, %v; want rows present before the delete", residue, err)
	}

	if err := DeleteRoom(db, "cart-a"); err != nil {
		t.Fatalf("DeleteRoom: %v", err)
	}

	for _, tc := range []struct{ table, query string }{
		{"members", `SELECT COUNT(*) FROM members WHERE cart_id = 'cart-a'`},
		{"items", `SELECT COUNT(*) FROM items WHERE cart_id = 'cart-a'`},
		{"consumption_events", `SELECT COUNT(*) FROM consumption_events WHERE cart_id = 'cart-a'`},
	} {
		var n int
		if err := db.QueryRow(tc.query).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tc.table, err)
		}
		if n != 0 {
			t.Errorf("%s: %d rows survived the room — the cascade is not firing", tc.table, n)
		}
	}
	// The neighbouring room is untouched: a cascade that took everything would
	// pass the assertions above for the wrong reason.
	var others int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rooms WHERE cart_id = 'cart-b'`).Scan(&others); err != nil {
		t.Fatalf("count rooms: %v", err)
	}
	if others != 1 {
		t.Errorf("cart-b rooms = %d, want 1", others)
	}
}
