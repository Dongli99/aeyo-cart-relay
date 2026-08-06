package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

// itemsTableBody is the single source for the items table definition — used by
// the fresh-DB schema below AND the composite-key rebuild migration. Item
// identity is (cart_id, id): the same item uuid legitimately exists in more
// than one room (an item moved between carts, or a cart torn down and
// re-shared, keeps its uuid). A global PK on id alone misfiled cross-room
// writes into whichever room first owned the uuid (2026-07-10 incident: a
// re-shared cart's room stayed empty forever while every add LWW-updated the
// old room's rows and acked "applied").
const itemsTableBody = `
    id            TEXT NOT NULL,
    cart_id       TEXT NOT NULL,
    title         TEXT,
    quantity      INTEGER DEFAULT 1,
    checked       INTEGER DEFAULT 0,
    notes         TEXT,
    specification TEXT,
    urgency_level INTEGER DEFAULT 1,
    global_id     TEXT,
    frequency_days INTEGER,
    is_active     INTEGER DEFAULT 1,
    paused_at     REAL,
    pause_reason  TEXT,
    deferred_guess_date REAL,
    deferred_guess_authored_at REAL,
    purchased_at  REAL,
    category_hint_name  TEXT,
    category_hint_icon  TEXT,
    category_hint_color TEXT,
    store_hint_brand    TEXT,
    last_wanted_at REAL,
    quiet_until_first_purchase INTEGER DEFAULT 0,
    attributed_to TEXT,
    ts            REAL NOT NULL,
    device_id     TEXT,
    deleted       INTEGER DEFAULT 0,
    deleted_at    REAL,
    PRIMARY KEY (cart_id, id),
    FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
`

var schema = `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    user_id      TEXT PRIMARY KEY,
    display_name TEXT,
    color        TEXT,
    tier         TEXT NOT NULL DEFAULT 'free'
);

CREATE TABLE IF NOT EXISTS rooms (
    cart_id    TEXT PRIMARY KEY,
    owner_id   TEXT NOT NULL,
    secret     TEXT NOT NULL,
    created_at REAL NOT NULL,
    cart_name  TEXT NOT NULL DEFAULT '',
    hex_color  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS members (
    cart_id   TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    joined_at REAL NOT NULL,
    PRIMARY KEY (cart_id, user_id),
    FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS items (` + itemsTableBody + `);

-- Consumption reservoir: per-(cart_id, spine) capped raw event log of
-- consumption events, used to warm-start cadence at item creation/join (§3.4).
-- The global_id column holds the reservoir SPINE = COALESCE(concept global_id,
-- cart-scoped item id) (ADR-052 — concept-less items key on their item id); the
-- column keeps its name (one table, no migration). NOT live-synced — read once
-- via GET/POST history endpoints (§5).
CREATE TABLE IF NOT EXISTS consumption_events (
    cart_id   TEXT NOT NULL,
    global_id TEXT NOT NULL,
    ts        REAL NOT NULL,
    PRIMARY KEY (cart_id, global_id, ts),
    FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_members_cart_id ON members(cart_id);
CREATE INDEX IF NOT EXISTS idx_items_cart_id   ON items(cart_id);
CREATE INDEX IF NOT EXISTS idx_items_tombstone ON items(deleted, deleted_at);
CREATE INDEX IF NOT EXISTS idx_consumption_concept ON consumption_events(cart_id, global_id);
`

// itemFactColumns lists the v3.6 fact columns (TDD §3.1) added to a pre-existing
// items table. CREATE TABLE IF NOT EXISTS above only takes effect on a fresh DB;
// a deployed DB needs these ALTER TABLE migrations to gain the same columns.
var itemFactColumns = []struct{ name, ddl string }{
	{"global_id", "ALTER TABLE items ADD COLUMN global_id TEXT"},
	{"frequency_days", "ALTER TABLE items ADD COLUMN frequency_days INTEGER"},
	{"is_active", "ALTER TABLE items ADD COLUMN is_active INTEGER DEFAULT 1"},
	{"paused_at", "ALTER TABLE items ADD COLUMN paused_at REAL"},
	{"pause_reason", "ALTER TABLE items ADD COLUMN pause_reason TEXT"},
	{"deferred_guess_date", "ALTER TABLE items ADD COLUMN deferred_guess_date REAL"},
	{"deferred_guess_authored_at", "ALTER TABLE items ADD COLUMN deferred_guess_authored_at REAL"}, // ADR-053 anchor authorship time
	{"purchased_at", "ALTER TABLE items ADD COLUMN purchased_at REAL"}, // ADR-050 purchase fact time
	// ADR-051 label hints — the sender's category/store labels, a receiver-side hydration backstop.
	{"category_hint_name", "ALTER TABLE items ADD COLUMN category_hint_name TEXT"},
	{"category_hint_icon", "ALTER TABLE items ADD COLUMN category_hint_icon TEXT"},
	{"category_hint_color", "ALTER TABLE items ADD COLUMN category_hint_color TEXT"},
	{"store_hint_brand", "ALTER TABLE items ADD COLUMN store_hint_brand TEXT"},
	// The instant a member last said they still want this item (resuming it, or marking it needed
	// now). Stored so the fact survives a peer being offline: the app's staleness clock reads it, and
	// without it here a reconnecting device would see only the older row timestamp and could pause an
	// item somebody had just re-confirmed.
	{"last_wanted_at", "ALTER TABLE items ADD COLUMN last_wanted_at REAL"},
	// A flag meaning "make no prediction about this item until the NEXT purchase of it". The wire key
	// says "first" for compatibility with clients already sending it; the meaning is the next
	// purchase, not only the first one ever. Do not re-key it to match the meaning — the name is the
	// contract with deployed apps. Stored so a member who was offline when the flag was set still
	// learns the item is quiet instead of predicting for it.
	{"quiet_until_first_purchase", "ALTER TABLE items ADD COLUMN quiet_until_first_purchase INTEGER DEFAULT 0"},
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

// InitDB opens the SQLite database, applies the schema, and prunes old tombstones.
func InitDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("InitDB open: %w", err)
	}
	// SQLite performs best with a single writer connection.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("InitDB schema: %w", err)
	}
	if err := migrateItemFactColumns(db); err != nil {
		return nil, fmt.Errorf("InitDB migrate: %w", err)
	}
	if err := migrateItemsCompositeKey(db); err != nil {
		return nil, fmt.Errorf("InitDB composite-key migrate: %w", err)
	}

	pruneDeletedItems(db)
	return db, nil
}

// migrateItemFactColumns adds any of itemFactColumns missing from an existing
// items table. Idempotent: a fresh DB (columns already created above) and a
// previously-migrated DB both no-op.
func migrateItemFactColumns(db *sql.DB) error {
	existing, err := existingColumns(db, "items")
	if err != nil {
		return err
	}
	for _, m := range itemFactColumns {
		if existing[m.name] {
			continue
		}
		if _, err := db.Exec(m.ddl); err != nil {
			return fmt.Errorf("add column %s: %w", m.name, err)
		}
	}
	return nil
}

// migrateItemsCompositeKey rebuilds a pre-existing items table whose primary
// key is the legacy global `id` onto the composite (cart_id, id) key (see
// itemsTableBody). SQLite cannot alter a primary key in place, so the table is
// recreated and rows copied by explicit column list (order-independent — a
// deployed table's ALTER-appended fact columns sit in a different order than a
// fresh one's). Runs AFTER migrateItemFactColumns so every copied column
// exists. Idempotent: detected via table_info — on the composite key, cart_id
// is part of the PK; on the legacy key it is not. A failure aborts startup —
// better down than misfiling writes.
func migrateItemsCompositeKey(db *sql.DB) error {
	pkCols, err := primaryKeyColumns(db, "items")
	if err != nil {
		return err
	}
	if pkCols["cart_id"] {
		return nil // already composite
	}

	const cols = `id, cart_id, title, quantity, checked, notes, specification,
		urgency_level, global_id, frequency_days, is_active, paused_at,
		pause_reason, deferred_guess_date, deferred_guess_authored_at, purchased_at,
		category_hint_name, category_hint_icon, category_hint_color, store_hint_brand,
		last_wanted_at, quiet_until_first_purchase,
		attributed_to, ts, device_id, deleted, deleted_at`

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	steps := []string{
		"CREATE TABLE items_new (" + itemsTableBody + ")",
		"INSERT INTO items_new (" + cols + ") SELECT " + cols + " FROM items",
		"DROP TABLE items",
		"ALTER TABLE items_new RENAME TO items",
		"CREATE INDEX idx_items_cart_id   ON items(cart_id)",
		"CREATE INDEX idx_items_tombstone ON items(deleted, deleted_at)",
	}
	for _, s := range steps {
		if _, err := tx.Exec(s); err != nil {
			return fmt.Errorf("composite-key step %q: %w", s[:min(40, len(s))], err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("migrateItemsCompositeKey: items rebuilt with PRIMARY KEY (cart_id, id)")
	return nil
}

// primaryKeyColumns returns the set of column names in table's primary key.
func primaryKeyColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pk := make(map[string]bool)
	for rows.Next() {
		var cid, notnull, pkOrd int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pkOrd); err != nil {
			return nil, err
		}
		if pkOrd > 0 {
			pk[name] = true
		}
	}
	return pk, rows.Err()
}

// existingColumns returns the set of column names currently on table.
func existingColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

// pruneDeletedItems removes tombstones older than 30 days.
func pruneDeletedItems(db *sql.DB) {
	cutoff := float64(time.Now().Add(-30 * 24 * time.Hour).Unix())
	res, err := db.Exec(`DELETE FROM items WHERE deleted=1 AND deleted_at < ?`, cutoff)
	if err != nil {
		log.Printf("pruneDeletedItems: %v", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("pruneDeletedItems: removed %d tombstones", n)
	}
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

// UpsertUser inserts or updates a user record.
func UpsertUser(db *sql.DB, userID, displayName, color string) error {
	_, err := db.Exec(`
		INSERT INTO users (user_id, display_name, color)
		VALUES (?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
		    display_name = excluded.display_name,
		    color        = excluded.color
	`, userID, displayName, color)
	return err
}

// ---------------------------------------------------------------------------
// Rooms
// ---------------------------------------------------------------------------

// CreateRoom inserts a new room record.
func CreateRoom(db *sql.DB, cartID, ownerID, secret, cartName, hexColor string) error {
	_, err := db.Exec(`
		INSERT INTO rooms (cart_id, owner_id, secret, created_at, cart_name, hex_color)
		VALUES (?, ?, ?, ?, ?, ?)
	`, cartID, ownerID, secret, float64(time.Now().Unix()), cartName, hexColor)
	return err
}

// GetSecret returns the current invite secret for a cart.
func GetSecret(db *sql.DB, cartID string) (string, error) {
	var secret string
	err := db.QueryRow(`SELECT secret FROM rooms WHERE cart_id = ?`, cartID).Scan(&secret)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("cart not found")
	}
	return secret, err
}

// GetOwner returns the current owner user_id for a cart.
func GetOwner(db *sql.DB, cartID string) (string, error) {
	var ownerID string
	err := db.QueryRow(`SELECT owner_id FROM rooms WHERE cart_id = ?`, cartID).Scan(&ownerID)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("cart not found")
	}
	return ownerID, err
}

// TransferOwner updates the owner of a cart.
func TransferOwner(db *sql.DB, cartID, newOwnerID string) error {
	_, err := db.Exec(`UPDATE rooms SET owner_id = ? WHERE cart_id = ?`, newOwnerID, cartID)
	return err
}

// RotateSecret generates a new secret for a cart and persists it.
func RotateSecret(db *sql.DB, cartID string) (string, error) {
	newSecret := generateSecret()
	_, err := db.Exec(`UPDATE rooms SET secret = ? WHERE cart_id = ?`, newSecret, cartID)
	if err != nil {
		return "", err
	}
	return newSecret, nil
}

// DeleteRoom removes the room and all cascaded members and items.
func DeleteRoom(db *sql.DB, cartID string) error {
	_, err := db.Exec(`DELETE FROM rooms WHERE cart_id = ?`, cartID)
	return err
}

// ---------------------------------------------------------------------------
// Members
// ---------------------------------------------------------------------------

// IsMember reports whether userID is a member of cartID.
func IsMember(db *sql.DB, cartID, userID string) (bool, error) {
	var n int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM members WHERE cart_id = ? AND user_id = ?
	`, cartID, userID).Scan(&n)
	return n > 0, err
}

// GetMemberCount returns the number of members in a cart.
func GetMemberCount(db *sql.DB, cartID string) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM members WHERE cart_id = ?`, cartID).Scan(&n)
	return n, err
}

// HasPremiumOwner reports whether the cart's current owner has tier = 'premium'.
func HasPremiumOwner(db *sql.DB, cartID string) (bool, error) {
	var n int
	err := db.QueryRow(`
		SELECT EXISTS (
		    SELECT 1 FROM rooms r
		    JOIN users u ON r.owner_id = u.user_id
		    WHERE r.cart_id = ? AND u.tier = 'premium'
		)
	`, cartID).Scan(&n)
	return n > 0, err
}

// AddMember upserts a member record (idempotent — re-join is safe).
func AddMember(db *sql.DB, cartID, userID string) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO members (cart_id, user_id, joined_at)
		VALUES (?, ?, ?)
	`, cartID, userID, float64(time.Now().Unix()))
	return err
}

// RemoveMember deletes a member record.
func RemoveMember(db *sql.DB, cartID, userID string) error {
	_, err := db.Exec(`DELETE FROM members WHERE cart_id = ? AND user_id = ?`, cartID, userID)
	return err
}

// GetMembers returns all members of a cart with their user metadata.
func GetMembers(db *sql.DB, cartID string) ([]MemberInfo, error) {
	rows, err := db.Query(`
		SELECT u.user_id, COALESCE(u.display_name,''), COALESCE(u.color,''), u.tier
		FROM members m
		JOIN users u ON m.user_id = u.user_id
		WHERE m.cart_id = ?
		ORDER BY m.joined_at ASC
	`, cartID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []MemberInfo
	for rows.Next() {
		var mi MemberInfo
		if err := rows.Scan(&mi.UserID, &mi.DisplayName, &mi.Color, &mi.Tier); err != nil {
			return nil, err
		}
		members = append(members, mi)
	}
	return members, rows.Err()
}

// ---------------------------------------------------------------------------
// Items
// ---------------------------------------------------------------------------

// GetItems returns all non-deleted items for a cart.
func GetItems(db *sql.DB, cartID string) ([]ItemRow, error) {
	rows, err := db.Query(`
		SELECT id, COALESCE(title,''), quantity, checked, attributed_to, ts,
		       notes, specification, COALESCE(urgency_level,1),
		       global_id, frequency_days, COALESCE(is_active,1),
		       paused_at, pause_reason, deferred_guess_date, deferred_guess_authored_at, purchased_at,
		       category_hint_name, category_hint_icon, category_hint_color, store_hint_brand,
		       last_wanted_at, COALESCE(quiet_until_first_purchase,0)
		FROM items
		WHERE cart_id = ? AND deleted = 0
		ORDER BY ts ASC
	`, cartID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []ItemRow
	for rows.Next() {
		var item ItemRow
		var attributedTo sql.NullString
		var notes sql.NullString
		var specification sql.NullString
		var globalID sql.NullString
		var frequencyDays sql.NullInt64
		var pausedAt sql.NullFloat64
		var pauseReason sql.NullString
		var deferredGuessDate sql.NullFloat64
		var deferredGuessAuthoredAt sql.NullFloat64
		var purchasedAt sql.NullFloat64
		var categoryHintName sql.NullString
		var categoryHintIcon sql.NullString
		var categoryHintColor sql.NullString
		var storeHintBrand sql.NullString
		var lastWantedAt sql.NullFloat64
		if err := rows.Scan(
			&item.ID, &item.Title, &item.Quantity, &item.Checked,
			&attributedTo, &item.Ts,
			&notes, &specification, &item.UrgencyLevel,
			&globalID, &frequencyDays, &item.IsActive,
			&pausedAt, &pauseReason, &deferredGuessDate, &deferredGuessAuthoredAt, &purchasedAt,
			&categoryHintName, &categoryHintIcon, &categoryHintColor, &storeHintBrand,
			&lastWantedAt, &item.QuietUntilFirstPurchase,
		); err != nil {
			return nil, err
		}
		if attributedTo.Valid {
			item.AttributedTo = &attributedTo.String
		}
		if notes.Valid {
			item.Notes = &notes.String
		}
		if specification.Valid {
			item.Specification = &specification.String
		}
		if globalID.Valid {
			item.GlobalID = &globalID.String
		}
		if frequencyDays.Valid {
			fd := int(frequencyDays.Int64)
			item.FrequencyDays = &fd
		}
		if pausedAt.Valid {
			item.PausedAt = &pausedAt.Float64
		}
		if pauseReason.Valid {
			item.PauseReason = &pauseReason.String
		}
		if deferredGuessDate.Valid {
			item.DeferredGuessDate = &deferredGuessDate.Float64
		}
		if deferredGuessAuthoredAt.Valid {
			item.DeferredGuessAuthoredAt = &deferredGuessAuthoredAt.Float64
		}
		if purchasedAt.Valid {
			item.PurchasedAt = &purchasedAt.Float64
		}
		if categoryHintName.Valid {
			item.CategoryHintName = &categoryHintName.String
		}
		if categoryHintIcon.Valid {
			item.CategoryHintIcon = &categoryHintIcon.String
		}
		if categoryHintColor.Valid {
			item.CategoryHintColor = &categoryHintColor.String
		}
		if storeHintBrand.Valid {
			item.StoreHintBrand = &storeHintBrand.String
		}
		if lastWantedAt.Valid {
			item.LastWantedAt = &lastWantedAt.Float64
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// GetTombstones returns all soft-deleted rows for a cart (ADR-038 invariant A).
func GetTombstones(db *sql.DB, cartID string) ([]TombstoneRow, error) {
	rows, err := db.Query(`
		SELECT id, COALESCE(deleted_at, ts) FROM items WHERE cart_id = ? AND deleted = 1
	`, cartID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tombstones []TombstoneRow
	for rows.Next() {
		var t TombstoneRow
		if err := rows.Scan(&t.ID, &t.DeletedAt); err != nil {
			return nil, err
		}
		tombstones = append(tombstones, t)
	}
	return tombstones, rows.Err()
}

// ---------------------------------------------------------------------------
// Consumption Reservoir (§3.4)
// ---------------------------------------------------------------------------

const (
	// reservoirCap is a dumb storage/abuse ceiling on rows retained per
	// (cart_id, spine) — NOT the semantic bound. The client caps the send side by
	// purchase-DAY to the learning window (ADR-052 §2/§3); this row cap must stay
	// ≥ that client cap and is never synced cross-repo — a Swift and a Go repo
	// sharing a live constant is the forbidden one-directional sync-rule drift.
	// With day-deduped sends, 20 rows now means ~20 days, comfortably above the
	// window.
	reservoirCap = 20

	// consumptionRetractWindow bounds how long after a check-off an uncheck may
	// retract it. Chosen as a generous "I just tapped wrong" correction window
	// across multi-device LWW propagation — long enough to cover realistic
	// accidental-tap + quick-correct latency, short enough that a deliberate
	// later re-purchase cycle is never mistaken for an undo.
	consumptionRetractWindow = 30.0 // seconds
)

// captureConsumption appends or retracts a consumption_events row for an
// accepted checked op. Keyed on the reservoir SPINE = COALESCE(global_id, id):
// the concept global_id when the item has one, else the cart-scoped item id
// (ADR-052 — concept-less items keep accreting for late joiners, matching the
// client's COALESCE(concept, item.uuid) spine). The row is looked up from the
// DB, not the op's fields, because a checked:true update typically carries only
// {"checked": true} with no globalID present.
func captureConsumption(db *sql.DB, cartID, itemID string, checked bool) {
	var globalID sql.NullString
	var ts float64
	var purchasedAt sql.NullFloat64
	err := db.QueryRow(`
		SELECT global_id, ts, purchased_at FROM items WHERE id = ? AND cart_id = ?
	`, itemID, cartID).Scan(&globalID, &ts, &purchasedAt)
	if err != nil {
		return
	}
	spine := itemID
	if globalID.Valid && globalID.String != "" {
		spine = globalID.String
	}

	if checked {
		// ADR-050: stamp the reservoir with the purchase FACT time, falling back to the LWW ts for an
		// old client that didn't carry it. A re-assert of the same completion carries the same fact →
		// same (cart_id, global_id, ts) PK → INSERT OR IGNORE collapses it (idempotent by construction,
		// no cross-day phantom).
		eventTs := ts
		if purchasedAt.Valid {
			eventTs = purchasedAt.Float64
		}
		if _, err := db.Exec(`
			INSERT OR IGNORE INTO consumption_events (cart_id, global_id, ts) VALUES (?, ?, ?)
		`, cartID, spine, eventTs); err != nil {
			log.Printf("captureConsumption: insert: %v", err)
			return
		}
		pruneConsumptionEvents(db, cartID, spine)
		return
	}

	// Uncheck: retract the most recent event only if it falls within the undo
	// window of this uncheck's ts — an uncheck of a long-ago-checked item must
	// never delete an older, legitimate consumption.
	var maxTs sql.NullFloat64
	if err := db.QueryRow(`
		SELECT MAX(ts) FROM consumption_events WHERE cart_id = ? AND global_id = ?
	`, cartID, spine).Scan(&maxTs); err != nil || !maxTs.Valid {
		return
	}
	if diff := ts - maxTs.Float64; diff >= -consumptionRetractWindow && diff <= consumptionRetractWindow {
		if _, err := db.Exec(`
			DELETE FROM consumption_events WHERE cart_id = ? AND global_id = ? AND ts = ?
		`, cartID, spine, maxTs.Float64); err != nil {
			log.Printf("captureConsumption: retract: %v", err)
		}
	}
}

// pruneConsumptionEvents caps a (cart_id, global_id) reservoir to the most
// recent reservoirCap rows.
func pruneConsumptionEvents(db *sql.DB, cartID, globalID string) {
	_, err := db.Exec(`
		DELETE FROM consumption_events
		WHERE cart_id = ? AND global_id = ? AND ts NOT IN (
			SELECT ts FROM consumption_events
			WHERE cart_id = ? AND global_id = ?
			ORDER BY ts DESC LIMIT ?
		)
	`, cartID, globalID, cartID, globalID, reservoirCap)
	if err != nil {
		log.Printf("pruneConsumptionEvents: %v", err)
	}
}

// GetConsumptionEvents returns all event timestamps for (cartID, globalID),
// oldest first.
func GetConsumptionEvents(db *sql.DB, cartID, globalID string) ([]float64, error) {
	rows, err := db.Query(`
		SELECT ts FROM consumption_events WHERE cart_id = ? AND global_id = ? ORDER BY ts ASC
	`, cartID, globalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []float64
	for rows.Next() {
		var ts float64
		if err := rows.Scan(&ts); err != nil {
			return nil, err
		}
		events = append(events, ts)
	}
	return events, rows.Err()
}

// BackfillConsumptionEvents merges device-uploaded pre-share history into the
// reservoir and caps it to reservoirCap (§3.4 pre-share backfill).
func BackfillConsumptionEvents(db *sql.DB, cartID, globalID string, events []float64) error {
	for _, ts := range events {
		if _, err := db.Exec(`
			INSERT OR IGNORE INTO consumption_events (cart_id, global_id, ts) VALUES (?, ?, ?)
		`, cartID, globalID, ts); err != nil {
			return err
		}
	}
	pruneConsumptionEvents(db, cartID, globalID)
	return nil
}

// ---------------------------------------------------------------------------
// LWW
// ---------------------------------------------------------------------------

// ApplyLWW applies a single operation using last-write-wins conflict resolution.
// Returns accepted=true if the write was applied to the DB.
// Algorithm matches TDD §4.5 exactly.
func ApplyLWW(
	db *sql.DB,
	cartID, itemID, op string,
	fields map[string]any,
	ts float64,
	deviceID, userID string,
) (bool, error) {
	// Clamp ts: prevent far-future clients from permanently winning all conflicts.
	effectiveTs := ts
	if maxTs := float64(time.Now().Unix()) + 60; ts > maxTs {
		effectiveTs = maxTs
	}

	if op == "delete" {
		// Soft-delete: mark deleted + deleted_at. If row doesn't exist, insert a
		// tombstone so stale add ops can't resurrect the item.
		res, err := db.Exec(`
			UPDATE items SET deleted=1, deleted_at=? WHERE id=? AND cart_id=?
		`, effectiveTs, itemID, cartID)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// No existing row — insert tombstone.
			_, err = db.Exec(`
				INSERT INTO items (id, cart_id, ts, device_id, deleted, deleted_at)
				VALUES (?, ?, ?, ?, 1, ?)
			`, itemID, cartID, effectiveTs, deviceID, effectiveTs)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	// op == "add" or "update"
	type existingRow struct {
		ts        float64
		deviceID  string
		deleted   int
		deletedAt sql.NullFloat64
	}

	// Item identity is (cart_id, id) — an unscoped id lookup here once matched
	// the same uuid living in ANOTHER room and LWW-updated that room's row
	// instead of inserting into this one (the 2026-07-10 re-share incident).
	var existing existingRow
	err := db.QueryRow(`
		SELECT ts, COALESCE(device_id,''), deleted, deleted_at
		FROM items WHERE id=? AND cart_id=?
	`, itemID, cartID).Scan(&existing.ts, &existing.deviceID, &existing.deleted, &existing.deletedAt)

	if err == sql.ErrNoRows {
		// No existing row → insert.
		return insertItem(db, cartID, itemID, op, fields, effectiveTs, deviceID, userID)
	}
	if err != nil {
		return false, err
	}

	// Tombstone exists — check if deletion wins.
	if existing.deleted == 1 && existing.deletedAt.Valid && existing.deletedAt.Float64 > effectiveTs {
		return false, nil // deletion wins; discard this write
	}

	// An update must never resurrect a tombstoned row (ADR-039): partial-field
	// resurrection can construct a ghost — e.g. an empty-titled item from a lone
	// {"checked": false}. Only an add — a deliberate re-add carrying full fields
	// — may bring a tombstoned row back (handled by updateItem's op=="add" clear
	// below, since the row already exists here). Skip and log.
	if existing.deleted == 1 && op == "update" {
		log.Printf("ApplyLWW: update skipped on tombstoned item %s/%s — no resurrect", cartID, itemID)
		return false, nil
	}

	// Live row or stale tombstone being re-added — check LWW.
	if effectiveTs > existing.ts {
		return updateItem(db, cartID, itemID, op, fields, effectiveTs, deviceID, userID)
	}
	if effectiveTs == existing.ts && deviceID < existing.deviceID {
		return updateItem(db, cartID, itemID, op, fields, effectiveTs, deviceID, userID)
	}
	return false, nil // discarded
}

func insertItem(
	db *sql.DB,
	cartID, itemID, op string,
	fields map[string]any,
	ts float64,
	deviceID, userID string,
) (bool, error) {
	title, _ := fields["title"].(string)
	quantity := 1
	if q, ok := fields["quantity"].(float64); ok {
		quantity = int(q)
	}
	checked := false
	if c, ok := fields["checked"].(bool); ok {
		checked = c
	}
	var notes any
	if v, ok := fields["notes"]; ok && v != nil {
		notes = v
	}
	var specification any
	if v, ok := fields["specification"]; ok && v != nil {
		specification = v
	}
	urgencyLevel := 1
	if v, ok := fields["urgencyLevel"].(float64); ok {
		urgencyLevel = int(v)
	}

	var globalID any
	if v, ok := fields["globalID"]; ok {
		globalID = v
	}
	var frequencyDays any
	if v, ok := fields["frequencyDays"]; ok {
		if arg, ok := coerceNumericOrNull("frequencyDays", v); ok {
			frequencyDays = arg
		}
	}
	isActive := true
	if v, ok := fields["isActive"].(bool); ok {
		isActive = v
	}
	var pausedAt any
	if v, ok := fields["pausedAt"]; ok {
		pausedAt = v
	}
	var pauseReason any
	if v, ok := fields["pauseReason"]; ok {
		pauseReason = v
	}
	var deferredGuessDate any
	if v, ok := fields["deferredGuessDate"]; ok {
		deferredGuessDate = v
	}
	// ADR-053: the anchor's authorship instant rides beside the anchor on the birth snapshot; persisted
	// so state rows carry the pair and every device derives consumption from facts.
	var deferredGuessAuthoredAt any
	if v, ok := fields["deferredGuessAuthoredAt"]; ok {
		deferredGuessAuthoredAt = v
	}
	// ADR-050: the purchase fact time rides a checked-carrying op; persisted so state rows carry it
	// and captureConsumption can stamp the reservoir with the fact, not the LWW ts.
	var purchasedAt any
	if v, ok := fields["purchasedAt"]; ok {
		purchasedAt = v
	}
	// ADR-051: the sender's label hints ride the add-op birth snapshot; persisted so state rows carry
	// them for joiners. Present-keys-only, like every other field here — an absent key stays NULL.
	var categoryHintName any
	if v, ok := fields["categoryHintName"]; ok {
		categoryHintName = v
	}
	var categoryHintIcon any
	if v, ok := fields["categoryHintIcon"]; ok {
		categoryHintIcon = v
	}
	var categoryHintColor any
	if v, ok := fields["categoryHintColor"]; ok {
		categoryHintColor = v
	}
	var storeHintBrand any
	if v, ok := fields["storeHintBrand"]; ok {
		storeHintBrand = v
	}
	// The instant a member last re-confirmed wanting this item. Persisted so it rides the reconnect
	// snapshot, not only a live message — a device that was offline still learns the item was wanted.
	var lastWantedAt any
	if v, ok := fields["lastWantedAt"]; ok {
		lastWantedAt = v
	}
	// "Predict nothing about this item until its next purchase." Present-keys-only like the rest; an
	// absent key leaves the flag off. (The wire key says "first"; the meaning is the next purchase.)
	quietUntilFirstPurchase := false
	if v, ok := fields["quietUntilFirstPurchase"].(bool); ok {
		quietUntilFirstPurchase = v
	}

	attributedTo := userID
	// Uncheck clears attribution (§3.3 attribution rule).
	if op == "update" {
		if c, ok := fields["checked"].(bool); ok && !c && len(fields) == 1 {
			attributedTo = ""
		}
	}

	var attrParam any
	if attributedTo == "" {
		attrParam = nil
	} else {
		attrParam = attributedTo
	}

	_, err := db.Exec(`
		INSERT INTO items
		    (id, cart_id, title, quantity, checked, notes, specification, urgency_level,
		     global_id, frequency_days, is_active, paused_at, pause_reason, deferred_guess_date,
		     deferred_guess_authored_at, purchased_at, category_hint_name, category_hint_icon,
		     category_hint_color, store_hint_brand, last_wanted_at, quiet_until_first_purchase,
		     attributed_to, ts, device_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, itemID, cartID, title, quantity, boolToInt(checked),
		notes, specification, urgencyLevel,
		globalID, frequencyDays, boolToInt(isActive), pausedAt, pauseReason, deferredGuessDate,
		deferredGuessAuthoredAt, purchasedAt, categoryHintName, categoryHintIcon,
		categoryHintColor, storeHintBrand, lastWantedAt, boolToInt(quietUntilFirstPurchase),
		attrParam, ts, deviceID)
	return err == nil, err
}

// coerceInt asserts a JSON-decoded numeric field (float64) to an int SQL arg.
// A wrong-typed value from a misbehaving/future client is reported ok=false —
// the caller skips the field rather than panicking the handler goroutine.
func coerceInt(field string, v any) (int, bool) {
	f, ok := v.(float64)
	if !ok {
		log.Printf("decode: field %q expected number, got %T — skipping", field, v)
		return 0, false
	}
	return int(f), true
}

// coerceBool asserts a JSON-decoded boolean field. ok=false (with a log) for a
// wrong-typed value, so the caller skips the field.
func coerceBool(field string, v any) (bool, bool) {
	b, ok := v.(bool)
	if !ok {
		log.Printf("decode: field %q expected bool, got %T — skipping", field, v)
	}
	return b, ok
}

// coerceNumericOrNull handles a field that is legitimately either a JSON number
// or an explicit null (frequencyDays: null = occasional). An explicit null maps
// to a nil SQL arg (applied); a wrong-typed value is reported ok=false (with a
// log) so the caller skips the field rather than overwriting or panicking.
func coerceNumericOrNull(field string, v any) (any, bool) {
	if v == nil {
		return nil, true
	}
	f, ok := v.(float64)
	if !ok {
		log.Printf("decode: field %q expected number or null, got %T — skipping", field, v)
		return nil, false
	}
	return int(f), true
}

func updateItem(
	db *sql.DB,
	cartID, itemID, op string,
	fields map[string]any,
	ts float64,
	deviceID, userID string,
) (bool, error) {
	// Determine attributed_to:
	// - update with only {"checked": false} → NULL
	// - all other ops/fields → userID
	attributedTo := userID
	if op == "update" {
		if c, ok := fields["checked"].(bool); ok && !c && len(fields) == 1 {
			attributedTo = ""
		}
	}

	var attrParam any
	if attributedTo == "" {
		attrParam = nil
	} else {
		attrParam = attributedTo
	}

	// Build SET clause from fields that are present.
	// We always update ts, device_id, and attributed_to.
	setClauses := "ts=?, device_id=?, attributed_to=?"
	args := []any{ts, deviceID, attrParam}

	// Only a deliberate re-add (op=="add") may clear a tombstone; an update must
	// never resurrect (ADR-039). ApplyLWW already blocks update-over-tombstone,
	// so this both effects the intended add-resurrect and is defense in depth.
	if op == "add" {
		setClauses += ", deleted=0, deleted_at=NULL"
	}

	if v, ok := fields["title"]; ok {
		setClauses += ", title=?"
		args = append(args, v)
	}
	if v, ok := fields["quantity"]; ok {
		if q, ok := coerceInt("quantity", v); ok {
			setClauses += ", quantity=?"
			args = append(args, q)
		}
	}
	if v, ok := fields["checked"]; ok {
		if b, ok := coerceBool("checked", v); ok {
			setClauses += ", checked=?"
			args = append(args, boolToInt(b))
		}
	}
	if v, ok := fields["notes"]; ok {
		setClauses += ", notes=?"
		args = append(args, v)
	}
	if v, ok := fields["specification"]; ok {
		setClauses += ", specification=?"
		args = append(args, v)
	}
	if v, ok := fields["urgencyLevel"]; ok {
		if u, ok := coerceInt("urgencyLevel", v); ok {
			setClauses += ", urgency_level=?"
			args = append(args, u)
		}
	}
	if v, ok := fields["globalID"]; ok {
		setClauses += ", global_id=?"
		args = append(args, v)
	}
	if v, ok := fields["frequencyDays"]; ok {
		if arg, ok := coerceNumericOrNull("frequencyDays", v); ok {
			setClauses += ", frequency_days=?"
			args = append(args, arg)
		}
	}
	if v, ok := fields["isActive"]; ok {
		if b, ok := coerceBool("isActive", v); ok {
			setClauses += ", is_active=?"
			args = append(args, boolToInt(b))
		}
	}
	if v, ok := fields["pausedAt"]; ok {
		setClauses += ", paused_at=?"
		args = append(args, v)
	}
	if v, ok := fields["pauseReason"]; ok {
		setClauses += ", pause_reason=?"
		args = append(args, v)
	}
	if v, ok := fields["deferredGuessDate"]; ok {
		setClauses += ", deferred_guess_date=?"
		args = append(args, v)
	}
	// ADR-053: the anchor's authorship instant rides beside the anchor on a field-targeted anchor update
	// (present-keys-only, pairs the anchor's NSNull on a clear) so the persisted pair stays consistent.
	if v, ok := fields["deferredGuessAuthoredAt"]; ok {
		setClauses += ", deferred_guess_authored_at=?"
		args = append(args, v)
	}
	// ADR-050: persist the purchase fact when the op carries it (a checked-carrying op). Never cleared
	// by an unchecked op — the field is simply absent there, leaving the last recorded fact intact.
	if v, ok := fields["purchasedAt"]; ok {
		setClauses += ", purchased_at=?"
		args = append(args, v)
	}
	// ADR-051: a member's manual relabel enqueues a field-targeted hint update. Present-keys-only —
	// an absent hint key leaves the last recorded hint intact (never cleared by an unrelated op).
	if v, ok := fields["categoryHintName"]; ok {
		setClauses += ", category_hint_name=?"
		args = append(args, v)
	}
	if v, ok := fields["categoryHintIcon"]; ok {
		setClauses += ", category_hint_icon=?"
		args = append(args, v)
	}
	if v, ok := fields["categoryHintColor"]; ok {
		setClauses += ", category_hint_color=?"
		args = append(args, v)
	}
	if v, ok := fields["storeHintBrand"]; ok {
		setClauses += ", store_hint_brand=?"
		args = append(args, v)
	}
	// A member re-confirming they still want the item stamps this. Present-keys-only — an unrelated op
	// never clears it, so the last re-confirmation stands until a newer one replaces it.
	if v, ok := fields["lastWantedAt"]; ok {
		setClauses += ", last_wanted_at=?"
		args = append(args, v)
	}
	// Set when a member asks for no predictions until the next purchase; cleared by the client sending
	// false once that purchase happens. (The wire key says "first"; the meaning is the next purchase.)
	if v, ok := fields["quietUntilFirstPurchase"]; ok {
		if b, ok := coerceBool("quietUntilFirstPurchase", v); ok {
			setClauses += ", quiet_until_first_purchase=?"
			args = append(args, boolToInt(b))
		}
	}

	args = append(args, itemID, cartID)
	_, err := db.Exec(
		fmt.Sprintf(`UPDATE items SET %s WHERE id=? AND cart_id=?`, setClauses),
		args...,
	)
	return err == nil, err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
