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
    purchase_store_brand TEXT,
    purchase_cancelled  INTEGER DEFAULT 0,
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

-- sub_key is the member's WebSocket subscription credential: a per-(cart, member)
-- secret minted at create/join and handed to that member alone. The socket is
-- user-scoped (one connection carries every cart a person belongs to), so the
-- cart id is no longer half of the address and can no longer stand in for proof
-- of membership — the key is what proves a connection may subscribe to a cart,
-- and it also fixes the identity the connection writes under. It dies with the
-- member row: a revoke or a leave removes the row, and the key stops resolving
-- in the same statement. No cascade rotation — revoking one member leaves every
-- other member's key working, which the shared invite secret cannot do.
CREATE TABLE IF NOT EXISTS members (
    cart_id   TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    joined_at REAL NOT NULL,
    sub_key   TEXT NOT NULL,
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
-- UNIQUE, not merely an index: sub_key is looked up on its own to resolve a
-- connection's (cart, user), so a collision would resolve one member's key to
-- another member's row. The uniqueness is the lookup's correctness condition.
CREATE UNIQUE INDEX IF NOT EXISTS idx_members_sub_key ON members(sub_key);
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
	{"purchased_at", "ALTER TABLE items ADD COLUMN purchased_at REAL"},                             // ADR-050 purchase fact time
	// The BRAND NAME of the shop a member bought at, riding the same checked-carrying ops as
	// purchased_at. It exists because a store RECORD never crosses this relay — stores are personal —
	// so a receiving device has nothing it could resolve for a partner's trip, and naming its own shop
	// would be inventing one. Stored so the fact survives the receiver being offline, exactly like
	// purchased_at: the app settles a purchase into history up to a day after the tap, and by then the
	// op is long gone.
	{"purchase_store_brand", "ALTER TABLE items ADD COLUMN purchase_store_brand TEXT"},
	// R-585: true only when an author's cancels:true verdict lowered or advanced the row's act, so a
	// reconnecting phone reads the cancel off the state row instead of keeping a purchase its author took
	// back. A bit, never the verdict itself: cancels:false is not stored, and neither is the offset. It
	// cannot be derived from the reservoir lacking the act — legacy rows lack acts nobody cancelled.
	{"purchase_cancelled", "ALTER TABLE items ADD COLUMN purchase_cancelled INTEGER DEFAULT 0"},
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
	// SQLite performs best with a single writer connection — and TWO things depend on
	// it for CORRECTNESS, not only for throughput. Both have to be discharged before it
	// is raised, and the second is the easy one to miss because nothing here asks for it.
	//
	// 1. joinCartHandler's capacity check reads the member count and inserts in separate
	//    statements, and it is this serialisation, not its transaction, that stops two
	//    people joining a full cart at the same instant. Give that handler a real
	//    EXCLUSIVE lock before raising this.
	// 2. ⚠️ `PRAGMA foreign_keys=ON` is a PER-CONNECTION setting, applied once in the
	//    schema below. With one connection for the process lifetime it holds for every
	//    statement; with a pool, connections opened later run with foreign keys OFF and
	//    every ON DELETE CASCADE in the schema silently stops firing. That is not
	//    housekeeping: createCartHandler accepts a client-PROPOSED cart id and promises
	//    it touches no existing data, which rests on a deleted room taking its members,
	//    items and consumption events with it. Set the pragma on every new connection
	//    (a connector hook, not a one-shot Exec) before raising this.
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
		purchase_store_brand, purchase_cancelled,
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

// RoomExists reports whether a room already occupies this cart id. Used by
// create to refuse a client-proposed id rather than adopt or overwrite the room
// already living at it.
func RoomExists(db *sql.DB, cartID string) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM rooms WHERE cart_id = ?`, cartID).Scan(&n)
	return n > 0, err
}

// CartIDResidue reports whether any row keyed to cartID survives in the tables
// that hang off rooms, with no room to own them.
//
// In a healthy database this is unreachable — every one of those tables declares
// ON DELETE CASCADE and the pragma is on — which is exactly why create calls it.
// A client may PROPOSE a cart id, and the promise attached to that (create
// "touches no existing data") must be true on its own terms rather than on the
// strength of a cascade nothing tests and one refactor could switch off. If this
// ever returns true, the cascade is not doing what the schema says and the right
// answer is to refuse rather than hand the caller somebody else's rows.
func CartIDResidue(db *sql.DB, cartID string) (bool, error) {
	var n int
	err := db.QueryRow(`
		SELECT
		    (SELECT COUNT(*) FROM members            WHERE cart_id = ?) +
		    (SELECT COUNT(*) FROM items              WHERE cart_id = ?) +
		    (SELECT COUNT(*) FROM consumption_events WHERE cart_id = ?)
	`, cartID, cartID, cartID).Scan(&n)
	return n > 0, err
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

// AddMember upserts a member record and returns that member's subscription key
// (idempotent — re-join is safe).
//
// A re-join KEEPS the existing key rather than minting a fresh one. The key is
// per (cart, member), not per device, so re-minting would silently lock out the
// member's other devices, which hold the old value and have no way to learn of
// the change. Only a real membership change — a revoke or a leave, both of which
// delete the row — retires a key.
func AddMember(db *sql.DB, cartID, userID string) (string, error) {
	if _, err := db.Exec(`
		INSERT INTO members (cart_id, user_id, joined_at, sub_key)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(cart_id, user_id) DO UPDATE SET joined_at = excluded.joined_at
	`, cartID, userID, float64(time.Now().Unix()), generateSubKey()); err != nil {
		return "", err
	}
	return GetSubKey(db, cartID, userID)
}

// GetSubKey returns the subscription key already minted for a member.
func GetSubKey(db *sql.DB, cartID, userID string) (string, error) {
	var subKey string
	err := db.QueryRow(
		`SELECT sub_key FROM members WHERE cart_id = ? AND user_id = ?`, cartID, userID,
	).Scan(&subKey)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("not a member")
	}
	return subKey, err
}

// ResolveSubKey turns a subscription key into the (cart, member) pair it was
// minted for. `ok` is false for an unknown key — including one that was valid
// until its member row was deleted by a revoke or a leave.
//
// The identifiers are returned together or not at all: nothing that fails to
// resolve may yield a cartID a caller could subscribe to, or a userID a caller
// could write under. That is why this returns the pair rather than two lookups.
func ResolveSubKey(db *sql.DB, subKey string) (cartID, userID string, ok bool, err error) {
	if subKey == "" {
		return "", "", false, nil
	}
	var c, u string
	scanErr := db.QueryRow(
		`SELECT cart_id, user_id FROM members WHERE sub_key = ?`, subKey,
	).Scan(&c, &u)
	if scanErr == sql.ErrNoRows {
		return "", "", false, nil
	}
	if scanErr != nil {
		return "", "", false, scanErr
	}
	return c, u, true, nil
}

// RemoveMember deletes a member record.
func RemoveMember(db *sql.DB, cartID, userID string) error {
	_, err := db.Exec(`DELETE FROM members WHERE cart_id = ? AND user_id = ?`, cartID, userID)
	return err
}

// GetMembers returns all members of a cart with their user metadata.
//
// Tier is reported for the cart OWNER only; every other member's tier comes back
// as the empty string. The owner's plan is the one a client acts on (it decides
// whether a shared cart is frozen), and nothing reads anyone else's — so nobody
// else's is sent. A plan is not a secret; this is simply not broadcasting a fact
// that has no reader.
//
// The join to rooms is a LEFT join so that a cart whose room row is missing still
// returns its members (with no tier) rather than none: losing the member list is a
// far worse failure than losing a tier, and it would be a silent one.
func GetMembers(db *sql.DB, cartID string) ([]MemberInfo, error) {
	rows, err := db.Query(`
		SELECT u.user_id, COALESCE(u.display_name,''), COALESCE(u.color,''),
		       CASE WHEN u.user_id = r.owner_id THEN u.tier ELSE '' END
		FROM members m
		JOIN users u ON m.user_id = u.user_id
		LEFT JOIN rooms r ON r.cart_id = m.cart_id
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
		       purchase_store_brand, COALESCE(purchase_cancelled,0),
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
		var purchaseStoreBrand sql.NullString
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
			&purchaseStoreBrand, &item.Cancels,
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
		if purchaseStoreBrand.Valid {
			item.PurchaseStoreBrand = &purchaseStoreBrand.String
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

// reservoirMove is what one op does to the consumption reservoir, decided by decideCompletion. The
// reservoir moves with the act, never with the row: it records an act only when a check-off advances
// the item, and retracts one by identity on its author's cancel.
type reservoirMove struct {
	kind reservoirMoveKind
	at   float64 // the act recorded or retracted; for retractRecent, the uncheck's own raw ts
}

type reservoirMoveKind int

const (
	reservoirNone reservoirMoveKind = iota
	reservoirRecord
	reservoirRetract
	// reservoirRetractRecent is an uncheck that carries no verdict (a client that predates it): it
	// retracts the most recent event only if it falls within consumptionRetractWindow of the uncheck's
	// raw ts — not the LWW value the row stored, which is capped at the relay's clock + 60 s — so an
	// uncheck of a long-ago check-off never deletes an older, legitimate consumption.
	reservoirRetractRecent
)

// captureConsumption applies a reservoirMove, keyed on the reservoir SPINE = COALESCE(global_id, id):
// the concept global_id when the item has one, else the cart-scoped item id (ADR-052 — concept-less
// items keep accreting for late joiners, matching the client's COALESCE(concept, item.uuid) spine). The
// spine is read from the row, because a completion op typically carries no globalID. The act is never
// read from the row: the owner hands it over, so there is no instant here to make one up from.
func captureConsumption(q querier, cartID, itemID string, m reservoirMove) {
	if m.kind == reservoirNone {
		return
	}
	var globalID sql.NullString
	if err := q.QueryRow(`
		SELECT global_id FROM items WHERE id = ? AND cart_id = ?
	`, itemID, cartID).Scan(&globalID); err != nil {
		return
	}
	spine := itemID
	if globalID.Valid && globalID.String != "" {
		spine = globalID.String
	}

	act := m.at
	switch m.kind {
	case reservoirRecord:
		// A re-assert of the same act keys the same (cart_id, global_id, ts) row, so INSERT OR IGNORE
		// collapses it (idempotent by construction, no cross-day phantom).
		if _, err := q.Exec(`
			INSERT OR IGNORE INTO consumption_events (cart_id, global_id, ts) VALUES (?, ?, ?)
		`, cartID, spine, act); err != nil {
			log.Printf("captureConsumption: insert: %v", err)
			return
		}
		pruneConsumptionEvents(q, cartID, spine)
		return
	case reservoirRetractRecent:
		var maxTs sql.NullFloat64
		if err := q.QueryRow(`
			SELECT MAX(ts) FROM consumption_events WHERE cart_id = ? AND global_id = ?
		`, cartID, spine).Scan(&maxTs); err != nil || !maxTs.Valid {
			return
		}
		if diff := m.at - maxTs.Float64; diff < -consumptionRetractWindow || diff > consumptionRetractWindow {
			return
		}
		act = maxTs.Float64
	}
	if _, err := q.Exec(`
		DELETE FROM consumption_events WHERE cart_id = ? AND global_id = ? AND ts = ?
	`, cartID, spine, act); err != nil {
		log.Printf("captureConsumption: retract: %v", err)
	}
}

// pruneConsumptionEvents caps a (cart_id, global_id) reservoir to the most
// recent reservoirCap rows.
func pruneConsumptionEvents(q querier, cartID, globalID string) {
	_, err := q.Exec(`
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

// querier is what *sql.DB and *sql.Tx both offer, so one op's reads and writes can share the
// transaction that makes them a single step.
type querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// completionKeys are the wire keys of an op's completion part (R-585). Every other key is content.
// purchaseUTCOffset passes through to peers beside an act and is never stored.
var completionKeys = map[string]bool{
	"checked": true, "purchasedAt": true, "purchaseStoreBrand": true, "cancels": true, "purchaseUTCOffset": true,
}

// actSkew bounds how far past the relay's clock an act instant may sit and still be an act: the same
// 60 s the LWW clamp allows a ts. A later purchasedAt is no act at all — admitted, it would sit above
// every real act of the following hours and refuse each of them.
const actSkew = 60.0

// OpOutcome is what one op did.
type OpOutcome struct {
	Applied   bool           // some part of the op took effect: the sender's ack reads "applied", else "discarded"
	Broadcast bool           // peers must hear of it
	Fields    map[string]any // the op's keys that took effect — exactly what a broadcast relays
}

// ApplyOp applies one op in one transaction. The op splits by its keys (R-585): the CONTENT part follows
// last-write-wins (ApplyLWW), and the COMPLETION part applies by act identity whatever LWW said
// (decideCompletion). A completion that loses LWW is still an act — an uncheck made offline before
// somebody renamed the item is still that member's uncheck — but its stale content must not ride in on
// it. The guards bind the whole op: an update to a missing or tombstoned row, and an add older than its
// tombstone, apply neither part.
func ApplyOp(
	db *sql.DB,
	cartID, itemID, op string,
	fields map[string]any,
	ts float64,
	deviceID, userID string,
) (OpOutcome, error) {
	tx, err := db.Begin()
	if err != nil {
		return OpOutcome{}, err
	}
	defer tx.Rollback()

	lww, err := ApplyLWW(tx, cartID, itemID, op, fields, ts, deviceID)
	if err != nil || lww == lwwRefused {
		return OpOutcome{}, err
	}
	if op == "delete" {
		return OpOutcome{Applied: true, Broadcast: true, Fields: fields}, tx.Commit()
	}

	won := lww == lwwWon
	var row completionPair
	var deleted bool
	if err := tx.QueryRow(`
		SELECT checked, purchased_at, attributed_to, purchase_store_brand, COALESCE(purchase_cancelled,0), deleted
		FROM items WHERE id=? AND cart_id=?
	`, itemID, cartID).Scan(&row.checked, &row.purchasedAt, &row.completer, &row.brand, &row.cancelled, &deleted); err != nil {
		return OpOutcome{}, err
	}
	var v completionVerdict
	if !deleted { // an add that lost LWW to a stale tombstone left the row deleted: no act lands on it
		v = decideCompletion(op, fields, won, row, float64(time.Now().Unix()), ts, userID)
	}
	if v.next != row || v.clearQuiet {
		if _, err := tx.Exec(`
			UPDATE items SET checked=?, purchased_at=?, attributed_to=?, purchase_store_brand=?, purchase_cancelled=?,
			    quiet_until_first_purchase = CASE WHEN ? THEN 0 ELSE quiet_until_first_purchase END
			WHERE id=? AND cart_id=?
		`, boolToInt(v.next.checked), v.next.purchasedAt, v.next.completer, v.next.brand, boolToInt(v.next.cancelled),
			v.clearQuiet, itemID, cartID); err != nil {
			return OpOutcome{}, err
		}
	}
	captureConsumption(tx, cartID, itemID, v.capture)

	out := OpOutcome{Applied: won || v.tookEffect}
	for k, val := range fields {
		if completionKeys[k] && v.tookEffect || !completionKeys[k] && won {
			if out.Fields == nil {
				out.Fields = map[string]any{}
			}
			out.Fields[k] = val
		}
	}
	out.Broadcast = len(out.Fields) > 0
	return out, tx.Commit()
}

// completionPair is an item row's completion state. (checked, purchasedAt) is a monotone register on the
// act instant; completer and brand describe the act it holds, and cancelled is its author's stored
// verdict that the act was taken back.
type completionPair struct {
	checked     bool
	purchasedAt sql.NullFloat64
	completer   sql.NullString
	brand       sql.NullString
	cancelled   bool
}

// completionVerdict is decideCompletion's ruling on one op.
type completionVerdict struct {
	next       completionPair // the row's completion state after the op
	capture    reservoirMove
	clearQuiet bool // the derived end of a "quiet until the next purchase" (R-549): a check-off advanced the act
	tookEffect bool // the op's completion keys go to peers
}

// decideCompletion is the one owner of an op's completion part (R-585): the row's new pair, its cancel
// bit, the reservoir move, and whether peers hear of it. Op shapes are read by their KEYS, never by
// which build sent them — the relay cannot tell an old client's snapshot from a new client's op, so a
// shape decides. `row` is the item's completion state before the op; `won` is LWW's verdict on its
// content; `rawTs` is the op's own ts, unclamped.
//
//   - A check-off with a valid act t advances the pair only to a later act (row P null, or t > P), and
//     records t. Anything else is refused whole: a re-assert of the act already held, or an older one.
//   - An uncheck's author verdict is followed, never recomputed. cancels:true retracts t by identity
//     whatever the pair did, lowers the pair only if t is the act it holds, and advances it if t is
//     later (the uncheck reached the relay before its own check-off). cancels:false keeps everything.
//   - An uncheck with no verdict (a client that predates it) stays under LWW: it lowers the pair only
//     when it won, the row is checked, and it names the row's act or nothing.
//   - An add's checked:false asserts nothing: an add is a birth snapshot, not an act.
//
// A valid act is a numeric purchasedAt no later than now + actSkew. No act ever comes from ts.
func decideCompletion(
	op string,
	fields map[string]any,
	won bool,
	row completionPair,
	now, rawTs float64,
	userID string,
) completionVerdict {
	v := completionVerdict{next: row}
	checked, ok := fields["checked"].(bool)
	if !ok {
		return v // a purchasedAt or a brand without checked asserts nothing
	}
	t, named := fields["purchasedAt"].(float64)
	isAct := named && t <= now+actSkew
	p := row.purchasedAt

	if checked {
		if !isAct || p.Valid && t <= p.Float64 {
			return v
		}
		brand, hasBrand := fields["purchaseStoreBrand"].(string)
		v.next = completionPair{
			checked:     true,
			purchasedAt: sql.NullFloat64{Float64: t, Valid: true},
			completer:   sql.NullString{String: userID, Valid: true},
			brand:       sql.NullString{String: brand, Valid: hasBrand}, // a new act never inherits the last one's shop
		}
		v.capture = reservoirMove{reservoirRecord, t}
		// An explicit flag in this op wins over the derived clear, but only if it applied: a losing op's
		// flag is stale content, and its act still ends the silence.
		_, explicitQuiet := fields["quietUntilFirstPurchase"]
		v.clearQuiet = !(won && explicitQuiet)
		v.tookEffect = true
		return v
	}
	if op == "add" {
		return v
	}

	cancels, hasVerdict := fields["cancels"].(bool)
	switch {
	case hasVerdict && !cancels:
		return v // the act stands as history
	case hasVerdict && isAct:
		v.capture = reservoirMove{reservoirRetract, t}
		v.tookEffect = true
		switch {
		case p.Valid && t == p.Float64:
			v.next = completionPair{purchasedAt: p, brand: row.brand, cancelled: true}
		case !p.Valid || t > p.Float64:
			v.next = completionPair{purchasedAt: sql.NullFloat64{Float64: t, Valid: true}, cancelled: true}
		}
		return v
	}
	// No verdict: bare, naming an act without one, or cancels with no act to name.
	namesRowAct := !named || p.Valid && t == p.Float64
	if !won || !row.checked || !namesRowAct {
		return v
	}
	v.next = completionPair{purchasedAt: p, brand: row.brand}
	v.capture = reservoirMove{reservoirRetractRecent, rawTs}
	v.tookEffect = true
	return v
}

// lwwOutcome is LWW's verdict on an op's content part.
type lwwOutcome int

const (
	lwwRefused lwwOutcome = iota // a guard refused the whole op: an update to a missing or tombstoned row, an add older than its tombstone
	lwwLost                      // the row's content is newer: nothing of the content part applied
	lwwWon                       // the content part applied, and ts and device_id moved
)

// ApplyLWW applies an op's CONTENT part using last-write-wins conflict resolution (TDD §4.5), and the
// guards that bind the whole op. It never writes a completion column: those are decideCompletion's.
func ApplyLWW(
	q querier,
	cartID, itemID, op string,
	fields map[string]any,
	ts float64,
	deviceID string,
) (lwwOutcome, error) {
	// Clamp ts: prevent far-future clients from permanently winning all conflicts.
	effectiveTs := ts
	if maxTs := float64(time.Now().Unix()) + 60; ts > maxTs {
		effectiveTs = maxTs
	}

	if op == "delete" {
		// Soft-delete: mark deleted + deleted_at. If row doesn't exist, insert a
		// tombstone so stale add ops can't resurrect the item.
		res, err := q.Exec(`
			UPDATE items SET deleted=1, deleted_at=? WHERE id=? AND cart_id=?
		`, effectiveTs, itemID, cartID)
		if err != nil {
			return lwwRefused, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// No existing row — insert tombstone.
			_, err = q.Exec(`
				INSERT INTO items (id, cart_id, ts, device_id, deleted, deleted_at)
				VALUES (?, ?, ?, ?, 1, ?)
			`, itemID, cartID, effectiveTs, deviceID, effectiveTs)
			if err != nil {
				return lwwRefused, err
			}
		}
		return lwwWon, nil
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
	err := q.QueryRow(`
		SELECT ts, COALESCE(device_id,''), deleted, deleted_at
		FROM items WHERE id=? AND cart_id=?
	`, itemID, cartID).Scan(&existing.ts, &existing.deviceID, &existing.deleted, &existing.deletedAt)

	if err == sql.ErrNoRows {
		// No existing row: only an add may create one. An update carries only the
		// fields its act changed, so a row built from it is partial (a notes edit
		// would become an item with an empty title). The same rule as ADR-039's
		// "an update must never resurrect", one step earlier: an update must never
		// create. An update can reach a missing row only through a lost add, and
		// the client's orphan self-heal repairs that by re-sending the whole add.
		// Discarding leaves the row absent so that repair can still see it.
		if op == "update" {
			log.Printf("ApplyLWW: update skipped on missing item %s/%s — only an add creates", cartID, itemID)
			return lwwRefused, nil
		}
		if err := insertItem(q, cartID, itemID, fields, effectiveTs, deviceID); err != nil {
			return lwwRefused, err
		}
		return lwwWon, nil
	}
	if err != nil {
		return lwwRefused, err
	}

	// Tombstone exists — check if deletion wins.
	if existing.deleted == 1 && existing.deletedAt.Valid && existing.deletedAt.Float64 > effectiveTs {
		return lwwRefused, nil // deletion wins; discard this write
	}

	// An update must never resurrect a tombstoned row (ADR-039): partial-field
	// resurrection can construct a ghost — e.g. an empty-titled item from a lone
	// {"checked": false}. Only an add — a deliberate re-add carrying full fields
	// — may bring a tombstoned row back (handled by updateItem's op=="add" clear
	// below, since the row already exists here). Skip and log.
	if existing.deleted == 1 && op == "update" {
		log.Printf("ApplyLWW: update skipped on tombstoned item %s/%s — no resurrect", cartID, itemID)
		return lwwRefused, nil
	}

	// Live row or stale tombstone being re-added — check LWW.
	if effectiveTs > existing.ts || effectiveTs == existing.ts && deviceID < existing.deviceID {
		if err := updateItem(q, cartID, itemID, op, fields, effectiveTs, deviceID); err != nil {
			return lwwRefused, err
		}
		return lwwWon, nil
	}
	return lwwLost, nil
}

// insertItem creates a row from an add's content part. Its completion columns start empty — an
// unchecked item nobody has bought — and decideCompletion applies the add's act, if it carries one.
func insertItem(
	q querier,
	cartID, itemID string,
	fields map[string]any,
	ts float64,
	deviceID string,
) error {
	title, _ := fields["title"].(string)
	quantity := 1
	if q, ok := fields["quantity"].(float64); ok {
		quantity = int(q)
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

	_, err := q.Exec(`
		INSERT INTO items
		    (id, cart_id, title, quantity, notes, specification, urgency_level,
		     global_id, frequency_days, is_active, paused_at, pause_reason, deferred_guess_date,
		     deferred_guess_authored_at, category_hint_name, category_hint_icon,
		     category_hint_color, store_hint_brand, last_wanted_at, quiet_until_first_purchase,
		     ts, device_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, itemID, cartID, title, quantity,
		notes, specification, urgencyLevel,
		globalID, frequencyDays, boolToInt(isActive), pausedAt, pauseReason, deferredGuessDate,
		deferredGuessAuthoredAt, categoryHintName, categoryHintIcon,
		categoryHintColor, storeHintBrand, lastWantedAt, boolToInt(quietUntilFirstPurchase),
		ts, deviceID)
	return err
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

// updateItem writes an op's content part over a row it won LWW against. Completion keys are
// decideCompletion's and are never read here — so a rename never moves the completer, and an old
// client's whole-row snapshot cannot write its stale checked state.
func updateItem(
	q querier,
	cartID, itemID, op string,
	fields map[string]any,
	ts float64,
	deviceID string,
) error {
	// Build SET clause from fields that are present.
	// ts and device_id move on every op that wins LWW, whichever parts of it applied.
	setClauses := "ts=?, device_id=?"
	args := []any{ts, deviceID}

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
	// Set when a member asks for no predictions until the next purchase or the next Need Now,
	// whichever comes first. An explicit key in THIS op is authoritative and wins outright — the
	// derived clears run only when this op is silent on the flag.
	_, explicitQuiet := fields["quietUntilFirstPurchase"]
	if v, ok := fields["quietUntilFirstPurchase"]; ok {
		if b, ok := coerceBool("quietUntilFirstPurchase", v); ok {
			setClauses += ", quiet_until_first_purchase=?"
			args = append(args, boolToInt(b))
		}
	}
	// R-549: the silence is a derived fact, not a sticky one — a strictly newer want or purchase ends
	// it, in the relay, so a build too old to ever send `false` still heals. This is the want half; the
	// purchase half is decideCompletion's, because only it knows whether an act advanced. "Strictly
	// newer", not "present": the anti-entropy push-back re-asserts the fact unchanged on every reconnect,
	// and a present-key rule would let that re-assert undo a later ×. SQLite evaluates every SET
	// expression against the OLD row, so the comparison lives in the same UPDATE as the write above.
	if v, ok := fields["lastWantedAt"]; ok && !explicitQuiet {
		setClauses += ", quiet_until_first_purchase = CASE WHEN last_wanted_at IS NULL OR ? > last_wanted_at" +
			" THEN 0 ELSE quiet_until_first_purchase END"
		args = append(args, v)
	}

	args = append(args, itemID, cartID)
	_, err := q.Exec(
		fmt.Sprintf(`UPDATE items SET %s WHERE id=? AND cart_id=?`, setClauses),
		args...,
	)
	return err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
