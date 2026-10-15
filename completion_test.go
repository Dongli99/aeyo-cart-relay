package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// R-585, the relay round. An op splits into two parts by its keys: the CONTENT part follows LWW exactly
// as before, and the COMPLETION part (checked, purchasedAt, purchaseStoreBrand, cancels,
// purchaseUTCOffset) applies by act identity, whatever LWW says. The row's (checked, purchased_at) pair
// is a monotone register on the act instant.
//
// Every test here enters at handleDelta, the outermost act a phone performs, and reads back the three
// places an act lives: the row's pair, the reservoir, and what a peer was told.

const relayCart = "cart-a"

type relayRig struct {
	t        *testing.T
	db       *sql.DB
	hub      *Hub
	clients  map[string]*Client
	observer *Client
}

// delivery is what one op produced: the sender's ack, and whether, and with which keys, a peer was told.
type delivery struct {
	ack     string
	relayed bool
	fields  map[string]any
}

func newRelayRig(t *testing.T) *relayRig {
	t.Helper()
	r := &relayRig{t: t, db: testDB(t), hub: NewHub(), clients: map[string]*Client{}}
	r.observer = joinHub(r.hub, newTestClient("user-c"), relayCart)
	return r
}

func drainAll(c *Client) [][]byte {
	var out [][]byte
	for {
		select {
		case b := <-c.send:
			out = append(out, b)
		default:
			return out
		}
	}
}

// send delivers one op from user's device at ts, the way a phone's outbox flush does.
func (r *relayRig) send(user string, ts float64, op Op) delivery {
	r.t.Helper()
	c, ok := r.clients[user]
	if !ok {
		c = joinHub(r.hub, newTestClient(user), relayCart)
		r.clients[user] = c
	}
	raw, _ := json.Marshal(DeltaMsg{Type: "delta", CartID: relayCart, DeviceID: "dev-" + user, Ts: ts, Ops: []Op{op}})
	handleDelta(c, r.hub, r.db, raw)

	var d delivery
	for _, b := range drainAll(c) {
		var ack AckMsg
		if json.Unmarshal(b, &ack) == nil && ack.Type == "ack" {
			d.ack = ack.Status
		}
	}
	for _, b := range drainAll(r.observer) {
		var msg DeltaMsg
		if json.Unmarshal(b, &msg) == nil && msg.Type == "delta" && len(msg.Ops) == 1 {
			d.relayed = true
			d.fields = msg.Ops[0].Fields
		}
	}
	for u, other := range r.clients {
		if u != user {
			drainAll(other)
		}
	}
	return d
}

func update(id string, fields map[string]any) Op { return Op{Op: "update", ID: id, Fields: fields} }
func add(id string, fields map[string]any) Op    { return Op{Op: "add", ID: id, Fields: fields} }

// seedChecked creates Milk, checked off by user-a at act 1000 in Acme: the row reads
// (checked, 1000, user-a, Acme) and the reservoir holds 1000.
func (r *relayRig) seedChecked() {
	r.t.Helper()
	if d := r.send("user-a", 1000, add("milk", map[string]any{
		"title": "Milk", "globalID": "g1", "checked": true, "purchasedAt": 1000.0, "purchaseStoreBrand": "Acme",
	})); d.ack != "applied" {
		r.t.Fatalf("seed: ack %q", d.ack)
	}
}

// pair renders the row's completion state: checked, purchased_at, attributed_to, brand, and the stored
// cancel bit. A NULL reads "-". The bit reads "?" when the column cannot be read.
func (r *relayRig) pair(id string) string {
	r.t.Helper()
	var checked bool
	var pa sql.NullFloat64
	var by, brand sql.NullString
	if err := r.db.QueryRow(
		`SELECT checked, purchased_at, attributed_to, purchase_store_brand FROM items WHERE cart_id=? AND id=?`,
		relayCart, id).Scan(&checked, &pa, &by, &brand); err != nil {
		r.t.Fatalf("read pair: %v", err)
	}
	bit := "?"
	var b int
	if err := r.db.QueryRow(`SELECT purchase_cancelled FROM items WHERE cart_id=? AND id=?`, relayCart, id).
		Scan(&b); err == nil {
		bit = fmt.Sprint(b)
	}
	s := "unchecked"
	if checked {
		s = "checked"
	}
	p := "-"
	if pa.Valid {
		p = fmt.Sprint(pa.Float64)
	}
	str := func(v sql.NullString) string {
		if v.Valid {
			return v.String
		}
		return "-"
	}
	return fmt.Sprintf("%s P=%s by=%s brand=%s bit=%s", s, p, str(by), str(brand), bit)
}

func (r *relayRig) content(id string) (title string, ts float64) {
	r.t.Helper()
	if err := r.db.QueryRow(`SELECT COALESCE(title,''), ts FROM items WHERE cart_id=? AND id=?`, relayCart, id).
		Scan(&title, &ts); err != nil {
		r.t.Fatalf("read content: %v", err)
	}
	return title, ts
}

func (r *relayRig) events(spine string) string {
	r.t.Helper()
	ev, err := GetConsumptionEvents(r.db, relayCart, spine)
	if err != nil {
		r.t.Fatalf("GetConsumptionEvents: %v", err)
	}
	return fmt.Sprint(ev)
}

// stateRow is the item as a reconnecting phone reads it: the STATE snapshot's bytes, decoded.
func (r *relayRig) stateRow(id string) map[string]any {
	r.t.Helper()
	b, err := json.Marshal(buildStateMsg(r.db, relayCart))
	if err != nil {
		r.t.Fatalf("marshal state: %v", err)
	}
	var state struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(b, &state); err != nil {
		r.t.Fatalf("decode state: %v", err)
	}
	for _, it := range state.Items {
		if it["id"] == id {
			return it
		}
	}
	r.t.Fatalf("item %s not in the STATE snapshot: %s", id, b)
	return nil
}

func keys(m map[string]any) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func expect(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", what, got, want)
	}
}

func expectDelivery(t *testing.T, d delivery, ack string, relayedKeys string) {
	t.Helper()
	expect(t, "ack", d.ack, ack)
	got := "(not relayed)"
	if d.relayed {
		got = keys(d.fields)
	}
	expect(t, "relayed keys", got, relayedKeys)
}

const notRelayed = "(not relayed)"

// ---------------------------------------------------------------------------
// The table, row by row
// ---------------------------------------------------------------------------

func TestCompletionTable(t *testing.T) {
	t.Run("row 1: a check-off with a later act advances the pair, whatever LWW says", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 3000, update("milk", map[string]any{"title": "Oat milk"}))
		// B checked off offline at 2000; the rename reached the relay first.
		d := r.send("user-b", 2000, update("milk", map[string]any{
			"checked": true, "purchasedAt": 2000.0, "purchaseUTCOffset": -14400.0}))
		expectDelivery(t, d, "applied", "checked,purchaseUTCOffset,purchasedAt")
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=- bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000 2000]")
		title, ts := r.content("milk")
		expect(t, "content", fmt.Sprint(title, " ", ts), "Oat milk 3000")
	})

	t.Run("row 1: a new act with no brand clears the last act's brand", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 2000, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-a brand=- bit=0")
	})

	t.Run("row 1: a check-off on a never-bought row advances it", func(t *testing.T) {
		r := newRelayRig(t)
		r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1"}))
		d := r.send("user-b", 1500, update("milk", map[string]any{
			"checked": true, "purchasedAt": 1500.0, "purchaseStoreBrand": "Bolt"}))
		expectDelivery(t, d, "applied", "checked,purchaseStoreBrand,purchasedAt")
		expect(t, "pair", r.pair("milk"), "checked P=1500 by=user-b brand=Bolt bit=0")
		expect(t, "reservoir", r.events("g1"), "[1500]")
	})

	t.Run("row 2: a check-off at or before the act is refused", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 2000, update("milk", map[string]any{
			"checked": true, "purchasedAt": 2000.0, "purchaseStoreBrand": "Bolt"}))
		// An older act re-asserted at a fresh ts: it wins LWW and is still refused.
		d := r.send("user-d", 5000, update("milk", map[string]any{"checked": true, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=Bolt bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000 2000]")
		_, ts := r.content("milk")
		expect(t, "row ts (it won LWW, so ts moves)", fmt.Sprint(ts), "5000")
		// The same act re-asserted: refused too.
		d = r.send("user-b", 6000, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=Bolt bit=0")
	})

	t.Run("row 2: a refused re-assert that also lost LWW is discarded", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 3000, update("milk", map[string]any{"title": "Oat milk"}))
		d := r.send("user-d", 2000, update("milk", map[string]any{"checked": true, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "discarded", notRelayed)
	})

	t.Run("row 3: a check-off with no valid act is no act", func(t *testing.T) {
		r := newRelayRig(t)
		r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1"}))
		d := r.send("user-a", 2000, update("milk", map[string]any{"checked": true}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")
		expect(t, "reservoir", r.events("g1"), "[]")

		future := float64(time.Now().Unix()) + 86_400
		d = r.send("user-a", 3000, update("milk", map[string]any{"checked": true, "purchasedAt": future}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")
		expect(t, "reservoir", r.events("g1"), "[]")
	})

	t.Run("row 4: a cancel of the row's act lowers it, keeps P and the brand, and sets the bit", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 3000, update("milk", map[string]any{"title": "Oat milk"}))
		d := r.send("user-a", 2000, update("milk", map[string]any{
			"checked": false, "purchasedAt": 1000.0, "cancels": true}))
		expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
		expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=1")
		expect(t, "reservoir", r.events("g1"), "[]")
		title, ts := r.content("milk")
		expect(t, "content", fmt.Sprint(title, " ", ts), "Oat milk 3000")
	})

	t.Run("row 5: a cancel naming a later act advances the register", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-a", 2001, update("milk", map[string]any{
			"checked": false, "purchasedAt": 2000.0, "cancels": true}))
		expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
		expect(t, "pair", r.pair("milk"), "unchecked P=2000 by=- brand=- bit=1")
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})

	t.Run("row 5: a cancel on a never-bought row advances it", func(t *testing.T) {
		r := newRelayRig(t)
		r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1"}))
		d := r.send("user-a", 1501, update("milk", map[string]any{
			"checked": false, "purchasedAt": 1500.0, "cancels": true}))
		expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
		expect(t, "pair", r.pair("milk"), "unchecked P=1500 by=- brand=- bit=1")
	})

	t.Run("row 6: a cancel of an older act leaves the pair and deletes its reservoir row", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 2000, update("milk", map[string]any{
			"checked": true, "purchasedAt": 2000.0, "purchaseStoreBrand": "Bolt"}))
		d := r.send("user-a", 3000, update("milk", map[string]any{
			"checked": false, "purchasedAt": 1000.0, "cancels": true}))
		expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=Bolt bit=0")
		expect(t, "reservoir", r.events("g1"), "[2000]")
	})

	t.Run("row 7: a keep verdict changes nothing and tells nobody", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-a", 1005, update("milk", map[string]any{
			"checked": false, "purchasedAt": 1000.0, "cancels": false}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})

	t.Run("row 8: a bare uncheck that wins LWW lowers the row's act", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-a", 1010, update("milk", map[string]any{"checked": false}))
		expectDelivery(t, d, "applied", "checked")
		expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=0")
		expect(t, "reservoir (inside the 30 s window)", r.events("g1"), "[]")
	})

	t.Run("row 8: an uncheck naming P without a verdict lowers it, outside the window keeps the event", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-a", 5000, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "applied", "checked,purchasedAt")
		expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})

	t.Run("row 8: cancels with no purchasedAt is the no-verdict shape", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-a", 5000, update("milk", map[string]any{"checked": false, "cancels": true}))
		expectDelivery(t, d, "applied", "cancels,checked")
		expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=0")
		expect(t, "reservoir (outside the window, so kept)", r.events("g1"), "[1000]")
	})

	t.Run("row 9: a bare uncheck that loses LWW changes nothing", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 3000, update("milk", map[string]any{"title": "Oat milk"}))
		d := r.send("user-a", 2000, update("milk", map[string]any{"checked": false}))
		expectDelivery(t, d, "discarded", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")
	})

	t.Run("row 9: an uncheck naming another act without a verdict changes nothing", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 2000, update("milk", map[string]any{
			"checked": true, "purchasedAt": 2000.0, "purchaseStoreBrand": "Bolt"}))
		d := r.send("user-a", 2010, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=Bolt bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000 2000]")
	})

	t.Run("row 9: a bare uncheck of an unchecked row changes nothing", func(t *testing.T) {
		r := newRelayRig(t)
		r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1"}))
		d := r.send("user-a", 2000, update("milk", map[string]any{"checked": false}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")
	})

	t.Run("row 10: an add with an act creates the first row checked", func(t *testing.T) {
		r := newRelayRig(t)
		d := r.send("user-a", 1000, add("milk", map[string]any{
			"title": "Milk", "globalID": "g1", "checked": true, "purchasedAt": 1000.0, "purchaseStoreBrand": "Acme"}))
		expectDelivery(t, d, "applied", "checked,globalID,purchaseStoreBrand,purchasedAt,title")
		expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})

	t.Run("row 10: an add without an act creates the row unchecked and ignores purchasedAt", func(t *testing.T) {
		r := newRelayRig(t)
		d := r.send("user-a", 1000, add("milk", map[string]any{
			"title": "Milk", "globalID": "g1", "checked": false, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "applied", "globalID,title")
		expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")
		expect(t, "reservoir", r.events("g1"), "[]")

		d = r.send("user-a", 1000, add("eggs", map[string]any{"title": "Eggs", "globalID": "g2", "checked": true}))
		expectDelivery(t, d, "applied", "globalID,title")
		expect(t, "pair", r.pair("eggs"), "unchecked P=- by=- brand=- bit=0")
		expect(t, "reservoir", r.events("g2"), "[]")
	})

	t.Run("row 11: an add over a live row asserts nothing with checked:false", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		// An orphan self-heal from a phone whose mirror is already off.
		d := r.send("user-b", 2000, add("milk", map[string]any{"title": "Milk", "globalID": "g1", "checked": false}))
		expectDelivery(t, d, "applied", "globalID,title")
		expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")
	})

	t.Run("row 11: an add over a live row with a later act is a check-off", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-b", 2000, add("milk", map[string]any{
			"title": "Milk", "globalID": "g1", "checked": true, "purchasedAt": 2000.0}))
		expectDelivery(t, d, "applied", "checked,globalID,purchasedAt,title")
		expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=- bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000 2000]")
	})

	t.Run("row 11: a re-add over a stale tombstone keeps the pair it held", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 1500, Op{Op: "delete", ID: "milk"})
		d := r.send("user-a", 2000, add("milk", map[string]any{"title": "Milk", "globalID": "g1", "checked": false}))
		expectDelivery(t, d, "applied", "globalID,title")
		if deleted, _ := itemDeleted(t, r.db, relayCart, "milk"); deleted != 0 {
			t.Errorf("re-add did not resurrect the row")
		}
		expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")
	})

	t.Run("guard: an update to a tombstoned row is discarded whole, completion included", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 1500, Op{Op: "delete", ID: "milk"})
		d := r.send("user-b", 2000, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
		expectDelivery(t, d, "discarded", notRelayed)
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})

	t.Run("guard: an update to a missing row is discarded whole", func(t *testing.T) {
		r := newRelayRig(t)
		d := r.send("user-b", 2000, update("milk", map[string]any{
			"checked": false, "purchasedAt": 2000.0, "cancels": true}))
		expectDelivery(t, d, "discarded", notRelayed)
		if _, found := itemDeleted(t, r.db, relayCart, "milk"); found {
			t.Errorf("an update created a row")
		}
	})

	t.Run("guard: an add past its tombstone that loses LWW leaves the row deleted, and lands no act on it", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 3000, update("milk", map[string]any{"title": "Oat milk"}))
		r.send("user-a", 1500, Op{Op: "delete", ID: "milk"}) // a delete ignores LWW: deleted_at 1500, ts 3000
		d := r.send("user-b", 2000, add("milk", map[string]any{
			"title": "Milk", "globalID": "g1", "checked": true, "purchasedAt": 2000.0}))
		expectDelivery(t, d, "discarded", notRelayed)
		if deleted, _ := itemDeleted(t, r.db, relayCart, "milk"); deleted != 1 {
			t.Fatalf("positive control: the add resurrected the row, so this case is not the one under test")
		}
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})

	t.Run("guard: an add that loses to a newer tombstone is discarded whole", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 3000, Op{Op: "delete", ID: "milk"})
		d := r.send("user-b", 2000, add("milk", map[string]any{
			"title": "Milk", "globalID": "g1", "checked": true, "purchasedAt": 2000.0}))
		expectDelivery(t, d, "discarded", notRelayed)
		expect(t, "reservoir", r.events("g1"), "[1000]")
	})
}

// ---------------------------------------------------------------------------
// The bit: stored, served, and never invented
// ---------------------------------------------------------------------------

func TestStateRowCarriesTheCancelVerdict(t *testing.T) {
	t.Run("a cancel is served as cancels:true, and a later act clears it", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 2000, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0, "cancels": true}))
		if got := r.stateRow("milk")["cancels"]; got != true {
			t.Errorf("state row cancels = %v, want true", got)
		}
		r.send("user-b", 3000, update("milk", map[string]any{"checked": true, "purchasedAt": 3000.0}))
		if got, ok := r.stateRow("milk")["cancels"]; ok {
			t.Errorf("state row after a later act carries cancels = %v, want the key absent", got)
		}
	})

	t.Run("a bare uncheck and a keep verdict never serve cancels", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 2000, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0, "cancels": false}))
		row := r.stateRow("milk")
		if got, ok := row["cancels"]; ok {
			t.Errorf("a keep verdict is served as cancels = %v; cancels:false is never stored", got)
		}
		r.send("user-a", 3000, update("milk", map[string]any{"checked": false}))
		row = r.stateRow("milk")
		if row["checked"] != false || row["purchasedAt"] != 1000.0 {
			t.Fatalf("positive control: the bare uncheck did not lower the row: %v", row)
		}
		if got, ok := row["cancels"]; ok {
			t.Errorf("a bare uncheck is served as cancels = %v", got)
		}
	})

	t.Run("a legacy row, unchecked with an act and no bit, never serves cancels", func(t *testing.T) {
		r := newRelayRig(t)
		// A shape legacy rows take in production: unchecked, a purchased_at, and that act absent from the
		// reservoir. Written directly, as a row the deployed relay left behind.
		if _, err := r.db.Exec(`INSERT INTO items (id, cart_id, title, checked, purchased_at, ts)
			VALUES ('legacy', ?, 'Soap', 0, 1000, 1000)`, relayCart); err != nil {
			t.Fatalf("seed legacy row: %v", err)
		}
		row := r.stateRow("legacy")
		if row["purchasedAt"] != 1000.0 {
			t.Fatalf("positive control: the legacy row's act is not served: %v", row)
		}
		if got, ok := row["cancels"]; ok {
			t.Errorf("a legacy row is served as cancels = %v; the bit defaults false", got)
		}
	})
}

// The bit lives in three places that must agree: the fresh-DB schema, the ALTER migration for a
// deployed table, and the composite-key rebuild's explicit column list.
func TestPurchaseCancelledColumn(t *testing.T) {
	t.Run("a fresh database has it", func(t *testing.T) {
		db := testDB(t)
		cols, err := existingColumns(db, "items")
		if err != nil {
			t.Fatalf("existingColumns: %v", err)
		}
		if !cols["purchase_cancelled"] {
			t.Error("fresh schema has no purchase_cancelled column")
		}
	})

	t.Run("a deployed table gains it, and its rows start false", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "precancel.db")
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
			    attributed_to TEXT, ts REAL NOT NULL, device_id TEXT, deleted INTEGER DEFAULT 0, deleted_at REAL,
			    purchased_at REAL,
			    PRIMARY KEY (cart_id, id), FOREIGN KEY (cart_id) REFERENCES rooms(cart_id) ON DELETE CASCADE
			);
			INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
			INSERT INTO items (id, cart_id, title, checked, purchased_at, ts) VALUES ('soap','cart-a','Soap',0,1000,1000);
		`); err != nil {
			t.Fatalf("seed pre-bit schema: %v", err)
		}
		legacy.Close()

		db, err := InitDB(path)
		if err != nil {
			t.Fatalf("InitDB: %v", err)
		}
		defer db.Close()
		var bit int
		if err := db.QueryRow(`SELECT purchase_cancelled FROM items WHERE id='soap'`).Scan(&bit); err != nil {
			t.Fatalf("migrated table has no purchase_cancelled: %v", err)
		}
		if bit != 0 {
			t.Errorf("a legacy row's bit = %d, want 0", bit)
		}
	})

	t.Run("the composite-key rebuild carries a set bit", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "legacypk-bit.db")
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
			    purchased_at REAL, purchase_store_brand TEXT, purchase_cancelled INTEGER DEFAULT 0,
			    category_hint_name TEXT, category_hint_icon TEXT,
			    category_hint_color TEXT, store_hint_brand TEXT,
			    last_wanted_at REAL, quiet_until_first_purchase INTEGER DEFAULT 0,
			    attributed_to TEXT, ts REAL NOT NULL, device_id TEXT,
			    deleted INTEGER DEFAULT 0, deleted_at REAL
			);
			INSERT INTO rooms VALUES ('cart-a','user-a','s',1,'A','');
			INSERT INTO items (id, cart_id, title, checked, purchased_at, purchase_cancelled, ts)
			VALUES ('milk','cart-a','Milk',0,1000,1,1000);
		`); err != nil {
			t.Fatalf("seed legacy-pk schema: %v", err)
		}
		legacy.Close()

		db, err := InitDB(path)
		if err != nil {
			t.Fatalf("InitDB: %v", err)
		}
		defer db.Close()
		if pk, _ := primaryKeyColumns(db, "items"); !pk["cart_id"] {
			t.Fatal("composite-key rebuild did not run")
		}
		var bit int
		if err := db.QueryRow(`SELECT purchase_cancelled FROM items WHERE id='milk'`).Scan(&bit); err != nil {
			t.Fatalf("rebuilt table has no purchase_cancelled: %v", err)
		}
		if bit != 1 {
			t.Errorf("rebuild dropped the bit: %d, want 1", bit)
		}
	})
}

// ---------------------------------------------------------------------------
// The derived quiet clear (R-549) follows the act
// ---------------------------------------------------------------------------

func TestQuietClearFollowsTheAct(t *testing.T) {
	seedQuiet := func(t *testing.T) *relayRig {
		t.Helper()
		r := newRelayRig(t)
		r.send("user-a", 1000, add("sun", map[string]any{
			"title": "Sunscreen", "checked": true, "purchasedAt": 1000.0, "quietUntilFirstPurchase": true}))
		return r
	}
	quiet := func(r *relayRig) bool { return r.stateRow("sun")["quietUntilFirstPurchase"] == true }

	t.Run("a later act clears it even when the check-off lost LWW", func(t *testing.T) {
		r := seedQuiet(t)
		r.send("user-a", 3000, update("sun", map[string]any{"title": "SPF 50"}))
		r.send("user-b", 2000, update("sun", map[string]any{"checked": true, "purchasedAt": 2000.0}))
		if quiet(r) {
			t.Error("quiet survived a later purchase that lost LWW")
		}
	})

	t.Run("a refused re-assert does not clear it", func(t *testing.T) {
		r := seedQuiet(t)
		r.send("user-b", 5000, update("sun", map[string]any{"checked": true, "purchasedAt": 500.0}))
		if !quiet(r) {
			t.Error("quiet cleared by an older act")
		}
	})

	t.Run("a cancel naming a later act does not clear it", func(t *testing.T) {
		r := seedQuiet(t)
		r.send("user-a", 2001, update("sun", map[string]any{"checked": false, "purchasedAt": 2000.0, "cancels": true}))
		if !quiet(r) {
			t.Error("quiet cleared by a cancel")
		}
	})

	// The flag's own key must lose (ADR-096): a newer write to the flag, not merely to the item.
	t.Run("an explicit flag that lost LWW does not hold off the clear", func(t *testing.T) {
		r := seedQuiet(t)
		r.send("user-a", 3000, update("sun", map[string]any{"quietUntilFirstPurchase": true}))
		r.send("user-b", 2000, update("sun", map[string]any{
			"checked": true, "purchasedAt": 2000.0, "quietUntilFirstPurchase": true}))
		if quiet(r) {
			t.Error("a losing op's explicit flag held off the clear its own later act earned")
		}
	})
}

// ---------------------------------------------------------------------------
// The design review's break sequences, end to end
// ---------------------------------------------------------------------------

// D-1: an offline completion op whose item was edited before it flushed is no longer lost.
func TestSequenceD1OfflineCompletionAfterANewerEdit(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked()
	r.send("user-b", 3000, update("milk", map[string]any{"title": "Oat milk"}))
	d := r.send("user-a", 2000, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0, "cancels": true}))
	expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
	expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=1")
	expect(t, "reservoir", r.events("g1"), "[]")

	// The same for an offline check-off.
	d = r.send("user-b", 2500, update("milk", map[string]any{"checked": true, "purchasedAt": 2500.0}))
	expectDelivery(t, d, "applied", "checked,purchasedAt")
	expect(t, "pair", r.pair("milk"), "checked P=2500 by=user-b brand=- bit=0")
	expect(t, "reservoir", r.events("g1"), "[2500]")
}

// D-2: A and B check off at t1 < t2; A's cancel of t1 leaves (checked, t2).
func TestSequenceD2CancelOfAnOlderAct(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked()
	r.send("user-b", 2000, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
	r.send("user-a", 3000, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0, "cancels": true}))
	expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=- bit=0")
	row := r.stateRow("milk")
	if row["checked"] != true || row["purchasedAt"] != 2000.0 {
		t.Errorf("state row = %v, want B's act standing", row)
	}
	if _, ok := row["cancels"]; ok {
		t.Errorf("state row names B's standing act as cancelled: %v", row)
	}
}

// B1: an older client's whole-snapshot check-off that lost LWW applies its act and never its stale content.
func TestSequenceB1SnapshotNeverRevertsANewerRename(t *testing.T) {
	r := newRelayRig(t)
	r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1", "quantity": 1.0}))
	r.send("user-b", 3000, update("milk", map[string]any{"title": "Oat milk"}))
	d := r.send("user-d", 2000, update("milk", map[string]any{
		"title": "Milk", "quantity": 1.0, "checked": true, "purchasedAt": 2000.0, "urgencyLevel": 1.0}))
	expectDelivery(t, d, "applied", "checked,purchasedAt")
	title, ts := r.content("milk")
	expect(t, "content", fmt.Sprint(title, " ", ts), "Oat milk 3000")
	expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-d brand=- bit=0")
}

// B2 / Q5: an uncheck reaches the relay before its own check-off. The late check-off is refused and the
// reservoir never holds the act.
func TestSequenceB2UncheckArrivesBeforeItsCheckOff(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked() // Sunday's act, 1000
	d := r.send("user-a", 2002, update("milk", map[string]any{"checked": false, "purchasedAt": 2000.0, "cancels": true}))
	expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
	expect(t, "pair", r.pair("milk"), "unchecked P=2000 by=- brand=- bit=1")

	// A1 reconnects and flushes the check-off it held, at a fresh ts.
	d = r.send("user-a", 2005, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
	expectDelivery(t, d, "applied", notRelayed)
	expect(t, "pair", r.pair("milk"), "unchecked P=2000 by=- brand=- bit=1")
	expect(t, "reservoir", r.events("g1"), "[1000]")
	if r.stateRow("milk")["cancels"] != true {
		t.Error("a state reader would not read the act as cancelled")
	}

	t.Run("Q5.3: a later act in between, and the late check-off is still refused", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-a", 2002, update("milk", map[string]any{"checked": false, "purchasedAt": 2000.0, "cancels": true}))
		r.send("user-b", 2100, update("milk", map[string]any{"checked": true, "purchasedAt": 2100.0}))
		d := r.send("user-a", 2200, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
		expectDelivery(t, d, "applied", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=2100 by=user-b brand=- bit=0")
		expect(t, "reservoir", r.events("g1"), "[1000 2100]")
	})
}

// B3: an older act re-asserted (an older client's push-back, or a fast clock) is not broadcast, so no
// older client replays a purchase that moves its lastPurchasedAt backwards.
func TestSequenceB3OlderReassertIsNotBroadcast(t *testing.T) {
	r := newRelayRig(t)
	r.send("user-a", 800, add("milk", map[string]any{"title": "Milk", "globalID": "g1"}))
	r.send("user-d", 1000, update("milk", map[string]any{"checked": true, "purchasedAt": 1000.0}))
	r.send("user-b", 2000, update("milk", map[string]any{"checked": true, "purchasedAt": 2000.0}))
	d := r.send("user-d", 2100, update("milk", map[string]any{"checked": true, "purchasedAt": 1000.0}))
	expectDelivery(t, d, "applied", notRelayed)
	expect(t, "pair", r.pair("milk"), "checked P=2000 by=user-b brand=- bit=0")
}

// B4: the overnight case. A cancels after C backgrounded; C's reconnect reads the verdict off the state
// row, and an older client's Need Now on the already-unchecked row does not erase it.
func TestSequenceB4StateRowCarriesTheVerdictOvernight(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked()
	r.send("user-a", 2000, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0, "cancels": true}))
	if r.stateRow("milk")["cancels"] != true {
		t.Fatal("state row does not carry the cancel")
	}
	// D (an older client) taps Need Now the next morning: a whole snapshot with checked:false.
	r.send("user-d", 90_000, update("milk", map[string]any{
		"title": "Milk", "quantity": 1.0, "checked": false, "urgencyLevel": 2.0}))
	row := r.stateRow("milk")
	if row["cancels"] != true || row["purchasedAt"] != 1000.0 {
		t.Errorf("state row after an older client's Need Now = %v, want the cancel still served", row)
	}
}

// B5: an act instant later than the relay's clock + 60 s is no act, and it does not poison the register
// against the real acts that follow.
func TestSequenceB5FutureActIsNoAct(t *testing.T) {
	r := newRelayRig(t)
	r.send("user-a", 1000, add("milk", map[string]any{"title": "Milk", "globalID": "g1"}))
	future := float64(time.Now().Unix()) + 86_400
	d := r.send("user-a", 1100, update("milk", map[string]any{"checked": true, "purchasedAt": future}))
	expectDelivery(t, d, "applied", notRelayed)
	expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")

	d = r.send("user-a", 1200, update("milk", map[string]any{"checked": false, "purchasedAt": future, "cancels": true}))
	expectDelivery(t, d, "applied", notRelayed)
	expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")

	d = r.send("user-b", 1300, update("milk", map[string]any{"checked": true, "purchasedAt": 1300.0}))
	expectDelivery(t, d, "applied", "checked,purchasedAt")
	expect(t, "pair", r.pair("milk"), "checked P=1300 by=user-b brand=- bit=0")
	expect(t, "reservoir", r.events("g1"), "[1300]")
}

// ---------------------------------------------------------------------------
// Mixed builds: shapes from clients that predate the verdict ("older clients") go through the
// table as the no-verdict rows
// ---------------------------------------------------------------------------

func TestMixedBuildShapes(t *testing.T) {
	snapshot := func(checked bool) map[string]any {
		return map[string]any{"title": "Milk", "quantity": 1.0, "checked": checked, "urgencyLevel": 2.0,
			"notes": nil, "specification": nil}
	}

	t.Run("an older client's snapshot with checked:false that wins LWW lowers the row and applies its content", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-d", 5000, update("milk", snapshot(false)))
		expectDelivery(t, d, "applied", "checked,notes,quantity,specification,title,urgencyLevel")
		expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=0")
	})

	t.Run("an older client's snapshot with checked:false that loses LWW changes nothing", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		r.send("user-b", 3000, update("milk", map[string]any{"title": "Oat milk"}))
		d := r.send("user-d", 2000, update("milk", snapshot(false)))
		expectDelivery(t, d, "discarded", notRelayed)
		expect(t, "pair", r.pair("milk"), "checked P=1000 by=user-a brand=Acme bit=0")
		title, _ := r.content("milk")
		expect(t, "title", title, "Oat milk")
	})

	t.Run("an older client's merged {checked:false, purchasedAt:t} names t: it lowers P and only P", func(t *testing.T) {
		r := newRelayRig(t)
		r.seedChecked()
		d := r.send("user-d", 1020, update("milk", map[string]any{"checked": false, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "applied", "checked,purchasedAt")
		expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=0")
		expect(t, "reservoir (inside the window)", r.events("g1"), "[]")
	})

	t.Run("an older client's merged add{checked:false, purchasedAt:t} creates an unchecked row with no act", func(t *testing.T) {
		r := newRelayRig(t)
		d := r.send("user-d", 1000, add("milk", map[string]any{
			"title": "Milk", "globalID": "g1", "checked": false, "purchasedAt": 1000.0}))
		expectDelivery(t, d, "applied", "globalID,title")
		expect(t, "pair", r.pair("milk"), "unchecked P=- by=- brand=- bit=0")
		if _, ok := r.stateRow("milk")["purchasedAt"]; ok {
			t.Error("the state row serves an act nobody performed")
		}
	})
}

// ---------------------------------------------------------------------------
// Judgment calls, pinned
// ---------------------------------------------------------------------------

// J1: a peer is told exactly the keys that took effect. A mixed op whose content key lost relays only its
// completion; one whose content won with a refused completion relays only its content. Under ADR-096 a
// key loses only to a newer write to that key, so the newer write here is to notes.
func TestRelayCarriesOnlyWhatTookEffect(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked()
	r.send("user-b", 3000, update("milk", map[string]any{"notes": "whole"}))
	d := r.send("user-a", 2000, update("milk", map[string]any{
		"notes": "2%", "checked": true, "purchasedAt": 2000.0, "purchaseUTCOffset": -14400.0}))
	expectDelivery(t, d, "applied", "checked,purchaseUTCOffset,purchasedAt")

	d = r.send("user-a", 5000, update("milk", map[string]any{"notes": "2%", "checked": true, "purchasedAt": 500.0}))
	expectDelivery(t, d, "applied", "notes")
}

// J3: a replayed cancel of an act already cancelled is broadcast, as the table says. It changes nothing
// at the relay and is idempotent at every receiver that cancels by identity.
func TestReplayedCancelIsBroadcast(t *testing.T) {
	r := newRelayRig(t)
	r.seedChecked()
	cancel := map[string]any{"checked": false, "purchasedAt": 1000.0, "cancels": true}
	r.send("user-a", 2000, update("milk", cancel))
	d := r.send("user-a", 2500, update("milk", cancel))
	expectDelivery(t, d, "applied", "cancels,checked,purchasedAt")
	expect(t, "pair", r.pair("milk"), "unchecked P=1000 by=- brand=Acme bit=1")
}
