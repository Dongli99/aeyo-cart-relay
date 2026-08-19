package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pingInterval  = 30 * time.Second
	pongDeadline  = 10 * time.Second
	writeDeadline = 10 * time.Second

	// subscribeDeadline bounds how long an upgraded-but-unidentified connection
	// may sit before it names itself. Identity now arrives in the first message
	// rather than in the handshake, so the window between upgrade and proof is
	// the one moment an unauthenticated peer holds a connection at all.
	subscribeDeadline = 10 * time.Second

	// maxSubscribeCarts bounds one subscription request. A person is in a
	// household's worth of carts, not a directory's; the cap keeps a malformed or
	// hostile request from turning into an unbounded burst of key lookups.
	maxSubscribeCarts = 64

	// The per-connection message budget: a token bucket of wsBurstMsgs refilling
	// at wsRefillPerSec, applied by WAITING for a token rather than by
	// disconnecting (readPump's awaitBudget).
	//
	// The old shape was 30 messages per 60s, terminal. Both halves were wrong for
	// a multiplexed socket. The ceiling was below a legitimate burst — sharing a
	// personal cart queues one add per existing item and flushes them back to back
	// the moment the connection opens, so any cart over 30 items disconnected the
	// sharer mid-flush and healed 30 items per reconnect. And a single budget
	// across a connection that now carries every one of a person's carts would
	// have divided that same ceiling by the number of carts, making the limit
	// tighter exactly for the people using the feature most.
	//
	// Waiting rather than disconnecting is what lets the burst be generous without
	// weakening the guard: the bucket bounds the sustained WRITE RATE, which is
	// the resource actually being protected, while a burst that exceeds it is
	// delayed instead of severed. Nothing is lost and nothing needs healing.
	// 2/sec sustained is far above a person editing a list and far below a flood.
	wsBurstMsgs    = 240
	wsRefillPerSec = 2.0

	// maxWSMessageBytes caps a single inbound WebSocket frame. The only
	// legitimate client→server messages are a single-op delta (Task F6 rejects
	// multi-op), a subscription request, and the tiny session start/end envelopes;
	// even a delta carrying long free-text notes/specification is a few KB. 512 KB
	// leaves ~64× headroom over any real message while bounding a hostile client —
	// gorilla closes the connection with a 1009 and ReadMessage returns an error
	// when it is exceeded.
	maxWSMessageBytes = 512 * 1024
)

// Client represents a single active WebSocket connection. One connection serves
// one PERSON — every cart they belong to is multiplexed over it — so the client
// holds a subscription SET rather than a cart.
//
// `subscribed` is owned by the Hub and read only through it (Subscribe /
// IsSubscribed / SubscribedCarts): a re-subscribe arrives on the read pump while
// broadcasts run on HTTP handler goroutines, so the set is shared state and the
// hub's mutex is the one thing guarding it.
type Client struct {
	userID     string
	deviceID   string
	conn       *websocket.Conn
	send       chan []byte
	subscribed map[string]bool

	// Message budget, touched only by the read pump.
	tokens    float64
	lastToken time.Time
}

// Hub manages WebSocket connections and in-memory active-shopping state.
//
// Two indexes, because the two questions are different: `clients` answers "which
// connections belong to this person" (a person on two devices), and `cartIndex`
// answers "who must hear about this cart" — the fan-out every broadcast needs.
// The second is derived from the subscription sets and maintained wherever they
// change, so a broadcast never walks every connection asking.
type Hub struct {
	mu             sync.RWMutex
	clients        map[string]map[*Client]bool  // userID → set of clients
	cartIndex      map[string]map[*Client]bool  // cartID → set of subscribed clients
	activeShopping map[string]map[string]string // cartID → userID → storeID
}

// NewHub constructs an empty Hub.
func NewHub() *Hub {
	return &Hub{
		clients:        make(map[string]map[*Client]bool),
		cartIndex:      make(map[string]map[*Client]bool),
		activeShopping: make(map[string]map[string]string),
	}
}

// Register adds a client to the hub. It subscribes to nothing until Subscribe.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[c.userID] == nil {
		h.clients[c.userID] = make(map[*Client]bool)
	}
	h.clients[c.userID][c] = true
	if c.subscribed == nil {
		c.subscribed = make(map[string]bool)
	}
}

// Subscribe REPLACES a client's subscription set with cartIDs and reindexes it.
// Returns the carts newly added by this call, so the caller can send a state
// snapshot for those alone — a re-subscribe that only adds one cart must not
// re-deliver every other cart's full state.
func (h *Hub) Subscribe(c *Client, cartIDs []string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	next := make(map[string]bool, len(cartIDs))
	var added []string
	for _, id := range cartIDs {
		if next[id] {
			continue
		}
		next[id] = true
		if !c.subscribed[id] {
			added = append(added, id)
		}
	}

	for id := range c.subscribed {
		if !next[id] {
			h.removeFromCartIndexLocked(c, id)
		}
	}
	for id := range next {
		if h.cartIndex[id] == nil {
			h.cartIndex[id] = make(map[*Client]bool)
		}
		h.cartIndex[id][c] = true
	}
	c.subscribed = next
	return added
}

// IsSubscribed reports whether this connection may act on cartID. It is the
// generalisation of the old "does this message's cartID match the connection's"
// check — the guard that keeps a client's writes inside the carts it proved.
func (h *Hub) IsSubscribed(c *Client, cartID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return c.subscribed[cartID]
}

// SubscribedCarts returns a snapshot of the carts this connection is subscribed to.
func (h *Hub) SubscribedCarts(c *Client) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(c.subscribed))
	for id := range c.subscribed {
		out = append(out, id)
	}
	return out
}

// UnsubscribeUserFromCart drops cartID from every connection belonging to
// userID, and returns whether any connection was actually subscribed.
//
// This is what makes a revoke or a leave take effect on a socket that stays open
// for the person's OTHER carts. While a connection served one cart, losing
// membership and losing the connection were the same event and the client could
// be trusted to tear itself down; now the connection outlives the membership, so
// the server has to end the subscription itself.
func (h *Hub) UnsubscribeUserFromCart(userID, cartID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	dropped := false
	for c := range h.clients[userID] {
		if c.subscribed[cartID] {
			delete(c.subscribed, cartID)
			h.removeFromCartIndexLocked(c, cartID)
			dropped = true
		}
	}
	return dropped
}

// removeFromCartIndexLocked drops c from cartID's fan-out set. Caller holds h.mu.
func (h *Hub) removeFromCartIndexLocked(c *Client, cartID string) {
	if h.cartIndex[cartID] == nil {
		return
	}
	delete(h.cartIndex[cartID], c)
	if len(h.cartIndex[cartID]) == 0 {
		delete(h.cartIndex, cartID)
	}
}

// Unregister removes a client. It relays member_session_end for EVERY cart the
// connection was active-shopping in — a connection now carries several, and a
// drop ends presence in all of them at once.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	var endedCarts []string
	for cartID := range c.subscribed {
		if h.activeShopping[cartID] != nil {
			if _, ok := h.activeShopping[cartID][c.userID]; ok {
				delete(h.activeShopping[cartID], c.userID)
				endedCarts = append(endedCarts, cartID)
			}
		}
		h.removeFromCartIndexLocked(c, cartID)
	}
	c.subscribed = make(map[string]bool)
	if h.clients[c.userID] != nil {
		delete(h.clients[c.userID], c)
		if len(h.clients[c.userID]) == 0 {
			delete(h.clients, c.userID)
		}
	}
	h.mu.Unlock()

	for _, cartID := range endedCarts {
		msg := MemberSessionMsg{Type: "member_session_end", CartID: cartID, UserID: c.userID}
		b, _ := json.Marshal(msg)
		h.BroadcastToCart(cartID, b, c.deviceID)
	}
}

// BroadcastToCart sends msg to all clients subscribed to cartID except the one
// with excludeDeviceID.
func (h *Hub) BroadcastToCart(cartID string, msg []byte, excludeDeviceID string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.cartIndex[cartID] {
		if c.deviceID == excludeDeviceID {
			continue
		}
		select {
		case c.send <- msg:
		default:
			// Slow client — drop the message rather than blocking the broadcast.
			log.Printf("hub: dropping message for slow client %s/%s", cartID, c.userID)
		}
	}
}

// BroadcastToCartExcludeUser sends msg to all clients subscribed to cartID except
// those belonging to excludeUserID.
func (h *Hub) BroadcastToCartExcludeUser(cartID string, msg []byte, excludeUserID string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.cartIndex[cartID] {
		if c.userID == excludeUserID {
			continue
		}
		select {
		case c.send <- msg:
		default:
			log.Printf("hub: dropping message for slow client %s/%s", cartID, c.userID)
		}
	}
}

// SetActiveShopping records that userID is shopping at storeID in cartID.
func (h *Hub) SetActiveShopping(cartID, userID, storeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.activeShopping[cartID] == nil {
		h.activeShopping[cartID] = make(map[string]string)
	}
	h.activeShopping[cartID][userID] = storeID
}

// ClearActiveShopping removes userID from the active-shopping set for cartID.
func (h *Hub) ClearActiveShopping(cartID, userID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.activeShopping[cartID] != nil {
		delete(h.activeShopping[cartID], userID)
	}
}

// GetActiveShopping returns all active-shopping entries for a cart.
func (h *Hub) GetActiveShopping(cartID string) []ActiveShoppingEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var entries []ActiveShoppingEntry
	for uid, sid := range h.activeShopping[cartID] {
		entries = append(entries, ActiveShoppingEntry{UserID: uid, StoreID: sid})
	}
	return entries
}

// ---------------------------------------------------------------------------
// Pumps
// ---------------------------------------------------------------------------

// writePump drains c.send and writes each message to the WebSocket.
// It also sends periodic pings.
//
// ⚠️ **Once this is running it is the ONLY goroutine that may write this
// connection.** gorilla/websocket permits exactly one concurrent writer and traps
// a second by design; net/http then recovers the panic per connection, so the
// service stays up, /health stays green, and the defect is invisible except in the
// journal. That is not hypothetical here: readPump's rate-limit branch wrote its
// own close frame until 1ec1f66, and it panicked in production for twelve days
// before anybody read the log (R-400). The branch is gone because the budget now
// WAITS instead of disconnecting — the fix was a side effect, not a decision,
// which is why the rule is written down here and asserted by
// TestOnlyTheWritePumpWritesTheConnection.
//
// A path that needs to say something to a client queues it on `c.send`. Writing
// directly is correct only BEFORE this pump starts — `closeUnsubscribed` is that
// case and the only one.
func writePump(c *Client) {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump reads incoming messages from the WebSocket, enforces rate limits,
// and dispatches to handleMessage. Blocks until the connection closes.
func readPump(c *Client, h *Hub, db *sql.DB) {
	defer func() {
		h.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxWSMessageBytes)

	// Pong handler resets the read deadline, effectively keeping the connection
	// alive as long as pongs arrive within pongDeadline of each ping.
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pingInterval + pongDeadline))
		return nil
	})
	c.conn.SetReadDeadline(time.Now().Add(pingInterval + pongDeadline))

	c.lastToken = time.Now()
	c.tokens = wsBurstMsgs

	for {
		_, rawMsg, err := c.conn.ReadMessage()
		if err != nil {
			// A clean disconnect is silent; an oversized frame (SetReadLimit →
			// 1009) or other protocol error is logged as the anomaly it is.
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("readPump: read error for %s: %v", c.userID, err)
			}
			return
		}

		c.awaitBudget()
		handleMessage(c, h, db, rawMsg)
	}
}

// awaitBudget spends one message token, waiting for it to refill if the bucket
// is empty. Called only from the read pump, so the bucket needs no lock.
//
// The wait is bounded by 1/wsRefillPerSec — a fraction of a second, well inside
// the read deadline, which the next ReadMessage resets from buffered pongs.
func (c *Client) awaitBudget() {
	now := time.Now()
	c.tokens += now.Sub(c.lastToken).Seconds() * wsRefillPerSec
	if c.tokens > wsBurstMsgs {
		c.tokens = wsBurstMsgs
	}
	c.lastToken = now

	if c.tokens < 1 {
		time.Sleep(time.Duration((1 - c.tokens) / wsRefillPerSec * float64(time.Second)))
		c.tokens = 1
		c.lastToken = time.Now()
	}
	c.tokens--
}

// handleMessage dispatches a raw WebSocket message to the appropriate handler.
func handleMessage(c *Client, h *Hub, db *sql.DB, rawMsg []byte) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rawMsg, &envelope); err != nil {
		return
	}

	switch envelope.Type {
	case "subscribe":
		handleSubscribe(c, h, db, rawMsg)
	case "delta":
		handleDelta(c, h, db, rawMsg)
	case "member_session_start":
		handleSessionStart(c, h, rawMsg)
	case "member_session_end":
		handleSessionEnd(c, h, rawMsg)
	}
}

// handleSubscribe applies a mid-connection subscription change: the person
// joined a cart, left one, or was revoked from one, and their client re-derived
// its set. The new set REPLACES the old, and only newly-added carts get a state
// snapshot.
//
// A re-subscribe is verified exactly like the opening one, against this
// connection's established userID — a connection can never widen into another
// person's carts by sending a second subscribe.
func handleSubscribe(c *Client, h *Hub, db *sql.DB, rawMsg []byte) {
	var msg SubscribeMsg
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return
	}

	_, accepted, rejected := verifySubscription(db, msg.Carts, c.userID)
	added := h.Subscribe(c, accepted)

	sendJSON(c, SubscribeResultMsg{
		Type:     "subscribe_result",
		V:        protocolVersion,
		MinV:     minClientVersion,
		Accepted: accepted,
		Rejected: rejected,
	})
	for _, cartID := range added {
		sendJSON(c, buildStateMsg(db, h, cartID))
	}
}

// sendJSON queues a message for one connection. Non-blocking, like every other
// send on the hub: a client too slow to drain its buffer misses the message and
// re-derives on its next reconnect rather than stalling the sender.
func sendJSON(c *Client, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("hub: marshal for %s: %v", c.userID, err)
		return
	}
	select {
	case c.send <- b:
	default:
		log.Printf("hub: dropping message for slow client %s", c.userID)
	}
}

// sendAck acknowledges a processed delta back to the originating connection
// only (ADR-038 invariant B) — never broadcast. Non-blocking send: a lost ack
// just means the client re-flushes an idempotent write later.
func sendAck(c *Client, cartID, itemID string, ts float64, status string) {
	ack := AckMsg{Type: "ack", CartID: strings.ToLower(cartID), ItemID: itemID, Ts: ts, Status: status}
	b, err := json.Marshal(ack)
	if err != nil {
		return
	}
	select {
	case c.send <- b:
	default:
		log.Printf("hub: dropping ack for slow client %s/%s", cartID, c.userID)
	}
}

func handleDelta(c *Client, h *Hub, db *sql.DB, rawMsg []byte) {
	var msg DeltaMsg
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return
	}

	// F6: exactly one op per delta. handleDelta only ever persists Ops[0] but
	// relays the whole message — a multi-op delta would diverge live peers from
	// the reconnect snapshot. Reject anything but a single op (folds in the old
	// len==0 guard); no persist, no relay, connection survives.
	if len(msg.Ops) != 1 {
		var itemID string
		if len(msg.Ops) > 0 {
			itemID = msg.Ops[0].ID
		}
		log.Printf("handleDelta: rejecting %d-op delta (exactly 1 required) for %s", len(msg.Ops), c.userID)
		sendAck(c, msg.CartID, itemID, msg.Ts, "rejected")
		return
	}
	op := msg.Ops[0]
	itemID := op.ID

	// The connection-scope guard, generalised: a delta may only touch a cart this
	// connection PROVED, and the proof is per cart, never per connection. This was
	// `msg.CartID != c.cartID` while a connection served one cart; widening the
	// flush to a set without widening this check the same way is precisely the
	// ADR-038 finding-3 shape — a socket carrying another cart's ops, discarded by
	// the server, lost by the client.
	cartID := strings.ToLower(msg.CartID)
	if !h.IsSubscribed(c, cartID) {
		log.Printf("handleDelta: cart %s not subscribed on this connection (%s)", cartID, c.userID)
		sendAck(c, msg.CartID, itemID, msg.Ts, "rejected")
		return
	}

	actor := c.userID
	if op.Actor == systemActor {
		actor = systemActor
	}

	accepted, err := ApplyLWW(db, cartID, op.ID, op.Op, op.Fields, msg.Ts, msg.DeviceID, actor)
	if err != nil {
		log.Printf("handleDelta: ApplyLWW error: %v", err)
		return
	}
	if !accepted {
		sendAck(c, msg.CartID, op.ID, msg.Ts, "discarded")
		return
	}

	// Consumption capture (§3.4): an accepted checked op appends/retracts a
	// consumption_events row, keyed by the reservoir spine COALESCE(global_id, id)
	// (ADR-052 — concept-less items accrete under their cart-scoped item id).
	if checkedVal, ok := op.Fields["checked"].(bool); ok {
		captureConsumption(db, cartID, op.ID, checkedVal)
	}

	// Inject the actor into the relayed message — the system sentinel for
	// system-attributed writes, else the connection's authenticated userID.
	msg.UserID = actor
	msg.CartID = cartID
	relayed, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.BroadcastToCart(cartID, relayed, c.deviceID)

	sendAck(c, cartID, op.ID, msg.Ts, "applied")
}

// handleSessionStart / handleSessionEnd read the cart from the MESSAGE — the
// connection no longer names one — and relay a re-marshalled envelope rather
// than the raw frame, so the cartID peers receive is the normalised one this
// connection was verified against.
func handleSessionStart(c *Client, h *Hub, rawMsg []byte) {
	msg, cartID, ok := parseSessionMsg(c, h, rawMsg)
	if !ok {
		return
	}
	h.SetActiveShopping(cartID, c.userID, msg.StoreID)
	relaySessionMsg(c, h, msg, cartID)
}

func handleSessionEnd(c *Client, h *Hub, rawMsg []byte) {
	msg, cartID, ok := parseSessionMsg(c, h, rawMsg)
	if !ok {
		return
	}
	h.ClearActiveShopping(cartID, c.userID)
	relaySessionMsg(c, h, msg, cartID)
}

// parseSessionMsg decodes a presence envelope and applies both guards: the
// sender may only speak for themselves, and only about a cart this connection
// subscribed to.
func parseSessionMsg(c *Client, h *Hub, rawMsg []byte) (MemberSessionMsg, string, bool) {
	var msg MemberSessionMsg
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return msg, "", false
	}
	if msg.UserID != c.userID {
		return msg, "", false // silently drop mismatched userID
	}
	cartID := strings.ToLower(msg.CartID)
	if !h.IsSubscribed(c, cartID) {
		return msg, "", false
	}
	return msg, cartID, true
}

func relaySessionMsg(c *Client, h *Hub, msg MemberSessionMsg, cartID string) {
	msg.CartID = cartID
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.BroadcastToCart(cartID, b, c.deviceID)
}
