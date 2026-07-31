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
	pingInterval   = 30 * time.Second
	pongDeadline   = 10 * time.Second
	writeDeadline  = 10 * time.Second
	rateWindowSize = 60 * time.Second
	rateLimitMsgs  = 30

	// maxWSMessageBytes caps a single inbound WebSocket frame. The only
	// legitimate client→server messages are a single-op delta (Task F6 rejects
	// multi-op) and the tiny session start/end envelopes; even a delta carrying
	// long free-text notes/specification is a few KB. 512 KB leaves ~64× headroom
	// over any real message while bounding a hostile client — gorilla closes the
	// connection with a 1009 and ReadMessage returns an error when it is exceeded.
	maxWSMessageBytes = 512 * 1024
)

// Client represents a single active WebSocket connection.
type Client struct {
	cartID      string
	userID      string
	deviceID    string
	conn        *websocket.Conn
	send        chan []byte
	msgCount    int
	windowStart time.Time
}

// Hub manages per-cart WebSocket connections and in-memory active-shopping state.
type Hub struct {
	mu             sync.RWMutex
	clients        map[string]map[*Client]bool  // cartID → set of clients
	activeShopping map[string]map[string]string // cartID → userID → storeID
}

// NewHub constructs an empty Hub.
func NewHub() *Hub {
	return &Hub{
		clients:        make(map[string]map[*Client]bool),
		activeShopping: make(map[string]map[string]string),
	}
}

// Register adds a client to the hub.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[c.cartID] == nil {
		h.clients[c.cartID] = make(map[*Client]bool)
	}
	h.clients[c.cartID][c] = true
}

// Unregister removes a client. If the user was in active-shopping, it relays
// member_session_end to remaining connected members automatically.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	wasActive := false
	if h.activeShopping[c.cartID] != nil {
		if _, ok := h.activeShopping[c.cartID][c.userID]; ok {
			delete(h.activeShopping[c.cartID], c.userID)
			wasActive = true
		}
	}
	if h.clients[c.cartID] != nil {
		delete(h.clients[c.cartID], c)
		if len(h.clients[c.cartID]) == 0 {
			delete(h.clients, c.cartID)
		}
	}
	h.mu.Unlock()

	if wasActive {
		msg := MemberSessionMsg{Type: "member_session_end", UserID: c.userID}
		b, _ := json.Marshal(msg)
		h.BroadcastToCart(c.cartID, b, c.deviceID)
	}
}

// BroadcastToCart sends msg to all clients in cartID except the one with excludeDeviceID.
func (h *Hub) BroadcastToCart(cartID string, msg []byte, excludeDeviceID string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients[cartID] {
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

// BroadcastToCartExcludeUser sends msg to all clients except those belonging to excludeUserID.
func (h *Hub) BroadcastToCartExcludeUser(cartID string, msg []byte, excludeUserID string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients[cartID] {
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

	c.windowStart = time.Now()

	for {
		_, rawMsg, err := c.conn.ReadMessage()
		if err != nil {
			// A clean disconnect is silent; an oversized frame (SetReadLimit →
			// 1009) or other protocol error is logged as the anomaly it is.
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("readPump: read error for %s/%s: %v", c.cartID, c.userID, err)
			}
			return
		}

		// Rate limit: >30 messages in any 60-second window → disconnect.
		now := time.Now()
		if now.Sub(c.windowStart) > rateWindowSize {
			c.msgCount = 0
			c.windowStart = now
		}
		c.msgCount++
		if c.msgCount > rateLimitMsgs {
			log.Printf("readPump: rate limit exceeded for %s/%s — disconnecting", c.cartID, c.userID)
			c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "rate limit exceeded"))
			return
		}

		handleMessage(c, h, db, rawMsg)
	}
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
	case "delta":
		handleDelta(c, h, db, rawMsg)
	case "member_session_start":
		handleSessionStart(c, h, rawMsg)
	case "member_session_end":
		handleSessionEnd(c, h, rawMsg)
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
		log.Printf("handleDelta: rejecting %d-op delta (exactly 1 required) for %s/%s", len(msg.Ops), c.cartID, c.userID)
		sendAck(c, msg.CartID, itemID, msg.Ts, "rejected")
		return
	}
	op := msg.Ops[0]
	itemID := op.ID

	if msg.CartID != c.cartID {
		log.Printf("handleDelta: cartID mismatch: msg=%s conn=%s", msg.CartID, c.cartID)
		sendAck(c, msg.CartID, itemID, msg.Ts, "rejected")
		return
	}

	actor := c.userID
	if op.Actor == systemActor {
		actor = systemActor
	}

	accepted, err := ApplyLWW(db, c.cartID, op.ID, op.Op, op.Fields, msg.Ts, msg.DeviceID, actor)
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
		captureConsumption(db, c.cartID, op.ID, checkedVal)
	}

	// Inject the actor into the relayed message — the system sentinel for
	// system-attributed writes, else the connection's authenticated userID.
	msg.UserID = actor
	relayed, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.BroadcastToCart(c.cartID, relayed, c.deviceID)

	sendAck(c, msg.CartID, op.ID, msg.Ts, "applied")
}

func handleSessionStart(c *Client, h *Hub, rawMsg []byte) {
	var msg MemberSessionMsg
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return
	}
	if msg.UserID != c.userID {
		return // silently drop mismatched userID
	}
	h.SetActiveShopping(c.cartID, c.userID, msg.StoreID)
	h.BroadcastToCart(c.cartID, rawMsg, c.deviceID)
}

func handleSessionEnd(c *Client, h *Hub, rawMsg []byte) {
	var msg MemberSessionMsg
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return
	}
	if msg.UserID != c.userID {
		return // silently drop mismatched userID
	}
	h.ClearActiveShopping(c.cartID, c.userID)
	h.BroadcastToCart(c.cartID, rawMsg, c.deviceID)
}
