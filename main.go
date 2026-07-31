package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

// premiumForAll, when AEYO_PREMIUM_FOR_ALL=true, treats every cart owner as premium: the join
// capacity check is bypassed and every member's tier is reported as "premium" in state messages
// (which also clears the client's optimistic frozen check). TestFlight-only — unset the env var and
// restart to reinstate per-tier limits. StoreKit-validated premium is the production path
// (Family Sharing TDD §4.9 / security table).
var premiumForAll bool

func main() {
	dbPath := os.Getenv("AEYO_DB_PATH")
	if dbPath == "" {
		dbPath = "./aeyo.db"
	}

	premiumForAll = os.Getenv("AEYO_PREMIUM_FOR_ALL") == "true"
	if premiumForAll {
		log.Printf("⚠️ AEYO_PREMIUM_FOR_ALL enabled — all cart owners treated as premium (TestFlight mode)")
	}

	db, err := InitDB(dbPath)
	if err != nil {
		log.Fatalf("InitDB: %v", err)
	}
	defer db.Close()

	// Hourly tombstone pruning.
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			pruneDeletedItems(db)
		}
	}()

	hub := NewHub()

	// Per-IP flood guard on the unauthenticated entry points (create/join):
	// burst of 15, refilling 1/sec. Generous for a human tapping share/join and
	// reconnect retries, tight enough to blunt scripted hammering.
	joinLimiter := newRateLimiter(1, 15)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/cart/create", rateLimit(joinLimiter, createCartHandler(db)))
	mux.HandleFunc("POST /api/cart/join", rateLimit(joinLimiter, joinCartHandler(db, hub)))
	mux.HandleFunc("POST /api/cart/leave", leaveCartHandler(db, hub))
	mux.HandleFunc("POST /api/cart/revoke", revokeHandler(db, hub))
	mux.HandleFunc("POST /api/cart/{cartID}/invite", inviteHandler(db, hub))
	mux.HandleFunc("POST /api/cart/transfer", transferHandler(db, hub))
	mux.HandleFunc("POST /api/user/tier", updateTierHandler(db))
	mux.HandleFunc("GET /api/cart/{cartID}/history", historyReadHandler(db))
	mux.HandleFunc("POST /api/cart/{cartID}/history/backfill", historyBackfillHandler(db))
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("/ws/", wsHandler(db, hub))

	log.Printf("aeyo-cart-relay listening on :8080 (db: %s)", dbPath)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
