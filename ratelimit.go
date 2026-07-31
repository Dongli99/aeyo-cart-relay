package main

import (
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Per-IP token-bucket rate limiter (join/create flood guard)
// ---------------------------------------------------------------------------

// tokenBucket is a single IP's allowance. tokens refills continuously at the
// limiter's rate up to burst; each allowed request costs one token.
type tokenBucket struct {
	tokens float64
	last   time.Time
}

// rateLimiter is an in-memory per-IP token bucket. Single-instance server, so a
// plain mutex-guarded map is sufficient (no external store, no new deps). A
// background sweep evicts idle (fully-refilled) buckets to bound memory.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64 // tokens added per second
	burst   float64 // bucket capacity
}

// newRateLimiter builds a limiter allowing bursts of `burst` requests, refilling
// at `rate` per second thereafter.
func newRateLimiter(rate, burst float64) *rateLimiter {
	rl := &rateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    rate,
		burst:   burst,
	}
	go rl.sweepLoop()
	return rl
}

// allow reports whether a request from ip may proceed, consuming a token if so.
func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[ip]
	if !ok {
		b = &tokenBucket{tokens: rl.burst, last: now}
		rl.buckets[ip] = b
	} else {
		b.tokens += now.Sub(b.last).Seconds() * rl.rate
		if b.tokens > rl.burst {
			b.tokens = rl.burst
		}
		b.last = now
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweepLoop periodically drops buckets that have fully refilled (idle callers),
// keeping the map proportional to active IPs rather than all-time IPs.
func (rl *rateLimiter) sweepLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		rl.mu.Lock()
		for ip, b := range rl.buckets {
			if b.tokens+now.Sub(b.last).Seconds()*rl.rate >= rl.burst {
				delete(rl.buckets, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// rateLimit wraps a handler, rejecting requests from an IP over its allowance
// with 429 rather than touching the DB.
func rateLimit(rl *rateLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !rl.allow(ip) {
			log.Printf("rateLimit: %s exceeded on %s", ip, r.URL.Path)
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r)
	}
}

// clientIP returns the originating client IP. Behind Caddy the real address is
// the first entry of X-Forwarded-For; RemoteAddr is the local proxy hop.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// ---------------------------------------------------------------------------
// Cart-ID hygiene
// ---------------------------------------------------------------------------

// isValidCartID reports whether s has the shape of a newUUID() cart id:
// 8-4-4-4-12 lowercase hex with hyphens (36 chars). Rejecting malformed ids up
// front avoids a pointless DB round-trip on garbage/probe input.
func isValidCartID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
