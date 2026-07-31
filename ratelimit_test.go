package main

import "testing"

// A burst up to capacity is allowed; the next request from the same IP is
// rejected until tokens refill.
func TestRateLimiterBurstThenBlock(t *testing.T) {
	rl := newRateLimiter(1, 5) // 5 burst, 1/sec refill

	for i := 0; i < 5; i++ {
		if !rl.allow("1.2.3.4") {
			t.Fatalf("request %d within burst should be allowed", i+1)
		}
	}
	if rl.allow("1.2.3.4") {
		t.Errorf("6th rapid request should be rate-limited")
	}
	// A different IP has its own bucket.
	if !rl.allow("5.6.7.8") {
		t.Errorf("a different IP must not be limited by another IP's traffic")
	}
}

func TestIsValidCartID(t *testing.T) {
	valid := newUUID()
	if !isValidCartID(valid) {
		t.Errorf("newUUID() output %q must validate", valid)
	}

	bad := []string{
		"",
		"not-a-uuid",
		"'; DROP TABLE items;--",
		"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",  // wrong charset
		"12345678-1234-1234-1234-1234567890AB",  // uppercase hex
		"12345678123412341234123456789012",      // no hyphens
		"12345678-1234-1234-1234-1234567890123", // too long
	}
	for _, s := range bad {
		if isValidCartID(s) {
			t.Errorf("malformed cartID %q must be rejected", s)
		}
	}
}
