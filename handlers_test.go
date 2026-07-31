package main

import (
	"net/http"
	"testing"
)

// The WebSocket origin policy is the one security-relevant behaviour that is decided
// by a header's ABSENCE, which makes it easy to break without noticing: a change that
// makes CheckOrigin permissive again looks harmless and passes every other test.
func TestCheckOriginAllowsOnlyOriginlessHandshakes(t *testing.T) {
	cases := []struct {
		name   string
		origin string
		want   bool
	}{
		{"native client sends no Origin", "", true},
		{"browser on another site", "https://evil.example", false},
		{"browser on our own site is still a browser", "https://aeyo.app", false},
		{"empty-ish but present", " ", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest("GET", "/ws/cart-a", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := upgrader.CheckOrigin(r); got != tc.want {
				t.Errorf("CheckOrigin(Origin=%q) = %v, want %v", tc.origin, got, tc.want)
			}
		})
	}
}
