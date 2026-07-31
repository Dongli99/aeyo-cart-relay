# aeyo-cart-relay

The shared-cart relay for [Aeyo](https://aeyo.app), an iOS shopping-list app.

Go · SQLite · WebSockets. One binary, no external services.

This server exists for exactly one feature: sharing a cart with other people. If you never
share a cart, your device never talks to this code.

---

## Why this is published

Aeyo's privacy claims are specific and checkable, and the weakest form of a privacy claim
is "trust my server." So here is the server.

Read what it does.

---

## What this is *not*

Four honest limits, stated up front rather than left to be discovered:

**1. This is not proof of what's deployed.** It is the source that the running binary is
built from, published in good faith. There are no reproducible builds here, so nothing in
this repository can prove that the process answering `aeyo.app` is this code. Anyone
telling you a public repo proves a deployment is overselling it, and so would we be.

**2. This is not the app.** Aeyo's actual work — predicting what you need, learning your
cadence, categorising items — runs on your phone, in Swift, and is not published. This
server is deliberately dumb: it stores rows and forwards them between the members of a
cart. It has no model, no inference, and no idea what any of it means. That is the design,
not an omission.

**3. This is not the community-signal path.** Aeyo's privacy policy describes anonymous
preference and purchase signals that are pooled and purged on a 12-hour cadence. That runs
in **Apple's CloudKit, not here**, and there is no code in this repository that implements
it. If you came to verify that claim, this is the wrong repository and we would rather say
so than let you leave with a false impression that you checked it.

**4. This is not accepting contributions.** Issues and pull requests are closed. This is a
published-for-inspection mirror of an internal repository, not a community project. It is
here to be read, not to be developed.

---

## The authentication model, stated plainly

**Any member of a shared cart can connect to the WebSocket as any other member of that same
cart.**

The WebSocket handler takes the user's identity from a self-asserted `X-User-ID` header and
checks only that this ID belongs to the cart being joined
([`handlers.go`](handlers.go), `wsHandler`). There is no per-session token and no proof of
identity. Cart members already see each other's user IDs in check-off attribution, so
impersonating a co-member is straightforward.

This is known and accepted, not overlooked. A shared cart is a household: the people in it
already trust each other with the groceries, and the failure mode is a roommate marking milk
as bought under your name. It is disclosed here because a reader would otherwise find it
themselves and reasonably wonder what else went unmentioned.

It is **not** the case that a non-member can read your cart — membership is checked, and the
cart ID plus an invite secret are required to become a member. The weakness is strictly
*within* a cart, not across carts.

If the stakes ever change — carts between strangers, anything of value in a cart — the fix
is a per-member session token minted at join. That is on file and not yet needed.

---

## What it stores

The authoritative answer is the schema at the top of [`db.go`](db.go), which is deliberately
not restated here — a schema copied into a README drifts on the first migration and then
lies. Read it directly.

The parts that matter for privacy, with somewhere to check each:

- **No account.** No email, no password, no phone number, no login. The `users` table holds
  an opaque identifier supplied by the app, a display name, and a colour. (The app derives
  that identifier from the user's iCloud account record — that half happens on the device
  and is not visible in this repository.)
- **Cart contents are stored in the clear.** Item names, quantities, categories, stores, due
  dates, and check-off state live in the `items` table so that a member joining later can be
  sent the current state. This is not end-to-end encrypted, and claiming otherwise would be
  false.
- **A device identifier is stored per item row.** It is the tie-break when two devices edit
  the same item at the same timestamp. This is disclosed in Aeyo's privacy policy.
- **Deleted items are kept as tombstones for 30 days**, then removed
  ([`db.go`](db.go), `pruneDeletedItems`). Tombstones exist so a delete propagates to a
  device that was offline; a device gone longer than 30 days can miss one.
- **A capped log of check-off timestamps** is kept per cart and item lineage, so a member who
  joins later can warm-start their cadence learning instead of starting blind. It is capped
  at 20 rows and holds timestamps only — no prices, no locations, no receipts.
- **No location, ever.** There is no location column, no location field on the wire, and no
  code path that receives one. Aeyo uses location on-device only.
- **No analytics, no third-party services, no outbound requests.** This binary talks to
  SQLite on local disk and to connected clients. That is the complete list.

---

## Reading the code

```
main.go        routes, startup, the hourly tombstone prune
handlers.go    HTTP endpoints — cart create/join/leave/revoke/transfer, invite, history
hub.go         WebSocket fan-out to the members of a cart
db.go          schema, migrations, last-write-wins merge, the consumption reservoir
types.go       the wire format — JSON field names as the client sends them
ratelimit.go   per-IP token bucket on the unauthenticated entry points
```

Tests sit beside the files they cover (`db_test.go`, `handlers_test.go`, `hub_test.go`, `ratelimit_test.go`).
The merge semantics in `db.go` are the interesting part: concurrent edits from several
phones, resolved last-write-wins per field, with rules about what may and may not resurrect
a deleted row.

**Comments reference internal documents** — `ADR-039`, `TDD §3.3`, and similar. Those are
Aeyo's decision records and are not published. They are left in place because this is a
byte-for-byte copy of the internal source, not a cleaned-up version prepared for an
audience. A stripped copy would read better and be worth less.

---

## Build and run

```bash
go build -o aeyo-cart-relay .
./aeyo-cart-relay
```

Listens on `:8080`. Set `AEYO_DB_PATH` to choose where the SQLite file lives (default
`./aeyo.db`). It is meant to sit behind a reverse proxy that terminates TLS and forwards
`/api/*` and `/ws/*`.

```bash
go test ./...
```

Go 1.22+. The SQLite driver is pure Go, so there is no CGO and no C toolchain required, and
the resulting binary is static.

---

## Licence

[Apache License 2.0](LICENSE). You may run it, modify it, and self-host it.

Aeyo's value is the app and the people already in your cart, not this relay — so if you want
to run your own, that is genuinely fine. The name and logo are not covered by the licence.

---

*Published for inspection. Aeyo is built by one person; the app is at
[aeyo.app](https://aeyo.app) and its privacy policy at
[aeyo.app/privacy](https://aeyo.app/privacy).*
