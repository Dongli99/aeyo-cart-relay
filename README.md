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

**4. This is not accepting contributions.** Issues are disabled. Pull requests cannot be
turned off on a public GitHub repository, so they stay technically possible — they will be
closed unread, and that is not meant unkindly. This is a published-for-inspection mirror of
an internal repository, not a community project. It is here to be read, not to be developed.

If you find something genuinely wrong — a security problem, or a claim in this file that
the code does not support — that is worth hearing: **dongliliu0@gmail.com**.

---

## The authentication model, stated plainly

**A WebSocket connection belongs to one person and carries every cart they are in. It proves
each of those carts separately, with a secret only that member holds.**

One connection per device, not per cart. The client opens `/ws` and its first message names
the carts it wants, each with a `subKey` — a 128-bit secret minted for that member of that
cart at create or join time, returned to that member alone and never broadcast, never in a
state message, never in another member's response. The server resolves each key back to the
`(cart, member)` pair it was minted for
([`handlers.go`](handlers.go), `verifySubscription`). The connection's identity comes from
those keys; nothing about who you are is taken on your word.

The same key authenticates **every cart-scoped request**, not only the socket
([`handlers.go`](handlers.go), `authorizeCartActor`): leaving a cart, revoking a member,
transferring ownership, reading or rotating the invite secret, and the two consumption-reservoir
endpoints behind cadence learning. One credential per membership, not one per surface — and the
cart a request acts on is taken from the key rather than from the URL or the body, so a valid key
cannot reach a cart it was not minted for.

**Naming who you are acting *on* is fine; naming who you *are* is not.** Requests still carry
their target in the body — the member being revoked, the incoming owner — because a target is
checkable against the room. The actor is not: a self-asserted actor is a claim, and comparing a
claim against a fact only resembles authorization.

Two endpoints are exempt, by construction rather than oversight: **create** and **join** are where
keys come from, so no key can exist yet. Create makes a new room owned by the caller, so an
unproved identity there grants nothing; join is gated by the invite secret, a credential the
caller must have been given.

**Create lets the caller propose the room's id, and that is worth stating in full**, because it is
the one place the exemption above was re-examined rather than inherited. A cart's id is immutable
once shared — every pending write, invite link and member device keys off it — so a client whose
room has been deleted can only recover the cart *under its own id*; minting a new one would strand
everything that already refers to the old. Sending `cartID` is therefore allowed, and omitting it
mints one as before.

What create will not do with a proposed id is the substance of it. **An id whose room exists is
refused** — never adopted, never re-owned, never re-secreted — and **an id with no room but with
surviving rows is refused too**. The second refusal is unreachable in a healthy database, where
deleting a room takes its members, items and events with it; it is there so that "create touches no
existing data" is true on its own terms rather than on the strength of a cascade, and if it ever
fires it is telling us the cascade stopped.

The residual, stated plainly because the point of publishing this is that a reader can check it:
**every member of a cart knows its id**, so once a room is deleted an ex-member can re-create it
with themselves as owner. What they get is an empty room and a fresh secret. They get no items, no
members, and no way to draw the real members in — those devices hold the old secret, and their join
is refused against the new one. The cost is that the cart loses its identifier and its owner must
re-share under a new one; the gain to the squatter is nothing. It is available only to someone who
was already a member of that cart, and only after its room is gone.

A pair that does not verify costs that one cart and nothing else — a member removed from one
cart keeps syncing every other cart they are in. A key belonging to a different person is
refused outright, so a connection cannot widen into somebody else's carts.

Keys are per membership, so revocation is local: removing a member deletes their row and
their key stops resolving in the same statement, while every other member's key keeps
working. The server also ends that member's subscription on any connection they still hold —
the socket now outlives the membership, so ending it is the server's job rather than the
client's good behaviour.

**Until 2026-08 this was much weaker, and the previous version of this file said so —
though it did not say all of it.** The socket was per cart, identity was a self-asserted
`X-User-ID` header, and any member of a cart could connect as any other member of that same
cart. That was survivable while the cart ID was half of the address; it stopped being survivable
the moment one connection could carry everything a person is in, which is why the credential
above landed as part of the same change rather than after it.

What the old disclosure **understated** is worth stating plainly, since the point of publishing
this is that a reader can check it. The same self-asserted-identity shape reached further than the
socket: because every member learns the owner's user ID from every state message, any member of a
cart could also **transfer that cart's ownership to themselves**, **revoke any other member**,
**force another member out**, and **read or rotate the invite secret** — each by writing the
owner's ID into their own request. Those handlers compared a claim against a fact, which looks
like an authorization check and is not one. All four were moved to the key in the same 2026-08
work, and the tests that pin them assert the *refusal* — a non-owner member's key rejected where
a claimed owner ID previously succeeded.

It is **not** the case that a non-member can read your cart — the cart ID plus an invite
secret are required to become a member, and membership is what mints a key.

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
handlers.go    HTTP endpoints — cart create/join/leave/revoke/transfer, invite, history;
               the WebSocket handshake and its subscription check
hub.go         WebSocket fan-out to the members of a cart, and the per-connection message budget
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

Listens on `:8080`. It is meant to sit behind a reverse proxy that terminates TLS and
forwards `/api/*` and `/ws`.

The `members` table gained a column in 2026-08 and there is no migration for it: the schema
is created only on a fresh database, so an older `aeyo.db` makes startup fail rather than run
half-configured. Point `AEYO_DB_PATH` at a new file, or delete the old one.

```bash
go test ./...
```

Go 1.22+. The SQLite driver is pure Go, so there is no CGO and no C toolchain required, and
the resulting binary is static.

### Configuration

Every environment variable the binary reads. This list is the contract: the process that
runs it sets the values, and this file is the only place that says what they mean.

| Variable | Default | Effect |
|---|---|---|
| `AEYO_DB_PATH` | `./aeyo.db` | Where the SQLite file lives. The directory must exist and be writable. |
| `AEYO_PREMIUM_FOR_ALL` | unset | When `true`, every cart owner is treated as premium: the join capacity check is bypassed and every member is reported as premium. Used during the public beta so testers can share without limits. Unset it to reinstate per-tier limits. |

There are no others, and there are **no secrets here** — nothing this server does requires
a credential, so none is read, stored, or needed to run it.

---

## Licence

[Apache License 2.0](LICENSE). You may run it, modify it, and self-host it.

Aeyo's value is the app and the people already in your cart, not this relay — so if you want
to run your own, that is genuinely fine. The name and logo are not covered by the licence.

---

*Published for inspection. Aeyo is built by one person; the app is at
[aeyo.app](https://aeyo.app) and its privacy policy at
[aeyo.app/privacy](https://aeyo.app/privacy).*
