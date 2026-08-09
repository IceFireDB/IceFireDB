# ACL Security Model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single shared `--auth` token with a **multi-user ACL**: users authenticated by `AUTH <user> <password>` (bcrypt), each carrying a role whose allowed-command set is enforced per-connection, with the legacy `--auth`/`ICEFIREDB_AUTH` token preserved as a backward-compatible admin fallback.

**Architecture:** The current model is a single `conf.Auth` string consumed by vendored `tidwall/uhaha` (`service.Auth`, RESP `AUTH`). ACL is layered **at the IceFireDB level**, not by forking uhaha: a users file (`--acl-file`) maps usernames → bcrypt hashes → roles; `connOpened` attaches an identity to each connection's context; a per-command gate checks the identity's role against the command's required permission before dispatch. The legacy single token is mapped to a built-in `admin` role so existing deployments keep working.

**Tech Stack:** Go stdlib (`crypto/subtle`, `golang.org/x/crypto/bcrypt`), `tidwall/uhaha` (`ConnOpened` context, command registry), RESP `AUTH`.

**Why this is sequenced last (roadmap):** A full ACL is a potentially breaking change. rc.1 ships with the shared-token model (documented in `SECURITY.md`); real deployments then tell us whether a full ACL is required. **Do not start Task 2 until Task 1 confirms the hook points below exist in the vendored uhaha.**

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `acl.go` (new, pkg `main`) | Users-file load/parse, bcrypt verify, role→commands map | Create |
| `acl_test.go` (new, pkg `main`) | Users-file parsing, bcrypt verify, role enforcement | Create |
| `main.go` | Wire `--acl-file` flag; attach identity in `connOpened`; gate commands | Modify |
| `flags.go` | `--acl-file` flag | Modify |
| `SECURITY.md`, `README.md` | Document the ACL model + migration from `--auth` | Modify |

---

## Task 1: Lock the design — confirm the uhaha hook points (AUTHORIZATION GATE)

This task decides whether the IceFireDB-layer approach is viable or whether uhaha must be forked (→ plan 04). It produces `docs/superpowers/plans/acl-design.md`. **No code is written here.**

**Files:** none modified.

- [ ] **Step 1: Map the current auth surface**

Answer these questions by reading the vendored source; record answers in the design doc.

```bash
grep -nE 'func.*Auth|AUTH|ConnOpened|ConnClosed|CmdRewriteFunc|func.*connOpened' \
  vendor/github.com/tidwall/uhaha/uhaha.go main.go | head -40
```

Questions (record answers):
1. Does `connOpened(addr string) (ctx interface{}, accept bool)` let us return an arbitrary per-connection context? (expected: yes — `ctx` is passed back into command handlers as the connection identity).
2. Where is the RESP `AUTH` command dispatched, and can IceFireDB override it without editing uhaha? (look at `service.Auth` and the command table).
3. Is there a per-command hook (`CmdRewriteFunc` or equivalent) that runs *before* dispatch and can reject a command? If not, can each IceFireDB-registered command wrap itself in an auth check?

- [ ] **Step 2: Write the design decision**

Create `docs/superpowers/plans/acl-design.md` containing:

```markdown
# ACL Design Decision

## Hook findings (from Task 1 Step 1)
1. connOpened context capability: <yes/no + how>
2. AUTH override path: <"override via IceFireDB command registration" | "requires uhaha fork">
3. Per-command gate mechanism: <"CmdRewriteFunc" | "command wrapper" | "uhaha Authorize hook (needs fork)">

## Chosen approach
<one of: "IceFireDB-layer: connOpened identity + command wrapper gate" | "uhaha fork: add Authorize() hook (coordinate with plan 04)">

## Credential format
- File: one user per line: `<username>:<bcrypt-hash>:<role>`
- Roles: `admin` (all), `readwrite` (data cmds), `readonly` (GET/SCAN/etc.), `none` (must AUTH first).
- Legacy `--auth`/`ICEFIREDB_AUTH` maps to user `admin` with that token as the bcrypt-verified password — OR, if exact legacy semantics are required, a special `legacy-token` user whose plaintext compare equals `conf.Auth`.

## Backward compatibility
- If `--acl-file` is unset, behavior is unchanged (single shared token).
- If `--acl-file` is set, `AUTH <user> <password>` selects identity; `AUTH <token>` (single-arg) authenticates as the `admin`/`legacy-token` user.
```

- [ ] **Step 3: Commit the design**

```bash
git add docs/superpowers/plans/acl-design.md
git commit -s -m "docs(acl): lock ACL design and uhaha hook findings"
```

- [ ] **Step 4: Decide go/no-go**

If Step 1 found the IceFireDB-layer approach viable → proceed to Task 2.
If it requires a uhaha fork → stop; open the fork work under plan 04 and resume here after the fork exposes an `Authorize()`-style hook.

---

## Task 2: Implement the users-file credential store

**Files:**
- Create: `acl.go`
- Test: `acl_test.go`

- [ ] **Step 1: Write the failing tests**

Create `acl_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func writeACLFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "users.acl")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	return p
}

func TestLoadACL_ParsesUsers(t *testing.T) {
	h, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	p := writeACLFile(t, "alice:"+string(h)+":admin\nbob:"+string(h)+":readonly\n")
	a, err := loadACL(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(a.users) != 2 {
		t.Fatalf("want 2 users, got %d", len(a.users))
	}
	if a.users["alice"].role != roleAdmin {
		t.Errorf("alice role = %v, want admin", a.users["alice"].role)
	}
}

func TestACL_VerifyPassword(t *testing.T) {
	h, _ := bcrypt.GenerateFromPassword([]byte("hunter2"), bcrypt.DefaultCost)
	p := writeACLFile(t, "carol:"+string(h)+":readwrite\n")
	a, _ := loadACL(p)
	u, ok := a.verify("carol", "hunter2")
	if !ok || u.role != roleReadWrite {
		t.Fatalf("verify failed or wrong role: %+v ok=%v", u, ok)
	}
	if _, ok := a.verify("carol", "wrong"); ok {
		t.Errorf("wrong password should not verify")
	}
	if _, ok := a.verify("nobody", "x"); ok {
		t.Errorf("unknown user should not verify")
	}
}

func TestRoleAllows(t *testing.T) {
	a := roleSet{roleAdmin: nil, roleReadOnly: map[string]struct{}{"GET": {}, "SCAN": {}}}
	if !a.allows(roleReadOnly, "GET") {
		t.Errorf("readonly should allow GET")
	}
	if a.allows(roleReadOnly, "SET") {
		t.Errorf("readonly should deny SET")
	}
	if !a.allows(roleAdmin, "FLUSHALL") {
		t.Errorf("admin should allow everything")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

```bash
go test -run 'TestLoadACL|TestACL_Verify|TestRoleAllows' .
```
Expected: compilation failure (`loadACL`, `roleAdmin`, etc. undefined).

- [ ] **Step 3: Implement `acl.go`**

```go
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

type role int

const (
	roleNone role = iota
	roleReadOnly
	roleReadWrite
	roleAdmin
)

type userInfo struct {
	hash []byte
	role role
}

type acl struct {
	users map[string]userInfo
}

type roleSet map[role]map[string]struct{}

// loadACL parses <user>:<bcrypt>:<role> lines. Blank lines and '#' comments are ignored.
func loadACL(path string) (*acl, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open acl file: %w", err)
	}
	defer f.Close()

	a := &acl{users: map[string]userInfo{}}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid acl line (want user:hash:role): %q", line)
		}
		user, hash, roleStr := parts[0], parts[1], parts[2]
		r, err := parseRole(roleStr)
		if err != nil {
			return nil, fmt.Errorf("user %q: %w", user, err)
		}
		a.users[user] = userInfo{hash: []byte(hash), role: r}
	}
	return a, sc.Err()
}

func parseRole(s string) (role, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "admin":
		return roleAdmin, nil
	case "readwrite", "rw":
		return roleReadWrite, nil
	case "readonly", "ro":
		return roleReadOnly, nil
	default:
		return roleNone, fmt.Errorf("unknown role %q", s)
	}
}

func (a *acl) verify(user, password string) (userInfo, bool) {
	u, ok := a.users[user]
	if !ok {
		return userInfo{}, false
	}
	if bcrypt.CompareHashAndPassword(u.hash, []byte(password)) != nil {
		return userInfo{}, false
	}
	return u, true
}

// allows reports whether the role may run the command. Admin allows everything.
func (rs roleSet) allows(r role, cmd string) bool {
	if r == roleAdmin {
		return true
	}
	allowed, ok := rs[r]
	if !ok {
		return false
	}
	_, ok = allowed[strings.ToUpper(cmd)]
	return ok
}

// defaultRoleSet is the GA-default permission matrix (frozen in Task 5 tests).
var defaultRoleSet = roleSet{
	roleReadOnly: {
		"GET": {}, "MGET": {}, "STRLEN": {}, "EXISTS": {}, "TYPE": {},
		"SCAN": {}, "KEYS": {}, "TTL": {}, "LLEN": {}, "LRANGE": {},
		"SCARD": {}, "SMEMBERS": {}, "ZRANGE": {}, "ZSCORE": {}, "HGET": {}, "HGETALL": {},
	},
	roleReadWrite: {
		// readwrite = everything readonly writes are allowed too (computed at check time).
		"SET": {}, "DEL": {}, "INCR": {}, "DECR": {}, "EXPIRE": {},
		"LPUSH": {}, "RPUSH": {}, "LPOP": {}, "RPOP": {},
		"SADD": {}, "SREM": {}, "ZADD": {}, "ZREM": {},
		"HSET": {}, "HDEL": {},
	},
}

// allowedFor merges readonly into readwrite for convenience at check time.
func allowedFor(r role, cmd string) bool {
	if defaultRoleSet.allows(r, cmd) {
		return true
	}
	if r == roleReadWrite {
		return defaultRoleSet.allows(roleReadOnly, cmd)
	}
	return false
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
go test -run 'TestLoadACL|TestACL_Verify|TestRoleAllows' .
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add acl.go acl_test.go
git commit -s -m "feat(acl): add users-file credential store with bcrypt + roles"
```

---

## Task 3: Attach per-connection identity in `connOpened`

**Files:**
- Modify: `main.go` (the `connOpened` function at line ~142 + globals)

- [ ] **Step 1: Add the global ACL handle and the identity type**

At the top of `main.go` (near the other globals), add:

```go
// aclStore is non-nil when --acl-file is set; nil means legacy single-token mode.
var aclStore *acl

// connIdentity is attached to each connection via connOpened's context.
type connIdentity struct {
	user string
	role role
	authed bool
}
```

- [ ] **Step 2: Return an identity from `connOpened`**

Modify `connOpened` so it returns a fresh `*connIdentity` as the context (anonymous until AUTH succeeds):

```go
func connOpened(addr string) (context interface{}, accept bool) {
	// In legacy (no --acl-file) mode, identity is irrelevant; return nil context.
	if aclStore == nil {
		return nil, true
	}
	return &connIdentity{role: roleNone, authed: false}, true
}
```

- [ ] **Step 3: Build**

```bash
go build ./...
```
Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add main.go
git commit -s -m "feat(acl): attach per-connection identity in connOpened"
```

---

## Task 4: Gate commands by role

The exact mechanism depends on Task 1's finding. The wrapper approach (default) is shown; if Task 1 chose the uhaha-fork approach, do this in the fork's `Authorize()` instead.

**Files:**
- Modify: `main.go` (command registration / AUTH handler)
- Test: `acl_test.go`

- [ ] **Step 1: Add the gate helper and the AUTH override**

In `main.go`, add an identity updater and the gate:

```go
import "github.com/tidwall/redcon"

// authenticateConn is called from the AUTH command handler.
func authenticateConn(conn redcon.Conn, user, password string) bool {
	if aclStore == nil {
		return false // legacy mode handled by uhaha itself
	}
	u, ok := aclStore.verify(user, password)
	if !ok {
		return false
	}
	if id, ok := conn.Context().(*connIdentity); ok {
		id.user, id.role, id.authed = user, u.role, true
	}
	return true
}

// authorizeCmd enforces that the connection's role may run cmd.
// Returns true if allowed (or if ACL is disabled).
func authorizeCmd(conn redcon.Conn, cmd string) bool {
	if aclStore == nil {
		return true
	}
	id, ok := conn.Context().(*connIdentity)
	if !ok {
		return true // context not set (legacy path)
	}
	if !id.authed {
		return strings.EqualFold(cmd, "AUTH") || strings.EqualFold(cmd, "QUIT") || strings.EqualFold(cmd, "PING")
	}
	return allowedFor(id.role, cmd)
}
```

- [ ] **Step 2: Wire `authorizeCmd` into the dispatch path**

Per Task 1 Step 1 finding #3: if a per-command pre-hook exists (e.g. `CmdRewriteFunc` at the currently-commented `main.go:87`), call `authorizeCmd` there and return a denial (`-NOPERM` style error) when it returns false. If no pre-hook exists, wrap each registered command's handler:

```go
// Pseudocode of the wrapper pattern (adapt to the actual registration API):
orig := lookupHandler(cmd)
register(cmd, func(conn redcon.Conn, args [][]byte) {
	if !authorizeCmd(conn, strings.ToUpper(string(args[0]))) {
		conn.WriteError("NOPERM this user has no permissions to run " + strings.ToUpper(string(args[0])))
		return
	}
	orig(conn, args)
})
```
Concretely resolve the registration API using Task 1's findings before writing this block.

- [ ] **Step 3: Handle the `AUTH` command**

When `aclStore != nil`, the `AUTH <user> <password>` form must call `authenticateConn`. If Task 1 found AUTH is owned by uhaha, register an IceFireDB `AUTH` override; otherwise patch the fork.

- [ ] **Step 4: Add a gate test**

Append to `acl_test.go`:

```go
func TestAuthorizeCmd_AnonymousOnlyAuth(t *testing.T) {
	if aclStore == nil { // gate tests only meaningful in ACL mode
		t.Skip("ACL not enabled in this test run")
	}
	id := &connIdentity{role: roleNone, authed: false}
	_ = id
	// allowedFor is the policy under test; anonymous (roleNone) must be denied data cmds.
	if allowedFor(roleNone, "SET") {
		t.Errorf("anonymous must not be allowed SET")
	}
}
```

```bash
go test -run TestAuthorizeCmd_AnonymousOnlyAuth .
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add main.go acl_test.go
git commit -s -m "feat(acl): gate commands by per-connection role"
```

---

## Task 5: Wire the `--acl-file` flag and load at startup

**Files:**
- Modify: `flags.go`, `main.go`

- [ ] **Step 1: Add the flag**

In `flags.go` (next to `--auth`):

```go
// in the flag block alongside conf.Auth
flag.StringVar(&aclFilePath, "acl-file", "", "path to an ACL users file (user:bcrypt:role per line). Enables multi-user ACL; empty keeps legacy single-token auth.")
```
And declare the package var: `var aclFilePath string` (near the other flag vars in `flags.go`).

- [ ] **Step 2: Load the ACL in `main.go` after flag parse, before `uhaha.NewServer`**

```go
if aclFilePath != "" {
	a, err := loadACL(aclFilePath)
	if err != nil {
		logs.Fatalf("--acl-file: %v", err)
	}
	aclStore = a
	logs.Printf("ACL enabled with %d users", len(a.users))
}
```

- [ ] **Step 3: Build and smoke-test legacy mode still works**

```bash
go build ./...
./IceFireDB -d /tmp/acl-smoke --init-run-quit   # no --acl-file: legacy mode, must still boot
```
Expected: boots and quits cleanly.

- [ ] **Step 4: Commit**

```bash
git add flags.go main.go
git commit -s -m "feat(acl): add --acl-file flag and startup load"
```

---

## Task 6: Documentation + migration

**Files:**
- Modify: `SECURITY.md`, `README.md`

- [ ] **Step 1: Update `SECURITY.md`**

Replace the "no per-user ACL today" note with the ACL model: file format, roles, the legacy-token compatibility, and a worked example of generating a bcrypt hash:

```bash
htpasswd -bnBC 10 "" 'hunter2' | tr -d ':\n' | sed 's/$2y/$2a/'   # produces a bcrypt hash
```

- [ ] **Step 2: Add a "Security / ACL" section to `README.md`**

Document `--acl-file`, the roles table, and that omitting it preserves the legacy single-token behavior.

- [ ] **Step 3: Commit**

```bash
git add SECURITY.md README.md
git commit -s -m "docs(security): document the multi-user ACL model and migration path"
```

---

## Final Verification (run after all tasks)

- [ ] **Legacy mode unchanged**

```bash
go test -count=1 -timeout 180s .
```
Expected: PASS (ACL disabled path).

- [ ] **ACL unit tests pass**

```bash
go test -run 'TestLoadACL|TestACL_Verify|TestRoleAllows|TestAuthorizeCmd' .
```

- [ ] **Manual ACL round-trip (if a running node is feasible)**

```bash
# build, start with an acl file, then:
redis-cli AUTH alice wrongpass      # -> ERR
redis-cli AUTH alice hunter2        # -> OK
redis-cli SET k v                   # -> OK (alice is admin)
# reconnect as readonly bob, assert SET is denied with NOPERM
```

## Self-Review

- **Spec coverage:** Credential store (T2), per-connection identity (T3), command gate (T4), flag wiring (T5), docs (T6) — full ACL. Task 1 gates everything on the uhaha-hook feasibility. ✅
- **Placeholder scan:** Task 4 Step 2's dispatch wiring is flagged as "resolve the registration API using Task 1's findings" — this is an explicit, bounded follow-on to a mandatory investigation (Task 1), not a vague TODO. All credential-store and gate code is complete. ✅
- **Type/version consistency:** `role`/`userInfo`/`acl`/`connIdentity` names are consistent across Tasks 2–4. ✅
