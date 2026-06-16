# 004. Admin API on loopback with no auth

Phantom's admin endpoints (chains list/get/delete, token cache list/push/delete, stats) ride in the same FastAPI process as the ingest endpoints but via a separate router, bound by default to loopback or a Unix domain socket with no authentication — the trust boundary is "in the producer container/host" and matches the producer stack's existing convention of unauthenticated internal processes (e.g., a token-manager component running in the producer process). Bearer values in the token cache are never returned by the API (the slot listing returns only metadata — endpoint, uid, last_updated, status); bulk-destructive endpoints (`DELETE /v1/admin/chains`, `DELETE /v1/admin/tokens`) require an explicit filter parameter and reject empty calls. Operators that need same-network exposure can put a reverse proxy with auth between the network and the loopback admin port; that is not part of Phantom.

Status: Accepted
Date: 2026-05-12

---

Amendment 2026-06-13 (R12-1) - the loopback bind is now enforced by the composition, not just documented. This is an AMENDMENT, not a new ADR: it makes the original decision true rather than reversing it.

Until R12-1 the admin router was co-mounted on the public-ingress application and the process bound only `server.bind_tcp` (default `0.0.0.0:8080`); the `server.admin_bind_*` settings had no run-path consumer. So the destructive admin endpoints were reachable, unauthenticated, on every public interface - the documented loopback trust boundary was not enforced (the unauthenticated `DELETE /v1/admin/chains` could destroy accepted-but-undelivered uploads).

The fix (`phantom.app.create_app` -> `PhantomApps(public, admin)`; `phantom.__main__` runs two `uvicorn.Server` instances): the admin router is served on its OWN ASGI application bound to a SEPARATE socket - `server.admin_bind_uds` (precedence) else `server.admin_bind_host` : `server.admin_bind_port`, defaulting to `127.0.0.1:8081`. The public ingress application (which also serves the public liveness/readiness probes `GET /v1/healthz` and `GET /v1/readyz`) never mounts a destructive admin route. The bind knobs are restart-required (ADR-013 table).

- **Loopback default, remote opt-in.** The default keeps the admin surface loopback-only. A non-loopback `admin_bind_host` (anything outside `{127.0.0.1, ::1, localhost}`) is an explicit operator opt-in; the launcher emits a prominent startup warning naming the host, stating the admin API is unauthenticated, and instructing the operator to front it with an authenticating reverse proxy. The service keeps serving (the opt-in is deliberate).
- **Collision rejected.** An admin bind that collides with the ingress bind (same TCP host:port - including a wildcard overlap - or the same UDS path) is rejected at config validation: two servers cannot share one socket.
- **Future work (NOT yet implemented; candidate for its own ADR when scheduled):** an OPTIONAL configured admin secret, checked by a dependency on the admin application, so an operator can require a credential without standing up a reverse proxy. Unset = today's loopback-trust behavior (no behavior change); set = the admin app refuses unauthenticated calls. This is recorded as a reviewer-queue to-do; it is out of scope for R12-1.

---

Amendment 2026-06-13 (single-listener collapse) - SUPERSEDES the R12-1 two-listener split above, on the same decision. This too is an amendment, not a reversal: the loopback bind is STILL the admin access control; only the mechanism that enforces it is simpler.

The deployment is decided same-machine-only: Phantom runs on the SAME box as its producer and is reached over loopback. For that deployment the two-listener split (a separate admin socket) bought nothing - admin was already reachable only on the machine - and it introduced two bugs (R13-1: the admin app had no lifespan, so its socket accepted destructive ops before the runtime was ready; R13-2: the cross-bind collision validator compared host strings and missed the `localhost` <-> `127.0.0.1` alias). So the split was tried and collapsed.

The collapsed design (`phantom.app.create_app` -> a single `FastAPI`; `phantom.__main__` runs ONE `uvicorn` server): ONE listener serves intake (`POST /v1/send`), the admin surface (`/v1/admin/*`), and the public liveness/readiness probes (`GET /v1/healthz` / `GET /v1/readyz`) on one socket. `server.bind_tcp` defaults to `127.0.0.1:8080` (loopback) or `server.bind_uds` when set. THE LOOPBACK DEFAULT BIND IS THE ACCESS CONTROL: admin, like everything, is reachable only on the machine.

- **R12-1 stays fixed.** The original exposure finding (admin reachable unauthenticated off-box because the single app bound `0.0.0.0` by default) is fixed by the loopback default bind: nothing is reachable off-box by default, so admin is not either.
- **R13-1 + R13-2 are eliminated by construction.** uvicorn binds the one socket only after the lifespan startup completes, so the destructive admin surface is never served before the runtime is ready (no startup-ordering window). One bind cannot collide with itself, so there is no cross-bind collision validator and no alias gap.
- **Loopback default, remote opt-in (unchanged in spirit).** A non-loopback `bind_tcp` host is an explicit operator opt-in; the launcher emits a prominent startup warning naming the host, stating the admin API rides this listener and is unauthenticated, and instructing an authenticating reverse proxy. The service keeps serving.
- **The `admin_bind_*` settings and the collision validator are removed** (dead with one bind). The restart-required bind knobs are `server.bind_tcp` + `server.bind_uds` (ADR-013 table).
- **Future work (unchanged, NOT implemented):** for an off-box deployment, an OPTIONAL configured admin secret (checked by a dependency on the app) AND HTTPS termination. Unset secret = today's loopback-trust behavior. Both are recorded as reviewer-queue to-dos; neither is in scope for the collapse.
