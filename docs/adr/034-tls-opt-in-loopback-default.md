# 034. TLS is opt-in; the loopback bind is the default trust boundary

Status: Accepted
Date: 2026-07-14

## Context

Phantom is a same-machine buffering sidecar. It runs on the same host as its
producer and is reached over loopback. The single listener (intake, admin, and
health on one socket) binds `127.0.0.1:8080` by default. ADR-004 fixes the
trust boundary as "in the producer container or host": the admin API carries no
application-level authentication, and the loopback bind is its access control.

The listener can serve HTTPS. `server.tls.enabled` flips the one socket to TLS
with no second listener. When `cert_path` and `key_path` are unset, Phantom
auto-generates a self-signed certificate; an operator may instead supply a PEM
pair, optionally with an encrypted key.

The open question is whether TLS should be on by default. The answer follows
from the threat model:

- On the default loopback bind, producer-to-Phantom traffic never crosses a
  network. On a normal operating system loopback traffic is not observable by
  another process without root. A same-host root attacker can already read the
  process memory, the SQLite body store, and the token-cache database directly,
  so encrypting the loopback hop defends against nothing that host access does
  not already grant.
- A non-loopback bind (for example `0.0.0.0:8080`) is an explicit operator
  opt-in that sends cached bearer tokens and full request bodies across a real
  network, and exposes the unauthenticated admin API. This is the case where
  wire encryption matters.
- The auto-generated certificate is self-signed. Turning TLS on by default with
  that certificate would push every client to disable certificate verification
  (`verify=False` / `curl -k`), which is a worse security posture than plaintext
  on loopback because it trains callers to skip verification everywhere.

## Decision

TLS stays OFF by default and is an opt-in flag (`server.tls.enabled`). On the
loopback default bind, plaintext is the supported and recommended posture,
because the wire never leaves the host.

An operator who binds the listener to a non-loopback interface, or otherwise
exposes Phantom on a network, should enable TLS with an operator-supplied
certificate from a trusted certificate authority, or front Phantom with a
TLS-terminating reverse proxy. The self-signed auto-generation path is a
localhost convenience for smoke tests only; it is not intended for network use.

TLS is not a substitute for admin authentication. It encrypts the wire; it does
not authenticate callers. The admin API remains unauthenticated by design
(ADR-004), so a network-exposed deployment still needs an authenticating proxy
in front of the admin surface regardless of whether TLS is enabled.

## Consequences

- The default deployment is plaintext on loopback: the simplest posture, with
  no certificate management and no handshake cost, and it matches the same-host
  trust boundary of ADR-004.
- A non-loopback plaintext bind remains permitted but continues to emit the
  loud startup warning (`_warn_if_bound_non_loopback` in `app.py`) naming the
  host and the unauthenticated admin exposure.
- Operators reading the documentation now get a decision keyed to the bind
  rather than only the mechanics of the flag.

## Alternatives considered

- **TLS on by default.** Rejected. On the loopback default it defends against
  nothing under the stated threat model, and the self-signed auto-generation
  default would train clients to disable certificate verification.
- **Refuse to boot on a non-loopback plaintext bind.** Deferred, not adopted.
  This is a stronger secure-by-default posture: couple the TLS requirement to
  the actual exposure by hard-stopping a plaintext bind on a network interface
  unless the operator sets an explicit escape hatch. It is a behavior change
  that could break an operator who deliberately runs non-loopback plaintext
  behind their own TLS-terminating proxy, so it is recorded here as a possible
  future hardening rather than a current decision. The current mitigation is
  the loud startup warning.
