# 007. Admin status surface for multi-instance Phantom

The admin status surface is two-tier: `GET /v1/admin/status` returns aggregate health (ready, disk usage, total backlog, per-instance summary, AD reachability when an instance's `ad_mint` block is configured), and `GET /v1/admin/instances/{id}/status` returns per-instance health. Resource endpoints scope by query parameter: `/v1/admin/chains` takes an optional `?instance=<id>` and aggregates across instances without it; `/v1/admin/tokens` takes an optional `?endpoint=`. Status responses contain only public-safe metadata; bearer values are never returned by any status endpoint per ADR-004. The goal is "the caller can decide whether to keep sending or back off" without exposing secrets.

Status: Accepted
Date: 2026-05-12
