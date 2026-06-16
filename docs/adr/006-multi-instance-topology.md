# 006. Multi-instance topology

Phantom-the-service runs N configured instances within one process; each instance binds to one or more upstream URL prefixes, has its own ID, its own storage partition (DB + body store), and its own refresh strategy. Inbound requests are dispatched to the matching instance by target URL prefix; unmatched targets are rejected with 4xx by default (an explicit per-request override header is available for advanced cases that knowingly route outside configured prefixes — Phantom logs a warning when it's used). Running one container per upstream is supported as the N=1 degenerate case of multi-instance, used when strong isolation between upstreams is required (independent restart cadences, separate resource caps); the multi-instance code path is identical in both shapes — deployment topology is a packaging choice, not an architectural one.

Status: Accepted
Date: 2026-05-12
