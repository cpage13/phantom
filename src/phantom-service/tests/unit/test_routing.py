"""Unit tests for :func:`phantom.routing.resolve_route`."""

from __future__ import annotations

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.routing import resolve_route


def _instance(routes: list[RouteCfg]) -> InstanceCfg:
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=routes,
    )


def test_wildcard_match() -> None:
    """``*.amazonaws.com`` matches ``bucket.amazonaws.com``."""
    inst = _instance(
        [
            RouteCfg(
                name="upstream-s3",
                hosts=["*.amazonaws.com"],
                auth_mode="none",
            )
        ]
    )
    res = resolve_route("https://bucket.amazonaws.com/foo", inst)
    assert res.route_name == "upstream-s3"
    assert res.auth_mode == "none"


def test_no_match_raises() -> None:
    """When no route matches, raise ValueError."""
    inst = _instance([RouteCfg(name="r", hosts=["only.example.com"], auth_mode="phantom_bearer")])
    with pytest.raises(ValueError):
        resolve_route("https://other.example.com/x", inst)


def test_route_declaration_order_wins() -> None:
    """Two routes both matching the URL — the first declared wins."""
    inst = _instance(
        [
            RouteCfg(name="specific", hosts=["files.example.com"], auth_mode="phantom_bearer"),
            RouteCfg(name="catchall", hosts=["*"], auth_mode="none"),
        ]
    )
    res = resolve_route("https://files.example.com/x", inst)
    assert res.route_name == "specific"


def test_route_wildcard_only_inside_matched_instance() -> None:
    """A wildcard route inside an instance with restrictive host_prefixes still works."""
    inst = InstanceCfg(
        id="primary",
        host_prefixes=["upstream.example.com", "*.amazonaws.com"],
        data_dir="primary",
        routes=[RouteCfg(name="all", hosts=["*"], auth_mode="none")],
    )
    res = resolve_route("https://bucket.amazonaws.com/foo", inst)
    assert res.route_name == "all"


def test_resolved_route_carries_timeout_when_set() -> None:
    """``RouteCfg.timeout_seconds`` is plumbed onto the ResolvedRoute (§5.2)."""
    inst = _instance(
        [
            RouteCfg(
                name="upstream-s3",
                hosts=["*.amazonaws.com"],
                auth_mode="none",
                timeout_seconds=600.0,
            )
        ]
    )
    res = resolve_route("https://bucket.amazonaws.com/x", inst)
    assert res.timeout_seconds == 600.0


def test_resolved_route_timeout_defaults_to_none() -> None:
    """Routes without a per-route override get ``timeout_seconds=None`` (use global)."""
    inst = _instance(
        [RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")]
    )
    res = resolve_route("https://files.example.com/x", inst)
    assert res.timeout_seconds is None
