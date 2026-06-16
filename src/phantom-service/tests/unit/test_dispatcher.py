"""Unit tests for phantom.instances.dispatcher."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances import (
    InstanceDispatcher,
    InstanceNotFoundError,
    NoMatchingInstanceError,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import resolve_configured_instance_id


def _cfg(instance_id: str, host_prefixes: list[str]) -> InstanceCfg:
    """Return an InstanceCfg with the given id and host_prefixes."""
    return InstanceCfg(
        id=instance_id,
        host_prefixes=host_prefixes,
        data_dir=instance_id,
        routes=[RouteCfg(name="all", hosts=["*"], auth_mode="none")],
    )


def _ctx(instance_id: str, host_prefixes: list[str]) -> InstanceContext:
    """Return a stub InstanceContext with just enough fields populated."""
    ctx = MagicMock(spec=InstanceContext)
    ctx.cfg = _cfg(instance_id, host_prefixes)
    return ctx


def test_resolve_by_url() -> None:
    """First instance whose host_prefixes matches wins."""
    a = _ctx("a", ["files.example.com"])
    b = _ctx("b", ["*.amazonaws.com"])
    dispatcher = InstanceDispatcher([a, b])
    assert dispatcher.resolve("https://files.example.com/x", None) is a
    assert dispatcher.resolve("https://bucket.amazonaws.com/x", None) is b


def test_resolve_by_header() -> None:
    """``X-Phantom-Instance`` header bypasses URL matching."""
    a = _ctx("a", ["files.example.com"])
    b = _ctx("b", ["*.amazonaws.com"])
    dispatcher = InstanceDispatcher([a, b])
    assert dispatcher.resolve("https://other.example.com/x", "b") is b


def test_no_match_raises() -> None:
    """No matching host_prefixes raises NoMatchingInstanceError."""
    a = _ctx("a", ["files.example.com"])
    dispatcher = InstanceDispatcher([a])
    with pytest.raises(NoMatchingInstanceError):
        dispatcher.resolve("https://other.example.com/x", None)


def test_unknown_instance_header_raises() -> None:
    """Unknown header value raises InstanceNotFoundError."""
    a = _ctx("a", ["files.example.com"])
    dispatcher = InstanceDispatcher([a])
    with pytest.raises(InstanceNotFoundError):
        dispatcher.resolve("https://files.example.com/x", "missing")


def test_all_and_by_id() -> None:
    """Helpers expose every instance and by-id lookup."""
    a = _ctx("a", ["a.example.com"])
    b = _ctx("b", ["b.example.com"])
    dispatcher = InstanceDispatcher([a, b])
    assert dispatcher.all_instances() == [a, b]
    assert dispatcher.by_id("a") is a
    assert dispatcher.by_id("missing") is None


# ---------------------------------------------------------------------------
# resolve_configured_instance_id (§ 4D.2) - the config-derived resolver the
# degraded-boot guard uses. It mirrors dispatcher.resolve's precedence but
# operates over InstanceCfg (so it can name an instance with no live context).
# ---------------------------------------------------------------------------


def test_configured_id_by_url_first_match_wins() -> None:
    """Host-prefix match returns the first configured instance's id (YAML order)."""
    cfgs = [_cfg("a", ["files.example.com"]), _cfg("b", ["*.amazonaws.com"])]
    assert resolve_configured_instance_id(cfgs, "https://files.example.com/x", None) == "a"
    assert resolve_configured_instance_id(cfgs, "https://bucket.amazonaws.com/x", None) == "b"


def test_configured_id_by_header_bypasses_url() -> None:
    """An X-Phantom-Instance header resolves the id without consulting the URL."""
    cfgs = [_cfg("a", ["files.example.com"]), _cfg("b", ["*.amazonaws.com"])]
    # URL would match no instance, but the header names b directly. The empty
    # URL models the pre-_parse_body call site (header known, body not read).
    assert resolve_configured_instance_id(cfgs, "", "b") == "b"
    assert resolve_configured_instance_id(cfgs, "https://other.example.com/x", "b") == "b"


def test_configured_id_unknown_header_returns_none() -> None:
    """An unknown header value resolves to None (not an exception)."""
    cfgs = [_cfg("a", ["files.example.com"])]
    assert resolve_configured_instance_id(cfgs, "https://files.example.com/x", "missing") is None


def test_configured_id_no_host_match_returns_none() -> None:
    """A host matching no instance's prefixes resolves to None."""
    cfgs = [_cfg("a", ["files.example.com"])]
    assert resolve_configured_instance_id(cfgs, "https://other.example.com/x", None) is None


def test_configured_id_resolves_instance_with_no_live_context() -> None:
    """The resolver names an instance present in config but absent from any dispatcher.

    This is the § 4D.2 case: a degraded instance has no InstanceContext and
    no dispatcher entry, but its InstanceCfg is still in settings.instances,
    so the configured-id resolver can map a request to it and the guard can
    500. The dispatcher (built from live contexts only) could not.
    """
    # The dispatcher is built WITHOUT instance "b" (it booted degraded).
    live = _ctx("a", ["files.example.com"])
    dispatcher = InstanceDispatcher([live])
    with pytest.raises(NoMatchingInstanceError):
        dispatcher.resolve("https://bucket.amazonaws.com/x", None)
    # But the config (which still lists b) resolves the request to "b".
    cfgs = [_cfg("a", ["files.example.com"]), _cfg("b", ["*.amazonaws.com"])]
    assert resolve_configured_instance_id(cfgs, "https://bucket.amazonaws.com/x", None) == "b"
