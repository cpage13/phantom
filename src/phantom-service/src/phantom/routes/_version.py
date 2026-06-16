"""Single source of truth for Phantom's public HTTP API version (plan § 4S.5).

The public surface is versioned under a ``/v1`` path prefix. Rather than
repeating the bare ``"/v1"`` literal at each router, the version is named
once here and the two router prefixes are derived from it, so the version
lives in exactly one place and the two prefixes can never drift.

This is a leaf module: it imports nothing from :mod:`phantom.routes.send`
or :mod:`phantom.routes.admin` (or anything else in the package), so the
routers can import these constants without any risk of a circular import.

De-literalizing the version leaves every resolved URL byte-identical
(``/v1/send``, ``/v1/admin/...``); it changes no route path strings, so
the SDK and the contract surface are unaffected.
"""

from __future__ import annotations

# The public API version. The only place the version token is written.
API_VERSION: str = "v1"

# Router prefixes derived from ``API_VERSION``. ``routes/send.py`` mounts
# the public ingress router under ``SEND_ROUTER_PREFIX`` and
# ``routes/admin.py`` mounts the loopback admin router under
# ``ADMIN_ROUTER_PREFIX``; resolved URLs stay ``/v1/send`` and
# ``/v1/admin/...``.
SEND_ROUTER_PREFIX: str = f"/{API_VERSION}"
ADMIN_ROUTER_PREFIX: str = f"/{API_VERSION}/admin"
