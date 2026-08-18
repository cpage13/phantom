"""Shared FastAPI dependencies and constants for the router modules.

The emulator stores its mutable observables on a single
:class:`EmulatorState` per process. Routers acquire it through
``Depends(get_state)`` so the app factory can rebind a fresh state
between tests if it wants.
"""

from __future__ import annotations

from fastapi import Request

from phantom_emulator.state import EmulatorState

# The upload verbs both write-sinks register. INVARIANT: this set MUST equal
# the Phantom catch-all's forwarded upload-verb set - the
# ``["PUT", "POST", "PATCH"]`` literal at
# ``src/phantom-service/src/phantom/routes/catch_all.py`` (the ``raw_intake``
# ``@router.api_route(..., methods=[...])``). A forwarded verb the sinks do not
# register would 405 unvalidated/unsunk; the drift-guard test in the emulator
# unit suite pins this against that source of truth. GET is the read-back and
# is registered separately (never forwarded). Defined ONCE here and imported by
# both ``routers/s3.py`` and ``routers/raw_sink.py``.
UPLOAD_METHODS: tuple[str, ...] = ("PUT", "POST", "PATCH")


def get_state(request: Request) -> EmulatorState:
    """Return the :class:`EmulatorState` attached to the running app.

    The app factory stores the state on ``app.state.emulator_state``.
    """
    state: EmulatorState = request.app.state.emulator_state
    return state
