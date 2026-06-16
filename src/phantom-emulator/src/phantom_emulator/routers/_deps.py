"""Shared FastAPI dependencies for the router modules.

The emulator stores its mutable observables on a single
:class:`EmulatorState` per process. Routers acquire it through
``Depends(get_state)`` so the app factory can rebind a fresh state
between tests if it wants.
"""

from __future__ import annotations

from fastapi import Request

from phantom_emulator.state import EmulatorState


def get_state(request: Request) -> EmulatorState:
    """Return the :class:`EmulatorState` attached to the running app.

    The app factory stores the state on ``app.state.emulator_state``.
    """
    state: EmulatorState = request.app.state.emulator_state
    return state
