"""Background worker coroutines (plan §3.3)."""

from __future__ import annotations

from phantom.workers.auth_kicker import AuthKicker
from phantom.workers.body_orphan_janitor import BodyOrphanJanitor
from phantom.workers.disk_pressure import DiskPressureProbe
from phantom.workers.persist_controller import PersistController
from phantom.workers.ram_pressure import RamPressureWatcher
from phantom.workers.reaper import Reaper
from phantom.workers.recovery import run_recovery
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedDiskPressure,
    AdmissionRefusedSaturation,
    AdmissionResult,
    SaturationGate,
)
from phantom.workers.sender import Sender
from phantom.workers.vacuum import VacuumScheduler

__all__ = [
    "AdmissionGranted",
    "AdmissionRefusedDiskPressure",
    "AdmissionRefusedSaturation",
    "AdmissionResult",
    "AuthKicker",
    "BodyOrphanJanitor",
    "DiskPressureProbe",
    "PersistController",
    "RamPressureWatcher",
    "Reaper",
    "SaturationGate",
    "Sender",
    "VacuumScheduler",
    "run_recovery",
]
