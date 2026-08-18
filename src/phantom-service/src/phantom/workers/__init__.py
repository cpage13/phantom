"""Background worker coroutines (plan §3.3)."""

from __future__ import annotations

from phantom.workers.body_orphan_janitor import BodyOrphanJanitor
from phantom.workers.disk_pressure import DiskPressureProbe
from phantom.workers.kicker import (
    AWS_SIGV4_FLAVOUR,
    PHANTOM_BEARER_FLAVOUR,
    Kicker,
    KickerFlavour,
)
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
    "AWS_SIGV4_FLAVOUR",
    "PHANTOM_BEARER_FLAVOUR",
    "AdmissionGranted",
    "AdmissionRefusedDiskPressure",
    "AdmissionRefusedSaturation",
    "AdmissionResult",
    "BodyOrphanJanitor",
    "DiskPressureProbe",
    "Kicker",
    "KickerFlavour",
    "PersistController",
    "RamPressureWatcher",
    "Reaper",
    "SaturationGate",
    "Sender",
    "VacuumScheduler",
    "run_recovery",
]
