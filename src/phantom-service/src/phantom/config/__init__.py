"""Configuration package - re-exports the top-level Settings model.

Modules:

* :mod:`phantom.config.settings` - :class:`Settings`, YAML loader.
* :mod:`phantom.config.probe` - host probe (RAM/disk/CPU facts).
* :mod:`phantom.config.defaults` - probe-derived default values.
* :mod:`phantom.config.ad_mint` - AD-mint typed config.
"""

from __future__ import annotations

from phantom.config.settings import (
    CompressionCfg,
    InstanceCfg,
    ObservabilityCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RetryCfg,
    RetryStrategyCfg,
    RouteCfg,
    SaturationCfg,
    ServerCfg,
    Settings,
    SettingsError,
    SqliteCfg,
    StorageCfg,
    load_settings,
)

__all__ = [
    "CompressionCfg",
    "InstanceCfg",
    "ObservabilityCfg",
    "PersistTriggerCfg",
    "RetentionCfg",
    "RetryCfg",
    "RetryStrategyCfg",
    "RouteCfg",
    "SaturationCfg",
    "ServerCfg",
    "Settings",
    "SettingsError",
    "SqliteCfg",
    "StorageCfg",
    "load_settings",
]
