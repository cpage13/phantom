"""The isolation guard measures data_dirs where the stores actually live.

S3-3. ``check_instance_isolation`` resolved each ``cfg.data_dir`` on its
own, against the process CWD, instead of composing it against
``settings.storage.data_dir`` the way ``instance_storage_paths`` (and
therefore every running store) does. An adversarial verification pass
(V1, 2026-09-08) executed the cases and narrowed the finding:

* two instances declaring the SAME ABSOLUTE ``data_dir`` were ALREADY
  caught, because ``Path(abs).resolve()`` equals the join result;
* the admitted collision is the MIXED form - ``beta`` in one block and
  ``<root>/beta`` in another - which is the accident an operator actually
  makes when converting one block to an explicit path;
* a symlinked directory is admitted too, since the two spellings only
  collapse to one real path after the join is resolved.

V1 also corrected the harm: SQLite serialises two connections on one
file, so the DB does not corrupt. What the guard is really preventing is
two complete ``InstanceContext`` bundles - independent SaturationGates,
sender pools, kickers and reapers - driving the same ``uploads`` rows and
the same ``bodies/`` tree. There is no ``flock`` and no lockfile anywhere
in ``src/``, so this guard is the only thing in the way.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.runtime.startup_checks import ConfigInvariantError, check_instance_isolation


def _instance(instance_id: str, data_dir: str) -> InstanceCfg:
    """Build a minimal instance with a unique host prefix and the given data_dir."""
    hosts = [f"{instance_id}.example.com"]
    return InstanceCfg(
        id=instance_id,
        host_prefixes=hosts,
        data_dir=data_dir,
        routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def test_mixed_relative_and_absolute_data_dirs_collide(tmp_path: Path) -> None:
    """S3-3. ``beta`` and ``<root>/beta`` are ONE directory and must be rejected.

    Objective: the operator's real accident. One instance block keeps the
    relative ``data_dir`` while the other is rewritten as an explicit
    absolute path under the same storage root. Both name the same
    directory, so both instances would open the same ``uploads.db`` and the
    same ``bodies/`` tree with two of every worker.

    Expected outcome: ``ConfigInvariantError`` naming the shared data_dir.
    Resolving ``cfg.data_dir`` against the process CWD instead compares
    ``<cwd>/beta`` against ``<root>/beta``, sees two different paths, and
    admits the pair.
    """
    with pytest.raises(ConfigInvariantError, match=r"share data_dir"):
        check_instance_isolation(
            [
                _instance("alpha", "beta"),
                _instance("gamma", str(tmp_path / "beta")),
            ],
            data_dir_root=tmp_path,
        )


def test_symlinked_data_dirs_pointing_at_one_directory_collide(tmp_path: Path) -> None:
    """S3-3. Two names for one real directory via a symlink are rejected.

    Objective: ``<root>/beta`` is a symlink to ``<root>/alpha``. The two
    instance blocks look distinct as strings, and distinct before the join
    too; only resolving the COMPOSED path collapses the link and exposes
    the single storage partition underneath.

    Expected outcome: ``ConfigInvariantError`` naming the shared data_dir.
    """
    (tmp_path / "alpha").mkdir()
    (tmp_path / "beta").symlink_to(tmp_path / "alpha", target_is_directory=True)

    with pytest.raises(ConfigInvariantError, match=r"share data_dir"):
        check_instance_isolation(
            [_instance("one", "alpha"), _instance("two", "beta")],
            data_dir_root=tmp_path,
        )


def test_distinct_data_dirs_under_the_storage_root_are_admitted(tmp_path: Path) -> None:
    """S3-3. The guard must not become a false rejector.

    Objective: composing against the storage root must still admit the
    ordinary multi-instance config - two sibling directories under one
    root, one written relative and one written absolute. Expected outcome:
    no exception.
    """
    check_instance_isolation(
        [
            _instance("alpha", "alpha"),
            _instance("gamma", str(tmp_path / "gamma")),
        ],
        data_dir_root=tmp_path,
    )
