"""Docker/Compose container replacement preserving the named volume (audit T7 / G4).

The shipped image and compose boundary had zero executable proof: the prior
docker lane was a stub, so image assembly, the nonroot runtime user, the
mounted config path, compose service DNS, health wiring, and — the heart of
the deployment story — "replace the Phantom container, keep the named volume,
recover the buffered backlog" were all unproven.

This module builds both images from the repo's Dockerfiles, boots the exact
test-owned compose topology (``tests/e2e/docker/compose.yml``), and drives the
audit's replacement matrix with two pre-created run-unique external volumes:

* Smoke: both services healthy; a public-SDK upload lands byte-identically at
  the emulator container (service-name DNS proven from inside the phantom
  container as well).
* Evidence: with upstream creates 5xx-ing, an all-disk chain is buffered and
  recorded under the ``<run>-evidence`` volume, then the phantom container is
  stopped and removed.
* Control: the same service force-recreated on the empty ``<run>-control``
  volume must NOT know the evidence chain (admin 404) and must not deliver it
  once the fault clears — fresh volume, fresh state.
* Restore: recreated back on ``<run>-evidence``, the exact buffered row is
  found in admin, recovers, and delivers exactly once.

At every phase the ``/var/lib/phantom`` mount source is inspected and must
equal the selected explicit volume name; runtime UID/GID must be the Wolfi
nonroot 65532 with a writable data dir. Logs and ``compose ps`` are captured
before each recreate and at teardown (under the pytest tmp dir).

Lane notes: requires a reachable Docker daemon (skips otherwise — CI runs
this module in its own ``e2e-docker`` job, excluded from e2e-core); the
``docker`` CLI is resolved from PATH with a Docker Desktop fallback, and the
CLI's own directory is appended to the subprocess PATH so Desktop credential
helpers resolve. Container Phantom runs all-disk bodies with an hour-free
retry ladder (one immediate attempt, then 20 s rungs) so the evidence row
stays non-terminal across the replacement and retries promptly after restore.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import jwt
import pytest
import yaml
from phantom_client import PhantomClient

from tests.e2e._harness.subprocess_harness import (
    _REPO_ROOT,
    DEFAULT_SUB,
    SIGNING_SECRET,
    submit_one,
)
from tests.e2e.helpers.timing import await_until

_DOCKER_DESKTOP_BIN = "/Applications/Docker.app/Contents/Resources/bin/docker"


def _docker_binary() -> str | None:
    """Resolve the docker CLI from PATH, with the Docker Desktop fallback."""
    found = shutil.which("docker")
    if found:
        return found
    if Path(_DOCKER_DESKTOP_BIN).is_file():
        return _DOCKER_DESKTOP_BIN
    return None


def _daemon_reachable(binary: str | None) -> bool:
    """True when `docker info` answers (daemon up and socket reachable)."""
    if binary is None:
        return False
    try:
        probe = subprocess.run(
            [binary, "info", "--format", "{{.ServerVersion}}"],
            env=_docker_env(binary),
            capture_output=True,
            timeout=20,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return False
    return probe.returncode == 0


def _docker_env(binary: str) -> dict[str, str]:
    """Child env for docker calls: CLI dir on PATH so credential helpers resolve."""
    env = dict(os.environ)
    env["PATH"] = f"{Path(binary).parent}{os.pathsep}{env.get('PATH', '')}"
    return env


_DOCKER = _docker_binary()

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.e2e,
    pytest.mark.docker,
    pytest.mark.skipif(
        not _daemon_reachable(_DOCKER),
        reason="docker daemon unreachable (the e2e-docker CI job owns this lane)",
    ),
]

# Container-internal endpoints (compose service DNS + fixed internal ports).
_EMULATOR_INTERNAL_URL = "http://emulator:8000"
_PHANTOM_INTERNAL_PORT = 8080
_EMULATOR_INTERNAL_PORT = 8000
_DATA_DIR_IN_CONTAINER = "/var/lib/phantom"
# The Wolfi `nonroot` user both runtime images run as (audit assertion).
_NONROOT_UID_GID = (65532, 65532)

_PAYLOAD_SMOKE = b"phantom-t7-smoke-body\x00\xff\xfe-byte-identity"
_PAYLOAD_EVIDENCE = b"phantom-t7-evidence-body\x00\xfe\xfd-survives-replacement"

# One immediate attempt, then 20 s rungs for over half an hour: the evidence
# row stays non-terminal across stop/replace phases, and the restored
# process retries within one rung of boot.
_EVIDENCE_RETRY_RUNG_SECONDS = 20
_EVIDENCE_RETRY_RUNG_COUNT = 100

_BUILD_BUDGET_SECONDS = 600
_COMPOSE_BUDGET_SECONDS = 240
_DOCKER_CMD_BUDGET_SECONDS = 60
_ATTEMPT_BUDGET_SECONDS = 30.0
_SUCCEEDED_BUDGET_SECONDS = 60.0
_CONTROL_QUIET_SECONDS = 5.0


def _run_docker(
    args: list[str],
    *,
    env: dict[str, str],
    budget_seconds: float = _DOCKER_CMD_BUDGET_SECONDS,
) -> str:
    """Run one docker CLI command, asserting success; returns stdout."""
    assert _DOCKER is not None
    result = subprocess.run(
        [_DOCKER, *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=budget_seconds,
        check=False,
    )
    assert result.returncode == 0, (
        f"docker {' '.join(args)} failed rc={result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return result.stdout


def _write_container_phantom_config(path: Path) -> None:
    """Derive the container Phantom YAML from the suite's pinned base config.

    The base already carries the container-correct ``bind_tcp 0.0.0.0:8080``
    and ``data_dir /var/lib/phantom``; the overlay pins all-disk bodies (the
    audit's durable-mode boundary) and the evidence retry ladder.
    """
    base = yaml.safe_load((_REPO_ROOT / "tests" / "e2e" / "phantom-config.yml").read_text())
    assert isinstance(base, dict)
    base["storage"]["body_store"] = {"mode": "all_disk"}
    base["retry"]["default_strategy"] = {
        "type": "fixed_intervals",
        "intervals_seconds": [0] + [_EVIDENCE_RETRY_RUNG_SECONDS] * _EVIDENCE_RETRY_RUNG_COUNT,
    }
    path.write_text(yaml.safe_dump(base))


def _container_bearer(emulator_config: Path) -> str:
    """Mint an HS256 bearer the CONTAINER emulator accepts (claims from its YAML)."""
    auth = yaml.safe_load(emulator_config.read_text())["auth"]
    now = int(time.time())
    return jwt.encode(
        {
            "iss": auth["issuer"],
            "sub": DEFAULT_SUB,
            "aud": auth["audience"],
            "exp": now + 3600,
            "iat": now,
            "tid": auth["tenant_id"],
        },
        SIGNING_SECRET,
        algorithm="HS256",
    )


def _published_url(compose_env: dict[str, str], service: str, target_port: int) -> str:
    """Discover the loopback-published URL for ``service`` post-startup."""
    out = _run_docker(
        ["compose", "-f", str(_COMPOSE_FILE), "port", service, str(target_port)],
        env=compose_env,
    ).strip()
    assert out, f"no published port for {service}:{target_port}"
    host, _, port = out.rpartition(":")
    assert port.isdigit(), f"unparseable compose port output: {out!r}"
    host = "127.0.0.1" if host in {"0.0.0.0", ""} else host
    return f"http://{host}:{port}"


_COMPOSE_FILE = Path(__file__).parent / "docker" / "compose.yml"


def _phantom_container_id(compose_env: dict[str, str]) -> str:
    """Return the live phantom service container id."""
    out = _run_docker(
        ["compose", "-f", str(_COMPOSE_FILE), "ps", "-q", "phantom"], env=compose_env
    ).strip()
    assert out, "no phantom container found"
    return out.splitlines()[0]


def _exec_python(compose_env: dict[str, str], container_id: str, code: str) -> str:
    """Run a python one-liner inside the (shell-less) container via exec-form."""
    return _run_docker(["exec", container_id, "python", "-c", code], env=compose_env).strip()


def _assert_phantom_container_contract(
    compose_env: dict[str, str], *, expected_volume: str
) -> None:
    """The audit's per-phase inspections: health, mount identity, UID, DNS.

    Asserted on the LIVE container: health status healthy; the
    ``/var/lib/phantom`` mount source is exactly the selected external
    volume; runtime UID/GID is the Wolfi nonroot 65532 with a writable data
    dir; and the emulator resolves + answers over compose service DNS from
    INSIDE the phantom container.
    """
    container_id = _phantom_container_id(compose_env)

    health = _run_docker(
        ["inspect", "-f", "{{.State.Health.Status}}", container_id], env=compose_env
    ).strip()
    assert health == "healthy", f"phantom container health is {health!r}"

    mounts: list[dict[str, Any]] = json.loads(
        _run_docker(["inspect", "-f", "{{json .Mounts}}", container_id], env=compose_env)
    )
    data_mounts = [m for m in mounts if m.get("Destination") == _DATA_DIR_IN_CONTAINER]
    assert len(data_mounts) == 1, f"expected one data mount, got {mounts}"
    assert data_mounts[0].get("Name") == expected_volume, (
        f"data mount rides {data_mounts[0].get('Name')!r}, expected {expected_volume!r}"
    )

    uid_gid = _exec_python(compose_env, container_id, "import os; print(os.getuid(), os.getgid())")
    assert uid_gid == f"{_NONROOT_UID_GID[0]} {_NONROOT_UID_GID[1]}", (
        f"container runs as {uid_gid!r}, expected nonroot 65532 65532"
    )
    writable = _exec_python(
        compose_env,
        container_id,
        f"import os; print(os.access({_DATA_DIR_IN_CONTAINER!r}, os.W_OK))",
    )
    assert writable == "True", "data dir is not writable by the runtime user"

    dns_status = _exec_python(
        compose_env,
        container_id,
        "import urllib.request; print(urllib.request.urlopen("
        f"'{_EMULATOR_INTERNAL_URL}/.well-known/openid-configuration', timeout=5).status)",
    )
    assert dns_status == "200", "emulator unreachable over compose service DNS"


def _capture_phase_artifacts(compose_env: dict[str, str], out_dir: Path, phase: str) -> None:
    """Capture service logs and ``compose ps`` for one phase (audit cleanup rule)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    logs = subprocess.run(
        [_DOCKER, "compose", "-f", str(_COMPOSE_FILE), "logs", "--no-color"],
        env=compose_env,
        capture_output=True,
        text=True,
        timeout=_DOCKER_CMD_BUDGET_SECONDS,
        check=False,
    )
    (out_dir / f"{phase}-logs.txt").write_text(logs.stdout + logs.stderr)
    ps = subprocess.run(
        [_DOCKER, "compose", "-f", str(_COMPOSE_FILE), "ps"],
        env=compose_env,
        capture_output=True,
        text=True,
        timeout=_DOCKER_CMD_BUDGET_SECONDS,
        check=False,
    )
    (out_dir / f"{phase}-ps.txt").write_text(ps.stdout + ps.stderr)


async def _received_entries(emulator_url: str) -> list[dict[str, Any]]:
    """Read the emulator container's accepted-body log over its control surface."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{emulator_url}/control/received")
    assert response.status_code == 200, f"control/received: {response.status_code}"
    payload = response.json()
    entries = payload["received"] if isinstance(payload, dict) else payload
    assert isinstance(entries, list)
    return entries


async def _set_create_fault(emulator_url: str, *, active: bool) -> None:
    """Install or clear a 100% 5xx policy on the upstream create endpoint."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        if active:
            response = await client.post(
                f"{emulator_url}/control/inject-failure",
                json={"scope": "upstream.files.create", "error_rate_5xx": 1.0},
            )
        else:
            response = await client.post(f"{emulator_url}/control/clear-failures")
    assert response.status_code == 204, f"failure control: {response.status_code}"


async def test_container_replacement_preserves_named_volume_backlog(tmp_path: Path) -> None:
    """Replace the Phantom container; the named volume is the durability boundary.

    Objective: the full audit matrix — smoke on evidence, buffered hold,
    container replacement onto a control volume (backlog absent, never
    delivered), replacement back onto evidence (backlog found and delivered
    exactly once), with mount/UID/DNS contract inspections at every phase.
    """
    assert _DOCKER is not None
    docker_env = _docker_env(_DOCKER)
    suffix = uuid4().hex[:8]
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    project = f"phantom-e2e-{run_id}-{attempt}-{suffix}"
    image_tag = os.environ.get("GITHUB_SHA", suffix)
    phantom_image = f"phantom-e2e:{image_tag}"
    emulator_image = f"phantom-emulator-e2e:{image_tag}"
    evidence_volume = f"{project}-evidence"
    control_volume = f"{project}-control"
    artifacts_dir = tmp_path / "docker-artifacts"

    phantom_config = tmp_path / "phantom.yaml"
    _write_container_phantom_config(phantom_config)
    emulator_config = _REPO_ROOT / "tests" / "e2e" / "emulator-config.yml"

    compose_env = dict(docker_env)
    compose_env.update(
        {
            "COMPOSE_PROJECT_NAME": project,
            "PHANTOM_IMAGE": phantom_image,
            "EMULATOR_IMAGE": emulator_image,
            "EMULATOR_SIGNING_KEY": SIGNING_SECRET,
            "PHANTOM_CONFIG_FILE": str(phantom_config),
            "EMULATOR_CONFIG_FILE": str(emulator_config),
            "PHANTOM_DATA_VOLUME": evidence_volume,
        }
    )

    # Build both images from the repo's Dockerfiles (layer-cached after the
    # first local run; CI builds fresh per run).
    _run_docker(
        ["build", "-f", "src/phantom-deploy/Dockerfile", "-t", phantom_image, str(_REPO_ROOT)],
        env=docker_env,
        budget_seconds=_BUILD_BUDGET_SECONDS,
    )
    _run_docker(
        ["build", "-f", "src/phantom-emulator/Dockerfile", "-t", emulator_image, str(_REPO_ROOT)],
        env=docker_env,
        budget_seconds=_BUILD_BUDGET_SECONDS,
    )

    _run_docker(["volume", "create", evidence_volume], env=docker_env)
    _run_docker(["volume", "create", control_volume], env=docker_env)
    try:
        _run_docker(
            ["compose", "-f", str(_COMPOSE_FILE), "up", "-d", "--wait"],
            env=compose_env,
            budget_seconds=_COMPOSE_BUDGET_SECONDS,
        )
        emulator_url = _published_url(compose_env, "emulator", _EMULATOR_INTERNAL_PORT)
        phantom_url = _published_url(compose_env, "phantom", _PHANTOM_INTERNAL_PORT)
        bearer = _container_bearer(emulator_config)

        # Phase 1 — smoke on the evidence volume, full contract inspection.
        _assert_phantom_container_contract(compose_env, expected_volume=evidence_volume)
        smoke_chain_id = uuid4()
        async with PhantomClient(phantom_url) as client:
            await submit_one(
                client,
                emulator_url=_EMULATOR_INTERNAL_URL,
                bearer=bearer,
                body=_PAYLOAD_SMOKE,
                chain_id=smoke_chain_id,
            )

            async def _smoke_delivered() -> bool:
                detail = await client.get_upload(smoke_chain_id)
                return detail.state == "succeeded"

            await await_until(_smoke_delivered, timeout_seconds=_SUCCEEDED_BUDGET_SECONDS)
        entries = await _received_entries(emulator_url)
        assert len(entries) == 1, f"expected one smoke delivery, got {len(entries)}"
        assert entries[0]["metadata_kvs"].get("phantom_local_uuid") == str(smoke_chain_id)
        assert entries[0]["body_hash"] == hashlib.sha256(_PAYLOAD_SMOKE).hexdigest(), (
            "smoke byte round-trip broke through the container path"
        )

        # Phase 2 — hold an all-disk chain under a 100% create fault.
        await _set_create_fault(emulator_url, active=True)
        evidence_chain_id = uuid4()
        async with PhantomClient(phantom_url) as client:
            await submit_one(
                client,
                emulator_url=_EMULATOR_INTERNAL_URL,
                bearer=bearer,
                body=_PAYLOAD_EVIDENCE,
                chain_id=evidence_chain_id,
            )

            async def _evidence_attempted() -> bool:
                detail = await client.get_upload(evidence_chain_id)
                return detail.attempts >= 1 and detail.state in {"queued", "attempting"}

            await await_until(_evidence_attempted, timeout_seconds=_ATTEMPT_BUDGET_SECONDS)

        _capture_phase_artifacts(compose_env, artifacts_dir, "1-evidence-held")
        _run_docker(["compose", "-f", str(_COMPOSE_FILE), "stop", "phantom"], env=compose_env)
        _run_docker(["compose", "-f", str(_COMPOSE_FILE), "rm", "-f", "phantom"], env=compose_env)

        # Phase 3 — force-recreate on the EMPTY control volume.
        compose_env["PHANTOM_DATA_VOLUME"] = control_volume
        _run_docker(
            [
                "compose",
                "-f",
                str(_COMPOSE_FILE),
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "--wait",
                "phantom",
            ],
            env=compose_env,
            budget_seconds=_COMPOSE_BUDGET_SECONDS,
        )
        phantom_url = _published_url(compose_env, "phantom", _PHANTOM_INTERNAL_PORT)
        _assert_phantom_container_contract(compose_env, expected_volume=control_volume)

        # The evidence chain must be ABSENT from the control admin surface.
        async with httpx.AsyncClient(timeout=10.0) as raw:
            absent = await raw.get(f"{phantom_url}/v1/admin/chains/{evidence_chain_id}")
        assert absent.status_code == 404, (
            f"evidence chain visible on the control volume: {absent.status_code}"
        )

        # Fault clears; the control process has nothing buffered, so nothing
        # new may land at the emulator.
        await _set_create_fault(emulator_url, active=False)
        await asyncio.sleep(_CONTROL_QUIET_SECONDS)  # pre-commit-allow: sleep
        entries = await _received_entries(emulator_url)
        assert len(entries) == 1, (
            "the control-volume phantom delivered a chain it should not know about"
        )

        _capture_phase_artifacts(compose_env, artifacts_dir, "2-control-empty")
        _run_docker(["compose", "-f", str(_COMPOSE_FILE), "stop", "phantom"], env=compose_env)
        _run_docker(["compose", "-f", str(_COMPOSE_FILE), "rm", "-f", "phantom"], env=compose_env)

        # Phase 4 — force-recreate back on the EVIDENCE volume: the buffered
        # row must be found and delivered exactly once.
        compose_env["PHANTOM_DATA_VOLUME"] = evidence_volume
        _run_docker(
            [
                "compose",
                "-f",
                str(_COMPOSE_FILE),
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "--wait",
                "phantom",
            ],
            env=compose_env,
            budget_seconds=_COMPOSE_BUDGET_SECONDS,
        )
        phantom_url = _published_url(compose_env, "phantom", _PHANTOM_INTERNAL_PORT)
        _assert_phantom_container_contract(compose_env, expected_volume=evidence_volume)

        async with PhantomClient(phantom_url) as client:
            detail = await client.get_upload(evidence_chain_id)
            assert detail.state in {"queued", "attempting", "succeeded"}, (
                f"restored evidence chain in unexpected state {detail.state!r}"
            )

            async def _evidence_delivered() -> bool:
                restored = await client.get_upload(evidence_chain_id)
                return restored.state == "succeeded"

            await await_until(_evidence_delivered, timeout_seconds=_SUCCEEDED_BUDGET_SECONDS)

        entries = await _received_entries(emulator_url)
        evidence_entries = [
            e
            for e in entries
            if e["metadata_kvs"].get("phantom_local_uuid") == str(evidence_chain_id)
        ]
        assert len(evidence_entries) == 1, (
            f"evidence chain delivered {len(evidence_entries)} times (expected exactly once)"
        )
        assert evidence_entries[0]["body_hash"] == (
            hashlib.sha256(_PAYLOAD_EVIDENCE).hexdigest()
        ), "evidence byte round-trip broke across the container replacement"
        assert len(entries) == 2, f"unexpected extra deliveries: {len(entries)}"

        _capture_phase_artifacts(compose_env, artifacts_dir, "3-evidence-restored")
    finally:
        _capture_phase_artifacts(compose_env, artifacts_dir, "9-teardown")
        subprocess.run(
            [_DOCKER, "compose", "-f", str(_COMPOSE_FILE), "down", "--remove-orphans"],
            env=compose_env,
            capture_output=True,
            timeout=_COMPOSE_BUDGET_SECONDS,
            check=False,
        )
        for volume in (evidence_volume, control_volume):
            subprocess.run(
                [_DOCKER, "volume", "rm", volume],
                env=docker_env,
                capture_output=True,
                timeout=_DOCKER_CMD_BUDGET_SECONDS,
                check=False,
            )
        for image in (phantom_image, emulator_image):
            subprocess.run(
                [_DOCKER, "rmi", image],
                env=docker_env,
                capture_output=True,
                timeout=_DOCKER_CMD_BUDGET_SECONDS,
                check=False,
            )
