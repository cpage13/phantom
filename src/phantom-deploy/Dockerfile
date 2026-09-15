# phantom-service container image — Wolfi multi-arch (linux/amd64 + linux/arm64).
#
# Deliberately NO `# syntax=docker/dockerfile:X` pin: the directive makes
# every build fetch the frontend image from docker.io (an availability and
# credential dependency that has broken builds), and this file uses only
# core features the engine's built-in frontend has carried for years.
#
# This is THE Dockerfile for phantom-service. ADR-020 (Phase 6)
# supersedes the prior three Dockerfiles under src/phantom-service/docker/ —
# this single image is what ships to GHCR.
#
# Base image rationale: Chainguard Wolfi is a minimal, non-Debian
# distro designed for containers. It ships glibc (not musl) so Python
# wheels with C extensions install without musl-rebuild surprises.
#
# Layout:
#   Stage 1 `build`   — cgr.dev/chainguard/python:latest-dev
#                       installs `uv`, builds the wheel, pip-installs
#                       it into /install, prunes debug symbols +
#                       __pycache__ + bundled tests + unused CFFI
#                       backend.
#   Stage 2 `runtime` — cgr.dev/chainguard/python:latest
#                       shell-less runtime carrying only /install,
#                       runs as the Wolfi `nonroot` user (65532).
#
# Build (multi-arch from the repository root):
#     docker buildx build \
#       --platform linux/amd64,linux/arm64 \
#       -f src/phantom-deploy/Dockerfile \
#       -t phantom-service:dev \
#       .
#
# The image bakes no default config. The operator supplies the YAML
# at deploy time via a volume mount + the `-c <path>` argument.


# ---------- Base images: pinned by digest (finding S13-10) ------------------
#
# Both stages used to name a floating `latest` tag with no digest. Two
# consequences, both real:
#
#   1. NOT REPRODUCIBLE. The same Dockerfile produced different bytes on
#      different days, so "the deployment artifact" (ADR-020) named nothing
#      in particular.
#   2. TWO INDEPENDENTLY-RESOLVED TAGS WHOSE PYTHON ABI MUST MATCH. The
#      wheel's C extensions (pydantic-core, zstandard, cryptography, ...) are
#      built under `latest-dev` and COPYd into `latest`. An extension module
#      carries its interpreter's ABI tag in its filename, so if the two tags
#      straddled a Python minor bump the runtime could not import them, and
#      it failed at CONTAINER START rather than at build.
#
# The digests below are OCI image INDEX digests, so multi-arch
# (linux/amd64 + linux/arm64) buildx builds still work; the tag in each
# reference is documentation, the digest is what is pulled. The
# runtime-stage ABI assertion further down turns consequence 2 into a BUILD
# failure even if the two pins are ever repinned inconsistently.
#
# REPINNING. Chainguard's public tier keeps only the moving `latest` tags, so
# a pinned digest eventually stops resolving and the build fails loudly with
# a pull error, which is the correct failure for a stale pin. To refresh,
# resolve both index digests and update the two defaults:
#
#     TOKEN=$(curl -s 'https://cgr.dev/token?scope=repository:chainguard/python:pull&service=cgr.dev' | jq -r .token)
#     for TAG in latest latest-dev; do
#       curl -sI -H "Authorization: Bearer $TOKEN" \
#         -H 'Accept: application/vnd.oci.image.index.v1+json' \
#         "https://cgr.dev/v2/chainguard/python/manifests/$TAG" \
#         | grep -i docker-content-digest
#     done
#
# Refresh BOTH together; they must come from the same resolution, which is
# the only thing that keeps their Python minor in step. To build against the
# moving tags without editing this file (a deliberate, temporary choice):
#
#     docker build --build-arg PYTHON_BUILD_IMAGE=cgr.dev/chainguard/python:latest-dev ...
#
# Pinned 2026-09-15.
ARG PYTHON_BUILD_IMAGE=cgr.dev/chainguard/python:latest-dev@sha256:666971ccbb01312b3e12799879a76e8b8cb574c6ed8218d6a8faa4d7ac2e493e
ARG PYTHON_RUNTIME_IMAGE=cgr.dev/chainguard/python:latest@sha256:cbe0e6d0e3abdfc5b54938b140be046e345ef765f8658e2a165b5417cb2bb6a9


# ---------- Stage 1: build the phantom wheel with uv, then prune ------------
FROM ${PYTHON_BUILD_IMAGE} AS build

WORKDIR /work

# Install uv as the build front-end. Build stage is throwaway.
RUN pip install --no-cache-dir --user uv

# Copy the phantom-service source. phantom-service's pyproject.toml
# carries all of its build config (hatchling) and all of its runtime
# deps from public PyPI; no workspace-level files are needed.
COPY src/phantom-service/ ./phantom/

# Build the wheel, pip-install it (and its public-PyPI deps) into
# /install, create the empty /work/phantom-data (COPYd into the
# shell-less runtime as the data dir), then PRUNE /install:
#   * strip --strip-unneeded every .so — drops debug symbols.
#   * remove __pycache__ — regenerated lazily at runtime.
#   * remove bundled test suites.
#   * remove the unused zstandard CFFI backend — phantom's zstd codec
#     uses the C-extension backend (zstandard.backend == "cext").
RUN cd phantom \
 && python -m uv build --wheel --out-dir /work/dist \
 && mkdir -p /work/install \
 && pip install --no-cache-dir --no-warn-script-location --target /work/install /work/dist/phantom_service-*.whl \
 && mkdir -p /work/phantom-data \
 && find /work/install -name '*.so' -exec strip --strip-unneeded {} + \
 && find /work/install -name '__pycache__' -type d -exec rm -rf {} + \
 && find /work/install -type d \( -name tests -o -name test \) -exec rm -rf {} + \
 && rm -f /work/install/zstandard/_cffi*.so /work/install/zstandard/backend_cffi.py


# ---------- Stage 2: minimal runtime ----------------------------------------
FROM ${PYTHON_RUNTIME_IMAGE} AS runtime

# Drop /install on PYTHONPATH so the runtime Python finds the
# installed packages without a venv.
ENV PYTHONPATH=/install \
    PYTHONUNBUFFERED=1

COPY --from=build /work/install /install

# ABI GATE (finding S13-10). The C extensions in /install were compiled
# against the BUILD stage's interpreter. A version-locked extension carries
# that interpreter's ABI tag in its filename (`.cpython-3XX-<plat>.so`), so if
# the two pinned base images ever straddle a Python minor bump, the runtime
# interpreter cannot import them. Without this step that surfaced at CONTAINER
# START, on the operator's machine, as an ImportError from a service that had
# built and pushed green. Here it is a BUILD failure with the mismatch named.
#
# Stable-ABI extensions (`.abi3.so`) are portable across minors by definition
# and are counted, not checked. Exec form because the runtime image is
# shell-less Wolfi.
RUN ["python", "-c", "import pathlib, sys, sysconfig; suffix = sysconfig.get_config_var('EXT_SUFFIX'); shared = sorted(p.name for p in pathlib.Path('/install').rglob('*.so')); locked = [n for n in shared if '.cpython-' in n]; bad = [n for n in locked if not n.endswith(suffix)]; sys.exit('ABI GATE INCONCLUSIVE: no shared objects under /install; the wheel install or the prune step changed shape, so this check proves nothing') if not shared else None; sys.exit('ABI MISMATCH between the pinned build and runtime base images: this interpreter loads ' + suffix + ' but /install carries version-locked extensions ' + repr(bad) + '. Repin PYTHON_BUILD_IMAGE and PYTHON_RUNTIME_IMAGE to one Python minor.') if bad else None; print('ABI OK: ' + repr(len(locked)) + ' version-locked and ' + repr(len(shared) - len(locked)) + ' stable-ABI extensions match ' + suffix)"]

# Provision the data directory. Phantom's storage.data_dir defaults
# to /var/lib/phantom; at startup the service creates
# /var/lib/phantom/<instance> under it. The runtime image runs as the
# Wolfi `nonroot` user (UID/GID 65532) and is shell-less (no /bin/sh,
# so a `RUN` cannot mkdir here); /var/lib is also root-owned. The
# directory is created empty in the build stage and COPYd in with
# nonroot ownership. This also makes a fresh named volume mounted
# here inherit nonroot ownership.
COPY --from=build --chown=65532:65532 /work/phantom-data /var/lib/phantom

# Phantom binds ONE listener: 8080 serves intake (POST /v1/send), the admin
# surface (/v1/admin/*), and liveness/readiness (GET /v1/healthz, GET
# /v1/readyz). The single listener defaults to loopback (127.0.0.1:8080) -
# the loopback bind IS the admin access control (ADR-004). For network
# reachability across containers/hosts the operator sets bind_tcp to
# 0.0.0.0:8080 in the mounted YAML (which exposes the unauthenticated admin
# API and warns at startup); EXPOSE documents the port either way.
EXPOSE 8080

# Health probe on the single listener. The check runs inside the container
# against 127.0.0.1, so it reaches the loopback-bound listener regardless of
# the bind_tcp host. The runtime image is shell-less Wolfi, so the check runs
# Python's stdlib urllib (no curl/wget present) in exec form rather than
# CMD-SHELL.
#
# IT PROBES /v1/readyz, NOT /v1/healthz, AND IT READS THE BODY (finding
# S13-5). /v1/healthz is LIVENESS: it returns HTTP 200 with status "ok" for a
# process that is alive, including one whose data directory is unwritable and
# which is therefore rejecting every POST /v1/send and buffering nothing
# (DegradeReason.SUBSTRATE_UNWRITABLE, ADR-025 / ADR-027). A probe that
# checked only the status code reported that dead deployment as healthy.
# /v1/readyz is the readiness signal, and it ALSO returns HTTP 200 while
# saying ready=false, so the status code alone is not enough there either:
# the probe must parse the JSON and require ready to be true.
#
# start-period covers boot, during which readiness is legitimately false.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import json,sys,urllib.request; r = urllib.request.urlopen('http://127.0.0.1:8080/v1/readyz', timeout=3); sys.exit(0 if r.status == 200 and json.load(r).get('ready') is True else 1)"]

# The phantom service requires `-c <path>` (the YAML config); the
# operator supplies it at deploy time.
ENTRYPOINT ["python", "-m", "phantom"]
