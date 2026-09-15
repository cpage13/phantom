# phantom-emulator container image — Wolfi, test/CI infrastructure ONLY.
#
# Deliberately NO `# syntax=docker/dockerfile:X` pin, matching the service
# image: the directive adds a docker.io fetch (availability + credential
# dependency) and nothing here needs a non-built-in frontend feature.
#
# This image exists for the Docker-lane E2Es (the audit T7 compose
# topology: a real phantom-service container delivering to a real
# emulator container over the compose network) and for local operator
# experiments. It is NOT shipped to a registry; the E2E builds it
# locally as phantom-emulator-e2e:<tag>.
#
# Structure mirrors src/phantom-deploy/Dockerfile (the service image):
#   Stage 1 `build`   — cgr.dev/chainguard/python:latest-dev
#                       installs `uv`, builds the wheel, pip-installs it
#                       into /install, prunes debug symbols + __pycache__
#                       + bundled tests.
#   Stage 2 `runtime` — cgr.dev/chainguard/python:latest
#                       shell-less runtime carrying only /install, runs
#                       as the Wolfi `nonroot` user (65532).
#
# Build (from the repository root):
#     docker build -f src/phantom-emulator/Dockerfile -t phantom-emulator-e2e:dev .
#
# The image bakes no default config. The test supplies the YAML at run
# time via a read-only mount + the `-c <path>` argument, and passes the
# HS256 signing secret via the EMULATOR_SIGNING_KEY environment variable.


# ---------- Base images: pinned by digest (finding S13-10) ------------------
#
# Same pins, same reasons, same refresh procedure as
# src/phantom-deploy/Dockerfile, whose header documents both at length: a
# floating `latest` is not reproducible, and the build and runtime stages
# resolve two INDEPENDENT tags whose Python ABI must match or the copied C
# extensions cannot be imported at container start. These are OCI image INDEX
# digests, so multi-arch builds still resolve per platform. The ABI assertion
# in the runtime stage turns a mismatched repin into a build failure.
#
# Keep these two digests equal to the service image's. They are the same two
# upstream references, and letting the images drift apart would mean the
# Docker-lane E2E exercises a pair the shipped service image never uses.
#
# Pinned 2026-09-15.
ARG PYTHON_BUILD_IMAGE=cgr.dev/chainguard/python:latest-dev@sha256:666971ccbb01312b3e12799879a76e8b8cb574c6ed8218d6a8faa4d7ac2e493e
ARG PYTHON_RUNTIME_IMAGE=cgr.dev/chainguard/python:latest@sha256:cbe0e6d0e3abdfc5b54938b140be046e345ef765f8658e2a165b5417cb2bb6a9


# ---------- Stage 1: build the phantom-emulator wheel with uv, then prune ---
FROM ${PYTHON_BUILD_IMAGE} AS build

WORKDIR /work

# Install uv as the build front-end. Build stage is throwaway.
RUN pip install --no-cache-dir --user uv

# Copy the phantom-emulator source. Its pyproject.toml carries all of
# its build config (hatchling) and runtime deps from public PyPI; no
# workspace-level files are needed.
COPY src/phantom-emulator/ ./emulator/

# Build the wheel, pip-install it (and its public-PyPI deps) into
# /install, then PRUNE /install the same way the service image does:
# strip .so debug symbols, drop __pycache__, drop bundled test suites.
RUN cd emulator \
 && python -m uv build --wheel --out-dir /work/dist \
 && mkdir -p /work/install \
 && pip install --no-cache-dir --no-warn-script-location --target /work/install /work/dist/phantom_emulator-*.whl \
 && find /work/install -name '*.so' -exec strip --strip-unneeded {} + \
 && find /work/install -name '__pycache__' -type d -exec rm -rf {} + \
 && find /work/install -type d \( -name tests -o -name test \) -exec rm -rf {} +


# ---------- Stage 2: minimal runtime ----------------------------------------
FROM ${PYTHON_RUNTIME_IMAGE} AS runtime

# Drop /install on PYTHONPATH so the runtime Python finds the installed
# packages without a venv.
ENV PYTHONPATH=/install \
    PYTHONUNBUFFERED=1

COPY --from=build /work/install /install

# ABI GATE (finding S13-10), identical to the service image's. A version-locked
# C extension built under the build stage's interpreter carries that
# interpreter's ABI tag in its filename, so a build/runtime pair that straddles
# a Python minor bump cannot import it. Fail here, at build, rather than at
# container start inside a Docker-lane E2E where it reads as a flaky test.
RUN ["python", "-c", "import pathlib, sys, sysconfig; suffix = sysconfig.get_config_var('EXT_SUFFIX'); shared = sorted(p.name for p in pathlib.Path('/install').rglob('*.so')); locked = [n for n in shared if '.cpython-' in n]; bad = [n for n in locked if not n.endswith(suffix)]; sys.exit('ABI GATE INCONCLUSIVE: no shared objects under /install; the wheel install or the prune step changed shape, so this check proves nothing') if not shared else None; sys.exit('ABI MISMATCH between the pinned build and runtime base images: this interpreter loads ' + suffix + ' but /install carries version-locked extensions ' + repr(bad) + '. Repin PYTHON_BUILD_IMAGE and PYTHON_RUNTIME_IMAGE to one Python minor.') if bad else None; print('ABI OK: ' + repr(len(locked)) + ' version-locked and ' + repr(len(shared) - len(locked)) + ' stable-ABI extensions match ' + suffix)"]

# The emulator serves the upstream-shaped API + /control surface on one
# port (8000 by convention in the E2E compose topology; the mounted
# YAML's server block decides the actual bind). EXPOSE documents it.
EXPOSE 8000

# Liveness probe: the OpenID discovery document is served unauthenticated
# and proves the app booted and routing works. The runtime image is
# shell-less Wolfi, so the check runs Python's stdlib urllib in exec form.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/.well-known/openid-configuration', timeout=3).status == 200 else 1)"]

# The emulator takes `-c <path>` (the YAML config); the test supplies it
# at run time.
ENTRYPOINT ["python", "-m", "phantom_emulator"]
