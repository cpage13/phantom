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


# ---------- Stage 1: build the phantom-emulator wheel with uv, then prune ---
FROM cgr.dev/chainguard/python:latest-dev AS build

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
FROM cgr.dev/chainguard/python:latest AS runtime

# Drop /install on PYTHONPATH so the runtime Python finds the installed
# packages without a venv.
ENV PYTHONPATH=/install \
    PYTHONUNBUFFERED=1

COPY --from=build /work/install /install

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
