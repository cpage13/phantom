# syntax=docker/dockerfile:1.7
#
# phantom-service container image — Wolfi multi-arch (linux/amd64 + linux/arm64).
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


# ---------- Stage 1: build the phantom wheel with uv, then prune ------------
FROM cgr.dev/chainguard/python:latest-dev AS build

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
FROM cgr.dev/chainguard/python:latest AS runtime

# Drop /install on PYTHONPATH so the runtime Python finds the
# installed packages without a venv.
ENV PYTHONPATH=/install \
    PYTHONUNBUFFERED=1

COPY --from=build /work/install /install

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

# Liveness probe on the single listener (GET /v1/healthz). The check runs
# inside the container against 127.0.0.1, so it reaches the loopback-bound
# listener regardless of the bind_tcp host. The runtime image is shell-less
# Wolfi, so the check runs Python's stdlib urllib (no curl/wget present) in
# exec form rather than CMD-SHELL.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/v1/healthz', timeout=3).status == 200 else 1)"]

# The phantom service requires `-c <path>` (the YAML config); the
# operator supplies it at deploy time.
ENTRYPOINT ["python", "-m", "phantom"]
