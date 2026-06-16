# syntax=docker/dockerfile:1.7
#
# phantom-emulator container image (Phase 3 — Wolfi multi-stage).
#
# TEST INFRASTRUCTURE ONLY. The emulator's control plane is permissive
# by design (no auth on `/control/*` — see CONTEXT.md and the emulator
# README). Deploy on a loopback or test-network bind; never on a
# production interface.
#
# Base image rationale: Chainguard Wolfi (see src/phantom-service/docker/Dockerfile
# for the full rationale). glibc-based; minimal; non-Debian.
#
# Multi-arch: `docker buildx build --platform linux/arm64,linux/amd64`.
#
# Build context: the repository root (the package path
# `src/phantom-emulator/` is relative to it).
#
# Entry point: `python -m phantom_emulator` (no required args; the
# CLI takes an optional `-c <path>`).

# ---------- Stage 1: build the emulator wheel with uv -----------------------
FROM cgr.dev/chainguard/python:latest-dev AS build

WORKDIR /work

RUN pip install --no-cache-dir --user uv

# Copy the package source. phantom-emulator's pyproject.toml carries its
# own hatchling build config and all of its runtime deps from public PyPI.
COPY src/phantom-emulator/ ./phantom-emulator/

RUN cd phantom-emulator \
 && python -m uv build --wheel --out-dir /work/dist \
 && mkdir -p /work/install \
 && pip install --no-cache-dir --no-warn-script-location --target /work/install /work/dist/phantom_emulator-*.whl

# ---------- Stage 2: minimal runtime ----------------------------------------
FROM cgr.dev/chainguard/python:latest AS runtime

# /install drops the installed wheel + deps onto PYTHONPATH; no venv.
# DEFAULT_PORT in phantom_emulator.config is 8000 — the container
# listens on 8000 unless the operator overrides it via
# `PHANTOM_EMULATOR_SERVER__PORT` or a mounted config YAML.
ENV PYTHONPATH=/install \
    PYTHONUNBUFFERED=1 \
    PHANTOM_EMULATOR_SERVER__HOST=0.0.0.0 \
    PHANTOM_EMULATOR_SERVER__PORT=8000

COPY --from=build /work/install /install

EXPOSE 8000

ENTRYPOINT ["python", "-m", "phantom_emulator"]
