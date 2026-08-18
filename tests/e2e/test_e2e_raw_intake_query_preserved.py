"""F4 gate: a query-addressed raw upload keeps its query to the upstream.

Before F4 both destination carriers dropped the inbound query string, so a
stock ``PUT /bucket/key?partNumber=3&uploadId=ABC`` reached the upstream as a
whole-object ``PUT /bucket/key``. The upstream accepted it, the object was
overwritten with one part's bytes, and Phantom reported ``succeeded``. That is
silent data destruction, and every query-addressed S3 operation (``?uploads``,
``?tagging``, ``?acl``, ``?versionId``) broke the same way, as did presigned
authentication, whose whole credential lives in the query.

This is the end-to-end proof over the real wire: what the emulator's ``/raw``
sink recorded is what Phantom forwarded. The sink records the inbound query in
``RawBody.query`` for exactly this assertion; the ``{path:path}`` convertor
never captures it, so without that field a preserved and a dropped query would
produce the same record.

Boot and raw-PUT shape are taken from
``tests/e2e/test_e2e_raw_intake_forward_as_is.py``: a stock ``httpx`` request
through the catch-all against the auth-free ``/raw`` sink, read back through
the typed emulator state.
"""

from __future__ import annotations

import httpx
import pytest
from phantom_client.models.envelope import parse_response_headers

from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

# Phantom's buffering ack for an admitted raw intake (delivery is async).
INTAKE_ACCEPTED_STATUS: int = 202

# Upper bound on the forwarded body landing in the /raw sink. Matches the
# forward-as-is gate's budget; delivery rides the retry worker so the
# read-back polls.
DELIVERY_TIMEOUT_SECONDS: float = 10.0

# The distinctive payload, so the delivered record is unambiguously this test's.
QUERY_PAYLOAD: bytes = b"phantom-e2e-query-preserved-payload"

# The object path and the query that addresses a multipart PART rather than
# the whole object. Dropping this query is the difference between uploading
# part 3 and overwriting the object with part 3's bytes.
OBJECT_PATH: str = "querybucket/nested/part-object.bin"
FORWARDED_QUERY: str = "partNumber=3&uploadId=ABC"


def _forward_as_is_overrides(default_target: str) -> dict[str, object]:
    """Build the ``config_overrides`` overlay for the forward-as-is path.

    Reproduces the suite's ``primary`` instance with the route's ``auth_mode``
    set to ``none``, because the tokenless raw-intake path has no bearer for
    the emulator host and would otherwise fail the forward 401.

    Args:
        default_target: The ``phantom_default_target`` value, carrying the
            literal ``{EMULATOR_URL}`` token that ``_build_phantom_settings``
            rewrites at merge time.

    Returns:
        The overlay mapping for :func:`boot_stack`'s ``config_overrides``.
    """
    return {
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                "data_dir": "primary",
                "capture_reexecution": False,
                "routes": [
                    {
                        "name": "emulator",
                        "hosts": ["emulator", "127.0.0.1", "localhost"],
                        "auth_mode": "none",
                    },
                ],
            },
        ],
        "phantom_default_target": default_target,
    }


async def _await_raw_delivery(stack: E2EStack, path: str) -> None:
    """Poll the emulator's /raw sink until ``path`` is stored.

    Args:
        stack: The running stack (for ``emulator_url``).
        path: The full forwarded path the sink keys on.
    """
    read_url = f"{stack.emulator_url}/raw/{path}"
    async with httpx.AsyncClient() as client:

        async def _delivered() -> bool:
            resp = await client.get(read_url)
            return resp.status_code == 200

        await await_until(
            _delivered,
            timeout_seconds=DELIVERY_TIMEOUT_SECONDS,
            message=f"forwarded body never reached the /raw sink at {path!r}",
        )


@pytest.mark.e2e
async def test_raw_intake_forwards_the_query_string_to_the_upstream() -> None:
    """A query-addressed raw upload arrives upstream with its query intact.

    Objective: prove end to end that the difference between a part upload and
    an object overwrite survives Phantom. Success is the row delivering and
    the sink's record for that path carrying the exact inbound query text.
    """
    stack = await boot_stack(
        config_overrides=_forward_as_is_overrides("{EMULATOR_URL}/raw"),
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/{OBJECT_PATH}?{FORWARDED_QUERY}",
                content=QUERY_PAYLOAD,
            )
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )

        await _await_raw_delivery(stack, OBJECT_PATH)
        raw = stack.emulator.raw_body(OBJECT_PATH)
        assert raw is not None, f"no RawBody stored under {OBJECT_PATH!r}"
        assert raw.body == QUERY_PAYLOAD, "RawBody.body is not byte-identical to the PUT body"
        assert raw.query == FORWARDED_QUERY, (
            "the inbound query must reach the upstream byte-for-byte; "
            f"expected {FORWARDED_QUERY!r}, got {raw.query!r}"
        )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_raw_intake_ack_is_sdk_parseable() -> None:
    """CL8: the live raw-intake 202 parses with the SDK's strict header model.

    Objective: end-to-end proof over the real wire that the raw-intake ack
    carries the canonical six rather than the hand-built two. The SDK's
    ``ResponseHeaders`` is strict and forbids extras, so before CL8 a
    SUCCESSFUL upload made ``parse_response_headers`` raise. Success: the live
    ack parses and its ``group_id`` equals its ``upload_id``, because a stock
    client sends no grouping header.

    ``resp.headers`` is passed rather than ``dict(resp.headers)``:
    ``parse_response_headers`` looks up canonical mixed-case names with a
    plain ``.get``, so its case-insensitivity comes from the caller's mapping.
    ``httpx.Headers`` is case-insensitive; a plain dict built from it is
    lower-cased and would raise whatever the service does.
    """
    stack = await boot_stack(
        config_overrides=_forward_as_is_overrides("{EMULATOR_URL}/raw"),
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{stack.phantom_url}/ackbucket/sdk-parseable.bin",
                content=QUERY_PAYLOAD,
            )
        assert resp.status_code == INTAKE_ACCEPTED_STATUS, (
            f"expected {INTAKE_ACCEPTED_STATUS} intake ack, got {resp.status_code}: {resp.text!r}"
        )

        parsed = parse_response_headers(resp.headers)

        assert parsed.upload_id == parsed.group_id, (
            "a raw upload is a group of one, so the ack's group id is its upload id"
        )
        assert parsed.status == "queued"
        assert parsed.attempts == 0
    finally:
        await stack.tear_down()
