"""Representative upload-sequence payload builders for the E2E suite.

Each builder mirrors one representative upload pattern: a sequence of
file uploads with byte sizes drawn from a realistic distribution. The
bodies are synthetic — random bytes of the right *size* — because the
payload *semantics* (parquet, HTML, JSON) are irrelevant to what
Phantom does. What matters is that the byte counts match the intended
distribution so the storage tier, throttler, and saturation caps
behave the way they will under real load.

Constants document the sizes as synthetic targets. Each builder
returns a list of (:class:`CreateFileRequest`, body bytes) pairs in
sequence order so the E2E test just iterates and submits.
"""

from __future__ import annotations

from dataclasses import dataclass

from tests.e2e._driver import CreateFileRequest, FileMetadata

# ---------------------------------------------------------------------------
# Body-size constants (synthetic targets matching a representative spread).
# ---------------------------------------------------------------------------

# Tiny tabular parquets (overview / summary / detail / order rows).
# Representative pattern puts these at 1.3-5.4 KB; we use round numbers
# near the medians.
COMMENT_PARQUET_BYTES: int = 1_500
HISTORY_PARQUET_BYTES: int = 1_500
SUMMARY_PARQUET_BYTES: int = 1_300
DETAIL_PARQUET_BYTES: int = 1_500
ORDER_PARQUET_BYTES: int = 1_900
OVERVIEW_PARQUET_BYTES: int = 5_000
TRACE_PARQUET_BYTES: int = 5_000
LIMIT_PARQUET_BYTES: int = 5_000
RANGE_PARQUET_BYTES: int = 5_000

# Small parquets that carry one row per entity.
SMALL_RANGE_PARQUET_BYTES: int = 3_600

# Primary-result table is ~3 KB.
PRIMARY_RESULT_BYTES: int = 3_000

# Medium large-binary parquet (~200 KB-1 MB; we use 500 KB, the
# midpoint, to match both sequential_batch and long_sequence).
LARGE_BINARY_PARQUET_BYTES: int = 500_000

# HTML reports embed Plotly JSON; 500 KB matches the midpoint.
HTML_REPORT_BYTES: int = 500_000

# Configuration JSON (model dump). Representative 10-50 KB; 20 KB is
# mid-range.
CONFIG_JSON_BYTES: int = 20_000

# Large-capture JSON: scales to "tens of MiB"; we target ~10 MiB so the
# test forces tier migration.
LARGE_CAPTURE_BYTES: int = 10 * 1024 * 1024


def deterministic_body(size: int, *, seed: bytes) -> bytes:
    """Return a deterministic byte string of the requested size.

    Uses a simple repeating-pattern over a caller-supplied seed so
    the body content varies between callers (avoiding accidental
    cross-test aliasing in the emulator's idempotency dedup) but is
    reproducible for diagnostic purposes.

    Args:
        size: Body length in bytes.
        seed: Bytes mixed into the pattern. Two callers with distinct
            seeds produce distinct bodies.

    Returns:
        Exactly ``size`` bytes.
    """
    if size <= 0:
        return b""
    # Pattern is the seed repeated to length; cheap and deterministic.
    repeats = (size // len(seed)) + 1
    return (seed * repeats)[:size]


@dataclass(frozen=True)
class SequenceUpload:
    """One upload in a sequence: a request plus its body bytes."""

    request: CreateFileRequest
    body: bytes
    # Display name so test failures show which sequence entry blew up.
    label: str


def _sequential_batch_metadata(
    *,
    uploader_id: str,
    label_name: str,
    serial_number: str,
    parcel_id: str,
    group_a: str,
    group_b: str,
    order_number: str,
    line_number: str,
    mode: str = "production",
) -> dict[str, str]:
    """Build the metadata-shape KVS for the sequential-batch sequence.

    Includes every key the sequence stamps onto every upload. All keys
    are generic made-up identifiers proving Phantom's opaque-KVS
    round-trip.
    """
    return {
        "uploader_id": uploader_id,
        "mode": mode,
        "label_name": label_name,
        "serial_number": serial_number,
        "parcel_id": parcel_id,
        "group_a": group_a,
        "group_b": group_b,
        "order_number": order_number,
        "line_number": line_number,
    }


def _make_request(
    *,
    file_name: str,
    lane_base_name: str,
    metadata_kvs: dict[str, str],
    domain: str = "generic",
) -> CreateFileRequest:
    """Build one :class:`CreateFileRequest` with the given KVS."""
    return CreateFileRequest(
        domain=domain,
        lane_base_name=lane_base_name,
        file_name=file_name,
        metadata=FileMetadata(key_value_store=dict(metadata_kvs)),
    )


def build_sequential_batch_sequence(
    *,
    serial_number: str = "SN-SB-0001",
    uploader_id: str = "12345",
    label_name: str = "sequential_batch_v1",
    parcel_id: str = "PCL-1",
    group_a: str = "GROUP-A",
    group_b: str = "GROUP-9",
    order_number: str = "ORD-12345",
    line_number: str = "LINE-1",
) -> list[SequenceUpload]:
    """Build the 13-upload sequential-batch sequence.

    A representative multi-file sequence: a run of small tabular
    parquets, one larger binary parquet, an HTML report, and a config
    JSON. Bodies are sized to synthetic targets.

    Args:
        serial_number: Source serial. Disambiguates per-run uploads.
        uploader_id: Stamped into ``metadata.key_value_store['uploader_id']``.
        label_name: sequence-specific metadata key.
        parcel_id, group_a, group_b, order_number, line_number:
            Sequence-specific metadata stamped on every upload.

    Returns:
        Thirteen :class:`SequenceUpload` entries in sequence order:

        1. ``history`` (1.5 KB parquet)
        2. ``summary`` (1.3 KB parquet)
        3. ``order`` (1.9 KB parquet)
        4. ``detail`` (1.5 KB parquet)
        5. ``comment`` (1.5 KB parquet)
        6. ``threshold_results`` (5 KB parquet)
        7. ``range_rows`` (3.6 KB parquet)
        8. ``overview`` (5 KB parquet)
        9. ``trace_information`` (5 KB parquet)
        10. ``limit_results`` (5 KB parquet)
        11. ``large_binary`` (~500 KB parquet)
        12. ``html_report`` (~500 KB HTML)
        13. ``configuration`` (~20 KB JSON)
    """
    kvs = _sequential_batch_metadata(
        uploader_id=uploader_id,
        label_name=label_name,
        serial_number=serial_number,
        parcel_id=parcel_id,
        group_a=group_a,
        group_b=group_b,
        order_number=order_number,
        line_number=line_number,
    )

    def _entry(
        idx: int, label: str, lane: str, size: int, extra_kv: dict[str, str] | None = None
    ) -> SequenceUpload:
        """Build one entry; ``idx`` is 1-based for file-name disambiguation."""
        # Each upload gets a per-table metadata key (ref_id) plus the
        # sequence-wide keys, so the emulator's received entries are
        # distinguishable on inspection.
        per_table = {**kvs, "ref_id": f"hist-{serial_number}-{idx:02d}"}
        if extra_kv:
            per_table.update(extra_kv)
        return SequenceUpload(
            request=_make_request(
                file_name=f"seqbatch_{lane}_{idx:02d}_{serial_number}",
                lane_base_name=lane,
                metadata_kvs=per_table,
            ),
            body=deterministic_body(size, seed=f"seqbatch-{idx:02d}-".encode()),
            label=label,
        )

    return [
        _entry(1, "history", "history_parquet_data", HISTORY_PARQUET_BYTES),
        _entry(2, "summary", "summary_parquet_data", SUMMARY_PARQUET_BYTES),
        _entry(3, "order", "order_parquet_data", ORDER_PARQUET_BYTES),
        _entry(
            4,
            "detail",
            "detail_parquet_data",
            DETAIL_PARQUET_BYTES,
        ),
        _entry(5, "comment", "comment_parquet_data", COMMENT_PARQUET_BYTES),
        _entry(
            6,
            "threshold_results",
            "threshold_parquet_data",
            LIMIT_PARQUET_BYTES,
        ),
        _entry(
            7,
            "range_rows",
            "range_parquet_data",
            SMALL_RANGE_PARQUET_BYTES,
        ),
        _entry(
            8,
            "overview",
            "overview_parquet_data",
            TRACE_PARQUET_BYTES,
        ),
        _entry(
            9,
            "trace_information",
            "trace_parquet_data",
            TRACE_PARQUET_BYTES,
        ),
        _entry(
            10,
            "limit_results",
            "limit_parquet_data",
            LIMIT_PARQUET_BYTES,
        ),
        _entry(
            11,
            "large_binary",
            "large_binary_parquet_data",
            LARGE_BINARY_PARQUET_BYTES,
        ),
        _entry(
            12,
            "html_report",
            "html_report_data",
            HTML_REPORT_BYTES,
        ),
        _entry(
            13,
            "configuration",
            "configuration_json",
            CONFIG_JSON_BYTES,
        ),
    ]


def build_concurrent_gather_payloads(
    *,
    ref_id: str = "hist-cg-001",
    uploader_id: str = "12345",
) -> list[SequenceUpload]:
    """Build the 3-upload per-source concurrent-gather payload.

    Three uploads (``detail``, ``large_binary``, ``ranges``) fired in
    parallel via ``asyncio.gather``. All three share
    ``metadata.key_value_store['ref_id']`` so the emulator-side
    assertion can prove they belong to the same source.

    Args:
        ref_id: Per-source reference identifier shared by all three.
        uploader_id: Stamped on every upload.

    Returns:
        Three :class:`SequenceUpload` entries with distinct lane names
        and per-lane body sizes, all carrying the same ``ref_id``.
    """
    shared = {
        "uploader_id": uploader_id,
        "mode": "production",
        "label_name": "concurrent_gather",
        "ref_id": ref_id,
    }
    return [
        SequenceUpload(
            request=_make_request(
                file_name=f"gather_detail_{ref_id}",
                lane_base_name="detail_parquet_data",
                metadata_kvs=shared,
            ),
            body=deterministic_body(DETAIL_PARQUET_BYTES, seed=b"gather-detail-"),
            label="detail",
        ),
        SequenceUpload(
            request=_make_request(
                file_name=f"gather_large_binary_{ref_id}",
                lane_base_name="large_binary_parquet_data",
                metadata_kvs=shared,
            ),
            body=deterministic_body(LARGE_BINARY_PARQUET_BYTES, seed=b"gather-large-binary-"),
            label="large_binary",
        ),
        SequenceUpload(
            request=_make_request(
                file_name=f"gather_ranges_{ref_id}",
                lane_base_name="range_parquet_data",
                metadata_kvs=shared,
            ),
            body=deterministic_body(SMALL_RANGE_PARQUET_BYTES, seed=b"gather-ranges-"),
            label="ranges",
        ),
    ]


def build_long_sequence(
    *,
    num_cycles: int = 5,
    uploader_id: str = "12345",
    batch_id: str = "BATCH-LS-001",
) -> list[SequenceUpload]:
    """Build the ~30-upload long sequence.

    A representative large sequential sequence:
    - 5 history x 1.5 KB
    - 5 summary x 1.3 KB
    - 1 primary_result x 3 KB
    - 5 details x 1.5 KB
    - 5 orders x 1.9 KB
    - 5 ranges x 3.6 KB
    - 10 captures (5 cycles x 2 parquets) x 500 KB
    - 1 HTML x 500 KB
    - 1 batch parquet x 1.5 KB
    - 5 config JSONs x 20 KB

    Total: 43 entries with ``num_cycles=5``.

    Args:
        num_cycles: How many capture cycles to emit. 5 gives a
            43-upload sequence consistent with the "long sequential"
            framing.
        uploader_id: Stamped on every upload.
        batch_id: Run-correlation token folded into each file name.

    Returns:
        The ordered list of :class:`SequenceUpload` entries.
    """
    base = {
        "uploader_id": uploader_id,
        "mode": "production",
        "label_name": "long_sequence",
    }

    def _entry(label: str, lane: str, size: int, idx: int, *, seed_prefix: str) -> SequenceUpload:
        """Per-entry builder; ``idx`` is 1-based and disambiguates files."""
        kvs = {**base, "ref_id": f"hist-ls-{idx:02d}"}
        return SequenceUpload(
            request=_make_request(
                file_name=f"longseq_{lane}_{idx:02d}_{batch_id}",
                lane_base_name=lane,
                metadata_kvs=kvs,
            ),
            body=deterministic_body(size, seed=f"{seed_prefix}{idx:02d}-".encode()),
            label=label,
        )

    uploads: list[SequenceUpload] = []

    # Stage 1: histories, one per cycle.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "history",
                "history_parquet_data",
                HISTORY_PARQUET_BYTES,
                i,
                seed_prefix="ls-hist-",
            )
        )

    # Stage 2: summaries, one per cycle.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "summary",
                "summary_parquet_data",
                SUMMARY_PARQUET_BYTES,
                i,
                seed_prefix="ls-summ-",
            )
        )

    # Stage 3: primary result, one for the whole run.
    uploads.append(
        _entry(
            "primary_result",
            "primary_result_parquet_data",
            PRIMARY_RESULT_BYTES,
            1,
            seed_prefix="ls-pr-",
        )
    )

    # Stage 4: details.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "detail",
                "detail_parquet_data",
                DETAIL_PARQUET_BYTES,
                i,
                seed_prefix="ls-dt-",
            )
        )

    # Stage 5: orders.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "order",
                "order_parquet_data",
                ORDER_PARQUET_BYTES,
                i,
                seed_prefix="ls-or-",
            )
        )

    # Stage 6: ranges.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "range",
                "range_parquet_data",
                SMALL_RANGE_PARQUET_BYTES,
                i,
                seed_prefix="ls-rg-",
            )
        )

    # Stage 7: 10 per-cycle large-binary parquets (2 variants per
    # cycle). These are the medium-bulk uploads.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "large_binary_a",
                "large_binary_a_parquet_data",
                LARGE_BINARY_PARQUET_BYTES,
                i,
                seed_prefix="ls-lba-",
            )
        )
        uploads.append(
            _entry(
                "large_binary_b",
                "large_binary_b_parquet_data",
                LARGE_BINARY_PARQUET_BYTES,
                i,
                seed_prefix="ls-lbb-",
            )
        )

    # Stage 8: HTML report (per run).
    uploads.append(
        _entry(
            "html_report",
            "html_report_data",
            HTML_REPORT_BYTES,
            1,
            seed_prefix="ls-html-",
        )
    )

    # Stage 9: batch parquet (per run).
    uploads.append(
        _entry(
            "batch",
            "batch_parquet_data",
            DETAIL_PARQUET_BYTES,
            1,
            seed_prefix="ls-batch-",
        )
    )

    # Stage 10: config JSONs, one per cycle.
    for i in range(1, num_cycles + 1):
        uploads.append(
            _entry(
                "configuration",
                "configuration_json",
                CONFIG_JSON_BYTES,
                i,
                seed_prefix="ls-cfg-",
            )
        )

    return uploads


def build_large_body(
    *,
    uploader_id: str = "12345",
    multistage_id: str = "MS-001",
) -> SequenceUpload:
    """Build the single 10 MiB large-capture JSON upload.

    The largest single body in the suite. The body is synthetic but is
    the documented 10 MiB target size so the test exercises Phantom's
    size-aware persist trigger.

    Args:
        uploader_id: Stamped on the upload.
        multistage_id: Large-body-specific metadata key.

    Returns:
        One :class:`SequenceUpload` with a 10 MiB body.
    """
    kvs = {
        "uploader_id": uploader_id,
        "mode": "production",
        "label_name": "large_body",
        "multistage_id": multistage_id,
    }
    return SequenceUpload(
        request=_make_request(
            file_name=f"largebody_capture_{multistage_id}",
            lane_base_name="large_capture_json",
            metadata_kvs=kvs,
        ),
        body=deterministic_body(LARGE_CAPTURE_BYTES, seed=b"large-capture-"),
        label="large_capture",
    )
