"""Unit tests for :mod:`phantom_emulator.upload.correlation`."""

from __future__ import annotations

from phantom_emulator.upload.correlation import echo_metadata_kvs, extract_metadata_kvs


def test_phantom_local_uuid_roundtrip() -> None:
    request = {
        "domain": "Acme",
        "fileName": "f.parquet",
        "metadata": {
            "keyValueStore": {
                "phantom_local_uuid": "a-b-c-d",
                "uploader_id": "operator-1",
            }
        },
    }
    kvs = extract_metadata_kvs(request)
    assert kvs == {"phantom_local_uuid": "a-b-c-d", "uploader_id": "operator-1"}

    response: dict[str, object] = {"id": "x"}
    echo_metadata_kvs(response, kvs)
    assert response["metadata"] == {
        "keyValueStore": {
            "phantom_local_uuid": "a-b-c-d",
            "uploader_id": "operator-1",
        }
    }


def test_arbitrary_keys_preserved() -> None:
    request = {"metadata": {"keyValueStore": {"k1": "v1", "k2": "v2", "k3": "v3"}}}
    kvs = extract_metadata_kvs(request)
    assert kvs == {"k1": "v1", "k2": "v2", "k3": "v3"}


def test_missing_metadata_returns_empty() -> None:
    assert extract_metadata_kvs({}) == {}
    assert extract_metadata_kvs({"metadata": None}) == {}
    assert extract_metadata_kvs({"metadata": "not-a-dict"}) == {}
    assert extract_metadata_kvs({"metadata": {"keyValueStore": "nope"}}) == {}


def test_non_string_values_coerced() -> None:
    kvs = extract_metadata_kvs({"metadata": {"keyValueStore": {"int": 7, "bool": True}}})
    assert kvs == {"int": "7", "bool": "True"}


def test_non_string_keys_dropped() -> None:
    # Keys that aren't strings are silently dropped — the upstream
    # contract is string-keyed only, and the emulator should not
    # invent encoding.
    kvs = extract_metadata_kvs({"metadata": {"keyValueStore": {7: "v", "ok": "v"}}})
    assert kvs == {"ok": "v"}
