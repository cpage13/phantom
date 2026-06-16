"""End-to-end smoke: mint → create → PUT → GET → /control/received.

Boots the emulator on an ephemeral port via :func:`start_server` and
walks through the canonical two-step upload sequence against the
real HTTP surface. Asserts that every step returns 200 and that the
``phantom_local_uuid`` correlation round-trips byte-for-byte.
"""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest
from phantom_emulator import AppConfig, start_server
from phantom_emulator.config import ServerCfg


@pytest.fixture(autouse=True)
def _emulator_signing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)


async def test_smoke_end_to_end() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        base = server.url()
        local_uuid = str(uuid4())

        async with httpx.AsyncClient(base_url=base) as client:
            # 1. Mint a JWT.
            token_r = await client.post(
                "/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": "test-client",
                    "client_secret": "test-secret",
                },
            )
            assert token_r.status_code == 200, token_r.text
            jwt = token_r.json()["access_token"]

            # 2. Create a file with phantom_local_uuid in the metadata.
            create_r = await client.post(
                "/v1/files/create",
                json={
                    "domain": "SmokeDomain",
                    "laneBaseName": "smoke-lane",
                    "fileName": "smoke.parquet",
                    "metadata": {
                        "keyValueStore": {
                            "phantom_local_uuid": local_uuid,
                            "uploader_id": "alice",
                        }
                    },
                },
                headers={
                    "Authorization": f"Bearer {jwt}",
                    "Idempotency-Key": "smoke-key-1",
                },
            )
            assert create_r.status_code == 200, create_r.text
            create_body = create_r.json()
            file_info = create_body["fileInformation"]
            upload_url = create_body["uploadUrl"]
            assert file_info["metadata"]["keyValueStore"]["phantom_local_uuid"] == local_uuid

            # 3. PUT the body.
            relative_upload = upload_url.replace(base, "")
            body = b"smoke-bytes-payload" * 32
            put_r = await client.put(
                relative_upload,
                content=body,
                headers={
                    "x-amz-meta-uploader-id": "alice",
                    "content-type": "application/octet-stream",
                },
            )
            assert put_r.status_code == 200, put_r.text

            # 4. GET the file by id — must echo the same FileInformation.
            file_id = file_info["id"]
            get_r = await client.get(
                f"/v1/files/{file_id}",
                headers={"Authorization": f"Bearer {jwt}"},
            )
            assert get_r.status_code == 200, get_r.text
            assert get_r.json()["id"] == file_id

            # 5. /control/received shows the upload.
            received_r = await client.get("/control/received")
            assert received_r.status_code == 200
            entries = received_r.json()["received"]
            assert len(entries) == 1
            entry = entries[0]
            assert entry["body_size"] == len(body)
            assert entry["metadata_kvs"]["phantom_local_uuid"] == local_uuid
            assert entry["x_amz_meta_headers"]["x-amz-meta-uploader-id"] == "alice"
            assert entry["idempotency_key"] == "smoke-key-1"
    finally:
        await server.stop()
