import pytest
from starlette.testclient import TestClient
from app import app
from operations.utils.db import run_create_database
from threading import Thread


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


def test_remove_db(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()


@pytest.mark.asyncio
async def test_clean_object(client):
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-delete-object-bucket",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1-a"],
        },
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    # set policy
    resp = client.post(
        "/update_policy",
        json={
            "bucket": "my-delete-object-bucket",
            "put_policy": "push",
        },
    )
    resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-delete-object-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for physical_object in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
                "ttl": 3600,  # 1 hour
            },
        ).raise_for_status()

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-delete-object-bucket",
        },
    )
    assert resp.json() != []

    # Test clean object (receive a timestamp and delete objects that have expired)
    # Try receiving timestamp after 1 hour
    resp = client.post(
        "/clean_object",
        json={
            "timestamp": "2020-01-01T01:00:00",
        },
    )
    assert resp.json() != []
