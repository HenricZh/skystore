import pytest
from starlette.testclient import TestClient
from app import app, rm_lock_on_timeout
from operations.utils.db import run_create_database
from threading import Thread
import datetime

@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


# NOTE: Do not change the position of this test, it should be the first test
def test_remove_db(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()

@pytest.mark.asyncio
async def test_remove_objects(client):
    # Tests eviction and fixed_ttl policy

    # Create Bucket
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-get-version-bucket",
            "client_from_region": "aws:us-east-1",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-get-version-bucket",
            "versioning": True,
        },
    )

    # set policy
    resp = client.post(
        "/update_policy",
        json={
            "bucket": "my-get-version-bucket",
            "put_policy": "fixed_ttl",
            "get_policy": "cheapest",
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for i, physical_object in enumerate(resp.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
                "version_id": f"version-{i}",
            },
        ).raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for i, physical_object in enumerate(resp.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T06:00:00",
                "version_id": f"version-{i}",
            },
        ).raise_for_status()


    # # this will locate the newest version of the object
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )
    #resp.raise_for_status()
    #resp_data = 
    #print(resp_data)
    
    # print(resp)

    resp = client.post(
        "/clean_object",
        json={
            "timestamp": datetime.strptime("2020-01-01T13:00:00", "%Y-%m-%dT%H:%M:%S")
        },
    )

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert len(resp.json()) == 0

    # TTL is 12hrs but object has only been in us-west-1 for 6.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert len(resp.json()) > 0

def test_remove_db2(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()

def test_remove_ready_objects(client):
    # Create Bucket
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-get-version-bucket",
            "client_from_region": "aws:us-east-1",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-get-version-bucket",
            "versioning": True,
        },
    )

    # set policy
    resp = client.post(
        "/update_policy",
        json={
            "bucket": "my-get-version-bucket",
            "put_policy": "push",
            "get_policy": "cheapest",
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "not-ready",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()


    resp = client.post(
        "/clean_object",
        json={
            "timestamp": datetime.strptime("2020-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")
        },
    )

    # Object Should not exist
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "not-ready",
            "client_from_region": "aws:us-east-1",
        },
    )
    resp.raise_for_status()

def test_remove_db3(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()


def test_policy(client):
    # Create Bucket
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-get-version-bucket",
            "client_from_region": "aws:us-east-1",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-get-version-bucket",
            "versioning": True,
        },
    )

    # set policy
    resp = client.post(
        "/update_policy",
        json={
            "bucket": "my-get-version-bucket",
            "put_policy": "t_even",
            "get_policy": "cheapest",
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for i, physical_object in enumerate(resp.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
                "version_id": f"version-{i}",
            },
        ).raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for i, physical_object in enumerate(resp.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-02T12:00:00",
                "version_id": f"version-{i}",
            },
        ).raise_for_status()


    # # this will locate the newest version of the object
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )

    resp = client.post(
        "/clean_object",
        json={
            "timestamp": datetime.strptime("2020-01-03T00:00:00", "%Y-%m-%dT%H:%M:%S")
        },
    )

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert len(resp.json()) == 0

    # TTL is 12hrs but object has only been in us-west-1 for 6.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert len(resp.json()) > 0