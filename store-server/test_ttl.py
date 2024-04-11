import csv
import json
import time
import pytest
from starlette.testclient import TestClient
from app import app, rm_lock_on_timeout
from operations.utils.db import run_create_database
from threading import Thread
from datetime import datetime
from conf import TEST_CONFIGURATION


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

    # # enable bucket versioning
    # resp = client.post(
    #     "/put_bucket_versioning",
    #     json={
    #         "bucket": "my-get-version-bucket",
    #         "versioning": True,
    #     },
    # )

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
                # "version_id": f"version-{i}",
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
                # "version_id": f"version-{i}",
            },
        ).raise_for_status()

    # # this will locate the newest version of the object
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-east-1",
        },
    )
    resp.raise_for_status()

    resp = client.post(
        "/clean_object",
        json={"timestamp": "2020-01-01T13:00:00"},
    )

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-east-1",
        },
    )
    assert resp.status_code == 404

    # TTL is 12hrs but object has only been in us-west-1 for 6.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()


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
    # resp = client.post(
    #     "/put_bucket_versioning",
    #     json={
    #         "bucket": "my-get-version-bucket",
    #         "versioning": True,
    #     },
    # )

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
        json={"timestamp": "2020-01-01T00:00:00"},
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
    assert resp.status_code == 404


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
    # resp = client.post(
    #     "/put_bucket_versioning",
    #     json={
    #         "bucket": "my-get-version-bucket",
    #         "versioning": True,
    #     },
    # )

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
                # "version_id": f"version-{i}",
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
                "last_modified": "2020-01-02T06:00:00",
                # "version_id": f"version-{i}",
            },
        ).raise_for_status()

    # # this will locate the newest version of the object
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-east-1",
        },
    )
    resp.raise_for_status()

    resp = client.post(
        "/clean_object",
        json={"timestamp": "2020-01-02T08:00:00"},
    )

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-evict",
            "client_from_region": "aws:us-east-1",
        },
    )
    assert resp.status_code == 404

    # TTL is 12hrs but object has only been in us-west-1 for 6.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "should-keep",
            "client_from_region": "aws:us-east-1",
        },
    )
    resp.raise_for_status()


def test_remove_db4(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()


def test_trace_fixedttl(client):
    # aws:us-west-1, aws:us-east-1, aws:eu-south-1, aws:eu-central-1, aws:eu-north-1, gcp:eu-west1-a,
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "test-bucket",  # + region[4:],
            "client_from_region": "aws:us-east-1",  # region,
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
            "put_policy": "fixed_ttl",
            "get_policy": "cheapest",
        },
    )

    responses = []
    with open("./experiment/trace/fixedttl12hr.csv", "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            timestamp_str, op, issue_region, data_id, size, ttl = row
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            # if previous_timestamp:
            #     wait_time = (timestamp - previous_timestamp).total_seconds()
            #     time.sleep(wait_time)

            if op == "write":
                resp = client.post(
                    "/start_upload",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                        "is_multipart": False,
                        "ttl": ttl,
                    },
                )
                resp.raise_for_status()

                for i, physical_object in enumerate(resp.json()["locators"]):
                    # print(physical_object)
                    client.patch(
                        "/complete_upload",
                        json={
                            "id": physical_object["id"],
                            "size": size,
                            "etag": "123",
                            "last_modified": timestamp_str.replace(" ", "T"),
                            # "version_id": f"version-{i}",
                        },
                    ).raise_for_status()

                responses.append((op, issue_region))

            elif op == "read":
                resp = client.post(
                    "/locate_object",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                    },
                )
                resp.raise_for_status()

                responses.append((op, json.loads(resp.text)["tag"]))

                ttl = json.loads(resp.text)["ttl"]
                resp = client.post(
                    "/start_upload",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                        "is_multipart": False,
                        "ttl": ttl,
                    },
                )

                if resp.status_code <= 200:
                    for i, physical_object in enumerate(resp.json()["locators"]):
                        print(physical_object)
                        client.patch(
                            "/complete_upload",
                            json={
                                "id": physical_object["id"],
                                "size": size,
                                "etag": "123",
                                "last_modified": timestamp_str.replace(" ", "T"),
                                # "version_id": f"version-{i}",
                            },
                        )

            resp = client.post(
                "/clean_object",
                json={
                    "timestamp": timestamp_str.replace(" ", "T"),
                },
            )
        assert responses == [
            ("write", "aws:us-west-1"),
            ("read", "aws:us-west-1"),
            ("write", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("write", "aws:eu-central-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:eu-central-1"),
            ("read", "aws:us-west-1"),
        ]


def test_remove_db5(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()


def test_trace_teven(client):
    # aws:us-west-1, aws:us-east-1, aws:eu-south-1, aws:eu-central-1, aws:eu-north-1, gcp:eu-west1-a,
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "test-bucket",  # + region[4:],
            "client_from_region": "aws:us-east-1",  # region,
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
            "put_policy": "t_even",
            "get_policy": "cheapest",
        },
    )

    responses = []
    with open("./experiment/trace/teven.csv", "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            timestamp_str, op, issue_region, data_id, size, ttl = row
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            # if previous_timestamp:
            #     wait_time = (timestamp - previous_timestamp).total_seconds()
            #     time.sleep(wait_time)

            if op == "write":
                resp = client.post(
                    "/start_upload",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                        "is_multipart": False,
                        "ttl": ttl,
                    },
                )
                resp.raise_for_status()

                for i, physical_object in enumerate(resp.json()["locators"]):
                    # print(physical_object)
                    client.patch(
                        "/complete_upload",
                        json={
                            "id": physical_object["id"],
                            "size": size,
                            "etag": "123",
                            "last_modified": timestamp_str.replace(" ", "T"),
                            # "version_id": f"version-{i}",
                        },
                    ).raise_for_status()

                responses.append((op, issue_region))

            elif op == "read":
                resp = client.post(
                    "/locate_object",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                    },
                )
                resp.raise_for_status()

                responses.append((op, json.loads(resp.text)["tag"]))

                ttl = json.loads(resp.text)["ttl"]
                print("ttl:", ttl)
                resp = client.post(
                    "/start_upload",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                        "is_multipart": False,
                        "ttl": ttl,
                    },
                )

                if resp.status_code <= 200:
                    for i, physical_object in enumerate(resp.json()["locators"]):
                        print(physical_object)
                        client.patch(
                            "/complete_upload",
                            json={
                                "id": physical_object["id"],
                                "size": size,
                                "etag": "123",
                                "last_modified": timestamp_str.replace(" ", "T"),
                                # "version_id": f"version-{i}",
                            },
                        )

            resp = client.post(
                "/clean_object",
                json={
                    "timestamp": timestamp_str.replace(" ", "T"),
                },
            )
        assert responses == [
            ("write", "aws:us-west-1"),
            ("read", "aws:us-west-1"),
            ("write", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("write", "aws:eu-central-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:eu-central-1"),
            ("read", "aws:us-west-1"),
        ]


def test_remove_db6(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()


def test_trace_tevict(client):
    # aws:us-west-1, aws:us-east-1, aws:eu-south-1, aws:eu-central-1, aws:eu-north-1, gcp:eu-west1-a,
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "test-bucket",  # + region[4:],
            "client_from_region": "aws:us-east-1",  # region,
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
            "put_policy": "fixed_ttl",  # Put fixed_ttl but the trace wil set ttl correctly
            "get_policy": "cheapest",
        },
    )

    responses = []
    with open("./experiment/trace/tevict.csv", "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            timestamp_str, op, issue_region, data_id, size, ttl = row
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            if op == "write":
                resp = client.post(
                    "/start_upload",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                        "is_multipart": False,
                        "ttl": 3600 * float(ttl) if float(ttl) != -1 else -1,
                    },
                )
                resp.raise_for_status()

                for i, physical_object in enumerate(resp.json()["locators"]):
                    # print(physical_object)
                    client.patch(
                        "/complete_upload",
                        json={
                            "id": physical_object["id"],
                            "size": size,
                            "etag": "123",
                            "last_modified": timestamp_str.replace(" ", "T"),
                            # "version_id": f"version-{i}",
                        },
                    ).raise_for_status()

                responses.append((op, issue_region))

            elif op == "read":
                resp = client.post(
                    "/locate_object",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                    },
                )
                resp.raise_for_status()

                responses.append((op, json.loads(resp.text)["tag"]))

                resp = client.post(
                    "/start_upload",
                    json={
                        "bucket": "test-bucket",
                        "key": str(data_id),
                        "client_from_region": issue_region,
                        "is_multipart": False,
                        "ttl": 3600 * float(ttl) if float(ttl) != -1 else -1,
                    },
                )

                if resp.status_code <= 200:
                    for i, physical_object in enumerate(resp.json()["locators"]):
                        print(physical_object)
                        client.patch(
                            "/complete_upload",
                            json={
                                "id": physical_object["id"],
                                "size": size,
                                "etag": "123",
                                "last_modified": timestamp_str.replace(" ", "T"),
                                # "version_id": f"version-{i}",
                            },
                        )

            resp = client.post(
                "/clean_object",
                json={
                    "timestamp": timestamp_str.replace(" ", "T"),
                },
            )
        print(responses)
        assert responses == [
            ("write", "aws:us-west-1"),
            ("read", "aws:us-west-1"),
            ("write", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:us-east-1"),
            ("write", "aws:eu-central-1"),
            ("read", "aws:us-east-1"),
            ("read", "aws:eu-central-1"),
            ("read", "aws:us-west-1"),
        ]


def test_remove_db7(client):
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()
