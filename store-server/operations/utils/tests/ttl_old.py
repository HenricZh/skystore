import pytest
from httpx import AsyncClient
from fastapi import FastAPI
import pytest
from starlette.testclient import TestClient
from datetime import datetime, timedelta
from app import app  # Make sure this imports your FastAPI app correctly
from operations.schemas.object_schemas import DBPhysicalObjectLocator, DBLogicalObject
from operations.schemas.bucket_schemas import DBLogicalBucket
from operations.utils.db import run_create_database
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Thread
from httpx import AsyncClient
import pytest
from sqlalchemy.orm import sessionmaker, scoped_session

app = FastAPI()  # Ensure this is your FastAPI app instance

DATABASE_URL = "postgresql://ubuntu:skystore@localhost/skystore"


@pytest.fixture(scope="module")
def test_db_session():
    engine = create_engine(DATABASE_URL)
    SessionLocal = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )

    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()

    engine.dispose()


class AsyncClientContextManager:
    def __init__(self, app, base_url="http://testserver"):
        self.app = app
        self.client = None
        self.base_url = base_url

    async def __aenter__(self):
        self.client = AsyncClient(app=self.app, base_url=self.base_url)
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()


@pytest.fixture
def client():
    return AsyncClientContextManager(app=app)


def test_setup_db():
    thread = Thread(target=run_create_database)
    thread.start()
    thread.join()


def insert_logical_bucket(session, bucket_name="test_bucket"):
    logical_bucket = DBLogicalBucket(
        bucket=bucket_name,
        prefix="test_prefix",
        status="ready",
        creation_date=datetime.utcnow(),
        version_enabled=True,
    )
    session.add(logical_bucket)
    session.commit()
    return logical_bucket.bucket


def insert_logical_object(session, bucket_name):
    logical_object = DBLogicalObject(
        bucket=bucket_name,
        key="test_logical_key",
        size=123,
        last_modified=datetime.utcnow(),
        etag="test_etag",
        status="ready",
        version_suspended=False,
        delete_marker=False,
    )
    session.add(logical_object)
    session.commit()
    return logical_object.id


def insert_test_objects(session, objects):
    session.bulk_save_objects(objects)
    session.commit()


@pytest.mark.asyncio
async def test_clean_object(client, test_db_session):
    async with client as ac:
        current_time = datetime.utcnow()
        past_time = current_time - timedelta(hours=1)
        future_time = current_time + timedelta(hours=1)

        bucket_name = insert_logical_bucket(test_db_session)
        logical_object_id = insert_logical_object(test_db_session, bucket_name)

        location_tag = "test_location"
        cloud = "test_cloud"
        region = "test_region"
        key = "test_key"
        multipart_upload_id = "test_multipart_upload_id"

        objects_to_clean = [
            DBPhysicalObjectLocator(
                storage_start_time=past_time, ttl=3600, status="ready"
            ),
            DBPhysicalObjectLocator(
                storage_start_time=past_time - timedelta(minutes=30),
                ttl=1800,
                status="ready",
            ),
        ]
        objects_to_keep = [
            DBPhysicalObjectLocator(
                storage_start_time=future_time, ttl=3600, status="ready"
            ),
            DBPhysicalObjectLocator(
                storage_start_time=current_time, ttl=7200, status="ready"
            ),
            DBPhysicalObjectLocator(
                storage_start_time=past_time, ttl=7200, status="pending"
            ),
        ]

        for obj in objects_to_clean + objects_to_keep:
            obj.location_tag = location_tag
            obj.cloud = cloud
            obj.region = region
            obj.key = key
            obj.multipart_upload_id = multipart_upload_id
            obj.logical_object_id = logical_object_id

        insert_test_objects(test_db_session, objects_to_clean + objects_to_keep)

        response = await ac.post(
            "/clean_object", json={"timestamp": current_time.isoformat()}
        )
        assert response.status_code == 200

        deleted_objects = response.json()["locators"]
        assert len(deleted_objects) == len(objects_to_clean)

        remaining_objects = test_db_session.query(DBPhysicalObjectLocator).all()
        assert len(remaining_objects) == len(objects_to_keep)
