from operations.schemas.object_schemas import (
    CleanObjectRequest,
    CleanObjectResponse,
    DBPhysicalObjectLocator,
    LocateObjectResponse,
)
from fastapi import APIRouter, Depends
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from operations.utils.db import get_session
from operations.schemas.bucket_schemas import Status
from sqlalchemy import text


router = APIRouter()


@router.post("/clean_object")
async def clean_object(
    request: CleanObjectRequest, db: Session = Depends(get_session)
) -> CleanObjectResponse:
    """Given the current timestamp, clean the object based on TTL."""
    current_timestamp = request.timestamp
    # Print all objects
    print("All objects:")
    all_objects = await db.execute(select(DBPhysicalObjectLocator))
    all_objects = all_objects.scalars().all()
    print(all_objects)

    # Fetch objects that meet the deletion criteria
    objects_to_delete_query = select(DBPhysicalObjectLocator).where(
        text(
            f"{DBPhysicalObjectLocator.__tablename__}.storage_start_time + ({DBPhysicalObjectLocator.__tablename__}.ttl || ' seconds')::interval <= :current_timestamp"
        ).bindparams(current_timestamp=current_timestamp),
        DBPhysicalObjectLocator.ttl != -1,
        DBPhysicalObjectLocator.status == Status.ready,
    )

    objects_to_delete = await db.execute(objects_to_delete_query)
    objects_to_delete = objects_to_delete.scalars().all()
    print(f"objects_to_delete: {objects_to_delete}")

    locators_response = [
        LocateObjectResponse(
            id=obj.id,
            tag=obj.location_tag,
            cloud=obj.cloud,
            bucket=obj.bucket,
            region=obj.region,
            key=obj.key,
            version_id=obj.version_id,
            version=obj.logical_object_id,
        )
        for obj in objects_to_delete
    ]

    # Delete the objects if any are found
    if objects_to_delete:
        _ = await db.execute(
            delete(DBPhysicalObjectLocator).where(
                DBPhysicalObjectLocator.id.in_([obj.id for obj in objects_to_delete])
            )
        )
        try:
            await db.commit()
        except Exception as e:
            print(e)
            await db.rollback()

    return CleanObjectResponse(locators=locators_response)
