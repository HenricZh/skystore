from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectRequest,
    LocateObjectResponse,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from sqlalchemy import and_
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends, status
from operations.utils.db import get_session, logger
from operations.policy.transfer_policy.get_transfer import get_transfer_policy
from operations.policy.placement_policy.get_placement import get_placement_policy
from operations.utils.helper import policy_ultra_dict, init_region_tags
from datetime import datetime

router = APIRouter()


@router.post(
    "/locate_object",
    responses={
        status.HTTP_200_OK: {"model": LocateObjectResponse},
        status.HTTP_404_NOT_FOUND: {"description": "Object not found"},
    },
)
async def locate_object(
    request: LocateObjectRequest, db: Session = Depends(get_session)
) -> LocateObjectResponse:
    """Given the logical object information, return one or zero physical object locators."""
    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)
    get_policy = get_transfer_policy(policy_ultra_dict["get_policy"])

    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()

    version_enabled = version_enabled[0][0]

    if version_enabled is None and request.version_id:
        return Response(status_code=400, content="Versioning is not enabled")

    stmt = (
        select(DBLogicalObject)
        .join(DBPhysicalObjectLocator)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                DBLogicalObject.status == Status.ready,
                DBPhysicalObjectLocator.status == Status.ready,
                DBLogicalObject.id == request.version_id
                if request.version_id is not None
                else True,
            )
        )
        .order_by(None if request.version_id is not None else DBLogicalObject.id.desc())
    )

    locators = (await db.scalars(stmt)).first()

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
    if locators is None or (locators.delete_marker and not request.version_id):
        return Response(status_code=404, content="Object Not Found")

    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
    if locators and locators.delete_marker and request.version_id:
        return Response(status_code=405, content="Not allowed to get a delete marker")

    await db.refresh(locators, ["physical_object_locators"])

    chosen_locator = get_policy.get(request, locators.physical_object_locators)

    logger.debug(
        f"locate_object: chosen locator out of {len(locators.physical_object_locators)}, {request} -> {chosen_locator}"
    )

    if request.ttl is not None:
        dst_object_ttl = request.ttl
    else:
        dst_object_ttl = put_policy.get_ttl(
            src=chosen_locator.location_tag, dst=request.client_from_region
        )

    # TODO: add logic of update GET TTL in local DB if in local region
    # Henric: This logic is correct. On a hit (request and read region are the same), refresh ttl
    # Reset the storage start time to the current time
    if chosen_locator.region == request.client_from_region and request.op == "GET":
        # NOTE: still time gap until actually start to download
        chosen_locator.storage_start_time = datetime.utcnow()

        # For policies like Teven, may need to calculate new ttl

        # Persist refreshed ttl data
        db.commit()    

    return LocateObjectResponse(
        id=chosen_locator.id,
        tag=chosen_locator.location_tag,
        cloud=chosen_locator.cloud,
        bucket=chosen_locator.bucket,
        region=chosen_locator.region,
        key=chosen_locator.key,
        size=locators.size,
        last_modified=locators.last_modified,
        etag=locators.etag,
        version_id=chosen_locator.version_id
        if version_enabled is not None
        else None,  # here must use the physical version
        version=locators.id if version_enabled is not None else None,
        ttl=dst_object_ttl,  # Only for those eviction based policy: need to locate objects then put
    )
