from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectResponse,
    StartUploadRequest,
    StartUploadResponse,
    PatchUploadIsCompleted,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import select
from sqlalchemy import or_, text, and_
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends
from operations.utils.db import get_session, logger
from operations.utils.helper import create_logical_object
from datetime import datetime
from itertools import chain
from operations.policy.placement_policy.get_placement import get_placement_policy
from operations.utils.helper import init_region_tags
from operations.utils.helper import policy_ultra_dict

router = APIRouter()


@router.post("/start_upload")
async def start_upload(
    request: StartUploadRequest, db: Session = Depends(get_session)
) -> StartUploadResponse:
    """
    Reference
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersioningEnabledBuckets.html

    Two possible operations:
    1. First Write: create new logical objects
    2. Pull on Read: reuse existing logical objects
    3. Copy: create new logical objects in dst locations

    """
    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)
    set_ttl = request.ttl if request.ttl is not None else put_policy.get_ttl()

    # Check if the bucket exists
    res = (
        (
            await db.execute(
                select(DBLogicalBucket.version_enabled, DBLogicalBucket)
                .where(DBLogicalBucket.bucket == request.bucket)
                .options(joinedload(DBLogicalBucket.physical_bucket_locators))
            )
        )
        .unique()
        .one_or_none()
    )
    if res is None:
        return Response(status_code=404, content="Bucket Not Found")

    version_enabled, logical_bucket = res
    if not version_enabled:
        # NOTE: two clients can't write to the same object at the same time
        await db.execute(text("LOCK TABLE logical_objects IN EXCLUSIVE MODE;"))

        if request.version_id:
            return Response(
                status_code=400,
                content="Versioning is NULL, make sure you enable versioning first.",
            )

    # Check if the object already exists
    existing_objects_stmt = (
        select(DBLogicalObject)
        .where(
            and_(
                DBLogicalObject.bucket == request.bucket,
                DBLogicalObject.key == request.key,
                or_(
                    DBLogicalObject.status == Status.ready,
                    DBLogicalObject.status == Status.pending,
                ),
                DBLogicalObject.id == request.version_id
                if request.version_id is not None
                else True,
            )
        )
        # Get the latest version if version_id is not specified
        .order_by(DBLogicalObject.id.desc() if request.version_id is None else None)
        .options(joinedload(DBLogicalObject.physical_object_locators))
    )
    # Get the latest version if version_id is not specified
    existing_object = (await db.scalars(existing_objects_stmt)).unique().first()

    # If version specified, and this is copy or pull-on-read operation, 404 if src object does not exist
    if (
        request.version_id
        and not existing_object
        and (request.copy_src_bucket is not None or put_policy.name() == "always_store")
    ):
        return Response(
            status_code=404,
            content="Object of version {} Not Found".format(request.version_id),
        )

    primary_exists = False
    existing_tags = ()
    logical_object = None

    if existing_object:
        # If object exists, check if it is in the client_from_region
        object_already_exists = any(
            locator.location_tag == request.client_from_region
            for locator in existing_object.physical_object_locators
        )

        # If object in client_from_region and version is not enabled, return 409
        if object_already_exists and not version_enabled:
            logger.error("This exact object already exists")
            return Response(status_code=409, content="Conflict, object already exists")

        # Get existing region tags and set primary_exists flag
        existing_tags = {
            locator.location_tag: locator.id
            for locator in existing_object.physical_object_locators
        }
        primary_exists = any(
            locator.is_primary for locator in existing_object.physical_object_locators
        )

        # NOTE: tbh I don't understand this part
        if (
            put_policy.name() == "always_store"
            or version_enabled is None
            or existing_object.version_suspended
        ):
            logical_object = existing_object
            logical_object.delete_marker = False
        else:
            logical_object = create_logical_object(
                existing_object, request, version_suspended=(not version_enabled)
            )
            db.add(logical_object)
    else:
        # If object does not exist, create a new logical object
        logical_object = create_logical_object(
            existing_object, request, version_suspended=(not version_enabled)
        )
        db.add(logical_object)

    physical_bucket_locators = logical_bucket.physical_bucket_locators
    primary_write_region = None
    upload_to_region_tags = put_policy.place(request)

    # when enabling versioning, primary exist does not equal to the pull-on-read case
    # make sure we use copy-on-read policy TODO: ??? what does this code have to do with versioning??
    if primary_exists and put_policy.name() == "always_store":
        # Assume that physical bucket locators for this region already exists and we don't need to create them
        # For pull-on-read
        primary_write_region = [
            locator.location_tag
            for locator in existing_object.physical_object_locators
            if locator.is_primary
        ]

        # Choose the first region as the primary write region
        primary_write_region = primary_write_region[0]

    # NOTE: Push-based: upload to primary region and broadcast to other regions marked with need_warmup
    elif put_policy.name() == "push" or put_policy.name() == "replicate_all":
        # Except this case, always set the first-write region of the OBJECT to be primary
        primary_write_region = [
            locator.location_tag
            for locator in physical_bucket_locators
            if locator.is_primary
        ]
        assert (
            len(primary_write_region) == 1
        ), "should only have one primary write region"
        primary_write_region = primary_write_region[0]
    elif put_policy.name() == "single_region":
        primary_write_region = upload_to_region_tags[0]
    else:
        # Write to the local region and set the first-write region of the OBJECT to be primary
        primary_write_region = request.client_from_region

    # Set base region for logical_object
    logical_object.base_region = primary_write_region

    # NOTE: Case for Copy
    copy_src_buckets = []
    copy_src_keys = []
    if (request.copy_src_bucket is not None) and (request.copy_src_key is not None):
        copy_src_stmt = (
            select(DBLogicalObject)
            .where(
                and_(
                    DBLogicalObject.bucket == request.copy_src_bucket,
                    DBLogicalObject.key == request.copy_src_key,
                    DBLogicalObject.status == Status.ready,
                    DBLogicalObject.id
                    == request.version_id  # copy the specific version if specified
                    if request.version_id is not None
                    else True,
                )
            )
            .order_by(DBLogicalObject.id.desc() if request.version_id is None else None)
            .options(joinedload(DBLogicalObject.physical_object_locators))
        )

        copy_src_locator = (await db.scalars(copy_src_stmt)).unique().first()

        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
        if copy_src_locator is None or (
            copy_src_locator.delete_marker and not request.version_id
        ):
            return Response(status_code=404, content="Object Not Found")

        # https://docs.aws.amazon.com/cli/latest/reference/s3api/upload-part-copy.html
        if copy_src_locator and copy_src_locator.delete_marker and request.version_id:
            return Response(
                status_code=400, content="Not allowed to copy from a delete marker"
            )

        copy_src_locators_map = {
            locator.location_tag: locator
            for locator in copy_src_locator.physical_object_locators
        }
        copy_src_locations = set(
            locator.location_tag
            for locator in copy_src_locator.physical_object_locators
        )
    else:
        copy_src_locations = None

    # Only copy where the source object exists.
    if copy_src_locations is not None:
        upload_to_region_tags = [
            tag for tag in upload_to_region_tags if tag in copy_src_locations
        ]
        if len(upload_to_region_tags) == 0:
            upload_to_region_tags = list(copy_src_locations)

        copy_src_buckets = [
            copy_src_locators_map[tag].bucket for tag in copy_src_locations
        ]
        copy_src_keys = [copy_src_locators_map[tag].key for tag in copy_src_locations]

        logger.debug(
            f"start_upload: copy_src_locations={copy_src_locations}, "
            f"upload_to_region_tags={upload_to_region_tags}, "
            f"copy_src_buckets={copy_src_buckets}, "
            f"copy_src_keys={copy_src_keys}"
        )

    locators, existing_locators = [], []

    for region_tag in upload_to_region_tags:
        # If the version_enabled is None, we should skip the existing tags, only create new physical object locators for the newly upload regions
        # If the version_enabled is True, we should create new physical object locators for the newly created logical object no matter what regions we are trying to upload
        # If the version_enabled is False, we should either create new physical object locators for the newly created logical object
        # or overwrite the existing physical object locators for the existing logical object depending on the version_suspended field

        if region_tag in existing_tags and version_enabled is None:
            continue

        physical_bucket_locator = next(
            (pbl for pbl in physical_bucket_locators if pbl.location_tag == region_tag),
            None,
        )
        if physical_bucket_locator is None:
            logger.error(
                f"No physical bucket locator found for region tag: {region_tag}"
            )
            return Response(
                status_code=500,
                content=f"No physical bucket locator found for upload region tag {region_tag}",
            )

        # For the region not in the existing tags, we need to create new physical object locators and add them to the DB
        # For the region in the existing tags, we need to decide whether creating new physical object locators or not based on:
        # - version_enabled is NULL, we already skipped them in the above if condition
        # - version_enabled is True, we should create new physical object locators for them
        # - Otherwise, check version_suspended field,
        # - if False, create new physical object locators
        # - if True, update existing physical object (but DO NOT add them to the DB) --> TODO: where does this happen?

        if (
            region_tag not in existing_tags
            or version_enabled
            or (existing_object and existing_object.version_suspended is False)
        ):
            locators.append(
                DBPhysicalObjectLocator(
                    logical_object=logical_object,
                    location_tag=region_tag,
                    cloud=physical_bucket_locator.cloud,
                    region=physical_bucket_locator.region,
                    bucket=physical_bucket_locator.bucket,
                    key=physical_bucket_locator.prefix + request.key,
                    lock_acquired_ts=datetime.utcnow(),
                    status=Status.pending,
                    is_primary=(region_tag == primary_write_region),
                    ttl=set_ttl,  # Set the TTL for object (passed in the request, look into get.py)
                )
            )
        else:
            # TODO: Need to update the existing physical object locators in DB (lock, status, ttl, etc.)
            existing_locators.append(
                DBPhysicalObjectLocator(
                    id=existing_tags[region_tag],
                    logical_object=logical_object,
                    location_tag=region_tag,
                    cloud=physical_bucket_locator.cloud,
                    region=physical_bucket_locator.region,
                    bucket=physical_bucket_locator.bucket,
                    key=physical_bucket_locator.prefix + request.key,
                    lock_acquired_ts=datetime.utcnow(),
                    status=Status.pending,
                    is_primary=(
                        region_tag == primary_write_region
                    ),  # NOTE: location of first write is primary
                    ttl=set_ttl,
                )
            )

    db.add_all(locators)
    await db.commit()

    logger.debug(f"start_upload: {request} -> {locators}")

    return StartUploadResponse(
        multipart_upload_id=logical_object.multipart_upload_id,
        locators=[
            LocateObjectResponse(
                id=locator.id,
                tag=locator.location_tag,
                cloud=locator.cloud,
                bucket=locator.bucket,
                region=locator.region,
                key=locator.key,
                version_id=locator.version_id,
                version=locator.logical_object.id
                if version_enabled is not None
                else None,
            )
            for locator in chain(locators, existing_locators)
        ],
        copy_src_buckets=copy_src_buckets,
        copy_src_keys=copy_src_keys,
    )


@router.patch("/complete_upload")
async def complete_upload(
    request: PatchUploadIsCompleted, db: Session = Depends(get_session)
):
    put_policy = get_placement_policy(policy_ultra_dict["put_policy"], init_region_tags)

    stmt = (
        select(DBPhysicalObjectLocator)
        .where(DBPhysicalObjectLocator.id == request.id)
        .options(joinedload(DBPhysicalObjectLocator.logical_object))
    )
    physical_locator = await db.scalar(stmt)
    if physical_locator is None:
        logger.error(f"physical locator not found: {request}")
        return Response(status_code=404, content="Not Found")

    logger.debug(f"complete_upload: {request} -> {physical_locator}")

    physical_locator.status = Status.ready
    physical_locator.lock_acquired_ts = None
    physical_locator.version_id = request.version_id
    physical_locator.storage_start_time = request.last_modified.replace(tzinfo=None)

    if request.ttl is not None:
        physical_locator.ttl = request.ttl

    # TODO: might need to change the if conditions for different policies
    policy_name = put_policy.name()
    if (
        (
            (policy_name == "push" or policy_name == "replicate_all")
            and physical_locator.is_primary
        )
        or policy_name == "always_store"
        or policy_name == "always_evict"
        or policy_name == "single_region"
        or policy_name == "fixed_ttl"
        or policy_name == "t_even"
    ):
        # NOTE: might not need to update the logical object for consecutive reads for copy_on_read
        # await db.refresh(physical_locator, ["logical_object"])
        logical_object = physical_locator.logical_object
        logical_object.status = Status.ready
        logical_object.size = request.size
        logical_object.etag = request.etag
        logical_object.last_modified = request.last_modified.replace(tzinfo=None)
    await db.commit()
