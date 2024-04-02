from operations.schemas.object_schemas import (
    DBLogicalObject,
    DBPhysicalObjectLocator,
    LocateObjectResponse,
    DeleteObjectsRequest,
    DeleteObjectsResponse,
    DeleteObjectsIsCompleted,
    DeleteMarker,
)
from operations.schemas.bucket_schemas import DBLogicalBucket
from sqlalchemy.orm import Session, joinedload
from itertools import zip_longest
from sqlalchemy.sql import select
from sqlalchemy import or_, text
from operations.utils.conf import Status
from fastapi import APIRouter, Response, Depends
from operations.utils.db import get_session, logger
from operations.utils.helper import create_logical_object
from datetime import datetime


router = APIRouter()


# TODO: when creating new logical object, we need to consider different put policy
@router.post("/start_delete_objects")
async def start_delete_objects(
    request: DeleteObjectsRequest, db: Session = Depends(get_session)
) -> DeleteObjectsResponse:
    version_enabled = (
        await db.execute(
            select(DBLogicalBucket.version_enabled).where(
                DBLogicalBucket.bucket == request.bucket
            )
        )
    ).all()[0][0]

    if version_enabled is not True:
        # await db.execute(text("BEGIN IMMEDIATE;"))
        await db.execute(text("LOCK TABLE logical_objects IN EXCLUSIVE MODE;"))

    specific_version = any(
        len(request.object_identifiers[key]) > 0 for key in request.object_identifiers
    )

    if version_enabled is None and specific_version:
        return Response(status_code=400, content="Versioning is not enabled")

    if request.multipart_upload_ids and len(request.object_identifiers) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    locator_dict = {}
    delete_marker_dict = {}
    op_type = {}
    for key, multipart_upload_id in zip_longest(
        request.object_identifiers, request.multipart_upload_ids or []
    ):
        if multipart_upload_id:
            stmt = (
                select(DBLogicalObject)
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(
                    or_(
                        DBLogicalObject.status == Status.ready,
                        DBLogicalObject.status == Status.pending,
                    )
                )
                .where(DBLogicalObject.multipart_upload_id == multipart_upload_id)
                .options(joinedload(DBLogicalObject.physical_object_locators))
            )
        else:
            stmt = (
                select(DBLogicalObject)
                .where(DBLogicalObject.bucket == request.bucket)
                .where(DBLogicalObject.key == key)
                .where(DBLogicalObject.status == Status.ready)
                .order_by(DBLogicalObject.id.desc())
                .options(joinedload(DBLogicalObject.physical_object_locators))
            )
        # multiple versioning support
        logical_objs = (await db.scalars(stmt)).unique().all()

        if len(logical_objs) == 0:
            return Response(status_code=404, content="Objects not found")

        version_suspended = logical_objs[0].version_suspended

        locators = []
        replaced = False
        add_obj = False
        pre_logical_obj = None
        # since we have ordered the logical objects before traversing them
        # the first one will be the most recent one
        # and if there is a version-suspended marker, it will be the first one
        # if it's a multipart, the upload_id is unique, so we can just delete the first one

        for idx, logical_obj in enumerate(logical_objs):
            # Follow the semantics of S3:
            # Check it here:
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectsfromVersioningSuspendedBuckets.html
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManagingDelMarkers.html

            # simple delete
            if len(request.object_identifiers[key]) == 0 and idx == 0:
                if version_enabled is True or (
                    version_enabled is False and version_suspended is False
                ):
                    # under both these cases, in S3 semantics, we will add things to the DB and doesn't touch the older ones
                    # so here, we need to add new logical object and physical object locators
                    # This also indicates, when we returning back to the complete_delete_objects function,
                    # The status of objects that need to be updated will be these newly created objects
                    # rather than the older ones. So what will be included in the returned locators list
                    # will be the newly created ones.
                    pre_logical_obj = logical_obj
                    # insert a delete marker
                    logical_obj = create_logical_object(
                        logical_obj,
                        request,
                        version_suspended=(version_enabled is not True),
                        delete_marker=True,
                    )
                    db.add(logical_obj)
                    new_physical_locators = []
                    # need to also add new physical object locators
                    for physical_locator in pre_logical_obj.physical_object_locators:
                        new_physical_locators.append(
                            DBPhysicalObjectLocator(
                                logical_object=logical_obj,
                                location_tag=physical_locator.location_tag,
                                cloud=physical_locator.cloud,
                                region=physical_locator.region,
                                bucket=physical_locator.bucket,
                                key=physical_locator.key,
                                status=Status.pending,
                                is_primary=physical_locator.is_primary,
                                # version_id=physical_locators.version_id,
                            )
                        )
                    db.add_all(new_physical_locators)
                    add_obj = True
                elif version_enabled is False and version_suspended is True:
                    # remove the null version and replace the one with a delete marker
                    # NOTE: The obj being removed can also be a delete marker with null version
                    # We just need to update the metadata of the existing objects, also don't remove anything in the DB
                    logical_obj.delete_marker = True
                    replaced = True
            # For the case adding new objs to the DB, we need to commit first
            # in order to use them in the following traversal
            if add_obj or replaced:
                await db.commit()

            if len(request.object_identifiers[key]) > 0 and (
                logical_obj.id not in request.object_identifiers[key]
            ):
                continue  # skip if the version_id is not in the request

            for physical_locator in logical_obj.physical_object_locators:
                await db.refresh(physical_locator, ["logical_object"])

                if (
                    not add_obj
                    and physical_locator.status not in Status.ready
                    and not multipart_upload_id
                ):
                    logger.error(
                        f"Cannot delete physical object. Current status is {physical_locator.status}"
                    )
                    return Response(
                        status_code=409,
                        content="Cannot delete physical object in current state",
                    )

                if not add_obj and not replaced:
                    physical_locator.status = Status.pending_deletion
                    physical_locator.lock_acquired_ts = datetime.utcnow()
                locators.append(
                    LocateObjectResponse(
                        id=physical_locator.id,
                        tag=physical_locator.location_tag,
                        cloud=physical_locator.cloud,
                        bucket=physical_locator.bucket,
                        region=physical_locator.region,
                        key=physical_locator.key,
                        size=physical_locator.logical_object.size,
                        last_modified=physical_locator.logical_object.last_modified,
                        etag=physical_locator.logical_object.etag,
                        multipart_upload_id=physical_locator.multipart_upload_id,
                        # the version_id should be the one that we want the client to operate on
                        # so when we add objects into the DB, we still want the client to cope with the previous version
                        version_id=physical_locator.version_id
                        if version_enabled is not None
                        else None,
                        version=physical_locator.logical_object.id
                        if version_enabled is not None
                        else None,
                    )
                )

            if not add_obj and not replaced:
                logical_obj.status = Status.pending_deletion

            # for these cases, we only need to deal with the first logical object
            if replaced or add_obj:
                break

            try:
                await db.commit()
            except Exception as e:
                logger.error(f"Error occurred while committing changes: {e}")
                return Response(status_code=500, content="Error committing changes")

            logger.debug(f"start_delete_object: {request} -> {logical_obj}")

        locator_dict[key] = locators
        delete_marker_dict[key] = DeleteMarker(
            delete_marker=logical_obj.delete_marker,
            version_id=None
            if logical_obj.version_suspended or version_enabled is None
            else str(logical_obj.id),
        )
        if add_obj:
            op_type[key] = "add"
        elif replaced:
            op_type[key] = "replace"
        else:
            op_type[key] = "delete"

    return DeleteObjectsResponse(
        locators=locator_dict,
        delete_markers=delete_marker_dict,
        op_type=op_type,
    )


@router.patch("/complete_delete_objects")
async def complete_delete_objects(
    request: DeleteObjectsIsCompleted, db: Session = Depends(get_session)
):
    # For the version support, we need to perform different operations
    # based on the initial delete op type
    # If the op type is delete, we need to delete the physical object locators and logical objects possibly
    # If the op type is replace, we don't need to do anything
    # If the op type is add, we need to update the metadata of the existing objects from pending to ready

    # TODO: need to deal with partial failures
    if request.multipart_upload_ids and len(request.ids) != len(
        request.multipart_upload_ids
    ):
        return Response(
            status_code=400,
            content="Mismatched lengths for ids and multipart_upload_ids",
        )

    for idx, (id, multipart_upload_id, op_type) in enumerate(
        zip_longest(
            request.ids,
            request.multipart_upload_ids or [],
            request.op_type,
        )
    ):
        if op_type == "delete":
            physical_locator_stmt = (
                select(DBPhysicalObjectLocator)
                .where(DBPhysicalObjectLocator.id == id)
                .where(
                    DBPhysicalObjectLocator.multipart_upload_id == multipart_upload_id
                    if multipart_upload_id
                    else True
                )
            )

            physical_locator = await db.scalar(physical_locator_stmt)

            if physical_locator is None:
                logger.error(f"physical locator not found: {request}")
                return Response(status_code=404, content="Physical Object Not Found")

            await db.refresh(physical_locator, ["logical_object"])

            logger.debug(f"complete_delete_object: {request} -> {physical_locator}")

            if physical_locator.status != Status.pending_deletion:
                return Response(
                    status_code=409,
                    content="Physical object is not marked for deletion",
                )

            await db.delete(physical_locator)

            # only delete the logical object with same version if there is no other physical object locator if possible
            remaining_physical_locators_stmt = select(DBPhysicalObjectLocator).where(
                DBPhysicalObjectLocator.logical_object_id
                == physical_locator.logical_object.id
            )
            remaining_physical_locators = await db.execute(
                remaining_physical_locators_stmt
            )
            if not remaining_physical_locators.all():
                await db.delete(physical_locator.logical_object)

        elif op_type == "replace":
            continue
        elif op_type == "add":
            physical_locator_stmt = (
                select(DBPhysicalObjectLocator)
                .where(DBPhysicalObjectLocator.id == id)
                .where(
                    DBPhysicalObjectLocator.multipart_upload_id == multipart_upload_id
                    if multipart_upload_id
                    else True
                )
            )

            physical_locator = await db.scalar(physical_locator_stmt)

            if physical_locator is None:
                logger.error(f"physical locator not found: {request}")
                return Response(status_code=404, content="Physical Object Not Found")

            # await db.refresh(physical_locator, ["logical_object"])

            logger.debug(f"complete_delete_object: {request} -> {physical_locator}")

            if physical_locator.status != Status.pending:
                return Response(
                    status_code=409, content="Physical object is not marked for pending"
                )

            physical_locator.status = Status.ready
            physical_locator.lock_acquired_ts = None

            # if this is the first physical locators we cope with linked with some
            # logical objects whose status is pending, we change the status of the logical object
            # to ready

            if idx == 0:
                stmt = select(DBLogicalObject).where(
                    DBLogicalObject.id == physical_locator.logical_object_id
                )

                logical_obj = await db.scalar(stmt)

                if logical_obj is None:
                    logger.error(f"logical object not found: {request}")
                    return Response(status_code=404, content="Logical Object Not Found")

                logical_obj.status = Status.ready

        else:
            logger.error(f"Invalid op_type: {op_type}")
            return Response(status_code=400, content="Invalid op_type")

    try:
        await db.commit()
    except Exception as e:
        logger.error(f"Error occurred while committing changes: {e}")
        return Response(status_code=500, content="Error committing changes")
