import uuid
from operations.schemas.object_schemas import DBLogicalObject
from operations.utils.conf import Status
import UltraDict as ud
import time
from operations.utils.conf import (
    Status,
    DEFAULT_INIT_REGIONS,
    DEFAULT_SKYSTORE_BUCKET_PREFIX,
)
import os

# Initialize a ultradict to store the policy name in shared memory
# so that can be used by multiple workers started by uvicorn
policy_ultra_dict = None
try:
    policy_ultra_dict = ud.UltraDict(name="policy_ultra_dict", create=True)
except Exception:
    time.sleep(3)
    policy_ultra_dict = ud.UltraDict(name="policy_ultra_dict", create=False)

policy_ultra_dict["get_policy"] = ""
policy_ultra_dict["put_policy"] = ""

# Default values for the environment variables
init_region_tags = (
    os.getenv("INIT_REGIONS").split(",")
    if os.getenv("INIT_REGIONS")
    else DEFAULT_INIT_REGIONS
)

skystore_bucket_prefix = (
    os.getenv("SKYSTORE_BUCKET_PREFIX")
    if os.getenv("SKYSTORE_BUCKET_PREFIX")
    else DEFAULT_SKYSTORE_BUCKET_PREFIX
)


# Helper function to create a logical object
def create_logical_object(
    existing_object, request, version_suspended=False, delete_marker=False
):
    return DBLogicalObject(
        bucket=existing_object.bucket if existing_object else request.bucket,
        key=existing_object.key if existing_object else request.key,
        size=existing_object.size if existing_object else None,
        last_modified=existing_object.last_modified
        if existing_object
        else None,  # to be updated later
        etag=existing_object.etag if existing_object else None,
        status=Status.pending,
        multipart_upload_id=(
            existing_object.multipart_upload_id
            if existing_object
            else (uuid.uuid4().hex if request.is_multipart else None)
        ),
        version_suspended=version_suspended,
        delete_marker=delete_marker,
    )
