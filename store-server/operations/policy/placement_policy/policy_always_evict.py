from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class AlwaysEvict(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the region client is from
        """

        return [req.client_from_region]

    def name(self) -> str:
        return "always_evict"
