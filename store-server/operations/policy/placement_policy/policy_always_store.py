from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class AlwaysStore(PlacementPolicy):
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
        return "always_store"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        return -1  # -1 means store forever
