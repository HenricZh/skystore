from typing import List
from operations.schemas.object_schemas import StartUploadRequest


class PlacementPolicy:
    def __init__(self, init_regions: List[str] = []) -> None:
        self.init_regions = init_regions

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req (StartUploadRequest): upload request

        Returns:
            List[str]: list of regions to write to
        """
        pass

    def name(self) -> str:
        """
        Returns:
            str: policy name
        """
        return ""
