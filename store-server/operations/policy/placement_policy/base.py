from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.transfer_policy.base import DataTransferGraph


class PlacementPolicy:
    def __init__(self, init_regions: List[str] = []) -> None:
        self.init_regions = init_regions
        self.stat_graph = DataTransferGraph.get_instance()

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
        pass

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        """
        Args:
            src (str): source region to read
            dst (str): destination region to read / write
        """
        pass
