from typing import List
from operations.schemas.object_schemas import StartUploadRequest
from operations.policy.placement_policy.base import PlacementPolicy


class Teven(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def __init__(self, init_regions: List[str] = ...) -> None:
        super().__init__(init_regions)
        self.ttl = 12 * 60 * 60  # TODO: set this to some default numbers

    def place(self, req: StartUploadRequest) -> List[str]:
        """
        Args:
            req: StartUploadRequest
        Returns:
            List[str]: the region client is from
        """
        return [req.client_from_region]

    def name(self) -> str:
        return "t_even"

    def get_ttl(
        self, src: str = None, dst: str = None, fixed_base_region: bool = False
    ) -> int:
        """_summary_

        Args:
            src (str): src region
            dst (str): dst region
            fixed_base_region (bool, optional): fixed based region or not. Defaults to False.

        Returns:
            int: time to live for this object
        """

        if not self.stat_graph.has_edge(src, dst) or not self.stat_graph.has_node(dst):
            return self.ttl

        network_cost = self.stat_graph[src][dst]["cost"]
        storage_cost = self.stat_graph.nodes[dst]["priceStorage"]
        # print("net cost:", network_cost)
        # print("storage cost: ", self.stat_graph.nodes[dst]["priceStorage"])
        if network_cost == 0:
            network_cost = 0.056
        return network_cost / storage_cost * 60 * 60
