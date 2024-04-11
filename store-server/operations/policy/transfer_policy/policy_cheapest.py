from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from typing import List
from operations.policy.transfer_policy.base import TransferPolicy


class CheapestTransfer(TransferPolicy):
    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req: LocateObjectRequest
            physical_locators: List[DBPhysicalObjectLocator]: physical locators of the object
        Returns:
            DBPhysicalObjectLocator: the cheapest physical locator to fetch from
        """

        client_from_region = req.client_from_region

        for locator in physical_locators:
            if client_from_region == locator.location_tag:
                return locator
        # find the cheapest region (network cost) to get from client_from_region
        return min(
            physical_locators,
            key=lambda loc: (
                self.stat_graph[loc.location_tag][client_from_region]["cost"],
                self.stat_graph[loc.location_tag][client_from_region]["latency"]
                if "latency" in self.stat_graph[loc.location_tag][client_from_region]
                else 0.7,
            ),
        )

    def name(self) -> str:
        return "cheapest"
