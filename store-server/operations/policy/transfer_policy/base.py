from operations.schemas.object_schemas import (
    LocateObjectRequest,
    DBPhysicalObjectLocator,
)
from operations.policy.utils.helpers import make_nx_graph
from typing import List


class DataTransferGraph:
    """
    A singleton class representing the graph used for data transfer calculations.
    This ensures that only one instance of the graph is created and used throughout the application.
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        """
        Returns the singleton instance of the graph. If it does not exist, it creates one.
        """
        if cls._instance is None:
            cls._instance = cls._create_graph()
        return cls._instance

    @staticmethod
    def _create_graph():
        """
        Creates the network graph. This method is internal to the class.
        """
        return make_nx_graph()


class TransferPolicy:
    def __init__(self) -> None:
        self.stat_graph = DataTransferGraph.get_instance()
        pass

    def get(
        self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]
    ) -> DBPhysicalObjectLocator:
        """
        Args:
            req (LocateObjectRequest): request to locate an object
            physical_locators (List[DBPhysicalObjectLocator]): list of candidate physical locators (ready, and contains the object)

        Returns:
            DBPhysicalObjectLocator: return the selected physical locator
        """
        pass

    def name(self) -> str:
        """
        Returns:
            str: policy name
        """
        return ""
