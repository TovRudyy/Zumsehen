import logging
from datetime import datetime
from typing import Dict, List

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TraceMetaData:
    """ Store Trace's Metadata """

    def __init__(
        self,
        Name: str = "",
        Path: str = "",
        Type: str = "",
        ExecTime: int = None,
        Date: datetime = None,
        # len(Nodes) = #nodes | Nodes[0] = #CPUs of Node 1
        Nodes: List[int] = None,
        # len(Apps) = #Apps | len(Apps[0]) = #Tasks of APP 1 | App[0][0] = {"nThreads": int, "node": int}
        Apps: List[List[Dict]] = None,
    ):
        self.Name = Name
        self.Path = Path
        self.Type = Type
        self.ExecTime = ExecTime
        self.Date = Date
        self.Nodes = Nodes[:]
        self.Apps = Apps[:]
        logger.debug(self)

    def __repr__(self):
        """ Print object's information """
        myself = f"INFORMATION OF OBJECT {type(self)}\n"
        myself += "--------------------\n"
        myself += f"Name: {self.Name}\n"
        myself += f"Path: {self.Path}\n"
        myself += f"Type: {self.Type} \n"
        myself += f"ExecTime: {self.ExecTime}\n"
        if self.Date is None:
            myself += "No date available\n"
        else:
            myself += f'Date: {self.Date.isoformat(" ")}\n'
        if self.Nodes is None:
            myself += "No node configuration available\n"
        else:
            myself += "Node\tCPU list\n"
            for i in range(len(self.Nodes)):
                myself += f"{i}\t"
                j = 0
                while j < self.Nodes[0] - 1:
                    myself += f"{j + 1} "
                    j += 1
                myself += f"{j + 1}\n"

        if self.Apps is None:
            myself += "No application configuration avaiable\n"
        else:
            myself += "APP\tTask\tThreads\tNode\n"
            app_id = 1
            for app in self.Apps:
                myself += "".join(
                    [f"{app_id}\t{task_id}\t{task['nThreads']}\t{task['node']}\n" for task_id, task in enumerate(app)]
                )
                app_id += 1

        myself += "--------------------"
        return myself

    def get_name(self):
        return self.Name

    def get_path(self):
        return self.Path

    def get_time(self):
        return self.ExecTime

    def get_date(self):
        return self.Date

    def get_Nodes(self):
        return self.Nodes

    def get_Apps(self):
        return self.Apps
