import logging
from datetime import datetime
from typing import List

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
        Nodes: List[int] = None,
        Apps: List[List[List[int]]] = None,
    ):
        self.Name = Name
        self.Path = Path
        self.Type = Type
        self.ExecTime = ExecTime
        self.Date = Date
        # 2D list. Dim[1]: node id; dim[2]: amount of CPUs
        self.Nodes = Nodes[:]
        # 4D list. Dim[1]: app id; dim[2]: task id; dim[3]: thread id; dim[4]: node id
        self.Apps = Apps
        logger.debug(self.print())

    def print(self):
        """ Print class' information """
        myself = f"IFORMATION OF OBJECT {type(self)}\n"
        myself += "--------------------\n"
        myself += f"Name: {self.Name}\n"
        myself += f"Path: {self.Path}\n"
        myself += f"Type: {self.Type} \n"
        myself += f"ExecTime: {self.ExecTime}\n"
        # si comparas con None, True o False es "is", no "=="
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
                while j < self.Nodes[i][0] - 1:
                    myself += f"{j + 1} "
                    j += 1
                myself += f"{j + 1}\n"

        if self.Apps is None:
            myself += "No application configuration avaiable\n"
        else:
            myself += "APP\tTask\tThread\tNode\n"
            app_id = 1
            for app in self.Apps:
                task_id = 1
                for task in app:
                    thread_id = 1
                    for thread in task:
                        node_id = thread
                        myself += f"{app_id}\t{task_id}\t{thread_id}\t{node_id}\n"
                        thread_id += 1
                    task_id += 1
                app_id += 1

        myself += "--------------------"
        return myself
