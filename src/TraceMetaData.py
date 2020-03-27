
class TraceMetaData:
    """ Store Trace's Metadata """
    Name: str
    Path: str
    Type: str
    ExecTime: int
    Date = datetime
    # dim[1]: node id; dim[2]: amount of CPUs
    Nodes = [[]]
    # dim[1]: app id; dim[2]: task id; dim[3]: thread id; dim[4]: node id
    Apps = [[[[]]]]

    def __init__(self, Name="", Path="", Type="", ExecTime=None, Date=None, Nodes=None, Apps=None):
        self.Name = Name
        self.Path = Path
        self.Type = Type
        self.ExecTime = ExecTime
        self.Date = Date
        self.Nodes = Nodes[:]
        self.Apps = Apps
        if DEBUG: dprint("DEBUG:", self.print(), sep=" ")

    def print(self):
        """ Print class' information """
        myself = f"IFORMATION OF OBJECT {type(self)}\n"
        myself += "--------------------\n"
        myself += f"Name: {self.Name}\n"
        myself += f"Path: {self.Path}\n"
        myself += f"Type: {self.Type} \n"
        myself += f"ExecTime: {self.ExecTime}\n"
        if self.Date == None:
            myself += "No date available\n"
        else:
            myself += f'Date: {self.Date.isoformat(" ")}\n'
        if self.Nodes == None:
            myself += "No node configuration available\n"
        else:
            myself += "Node\tCPU list\n"
            for i in range(len(self.Nodes)):
                myself += f"{i}\t"
                j = 0
                while j < self.Nodes[i][0]-1:
                    myself += f"{j+1} "
                    j += 1
                myself += f"{j+1}\n"

        if self.Apps == None:
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