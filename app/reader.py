import os
import sys
from datetime import datetime

EXIT_FAILURE = -1
DEBUG = 1

PARAVER_FILE = "Paraver (.prv)"
TRACE_PATH = "traces/bt-mz.2x2-+A.x.prv"
PARAVER_MAGIC_HEADER = "#Paraver"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    sys.exit(EXIT_FAILURE)


def dprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class TraceMetadata:
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
        if DEBUG:
            dprint("DEBUG:\n", self.print(), sep="")

    def print(self):
        """ Print class' information """
        myself = "IFORMATION OF: " + type(self) + "\n"
        myself += "--------------------"
        myself += "Name: " + self.Name + "\n"
        myself += "Path: " + self.Path + "\n"
        myself += "Type: " + self.Type + "\n"
        myself += "ExecTime: " + str(self.ExecTime) + "\n"
        if self.Date == None:
            myself += "No date available\n"
        else:
            myself += "Date: " + self.Date.isoformat(" ") + "\n"
        if self.Nodes == None:
            myself += "No node configuration available\n"
        else:
            myself += "Node\tCPU list\n"
            for i in range(len(self.Nodes)):
                myself += str(self.Nodes[i]) + "\t"
                for j in range(len(self.Nodes[i]) - 1):
                    myself += str(self.Nodes[i][j]) + " "
                myself += str(self.Nodes[i][-1]) + "\n"
        if self.Apps == None:
            myself += "No application configuration avaiable\n"
        else:
            myself += "APP\tTask\tThread\tNode\n"
            for i in range(len(self.Apps)):
                app_id = self.Apps[i]
                for j in range(len(self.Apps[i])):
                    task_id = self.Apps[i][j]
                    for k in range(len(self.Apps[i][j])):
                        thread_id = self.Apps[i][j][k]
                        node_id = self.Apps[i][j][k][0]
                        myself += str(app_id) + "\t" + str(task_id) + "\t" + str(thread_id) + "\t" + str(node_id) + "\n"

        myself += "--------------------"
        return myself


def header_parser(header):
    if not PARAVER_MAGIC_HEADER in header:
        eprint("ERROR: The file", TRACE_PATH, "is not a valid Paraver file!", sep=" ")
    else:
        header = "#Paraver (17/02/2020 at 11:37):1857922_ns:1(4):1:2(2:1,2:1)"
        header = header.replace("#Paraver (", "").replace("at ", "")
        traceDate, match, other = header.partition("):")
        traceDate = datetime.strptime(traceDate, "%d/%m/%Y %H:%M")
        traceName = os.path.basename(trace.name)
        tracePath = os.path.abspath(trace.name)
        traceType = PARAVER_FILE
        traceExecTime, match, other = other.partition(":")
        traceExecTime = int(traceExecTime)

        htraceNodes, match, other = other.partition(":")
        htraceCPUs = htraceNodes[htraceNodes.find("(") + 1 : -1].split(",")
        traceNodes = []
        for i in range(int(htraceNodes)):
            traceNodes.append([int(htraceCPUs[i])])

        appls_list, match, other = other.partition(":")
        number_apps = int(appls_list)
        traceApps = []
        for i in range(number_apps):
            traceTasks, match, other = other.partition("(")
            number_tasks = int(traceTasks)
            config, match, other = other.partition("):")
            traceTasks = []
            config = config.split(",")
            for j in range(number_tasks):
                config_threads = config[j].split(":")
                threads = []
                for k in range(int(config_threads[0])):
                    threads[k].append(int(config_threads[1]))
            traceTasks.append(threads)
        traceApps.append(traceTasks)


with open(TRACE_PATH, "r") as trace:
    header_info = ""
    header = trace.readline()
    print(header, end="")
    header_parser(header)
