import os
from datetime import datetime
from src.TraceMetaData import TraceMetaData
from src.utils.error import eprint, dprint

PARAVER_FILE = "Paraver (.prv)"
PARAVER_MAGIC_HEADER = "#Paraver"

def header_parser(file, header):
    if not PARAVER_MAGIC_HEADER in header:
        eprint("ERROR: The file", file.name, "is not a valid Paraver file!", sep=" ")
    else:
        header = header.replace("#Paraver (", "").replace("at ", "")
        traceDate, match, other = header.partition("):")
        traceDate = datetime.strptime(traceDate, "%d/%m/%Y %H:%M")
        traceName = os.path.basename(file.name)
        tracePath = os.path.abspath(file.name)
        traceType = PARAVER_FILE
        traceExecTime, match, other = other.partition(":")
        traceExecTime = int(traceExecTime[:-3])

        htraceNodes, match, other = other.partition("(")
        htraceCPUs, match, other = other.partition(')')
        htraceCPUs = htraceCPUs.split(",")
        traceNodes = []
        for i in range(int(htraceNodes)):
            traceNodes.append([int(htraceCPUs[i])])

        appls_list, match, other = other[1:].partition(":")
        number_apps = int(appls_list)
        traceApps = []
        for i in range(number_apps):
            traceTasks, match, other = other.partition("(")
            number_tasks = int(traceTasks)
            config, match, other = other.partition(")")
            config = config.split(",")
            traceTasks = []
            for j in range(number_tasks):
                config_threads = config[j].split(":")
                threads = []
                for k in range(int(config_threads[0])):
                    threads.append(int(config_threads[1]))
                
                traceTasks.append(threads)
        traceApps.append(traceTasks)
        
        return TraceMetaData(traceName, tracePath, traceType, traceExecTime, traceDate, traceNodes, traceApps)