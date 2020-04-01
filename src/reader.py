import logging
import os
from datetime import datetime

from TraceMetaData import TraceMetaData

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

PARAVER_FILE = "Paraver (.prv)"
PARAVER_MAGIC_HEADER = "#Paraver"

def paraver_header_date(header):
    date, _, other = header.replace("#Paraver (", "").replace("at ", "").partition("):")
    date = datetime.strptime(date, "%d/%m/%Y %H:%M")
    logger.debug(f"date = {date}")
    return date

def paraver_header_time(header):
    time, _, other = header[header.find("):")+2:].partition("_ns")
    time = int(time)
    logger.debug(f"time = {time}")
    return time

def paraver_header_nodes(header):
    nodes = header[header.find("_ns:")+4:]
    if nodes[0] == "0":
        nodes = None
    else:
        nodes = nodes[nodes.find("(")+1:nodes.find(")")]
        nodes = nodes.split(",")
        nodes = list(map(int, nodes))
    logger.debug(f"Nodes = {nodes}")
    return nodes

def paraver_header_apps(header):
    apps_list = []
    apps = header[header.find("_ns:")+4:]
    apps, _, other = apps.partition(":")
    apps, _, other = other.partition(":")
    number_apps = int(apps)
    i = 0
    while i < number_apps:
        apps, _, other = other.partition("(")
        number_tasks = int(apps)
        apps, _, other = other.partition(")")
        apps = apps.split(",")
        j = 0
        tasks_list = []
        while j < number_tasks:
            tmp = list(map(int, apps[j].split(":")))
            tasks_list.append(dict(nThreads=tmp[0],
                                   node=tmp[1]))
            j += 1
        apps_list.append(tasks_list)
        i += 1
    logger.debug(f"Applications: {apps_list}")
    return apps_list

def paraver_header_parser(header):
    header_time     = paraver_header_time(header)
    header_date     = paraver_header_date(header)
    header_nodes    = paraver_header_nodes(header)
    header_apps     = paraver_header_apps(header)
    return header_time, header_date, header_nodes, header_apps


def parse_file(file):
    try:
        with open(file) as f:
            header = f.readline()
            if PARAVER_MAGIC_HEADER not in header:
                logger.error(f"The file {f.name} is not a valid Paraver file!")

            trace_name = os.path.basename(f.name)
            trace_path = os.path.abspath(f.name)
            trace_type = PARAVER_FILE

            trace_exec_time, trace_date, trace_nodes, trace_apps = paraver_header_parser(header)
    except:
        logger.error(f"Not able to access the file {file}")

    return TraceMetaData(trace_name, trace_path, trace_type, trace_exec_time, trace_date, trace_nodes, trace_apps)