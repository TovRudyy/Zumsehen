import logging
import os
from datetime import datetime

from src.persistence.reader import Reader
from src.TraceMetaData import TraceMetaData

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PARAVER_FILE = "Paraver (.prv)"
PARAVER_MAGIC_HEADER = "#Paraver"


class ParaverReader(Reader):
    def header_date(self, header: str):
        date, _, other = header.replace("#Paraver (", "").replace("at ", "").partition("):")
        date = datetime.strptime(date, "%d/%m/%Y %H:%M")
        return date

    def header_time(self, header):
        time, _, other = header[header.find("):") + 2 :].partition("_ns")
        time = int(time)
        return time

    def header_nodes(self, header: str):
        nodes = header[header.find("_ns:") + 4 :]
        if nodes[0] == "0":
            nodes = None
        else:
            nodes = nodes[nodes.find("(") + 1 : nodes.find(")")]
            nodes = nodes.split(",")
            nodes = list(map(int, nodes))
        return nodes

    def header_apps(self, header: str):
        apps_list = []
        apps = header[header.find("_ns:") + 4 :]
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
                tasks_list.append(dict(nThreads=tmp[0], node=tmp[1]))
                j += 1
            apps_list.append(tasks_list)
            i += 1
            apps, _, other = other.partition(":")
        return apps_list

    def header_parser(self, header: str):
        header_time = self.header_time(header)
        header_date = self.header_date(header)
        header_nodes = self.header_nodes(header)
        header_apps = self.header_apps(header)
        return header_time, header_date, header_nodes, header_apps

    def parse_file(self, file: str):
        try:
            with open(file, "r") as f:
                header = f.readline()
                if PARAVER_MAGIC_HEADER not in header:
                    logger.error(f"The file {f.name} is not a valid Paraver file!")

                trace_name = os.path.basename(f.name)
                trace_path = os.path.abspath(f.name)
                trace_type = PARAVER_FILE

                trace_exec_time, trace_date, trace_nodes, trace_apps = self.header_parser(header)
        except FileNotFoundError:
            logger.error(f"Not able to access the file {file}")

        return TraceMetaData(trace_name, trace_path, trace_type, trace_exec_time, trace_date, trace_nodes, trace_apps)
