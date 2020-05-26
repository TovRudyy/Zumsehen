import os
import subprocess
from datetime import datetime
from typing import Dict, List
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import h5py

class TraceMetaData:
    """ Stores Trace Metadata """
    def __init__(
        self,
        name: str = "",
        path: str = "",
        trace_type: str = "",
        exec_time: int = None,
        date_time: datetime = None,
        # len(Nodes) = #nodes | Nodes[0] = #CPUs of Node 1, Nodes[n] = #CPUs of Node n
        nodes: List[int] = None,
        # len(Apps) = #Apps | len(Apps[0]) = #Tasks of APP 1 | App[0][0] = {"nTreads": int, "node": int}
        apps: List[List[Dict]] = None,
    ):
        self.name = name
        self.path = path
        self.type = trace_type
        self.exec_time = exec_time
        self.date_time = date_time
        self.nodes = nodes[:]
        self.apps = apps[:]
        logger.debug(self)
        
    def __repr__(self):
        """ Return self's representation as string """
        myself = f"INFORMATION OF OBJECT {type(self)}\n"
        myself += "--------------------\n"
        myself += f"Name: {self.name}\n"
        myself += f"Path: {self.path}\n"
        myself += f"Type: {self.type} \n"
        myself += f"ExecTime: {self.exec_time}\n"
        if self.date_time is None:
            myself += "No date available\n"
        else:
            myself += f'Date: {self.date_time.isoformat(" ")}\n'
        if self.nodes is None:
            myself += "No node configuration available\n"
        else:
            myself += "Node\tCPU list\n"
            for i in range(len(self.nodes)):
                myself += f"{i}\t"
                j = 0
                while j < self.nodes[0] - 1:
                    myself += f"{j + 1} "
                    j += 1
                myself += f"{j + 1}\n"

        if self.apps is None:
            myself += "No application configuration avaiable\n"
        else:
            myself += "APP\tTask\tThreads\tNode\n"
            app_id = 1
            for app in self.apps:
                myself += "".join(
                    [f"{app_id}\t{task_id}\t{task['nThreads']}\t{task['node']}\n" for task_id, task in enumerate(app)]
                )
                app_id += 1

        myself += "--------------------"
        return myself

HDF5_ROOT = "/"
BIN_UTILS_PATH = "utils/bin"
PRV_PARSER_BIN = BIN_UTILS_PATH+"prv_parser"
PARAVER_FILE = "Paraver (.prv)"
PARAVER_MAGIC_HEADER = "#Paraver"

class ParaverReader:
    def header_date(self, header: str):
        """ Returns the contained date in the header """
        date, _, other = header.replace("#Paraver (", "").replace("at ", "").partition("):")
        return datetime.strptime(date, "%d/%m/%Y %H:%M")
    
    def header_time(self, header):
        """ Returns total execution time (us) contained in the header """
        time_ns, _, other = header[header.find("):") + 2 :].partition("_ns") # Originally it's in ns
        return int(time_ns) // 1000
    
    def header_nodes(self, header: str):
        """ Returns a list telling how many CPUs has each Node.
        Origin machine's architectural informaion """
        nodes = header[header.find("_ns:") + 4 :]
        if nodes[0] == "0":
            nodes = None
        else:
            nodes = nodes[nodes.find("(") + 1 : nodes.find(")")]
            nodes = nodes.split(",")
            nodes = list(map(int, nodes))
        return nodes
    
    def header_apps(self, header: str):
        """ Returns a structure telling the threads/Node mappings of each task """
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
        """ Parses important fields of the header one by one """
        header_time = self.header_time(header)
        header_date = self.header_date(header)
        header_nodes = self.header_nodes(header)
        header_apps = self.header_apps(header)
        return header_time, header_date, header_nodes, header_apps
    
    def row_parser(self, file: str):
        """ Parses the .row file returning a list with the CPUs, nodes and threads names """
        row_file = file.replace(".prv", ".row")
        try:
            with open(row_file, "r") as f:
                lines = f.read().split("\n")
                cpu_size = int(lines[0].split()[3])
                cpu_list = lines[1:cpu_size+1]
                del lines[0:cpu_size+2]
                node_size = int(lines[0].split()[3])
                node_list = lines[1:node_size+1]
                del lines[0:node_size+2]
                thread_size = int(lines[0].split()[3])
                thread_list = lines[1:thread_size+1]
                return cpu_list, node_list, thread_list
        except FileNotFoundError:
            logger.error(f"Not able to access the .row file {row_file}")
    
    def pcf_states(self, l: List[str]):
        """ Returns a dictionary with the States translations """
        mylist = l[l.index("STATES"):]
        mylist = mylist[1:mylist.index("")]
        states_id = [int(s) for x in mylist for s in x.split() if s.isdigit()]
        states_state = [s for x in mylist for s in x.split if not s.isdigit()]
        return {int(e[0]):e[1] for e in mylist}
    
    def pcf_events(self, l: List[str]):
        """ """
        mylist = l[:]
        while (mylist):
            mylist = mylist[mylist.index("EVENT_TYPE"):]
            aux = mylist[:mylist.index("")]

    def pcf_parser(self, file: str):
        """ Parses the .pcf file returning dictionaries with States and Event translations """
        pcf_file = file.replace(".prv", ".pcf")
        try:
            with open(pcf_file, "r") as f:
                lines = f.read().split("\n")
                states = pcf_states(lines)
                
        except FileNotFoundError:
            logger.error(f"Not able to access the .pcf file {pcf_file}")
    def content_parser(self, file: str, output: str):
        """ Parses the contents of the .prv file. It invokes an efficient C program to do the task """
        try:
            subprocess.run(["/home/orudyy/Repositories/Zumsehen/utils/bin/prv_reader", file, output], check=True)
        except subprocess.CalledProcessError:
            logger.error("C paraver reader failed")
    
    def write_metadata(self, file: str, traceMetadata: TraceMetaData):
        """ Writes into a hdf5 file the metadata contained in the header of .prv file """
        with h5py.File(file, "a") as f:
            metadata = f[HDF5_ROOT]
            metadata.attrs["name"] = traceMetadata.name
            metadata.attrs["path"] = traceMetadata.path
            metadata.attrs["type"] = traceMetadata.type
            metadata.attrs["exec_time"] = traceMetadata.exec_time
            metadata.attrs["date_time"] = traceMetadata.date_time.isoformat()
            metadata.attrs["nodes"] = traceMetadata.nodes
            apps_str = []
            for app in apps_str:
                apps_str.append([])
                for task in app:
                    apps_str[-1].append(str(task))
            metadata.attrs["apps"] = apps_str
            
    def file_parser(self, file: str) -> TraceMetaData:
        """ Parses a .prv file """
        try:
            with open(file, "r") as f:
                header = f.readline()
                if PARAVER_MAGIC_HEADER not in header:
                    logger.error(f"The file {f.name} is not a valid Paraver file!")

                logger.info(f"Parsing {file}")
                trace_name = os.path.basename(f.name)
                trace_path = os.path.abspath(f.name)
                trace_type = PARAVER_FILE

                trace_exec_time, trace_date, trace_nodes, trace_apps = self.header_parser(header)
                output = file.replace(".prv", ".h5")
                self.content_parser(file, output)
                print(f"file {output}")
                trace_metadata = TraceMetaData(
                    trace_name, trace_path, trace_type, trace_exec_time, trace_date, trace_nodes, trace_apps
                )
                self.write_metadata(output, trace_metadata)
        except FileNotFoundError:
            logger.error(f"Not able to access the file {file}")
            raise

        return trace_metadata

if __name__ == "__main__":
    ParaverReader().file_parser("/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimpleExtrae10TimeSteps.00.chop1.prv")