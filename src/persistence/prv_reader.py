import logging
import os
from datetime import datetime, timedelta
from typing import Tuple

import h5py

import dask.dataframe as dd

from src.Trace import TraceMetaData
from src.persistence.prv_to_hdf5 import ParaverToHDF5
from src.persistence.reader import Reader
from src.persistence.writer import Writer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PARAVER_FILE = "Paraver (.prv)"
PARAVER_MAGIC_HEADER = "#Paraver"


class ParaverReader(Reader):
    def header_date(self, header: str):
        date, _, other = header.replace("#Paraver (", "").replace("at ", "").partition("):")
        return datetime.strptime(date, "%d/%m/%Y %H:%M")

    def header_time(self, header):
        # in microseconds
        time_ns, _, other = header[header.find("):") + 2 :].partition("_ns")
        return int(time_ns) // 1000

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

    def write_metadata_to_hdf5(self, file_hdf5, trace_metadata: TraceMetaData):
        with h5py.File(file_hdf5, "a") as f:
            try:
                records = f["RECORDS"]
            except KeyError:
                f.create_group("RECORDS")
                records = f["RECORDS"]
                
            records.attrs["name"] = trace_metadata.name
            records.attrs["path"] = trace_metadata.path
            records.attrs["type"] = trace_metadata.type
            records.attrs["exec_time"] = trace_metadata.exec_time
            records.attrs["date_time"] = trace_metadata.date_time.timestamp()
            records.attrs["nodes"] = trace_metadata.nodes
            apps_str = []
            for app in apps_str:
                apps_str.append([])
                for task in app:
                    apps_str[-1].append(str(task))
            records.attrs["apps"] = apps_str

    def parse_file(self, file: str) -> Tuple[TraceMetaData, dd.DataFrame, dd.DataFrame, dd.DataFrame]:
        try:
            with open(file, "r") as f:
                header = f.readline()
                if PARAVER_MAGIC_HEADER not in header:
                    logger.error(f"The file {f.name} is not a valid Paraver file!")

                logger.info(f"Parsing {f}")
                trace_name = os.path.basename(f.name)
                trace_path = os.path.abspath(f.name)
                trace_type = PARAVER_FILE

                trace_exec_time, trace_date, trace_nodes, trace_apps = self.header_parser(header)
                new_trace_name = trace_name.replace(".prv", ".hdf")
                new_trace_path = trace_path.replace(".prv", ".hdf")

                df_state, df_event, df_comm = ParaverToHDF5().parse_as_dataframe(file, use_dask=True)
                Writer().dataframe_to_hdf5(new_trace_name, df_state, df_event, df_comm)

                trace_metadata = TraceMetaData(
                    new_trace_name, new_trace_path, trace_type, trace_exec_time, trace_date, trace_nodes, trace_apps
                )
                self.write_metadata_to_hdf5(new_trace_name, trace_metadata)
        except FileNotFoundError:
            logger.error(f"Not able to access the file {file}")
            raise

        return trace_metadata, df_state, df_event, df_comm
