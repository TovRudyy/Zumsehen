import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import dask.dataframe as dd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclass
class TraceMetaData:
    """ Store Trace's Metadata """

    name: str = ""
    path: str = ""
    type: str = ""
    exec_time: int = None
    date: datetime = None
    # len(Nodes) = #nodes | Nodes[0] = #CPUs of Node 1
    nodes: List[int] = None
    # len(Apps) = #Apps | len(Apps[0]) = #Tasks of APP 1 | App[0][0] = {"nThreads": int, "node": int}
    apps: List[List[Dict]] = None


@dataclass
class Trace:
    """ Store Trace's data """

    metadata: TraceMetaData = None
    df_state: dd.DataFrame = None
    df_event: dd.DataFrame = None
    df_comm: dd.DataFrame = None
