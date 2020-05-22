import json
import logging
from datetime import datetime

import dask.dataframe as dd
import h5py
import numpy as np
import pandas as pd

from src.Trace import TraceMetaData

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _try_read_hdf(file, key, use_dask):
    if use_dask:
        try:
            return dd.read_hdf(file, key=key)
        except ValueError:
            return dd.from_array(np.array([[]]))
    else:
        try:
            return pd.read_hdf(file, key=key)
        except KeyError:
            return pd.DataFrame([])


class HDF5Reader:
    def parse_metadata(self, file: str):
        with h5py.File(file, "r") as f:
            records = f["RECORDS"]
            logger.info(str(records.attrs["apps"]))
            trace_metadata = TraceMetaData(
                records.attrs["name"],
                records.attrs["path"],
                records.attrs["type"],
                records.attrs["exec_time"],
                datetime.fromtimestamp(records.attrs["date_time"]),
                records.attrs["nodes"],
                json.loads(str(records.attrs["apps"])),
            )
        return trace_metadata

    def parse_file(self, file: str, use_dask=False):
        df_state_tmp = _try_read_hdf(file, key="States", use_dask=use_dask)
        df_event_tmp = _try_read_hdf(file, key="Events", use_dask=use_dask)
        df_comm_tmp = _try_read_hdf(file, key="Comm", use_dask=use_dask)
        trace_metadata = self.parse_metadata(file)
        return trace_metadata, df_state_tmp, df_event_tmp, df_comm_tmp
