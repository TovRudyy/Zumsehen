import dask.dataframe as dd
import numpy as np
import pandas as pd

from src.persistence.reader import Reader


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


class HDF5Reader(Reader):
    def header_time(self, header):
        pass

    def header_nodes(self, header):
        pass

    def header_apps(self, header):
        pass

    def header_parser(self, header):
        pass

    def parse_file(self, file: str, use_dask=False):
        df_state_tmp = _try_read_hdf(file, key="States", use_dask=use_dask)
        df_event_tmp = _try_read_hdf(file, key="Events", use_dask=use_dask)
        df_comm_tmp = _try_read_hdf(file, key="Comm", use_dask=use_dask)
        return df_state_tmp, df_event_tmp, df_comm_tmp

    def header_date(self, header):
        pass
