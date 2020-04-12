from src.persistence.hdf5_reader import HDF5Reader
from src.persistence.prv_reader import ParaverReader
from src.persistence.prv_to_hdf5 import ParaverToHDF5
from src.Trace import Trace, TraceMetaData


def parse_trace(trace_file):
    file_format = trace_file.filename.rsplit(".", 1)[1].lower()
    if file_format == "prv":
        format_converter = ParaverToHDF5()
        df_state, df_event, df_comm = format_converter.parse_as_dataframe(trace_file, use_dask=True)
        trace_metadata = ParaverReader().parse_file(trace_file)
    elif file_format == "hdf":
        hdf5_reader = HDF5Reader()
        df_state, df_event, df_comm = hdf5_reader.parse_file(trace_file, use_dask=True)
        trace_metadata = TraceMetaData()
    else:
        raise Exception("Incorrect file format.")

    trace = Trace(trace_metadata, df_state, df_event, df_comm)
    return trace
