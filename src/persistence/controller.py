import logging

from src.persistence.hdf5_reader import HDF5Reader
from src.persistence.prv_reader import ParaverReader
from src.persistence.prv_to_hdf5 import ParaverToHDF5
from src.Trace import Trace, TraceMetaData


logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_trace(trace_file):
    file_format = trace_file.rsplit(".", 1)[1].lower()
    if file_format == "prv":
        logger.info(f"Reading prv file {trace_file}")
        trace_metadata, df_state, df_event, df_comm = ParaverReader().parse_file(trace_file)
    elif file_format == "hdf":
        logger.info(f"Reading hdf file {trace_file}")
        trace_metadata, df_state, df_event, df_comm = HDF5Reader().parse_file(trace_file, use_dask=True)
    else:
        raise Exception("Incorrect file format.")
    trace = Trace(trace_metadata, df_state, df_event, df_comm)
    logger.info(f"Read file {trace.metadata.name}")
    return trace
