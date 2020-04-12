import logging
from unittest.mock import patch

import pandas as pd
import pytest

from src.persistence.hdf5_reader import HDF5Reader
from src.persistence.prv_to_hdf5 import ParaverToHDF5
from src.persistence.writer import Writer

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TRACES_DIR = "src/persistence/test/test_files/traces"

files_dir = "src/persistence/test/test_files/traces"
files = ["10MB.test.prv"]

format_converter = ParaverToHDF5()


def test_get_state_row():
    state_row = "1:2:1:1:1:0:200:1"
    assert [2, 1, 1, 1, 0, 200, 1] == format_converter._get_state_row(state_row)


def test_get_event_row():
    state_row = "1:2:1:1:1:0:200:1"
    assert [2, 1, 1, 1, 0, 200, 1] == format_converter._get_event_row(state_row)


def test_get_comm_row():
    # TODO
    pass


def get_prv_test_traces():
    data = []
    for test in files:
        parsed_test = f"{files_dir}/{test[:-8]}parsed.hdf"
        sol_state = pd.read_hdf(parsed_test, key="States").astype("int64")
        sol_event = pd.read_hdf(parsed_test, key="Events").astype("int64")
        sol_comm = pd.read_hdf(parsed_test, key="Comm").astype("int64")
        data.append(
            {
                "Input": f"{files_dir}/{test}",
                "states_records": sol_state,
                "event_records": sol_event,
                "comm_records": sol_comm,
            }
        )
    return data


all_parser_params = (
    {"STEPS": 200000, "MAX_READ_BYTES": 1024 * 1024 * 1024 * 2, "MIN_ELEM": 40000000},
    {"STEPS": 1000, "MAX_READ_BYTES": 1024 * 1024 * 1024, "MIN_ELEM": 2000},
    {"STEPS": 200000, "MAX_READ_BYTES": 1024 * 1024, "MIN_ELEM": 40000000},
)


@pytest.mark.parametrize("parser_params", all_parser_params)
def test_seq_prv_trace_parser(parser_params):
    with patch("src.persistence.prv_to_hdf5.STEPS", parser_params["STEPS"]), patch(
        "src.persistence.prv_to_hdf5.MAX_READ_BYTES", parser_params["MAX_READ_BYTES"]
    ), patch("src.persistence.prv_to_hdf5.MIN_ELEM", parser_params["MIN_ELEM"]):

        data = get_prv_test_traces()
        for test in data:
            df_state, df_event, df_comm = format_converter.parse_as_dataframe(test["Input"])
            df_state = df_state.astype("int64")
            df_event = df_event.astype("int64")
            df_comm = df_comm.astype("int64")
            assert test["states_records"].equals(df_state)
            assert test["event_records"].equals(df_event)
            assert test["comm_records"].equals(df_comm)


def assert_equals_if_rows(df1, df2):
    if df1.shape[0] != 0 and df2.shape[0] != 0:
        assert df1.equals(df2)


@pytest.mark.parametrize("use_dask", (False, True))
def test_e2e_parse_and_read(use_dask):
    writer = Writer()
    reader = HDF5Reader()
    data = get_prv_test_traces()
    for test in data:
        df_state, df_event, df_comm = format_converter.parse_as_dataframe(test["Input"], use_dask=use_dask)
        file_name = test["Input"].split("/")[-1]
        new_name = f"{files_dir}/tmp_{file_name}"
        writer.dataframe_to_hdf5(new_name, df_state, df_event, df_comm)
        df_state_tmp, df_event_tmp, df_comm_tmp = reader.parse_file(new_name, use_dask=use_dask)
        if use_dask:
            df_state, df_event, df_comm = df_state.compute(), df_event.compute(), df_comm.compute()
            df_state_tmp, df_event_tmp, df_comm_tmp = (
                df_state_tmp.compute(),
                df_event_tmp.compute(),
                df_comm_tmp.compute(),
            )
        assert_equals_if_rows(df_state, df_state_tmp)
        assert_equals_if_rows(df_event, df_event_tmp)
        assert_equals_if_rows(df_comm, df_comm_tmp)
