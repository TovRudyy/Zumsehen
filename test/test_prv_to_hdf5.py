import os
from unittest.mock import patch

import pandas as pd
import pytest

from src.persistence.prv_to_hdf5 import ParaverToHDF5

test_suite = [f"test/test_files/traces/{test}" for test in ["bt-mz.2x2.test.prv"]]


def get_prv_test_traces():
    data = []
    for input in sorted(os.listdir("test/test_files/traces")):
        input = "test/test_files/traces/" + input
        # for input in test_suite:
        if "test.prv" in input:
            sol_state = pd.read_hdf(f"{input[:-8]}parsed.hdf", key="States").astype("int64")
            sol_event = pd.read_hdf(f"{input[:-8]}parsed.hdf", key="Events").astype("int64")
            sol_comm = pd.read_hdf(f"{input[:-8]}parsed.hdf", key="Comm").astype("int64")
            data.append(
                {"Input": input, "states_records": sol_state, "event_records": sol_event, "comm_records": sol_comm}
            )
        else:
            pass
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
        format_converter = ParaverToHDF5()
        for test in data:
            df_state, df_event, df_comm = format_converter.parse_as_dataframe(test["Input"])
            df_state = df_state.astype("int64")
            df_event = df_event.astype("int64")
            df_comm = df_comm.astype("int64")
            assert test["states_records"].equals(df_state)
            assert test["event_records"].equals(df_event)
            assert test["comm_records"].equals(df_comm)
