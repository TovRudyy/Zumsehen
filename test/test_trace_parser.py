import logging
import os
from datetime import datetime

import pandas as pd
import pytest

from src.utils.prv_to_hdf5 import seq_parse_as_dataframe

test_suite = [f"test/test_files/traces/{test}" for test in ["1MB.test.prv", "10MB.test.prv", "bt-mz.2x2.test.prv"]]


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


def test_seq_prv_trace_parser():
    data = get_prv_test_traces()
    for test in data:
        df_state, df_event, df_comm = seq_parse_as_dataframe(test["Input"])
        df_state = df_state.astype("int64")
        df_event = df_event.astype("int64")
        df_comm = df_comm.astype("int64")
        assert test["states_records"].equals(df_state)
        assert test["event_records"].equals(df_event)
        assert test["comm_records"].equals(df_comm)
