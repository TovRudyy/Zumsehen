import json
import os
from datetime import datetime

# from src.persistence.hdf5_reader import HDF5Reader
from src.persistence.prv_reader import ParaverReader

# from src.persistence.prv_to_hdf5 import ParaverToHDF5
# from src.persistence.test.common import assert_equals_if_rows, files_dir, get_prv_test_traces
# from src.persistence.writer import Writer

TRACES_DIR = "src/persistence/test/test_files/headers"


def compare_trace_metadata(trace_a, trace_b):
    # For future usages
    if (
        trace_a.Name == trace_b.Name
        and trace_a.Path == trace_b.Path
        and trace_a.Type == trace_b.Type
        and trace_a.ExecTime == trace_b.ExecTime
        and trace_a.Date == trace_b.Date
        and trace_a.Nodes == trace_b.Nodes
        and trace_a.Apps == trace_b.Apps
    ):
        return True
    return False


def prv_header_test_data():
    data = []
    # os.chdir("test/test_files/headers")
    for file in sorted(os.listdir(TRACES_DIR)):
        file = f"{TRACES_DIR}/{file}"
        if "6.in" in file:
            header = None
            sol = None
            with open(file, "r") as input_file:
                header = input_file.readline()
            with open(f"{input_file.name[:-2]}sol.json", "r") as output:
                sol = json.load(output)

            data.append(
                (
                    header,
                    (
                        sol["ExecTime"] // 1000,
                        datetime.strptime(sol["Date"], "%d/%m/%Y %H:%M"),
                        sol["Nodes"],
                        sol["Apps"],
                    ),
                )
            )
    return data


# TODO fix


def test_prv_header_parser():
    header_test_data = prv_header_test_data()
    header_parser = ParaverReader()
    for header, expected_header in header_test_data:
        assert expected_header == header_parser.header_parser(header)


# def test_parse_file():
#     reader = HDF5Reader()
#     data = get_prv_test_traces()
#     format_converter = ParaverToHDF5()
#     paraver_reader = ParaverReader()
#     for test in data:
#         # Python prv to hdf5
#         df_state, df_event, df_comm = format_converter.parse_as_dataframe(test["Input"], use_dask=True)
#
#         # C prv to hdf5
#         trace_metadata = paraver_reader.parse_file(test["Input"])
#         df_state_c, df_event_c, df_comm_c = reader.parse_file(trace_metadata.path, use_dask=True)
#
#         assert_equals_if_rows(df_state, df_state_c)
#         assert_equals_if_rows(df_event, df_event_c)
#         assert_equals_if_rows(df_comm, df_comm_c)
