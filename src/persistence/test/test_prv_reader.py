import json
import os
from datetime import datetime

from src.persistence.prv_reader import ParaverReader

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
                (header, (sol["ExecTime"], datetime.strptime(sol["Date"], "%d/%m/%Y %H:%M"), sol["Nodes"], sol["Apps"]))
            )
    return data


# TODO fix


def test_prv_header_parser():
    header_test_data = prv_header_test_data()
    header_parser = ParaverReader()
    for header, expected_header in header_test_data:
        assert expected_header == header_parser.header_parser(header)
