import json
import logging
import os
from datetime import datetime

import pytest

from src.reader import prv_header_parser

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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


@pytest.fixture
def header_test_data():
    data = []
    # os.chdir("test/test_files/headers")
    for input in sorted(os.listdir("test/test_files/headers")):
        input = "test/test_files/headers/" + input
        if "6.in" in input:
            header = None
            sol = None
            with open(input, "r") as input:
                header = input.readline()
            with open(f"{input.name[:-2]}sol.json", "r") as output:
                sol = json.load(output)

            data.append(
                (header, (sol["ExecTime"], datetime.strptime(sol["Date"], "%d/%m/%Y %H:%M"), sol["Nodes"], sol["Apps"]))
            )
        else:
            pass
    return data


def test_header_parser(header_test_data):
    for header, expected_header in header_test_data:
        assert expected_header == prv_header_parser(header)
