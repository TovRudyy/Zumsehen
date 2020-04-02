import logging
import os
from datetime import datetime

import pytest

from src.reader import prv_header_apps, prv_header_date, prv_header_nodes, prv_header_parser, prv_header_time

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


# Python decorator
@pytest.mark.parametrize(
    "header,expected_header",
    (
        (
            "#Paraver (18/03/2020 at 11:15):1056311873701_ns:1(48):1:48(1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1,1:1),49",
            (
                1056311873701,
                datetime.strptime("18/03/2020 11:15", "%d/%m/%Y %H:%M"),
                [48],
                [
                    [
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                        {"nThreads": 1, "node": 1},
                    ]
                ],
            ),
        ),
        (
            "#Paraver (17/02/2020 at 11:37):1857922_ns:1(4):1:2(2:1,2:1)",
            (
                1857922,
                datetime.strptime("17/02/2020 11:37", "%d/%m/%Y %H:%M"),
                [4],
                [[{"nThreads": 2, "node": 1}, {"nThreads": 2, "node": 1}]],
            ),
        ),
        (
            "#Paraver (01/01/2000 at 00:00):139_ns:0:4:1(1:0):4(2:0,5:0,2:0,1:0):2(3:0,3:0):1(46:0)",
            (
                139,
                datetime.strptime("01/01/2000 00:00", "%d/%m/%Y %H:%M"),
                None,
                [
                    [{"nThreads": 1, "node": 0}],
                    [
                        {"nThreads": 2, "node": 0},
                        {"nThreads": 5, "node": 0},
                        {"nThreads": 2, "node": 0},
                        {"nThreads": 1, "node": 0},
                    ],
                    [{"nThreads": 3, "node": 0}, {"nThreads": 3, "node": 0}],
                    [{"nThreads": 46, "node": 0}],
                ],
            ),
        ),
        (
            "#Paraver (10/04/2001 at 18:21):620244_ns:0:1:1(4:0)",
            (620244, datetime.strptime("10/04/2001 18:21", "%d/%m/%Y %H:%M"), None, [[{"nThreads": 4, "node": 0}]],),
        ),
        (
            "#Paraver (18/03/2020 at 09:28):156230961812_ns:8(32,32,32,32,32,32,32,32):1:8(32:1,32:2,32:3,32:4,32:5,32:6,32:7,32:8),9",
            (
                156230961812,
                datetime.strptime("18/03/2020 09:28", "%d/%m/%Y %H:%M"),
                [32, 32, 32, 32, 32, 32, 32, 32],
                [
                    [
                        {"nThreads": 32, "node": 1},
                        {"nThreads": 32, "node": 2},
                        {"nThreads": 32, "node": 3},
                        {"nThreads": 32, "node": 4},
                        {"nThreads": 32, "node": 5},
                        {"nThreads": 32, "node": 6},
                        {"nThreads": 32, "node": 7},
                        {"nThreads": 32, "node": 8},
                    ]
                ],
            ),
        ),
    ),
)
def test_prv_header_parser(header, expected_header):
    assert expected_header == prv_header_parser(header)
