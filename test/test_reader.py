import logging
from datetime import datetime

import pytest

from src.reader import paraver_header_parser

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def compare_trace_metadata(trace_a, trace_b):
    # para el futuro
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


# esto ejecutara test_header_parser 2 veces, una con el primer #Paraver y otra con el segundo, asi puedes stackear
# tests iguales con diferentes inputs para probar muchos casos de la misma funcion
@pytest.mark.parametrize(
    "header,expected_header",
    (
        (
            "#Paraver (17/02/2020 at 11:37):1857922_ns:1(4):1:2(2:1,2:1)",
            (1857922, datetime.strptime("17/02/2020 11:37", "%d/%m/%Y %H:%M"), [4], [[[1, 1], [1, 1]]]),
        ),
        (
            "#Paraver (17/02/2020 at 11:37):1857922_ns:1(4):1:2(2:1,2:1)",
            (1857922, datetime.strptime("17/02/2020 11:37", "%d/%m/%Y %H:%M"), [4], [[[1, 1], [1, 1]]]),
        ),
    ),
)

def test_header_parser(header, expected_header):
    assert expected_header == paraver_header_parser(header)
