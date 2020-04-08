import logging

import dask.dataframe as dd
import numpy as np
import pytest

from src.core.transformation import filter_from_to

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

columns = ["cpu_id", "event_t", "time"]


@pytest.mark.parametrize(
    "df,attribute,start_value,end_value,expected_df",
    (
        (
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns),
            "time",
            1,
            3,
            dd.from_array(np.array([[0, 0, 1], [0, 1, 2]]), columns=columns),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns),
            "time",
            1,
            None,
            dd.from_array(np.array([[0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns),
            "time",
            None,
            3,
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2]]), columns=columns),
        ),
    ),
)
def test_filter_from_to(df, attribute, start_value, end_value, expected_df):
    logger.info(filter_from_to(df, attribute, start_value, end_value).compute())
    logger.info(expected_df)
    assert filter_from_to(df, attribute, start_value, end_value).compute().equals(expected_df).compute()


# @pytest.mark.parametrize("df,attribute,values,expected_df", ())
# def test_filter_by_values(df, attribute, values, expected_df):
#     assert filter_from_to(attribute, values) == expected_df
