import logging
from unittest.mock import Mock

import dask.dataframe as dd
import pandas as pd
import numpy as np
import pytest

from src.core.transformation import filter_from_to, filter_by_attribute_values, filter_by_attribute_names

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
            dd.from_array(np.array([[0, 0, 1], [0, 1, 2]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns),
            "time",
            1,
            None,
            dd.from_array(np.array([[0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2], [0, 1, 3]]), columns=columns),
            "time",
            None,
            3,
            dd.from_array(np.array([[0, 0, 0], [0, 0, 1], [0, 1, 2]]), columns=columns).compute(),
        ),
    ),
)
def test_filter_from_to(df, attribute, start_value, end_value, expected_df):
    filtered = filter_from_to(df, attribute, start_value, end_value).compute()
    assert np.array_equal(filtered, expected_df)


@pytest.mark.parametrize(
    "attribute,start_value,end_value,expected_exception",
    (
            ("state", None, None, "Cannot filter from None to None."),
            ("states", 1, 2, "Cannot filter by states."),
    ),
)
def test_filter_from_to_exception(attribute, start_value, end_value, expected_exception):
    with pytest.raises(Exception) as excinfo:
        filtered = filter_from_to(Mock(), attribute, None, None).compute()
    assert expected_exception in str(excinfo)


@pytest.mark.parametrize(
    "df,attribute,values,expected_df",
    (
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [0, 1, 3]]), columns=columns),
            "event_t",
            [1],
            dd.from_array(np.array([[0, 1, 2], [0, 1, 3]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 1, 0], [0, 1, 1]]), columns=columns),
            "event_t",
            [1],
            dd.from_array(np.array([[0, 1, 0], [0, 1, 1]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 1, 0], [0, 1, 1]]), columns=columns),
            "event_t",
            [0],
            dd.from_pandas(pd.DataFrame(np.empty(shape=(0, 3))), npartitions=1).compute(),
            # black magic, can't create empty dask dataframe unless done this way
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [0, 2, 3]]), columns=columns),
            "event_t",
            [1, 2],
            dd.from_array(np.array([[0, 1, 2], [0, 2, 3]]), columns=columns).compute(),
        ),
    ),
)
def test_filter_by_attribute_values(df, attribute, values, expected_df):
    filtered = filter_by_attribute_values(df, attribute, values).compute()
    assert np.array_equal(filtered.values, expected_df.values)


@pytest.mark.parametrize(
    "attribute,values,expected_exception",
    (
            ("states", [], "Cannot filter by states."),
    ),
)
def test_filter_by_attribute_values_exception(attribute, values, expected_exception):
    with pytest.raises(Exception) as excinfo:
        filtered = filter_by_attribute_values(Mock(), attribute, values).compute()
    assert expected_exception in str(excinfo)


@pytest.mark.parametrize(
    "df,attributes,expected_df",
    (
        (
            dd.from_array(np.array([[0, 1, 2], [0, 1, 3]]), columns=columns),
            ["event_t", "time"],
            dd.from_array(np.array([[1, 2], [1, 3]]), columns=["event_t", "time"]).compute(),
        ),
        (
            dd.from_array(np.array([[0, 1, 0], [0, 1, 1]]), columns=columns),
            ["event_t"],
            dd.from_array(np.array([[1], [1]]), columns=["event_t"]).compute(),
        ),
    ),
)
def test_filter_by_attribute_names(df, attributes, expected_df):
    filtered = filter_by_attribute_names(df, attributes).compute()
    assert np.array_equal(filtered.values, expected_df.values)


@pytest.mark.parametrize(
    "attributes,expected_exception",
    (
            (["states"], "Cannot filter by one or more attributes:"),
    ),
)
def test_filter_by_attribute_names_exception(attributes, expected_exception):
    with pytest.raises(Exception) as excinfo:
        filtered = filter_by_attribute_names(Mock(), attributes).compute()
    assert expected_exception in str(excinfo)
