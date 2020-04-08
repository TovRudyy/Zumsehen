import logging
from unittest.mock import Mock

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from src.core.transformation import Transformation

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
    transformation = Transformation()
    filtered = transformation.add_operator(df, attribute, "from_to", start_value, end_value).execute(df)
    assert np.array_equal(filtered.compute(), expected_df)


@pytest.mark.parametrize(
    "attribute,start_value,end_value,expected_exception",
    (("state", None, None, "Cannot filter from None to None."), ("states", 1, 2, "Cannot filter by states."),),
)
def test_filter_from_to_exception(attribute, start_value, end_value, expected_exception):
    with pytest.raises(Exception) as excinfo:
        transformation = Transformation()
        transformation.add_operator(Mock(), attribute, "from_to", start_value, end_value).execute()
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
def test_filter_contains(df, attribute, values, expected_df):
    transformation = Transformation()
    filtered = transformation.add_operator(df, attribute, "in", values).execute(df)
    assert np.array_equal(filtered.compute(), expected_df)


@pytest.mark.parametrize(
    "attribute,values,expected_exception", (("states", [], "Cannot filter by states."),),
)
def test_filter_contains_exception(attribute, values, expected_exception):
    with pytest.raises(Exception) as excinfo:
        transformation = Transformation()
        transformation.add_operator(Mock(), attribute, "in", values).execute()
    assert expected_exception in str(excinfo)


@pytest.mark.parametrize(
    "df,attributes,operators,args,expected_df",
    (
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [1, 2, 3]]), columns=columns),
            ("time", "event_t"),
            (">", "!="),
            (0, 1),
            dd.from_array(np.array([[1, 2, 3]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [1, 2, 3]]), columns=columns),
            ("time", "event_t"),
            ("<", "in"),
            (2, (0, 2)),
            dd.from_array(np.array([[0, 0, 0]]), columns=columns).compute(),
        ),
    ),
)
def test_nested_transformations(df, attributes, operators, args, expected_df):
    transformation = Transformation()
    for attribute, operator, arg in zip(attributes, operators, args):
        transformation = transformation.add_operator(df, attribute, operator, arg)
    result = transformation.execute(df).compute()
    assert np.array_equal(result, expected_df)


@pytest.mark.smoke
@pytest.mark.parametrize(
    "df,attribute,start_value,end_value,expected_df",
    (
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [1, 2, 3]]), columns=columns),
            "time",
            0,
            2.5,
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [1, 2, 3]]), columns=columns),
            "time",
            1,
            None,
            dd.from_array(np.array([[0, 1, 2], [1, 2, 3]]), columns=columns).compute(),
        ),
        (
            dd.from_array(np.array([[0, 0, 0], [0, 1, 2], [1, 2, 3]]), columns=columns),
            "time",
            None,
            1,
            dd.from_array(np.array([[0, 0, 0]]), columns=columns).compute(),
        ),
    ),
)
def test_transformation_from_to(df, attribute, start_value, end_value, expected_df):
    transformation = Transformation()
    transformation = transformation.add_operator(df, attribute, "from_to", start_value, end_value)
    result = transformation.execute(df).compute()
    assert np.array_equal(result, expected_df)
