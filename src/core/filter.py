import logging
from typing import Iterable

import dask.dataframe as dd

from src.CONST import Record

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _check_attribute(attribute: Record):
    if not attribute.can_filter:
        raise Exception(
            f"Cannot filter by {attribute}. List of possible attributes: {', '.join(Record.filter_attributes())}."
        )


# this functions compute the bit mask of the DataFrame indices that meet the condition


def _filter_equal(df, attribute, value):
    return df[attribute] == value


def _filter_not_equal(df, attribute, value):
    return df[attribute] != value


def _filter_lesser(df, attribute, value):
    return df[attribute] < value


def _filter_greater(df, attribute, value):
    return df[attribute] > value


def _filter_lesser_or_equal(df, attribute, value):
    return df[attribute] <= value


def _filter_greater_or_equal(df, attribute, value):
    return df[attribute] >= value


def _filter_from_to(df, attribute, start_value=None, end_value=None):
    logger.info(f"start_value {start_value}, end_value {end_value}")

    if start_value is not None and end_value is not None:
        mask = (df[attribute] >= start_value) & (df[attribute] < end_value)
    elif start_value is not None:
        mask = df[attribute] >= start_value
    elif end_value is not None:
        mask = df[attribute] < end_value
    else:
        raise Exception(f"Cannot filter from {start_value} to {end_value}.")

    return mask


def _filter_contains(df, attribute, values: Iterable):
    mask = df[attribute].isin(values)
    return mask


# Not used for now, useful in the future
# def _filter_by_attribute_names(df, attributes: List):
#     bad_attributes = [attribute for attribute in attributes if attribute not in filter_attributes]
#     if len(bad_attributes) > 0:
#         raise Exception(f"Cannot filter by one or more attributes: {', '.join(bad_attributes)}.")
#
#     return df[attributes]


# =, !=, <, <=, >, >=, [], [)


class Filter:
    _operator_function = {
        "==": _filter_equal,
        "=": _filter_equal,
        "!=": _filter_not_equal,
        "<=": _filter_lesser_or_equal,
        ">=": _filter_greater_or_equal,
        "<": _filter_lesser,
        ">": _filter_greater,
        "in": _filter_contains,
        "from_to": _filter_from_to,
    }

    def __init__(self):
        self.mask = None

    def add_operator(self, df: dd.DataFrame, attribute: Record, operator: str, *args):
        """
        Stores the result bit mask after adding an operator. Only filters in execute(), doesn't do any additional
        computation besides computing the bit masks.
        """
        _check_attribute(attribute)
        added_operator = self._operator_function[operator](df, attribute.name, *args)
        logger.debug(f"adding operator {attribute} {operator} {args}")
        if self.mask is None:
            self.mask = added_operator
        else:
            self.mask = self.mask & added_operator
        logger.debug(f"{self.mask}")
        return self

    def execute(self, df):
        return df.loc[self.mask]
