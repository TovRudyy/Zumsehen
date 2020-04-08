import logging
from typing import List

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

filter_attributes = {
    "state",
    "event_t",
    "event_v",
    "thread_id",
    "lsend",  # TODO verify
    "psend",  # TODO verify
    "size",
    "time",
}


def filter_from_to(df, attribute, start_value=None, end_value=None):
    if attribute not in filter_attributes:
        raise Exception(f"Cannot filter by {attribute}.")

    if start_value is not None and end_value is not None:
        mask = (df[attribute] >= start_value) & (df[attribute] < end_value)
    elif start_value is not None:
        mask = df[attribute] >= start_value
    elif end_value is not None:
        mask = df[attribute] < end_value
    else:
        raise Exception(f"Cannot filter from {start_value} to {end_value}.")

    return df.loc[mask]


def filter_by_attribute_values(df, attribute, values: List):
    if attribute not in filter_attributes:
        raise Exception(f"Cannot filter by {attribute}. List of possible attributes: {', '.join(filter_attributes)}.")

    mask = df[attribute].isin(values)
    return df.loc[mask]


def filter_by_attribute_names(df, attributes: List):
    bad_attributes = [attribute for attribute in attributes if attribute not in filter_attributes]
    if len(bad_attributes) > 0:
        raise Exception(f"Cannot filter by one or more attributes: {', '.join(bad_attributes)}.")

    return df[attributes]
