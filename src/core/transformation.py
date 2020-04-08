from typing import List

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

    mask = (df[attribute] >= start_value) & (df[attribute] < end_value)
    return df.loc[mask]


def filter_by_values(df, attribute, values: List):
    if attribute not in filter_attributes:
        raise Exception(f"Cannot filter by {attribute}.")

    mask = df[attribute].isin(values)
    return df.loc[[mask]]
