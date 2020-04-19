import logging

from src.CONST import Record

import dask.dataframe as dd

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _check_attribute(attribute: Record):
    if not attribute.can_group:
        raise Exception(
            f"Cannot filter by {attribute}. List of possible attributes: {', '.join(Record.group_attributes())}."
        )


class Group:
    def group_by(self, df: dd.DataFrame, attribute: Record):
        _check_attribute(attribute)
        return df.groupby(attribute.name)
