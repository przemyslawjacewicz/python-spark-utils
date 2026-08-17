import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


# todo: add tests
# todo: sorted ?
def is_equal_schema(left: StructType, right: StructType) -> bool:
    pass


# todo: add tests
# todo: add schema check as first step: check schema size -> check sorted schema column matching
def is_equal_dataframe(left: DataFrame, right: DataFrame) -> bool:
    left_count = left.count()
    right_count = right.count()
    if left_count != right_count:
        logger.debug(f"count mismatch: {left_count=}, {right_count=}.")
        return False
    logger.debug(f"count match: {left_count=}, {right_count=}.")

    left_diff_right = left.exceptAll(right)
    if not left_diff_right.isEmpty():
        logger.debug(f"left diff right row-level mismatch: {left_diff_right.count()=}.")
        return False
    logger.debug("left diff right row-level match.")

    right_diff_left = right.exceptAll(left)
    if not right_diff_left.isEmpty():
        logger.debug(f"right diff left row-level mismatch: {right_diff_left.count()=}.")
        return False
    logger.debug("right diff left row-level match.")

    return True
