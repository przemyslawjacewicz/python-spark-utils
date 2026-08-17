import uuid

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StructType


def normalize_col(col: Column | str, schema: StructType) -> Column:
    return F.from_json(F.to_json(col), schema)


# spark.createDataFrame(arg.withColumn(c, normalize_column(F.struct("*"), schema)).select(f"{c}.*").rdd, schema)
def normalize_dataframe(df: DataFrame, schema: StructType) -> DataFrame:
    c = str(uuid.uuid4())
    return df.withColumn(c, normalize_col(F.struct("*"), schema)).select(f"{c}.*")
