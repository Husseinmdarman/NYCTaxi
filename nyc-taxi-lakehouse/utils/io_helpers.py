from pyspark.sql import DataFrame

def read_csv(spark, path: str) -> DataFrame:
    """Reads a CSV file into a Spark DataFrame."""
    return spark.read.csv(path, header=True, inferSchema=True)

def read_parquet(spark, path: str) -> DataFrame:
    """Reads a Parquet file into a Spark DataFrame."""
    return spark.read.parquet(path)

def read_delta(spark, table_name: str) -> DataFrame:
    """Reads a Delta Lake table into a Spark DataFrame."""
    return spark.read.table(table_name)

def write_delta(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Writes a Spark DataFrame to a Delta Lake table."""
    df.write.format("delta").mode(mode).saveAsTable(table_name)
