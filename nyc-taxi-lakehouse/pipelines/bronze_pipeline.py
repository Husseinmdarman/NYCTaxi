from pyspark.sql import functions as F
from utils.logging_config import get_logger
from utils.decorators import run_before, log_timing
from utils.io_helpers import read_csv, read_parquet, write_delta

class BronzePipeline:
    def __init__(self, spark):
        """Initializes the BronzePipeline with a Spark session and sets up logging."""
        self.spark = spark
        self.logger = get_logger(self.__class__.__name__ )
        self.raw_df = None
        self.lookup_tables = {}

    @log_timing
    def _load_raw(self):
        """Loads raw data from S3 into a Spark DataFrame."""
        try:
            self.logger.info("loading raw yellow taxi data....")
            self.raw_df = read_parquet(self.spark,  "s3://historic-taxi-data/yellow_tripdata/")
            self.logger.info(f"Raw data loaded with {self.raw_df.count()} raw records.")
        except Exception as e:
            self.logger.error(f"Error loading raw data: {e}")
            raise

    @log_timing
    def _add_metadata(self):
        """Adds metadata columns to the raw DataFrame."""
        try:
            self.logger.info("Adding metadata columns...")
            self.raw_df = (self.raw_df
                           .withColumn("ingestion_timestamp", F.current_timestamp())
                            .withColumn("source", F.input_file_name())
                            )
            self.logger.info("Metadata columns added.")
        except Exception as e:
            self.logger.error(f"Error adding metadata: {e}")
            raise