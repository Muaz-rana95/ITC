from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, monotonically_increasing_id
from pyspark.sql.types import IntegerType
import logging

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Auto Increment Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_undergroundresul"

logger.info("Loading data from source table: {}.{}".format(HIVE_DB, SOURCE_TABLE))

# Load data from the source table
df_source = spark.sql(f"SELECT * FROM {HIVE_DB}.{SOURCE_TABLE}")

# Add an "ingestion_timestamp" column
df_transformed = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Clean "route" and "delay_time" columns by removing quotes
for col_name in ["route", "delay_time"]:
    df_transformed = df_transformed.withColumn(col_name, regexp_replace(col(col_name), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the "route" column
df_transformed = df_transformed.filter(col("route").isNotNull())

# Ensure target table exists before querying max(record_id)
try:
    max_record_id = spark.sql(f"SELECT MAX(record_id) FROM {HIVE_DB}.{TARGET_TABLE}").collect()[0][0] or 0
    logger.info(f"Max existing record_id: {max_record_id}")
except:
    logger.warning(f"Table {TARGET_TABLE} does not exist. Starting record_id from 1.")
    max_record_id = 0

# Generate a unique record_id using monotonically_increasing_id
df_transformed = df_transformed.withColumn("record_id", (monotonically_increasing_id() + max_record_id).cast(IntegerType()))

# Ensure column order matches the Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp"]
df_final = df_transformed.select(*expected_columns)

logger.info(f"Writing transformed data to Hive table: {HIVE_DB}.{TARGET_TABLE}")

# Append data into the existing Hive table
df_final.write.mode("append").insertInto(f"{HIVE_DB}.{TARGET_TABLE}")

logger.info("Data successfully written to Hive. Closing Spark session.")

# Stop Spark session
spark.stop()
