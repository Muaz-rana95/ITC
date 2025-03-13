from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("TfL Underground Data Transformation") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_undergroundresult"

# Load new data from the source table
df_source = spark.sql("SELECT * FROM {}.{}".format(HIVE_DB, SOURCE_TABLE))

# Transform data
df_transformed = df_source \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("route", regexp_replace(col("route"), r'^"|"$', '')) \
    .withColumn("line", regexp_replace(col("line"), r'^"|"$', ''))

# Retrieve max record_id from the target table
try:
    max_record_id = spark.sql("SELECT MAX(record_id) FROM {}.{}".format(HIVE_DB, TARGET_TABLE)).collect()[0][0]
    if max_record_id is None:
        max_record_id = 0
except:
    max_record_id = 0

# Assign incremental record_id
window_spec = Window.orderBy("ingestion_timestamp")
df_transformed = df_transformed.withColumn("record_id", (row_number().over(window_spec) + max_record_id).cast(IntegerType()))

# Select required columns
expected_columns = ["record_id", "timestamp", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp"]
df_final = df_transformed.select(*expected_columns)

# Append transformed data to the target table
df_final.write.mode("append").insertInto("{}.{}".format(HIVE_DB, TARGET_TABLE))

# Stop Spark session
spark.stop()
