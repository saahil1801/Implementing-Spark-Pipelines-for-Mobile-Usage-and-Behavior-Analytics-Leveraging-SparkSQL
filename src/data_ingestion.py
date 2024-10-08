# src/data_ingestion.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.utils import load_config, setup_logging

def load_batch_data(spark: SparkSession):
    """
    Load batch data from a CSV file into a Spark DataFrame.
    """
    logger = setup_logging()
    config = load_config()
    data_path = config['data']['batch_data_path']

    # Define the schema based on the dataset
    schema = StructType([
        StructField("User ID", IntegerType(), True),
        StructField("Device Model", StringType(), True),
        StructField("Operating System", StringType(), True),
        StructField("App Usage Time (min/day)", IntegerType(), True),
        StructField("Screen On Time (hours/day)", DoubleType(), True),
        StructField("Battery Drain (mAh/day)", IntegerType(), True),
        StructField("Number of Apps Installed", IntegerType(), True),
        StructField("Data Usage (MB/day)", IntegerType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("User Behavior Class", IntegerType(), True)
    ])

    # Read CSV file into DataFrame
    df = spark.read.csv(data_path, header=True, schema=schema)
    logger.info(f"Loaded batch data from {data_path}")
    return df

# def load_streaming_data(spark: SparkSession):
#     """
#     Load streaming data from Kafka.
#     """
#     logger = setup_logging()

#     # Define schema
#     schema = StructType([
#         StructField("User ID", IntegerType(), True),
#         StructField("Device Model", StringType(), True),
#         StructField("Operating System", StringType(), True),
#         StructField("App Usage Time (min/day)", IntegerType(), True),
#         StructField("Screen On Time (hours/day)", DoubleType(), True),
#         StructField("Battery Drain (mAh/day)", IntegerType(), True),
#         StructField("Number of Apps Installed", IntegerType(), True),
#         StructField("Data Usage (MB/day)", IntegerType(), True),
#         StructField("Age", IntegerType(), True),
#         StructField("Gender", StringType(), True),
#         StructField("User Behavior Class", IntegerType(), True)
#     ])

#     # Read from Kafka topic
#     df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "customer_topic") \
#         .option("startingOffsets", "latest") \
#         .load()

#     # Convert Kafka message to JSON and parse it using the schema
#     df = df.selectExpr("CAST(value AS STRING)") \
#            .select(from_json(col("value"), schema).alias("jsonData")) \
#            .select("jsonData.*")

#     logger.info("Loaded streaming data from Kafka")
#     return df