from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from src.utils import setup_logging

def run_complex_query(df: DataFrame, spark: SparkSession):
    # Register DataFrame as a temporary view
    logger = setup_logging()

    df.createOrReplaceTempView("user_behavior")

    # Run SQL query
    result_df = spark.sql("""
        SELECT OS, AVG(AppUsageTime) as AvgAppUsage
        FROM user_behavior
        GROUP BY OS
        ORDER BY AvgAppUsage DESC
    """)

    # Log the results row by row
    logger.info("Query result for average app usage per OS:")
    for row in result_df.collect():
        logger.info(f"OS: {row['OS']}, AvgAppUsage: {row['AvgAppUsage']:.2f}")

    # Optionally show the DataFrame in the console
    result_df.show()