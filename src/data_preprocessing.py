# src/preprocessing.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils import setup_logging

def preprocess_data(df: DataFrame) -> DataFrame:
    """
    Preprocess the data:
    - Handle missing values.
    - Rename columns for consistency.
    - Select relevant columns.
    """
    logger = setup_logging()


    gender_null_acc = df.sparkSession.sparkContext.accumulator(0)

    # Define a function to check 'Gender' and increment accumulator
    def count_null_gender(row):
        if row['Gender'] is None:
            gender_null_acc.add(1)
        return row
    
    df.foreach(count_null_gender)

    logger.info(f"Total rows with null 'Gender': {gender_null_acc.value}")

    
    # Drop rows with missing values
    df = df.dropna()
    logger.info("Dropped rows with missing values")
    
    # Rename columns for consistency
    df = df.withColumnRenamed("App Usage Time (min/day)", "AppUsageTime") \
           .withColumnRenamed("Screen On Time (hours/day)", "ScreenOnTime") \
           .withColumnRenamed("Battery Drain (mAh/day)", "BatteryDrain") \
           .withColumnRenamed("Number of Apps Installed", "NumAppsInstalled") \
           .withColumnRenamed("Data Usage (MB/day)", "DataUsageMB") \
           .withColumnRenamed("Operating System", "OS") \
           .withColumnRenamed("User Behavior Class", "label")  # For ML compatibility

    # Select relevant columns
    df = df.select('User ID', 'Device Model', 'OS', 'AppUsageTime', 'ScreenOnTime', 'BatteryDrain', 
                   'NumAppsInstalled', 'DataUsageMB', 'Age', 'Gender', 'label')

    logger.info("Renamed columns and selected relevant features")
    return df
