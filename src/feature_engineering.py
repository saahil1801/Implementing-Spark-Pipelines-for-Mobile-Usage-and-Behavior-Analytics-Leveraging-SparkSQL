# src/feature_engineering.py

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from src.utils import setup_logging
from pyspark.sql.functions import avg, col


def feature_engineering(df: DataFrame) -> DataFrame:
    """
    Perform feature engineering:
    - Index and encode categorical features.
    - Assemble features.
    - Scale features.
    """
    logger = setup_logging()

    # Index categorical columns
    gender_indexer = StringIndexer(inputCol='Gender', outputCol='GenderIndex')
    os_indexer = StringIndexer(inputCol='OS', outputCol='OSIndex')
    device_model_indexer = StringIndexer(inputCol='Device Model', outputCol='DeviceModelIndex')


    window_spec = Window.partitionBy("Age")
    df = df.withColumn("AvgAppUsageByAge", avg("AppUsageTime").over(window_spec))

    # One-Hot Encode the indexed categorical columns
    encoder = OneHotEncoder(inputCols=['GenderIndex', 'OSIndex', 'DeviceModelIndex'], 
                            outputCols=['GenderVec', 'OSVec', 'DeviceModelVec'])

    # Assemble features
    assembler = VectorAssembler(
        inputCols=['AppUsageTime', 'ScreenOnTime', 'BatteryDrain', 'NumAppsInstalled', 'DataUsageMB', 'Age',
                   'AvgAppUsageByAge', 'GenderVec', 'OSVec', 'DeviceModelVec'],
        outputCol='unscaledFeatures'
    )

    # Scale features
    scaler = StandardScaler(inputCol='unscaledFeatures', outputCol='features')

    # Create a Pipeline
    pipeline = Pipeline(stages=[gender_indexer, os_indexer, device_model_indexer, encoder, assembler, scaler])

    # Fit and transform the data
    pipeline_model = pipeline.fit(df)
    df_featurized = pipeline_model.transform(df)

    # Select only the features and label columns
    df_featurized = df_featurized.select('features', 'label')
    logger.info("Completed feature engineering")
    return df_featurized
