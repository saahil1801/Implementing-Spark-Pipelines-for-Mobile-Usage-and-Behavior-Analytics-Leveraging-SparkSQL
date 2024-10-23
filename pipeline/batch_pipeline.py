# pipelines/batch_pipeline.py

from pyspark.sql import SparkSession
from src.data_ingestion import load_batch_data
from src.data_preprocessing import preprocess_data
from src.feature_engineering import feature_engineering
from src.model_training import train_model
from src.model_evaluation import evaluate_model
from src.utils import load_config, setup_logging
from src.spark_sql_queries import run_complex_query

logger=setup_logging()

def run_batch_pipeline():
    config = load_config()
    spark = SparkSession.builder \
    .appName(config['spark']['app_name']) \
    .master(config['spark']['master']) \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "2") \
    .config("spark.driver.cores", "2") \
    .config("spark.executor.memoryOverhead", "512m") \
    .getOrCreate()
    
    try:
        logger.info("Starting data ingestion")
        df = load_batch_data(spark)
        
        logger.info("Starting data preprocessing")
        # df_preprocessed = preprocess_data(df)
        df_preprocessed = preprocess_data(df).cache()
        logger.info("Cached preprocessed data")

        run_complex_query(df_preprocessed, spark)

        logger.info("Starting feature engineering")
        # df_featurized = feature_engineering(df_preprocessed)
        df_featurized = feature_engineering(df_preprocessed).cache()
        logger.info("Cached featurized data")
        
        logger.info("Starting model training")
        model, test_df = train_model(df_featurized)
        
        df_featurized.unpersist()
        df_preprocessed.unpersist()

        logger.info("UnCached Both the data")
        
        logger.info("Starting model evaluation")
        accuracy, f1_score = evaluate_model(model, test_df)
        logger.info(f"Final Model Accuracy: {accuracy}")
        logger.info(f"Final Model F1 Score: {f1_score}")
        
        
                
    except Exception as e:
        print(f"An error occurred during the batch pipeline: {e}")
    finally:
        spark.stop()
        logger.info("Process completed successfully")
