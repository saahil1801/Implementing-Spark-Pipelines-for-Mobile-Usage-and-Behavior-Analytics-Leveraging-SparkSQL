# src/model_training.py

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import DataFrame
from src.utils import load_config, setup_logging

def train_model(df: DataFrame):
    """
    Train a classification model using Logistic Regression.
    """
    logger = setup_logging()
    config = load_config()

    # Split the data into training and test sets
    train_df, test_df = df.randomSplit([0.75, 0.25], seed=42)
    logger.info("Split data into training and test sets")

    # Initialize Logistic Regression
    lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)

    # Train the model
    model = lr.fit(train_df)
    logger.info("Trained Logistic Regression model")

    # Save the trained model
    model_path = config['model']['model_path']
    model.write().overwrite().save(model_path)
    logger.info(f"Saved model to {model_path}")

    return model, test_df
