# src/model_evaluation.py

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from src.utils import setup_logging

def evaluate_model(model, test_df: DataFrame):
    """
    Evaluate the trained model on the test data.
    """
    logger = setup_logging()

    # Make predictions
    predictions = model.transform(test_df)

    # Evaluate accuracy
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)
    logger.info(f"Model Accuracy: {accuracy}")

    # Evaluate F1 score
    f1_evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='f1')
    f1_score = f1_evaluator.evaluate(predictions)
    logger.info(f"Model F1 Score: {f1_score}")

    return accuracy, f1_score
