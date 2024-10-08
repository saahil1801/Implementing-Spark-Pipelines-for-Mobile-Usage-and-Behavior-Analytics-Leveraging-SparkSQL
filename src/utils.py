# src/utils.py

import yaml
import logging
import os

def load_config(config_path='configs/config.yaml'):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def setup_logging():
    # Create a logger
    logger = logging.getLogger('my_app')
    logger.setLevel(logging.DEBUG)  # Set the global log level

    # Create a directory for logs if it doesn't exist
    log_dir = 'data/logs'
    os.makedirs(log_dir, exist_ok=True)

    if not logger.hasHandlers():
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Log level for console
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)

        # File handler
        file_handler = logging.FileHandler(os.path.join(log_dir, 'app.log'))
        file_handler.setLevel(logging.DEBUG)  # Log level for file
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # Add handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger