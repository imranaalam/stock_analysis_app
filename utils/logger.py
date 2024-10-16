# utils/logger.py

import logging
import os

def setup_logging(log_file='app.log'):
    """
    Configures logging to output to both console and a file located at the project root.
    
    Args:
        log_file (str): Name of the log file.
    """
    # Determine the absolute path to the project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_path = os.path.join(project_root, log_file)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Create console handler and set level to INFO
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        c_handler.setFormatter(formatter)

        # Create file handler and set level to INFO
        f_handler = logging.FileHandler(log_path)
        f_handler.setLevel(logging.INFO)
        f_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
