import logging


def setup_logger():
    logger = logging.getLogger("dataset_transformation")

    # Check if the logger already has handlers attached to it
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)  # Set the logging level globally (can be DEBUG, ERROR, etc.)

        # Create a console handler
        console_handler = logging.StreamHandler()

        # Define the log format (including timestamp, level, and message)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Set the formatter for the console handler
        console_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(console_handler)

        # Disable propagation to parent loggers to avoid duplicate log entries
        logger.propagate = False

    return logger
