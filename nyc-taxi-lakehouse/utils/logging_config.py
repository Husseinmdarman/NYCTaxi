import logging

def get_logger(name) -> logging.Logger:
    """"
    Minimal logging configuration for the NYC Taxi Lakehouse project.
    Pratically for the pipeline.
    Args:
        name (str): Name of the logger, typically __name__.
        in this case it would be either bronze,silver or gold.
    Returns:
        logging.Logger: Configured logger instance.
    """

    logger = logging.getLogger(name) # Create a logger with the specified name

    if not logger.hasHandlers(): # Check if the logger already has handlers to avoid duplicate logs
        logger.setLevel(logging.INFO) # Set the logging level to INFO

        handler = logging.StreamHandler() # Create a stream handler to output logs to the console
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s') # Create a formatter for the log messages
        handler.setFormatter(formatter) # Set the formatter for the handler
        logger.addHandler(handler) # Add the handler to the logger

    return logger