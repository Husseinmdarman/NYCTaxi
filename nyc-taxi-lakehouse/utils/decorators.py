import time
from functools import wraps

def run_before(method_name):
    """A decorator to run a method before the decorated method."""
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            pre_method = getattr(self, method_name) # Get the method to run before
            pre_method() # Run the pre-method
            return func(self, *args, **kwargs) # Run the original method
        return wrapper # Return the wrapper function
    return decorator # Return the decorator function

def log_timing(func):
    """A decorator to log the execution time of any pipeline method.
       Assumes the class using it has 'self.logger'
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        start = time.time() # Start the timer
        self.logger.info(f"Starting '{func.__name__}'...") # Log the start of the method

        try:
            result = func(self, *args, **kwargs) # Run the original method
            return result # Return the result of the original method
        finally:
            end = time.time() # End the timer
            elapsed = round(end - start, 4) # Calculate elapsed time
            self.logger.info(f"Finished '{func.__name__}' in {elapsed} seconds.") # Log the end of the method and elapsed time
    return wrapper # Return the wrapper function
        