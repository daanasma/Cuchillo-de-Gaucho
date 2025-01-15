import logging
from timeit import default_timer as timer
import logging
def time_function(func):
    def wrapper(*args, **kwargs):
        t1 = timer()
        result = func(*args, **kwargs)
        t2 = timer()
        logging.info(f'{func.__name__}() executed in {(t2-t1):.6f}s')
        return result
    return wrapper
