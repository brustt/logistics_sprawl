import os 
from datetime import datetime 
from functools import wraps
import time


def check_dir(*path):
    os.makedirs(os.path.join(*path), exist_ok=True)
    return os.path.join(*path)


def make_path(file_name, *path):
    return os.path.join(*path, file_name)


def get_year_from_datestring(date_str):
    return str(datetime.strptime(date_str, "%Y-%m-%d").year)

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper