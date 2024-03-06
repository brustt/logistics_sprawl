import os 
from datetime import datetime 

def check_dir(*path):
    os.makedirs(os.path.join(*path), exist_ok=True)
    return os.path.join(*path)


def make_path(file_name, *path):
    return os.path.join(*path, file_name)


def get_year_from_datestring(date_str):
    return str(datetime.strptime(date_str, "%Y-%m-%d").year)