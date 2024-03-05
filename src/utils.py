import os 


def check_dir(*path):
    os.makedirs(os.path.join(*path), exist_ok=True)
    return os.path.join(*path)


def make_path(file_name, *path):
    return os.path.join(*path, file_name)