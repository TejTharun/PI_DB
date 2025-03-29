import os


def get_base_directory():
    curr_directory_path = os.getcwd().split('/')
    curr_directory_path.pop()
    curr_directory_path = '/'.join(curr_directory_path)
    return curr_directory_path


BASE_PATH = get_base_directory()
