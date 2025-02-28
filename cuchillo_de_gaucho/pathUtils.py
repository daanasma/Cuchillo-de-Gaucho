import os
import logging
def create_folder_if_not_exists(path: str):
    """
    Create a folder / folders if these folders do not exist in a given path

    :param path: The path to check
    """
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info("Created folder: {}".format(path))


def list_files_with_stringmatch(directory, prefix='', middle='', extension=''):
    """
    List all files in the specified directory that start with a given prefix, contain a specific substring,
    and have a specific extension.

    :param directory: Path to the directory to search
    :param prefix: The prefix the filename should start with
    :param middle: A substring that should be present in the filename
    :param extension: The file extension to filter by (e.g., ".txt")
    :return: A list of matching filenames
    """
    if not os.path.isdir(directory):
        raise ValueError(f"Invalid directory: {directory}")

    return [f for f in os.listdir(directory) if f.startswith(prefix) and middle in f and f.endswith(extension)]

