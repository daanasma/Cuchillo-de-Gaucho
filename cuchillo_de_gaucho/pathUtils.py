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
