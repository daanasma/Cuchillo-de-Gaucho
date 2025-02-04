import os
import subprocess
import logging
import json
import shutil

def run_subprocess(command_list):
	try:
		logging.info("Start running subprocess!")
		subprocess.run(command_list, check=True)
		logging.info("Finished running subprocess!")
	except subprocess.CalledProcessError as e:
		logging.error(f"Error in running subprocess: {e}")


def write_dict_to_json(file_path: str, dictionary: dict):
	try:
		with open(file_path, 'w', encoding='utf8') as outfile:
			json.dump(dictionary, outfile)
	except Exception as e:
		logging.info(f"Failed writing to file {file_path}. error message: {e}")


def read_dict_from_json(json_file: str):
	with open(json_file) as f:
		json_dict = json.load(f)
	return json_dict

## Directory ops
def create_folder_if_not_exists(path: str):
    """
    Create a folder and any necessary parent folders if they do not exist.

    Args:
    path (str): The path to check and create if necessary.
    """
    try:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Created folder (or already exists): {path}")
    except OSError as e:
        logging.error(f"Error creating directory {path}: {e}")


def find_file_extension(startpath: str, ext: str, wildcard: str = ""):
    """
    Search top down in a folder for the first file with a certain file extension.
    Optionally a wildcard can be specified to limit valid matches

    :param startpath: The directory from which to start the search
    :param ext: The extension for which to search
    :param wildcard: The (part of) filename
    :returns: The full path to the file
    """
    for root, dirs, files in os.walk(startpath):
        for file in files:
            if file.endswith(ext) and wildcard in file:
                return os.path.join(root, file)


def delete_path(path: str):
    """
    If the path is a file, the file is removed. When the path is a folder, the folder and all of its contents are removed

    :param path: The path, representing either a folder or a file
    """
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    else:
        logging.warning("No object found to remove at path %s", path)

