import os
import subprocess
import logging
import json
import shutil
from . import packageConfig

def run_subprocess(command_list):
	try:
		logging.info(f"Start running subprocess! {command_list}")
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


def create_encrypted_7z(zip_path, files, password=None, sevenzip_path=packageConfig.DEFAULT_SEVENZIP_PATH):
	"""
	Creates a 7z archive with optional password protection using 7-Zip.

	Args:
		zip_path (str): Path to the output 7z archive (e.g., "output.7z").
		files (list): List of file paths to include in the archive.
		password (str, optional): Password for encryption. If None, no password is set.
		sevenzip_path (str, optional): Path to 7z.exe. Defaults to the value provided in the project config.

	Raises:
		FileNotFoundError: If 7-Zip is not installed at the specified path.
		subprocess.CalledProcessError: If the 7z command fails.
		ValueError: If any file does not exist.
	"""

	# Check if 7-Zip exists
	if not os.path.exists(sevenzip_path):
		raise FileNotFoundError(f"Error: 7-Zip not found at {sevenzip_path}. Please install 7-Zip or update the path.")
	# Ensure all files exist
	for file in files:
		if not os.path.exists(file):
			raise ValueError(f"Error: File not found - {file}")
	# Base command
	# Check the extension and adjust the command
	file_extension = os.path.splitext(zip_path)[1].lower()
	if file_extension == '.7z':
		# .7z archive with AES-256 encryption
		command = [sevenzip_path, 'a', zip_path, '-mhe=on'] + files
	elif file_extension == '.zip':
		# .zip archive with AES-256 encryption
		command = [sevenzip_path, 'a', zip_path, '-tzip', '-mem=AES256'] + files
	else:
		raise ValueError(f"Error: Unsupported file extension '{file_extension}'. Use .7z or .zip.")

	# Add password only if provided
	if password:
		command.insert(3, f'-p{password}')  # Insert after 'a'

	# Run the 7-Zip command
	run_subprocess(command)


def read_dict_from_json(json_file: str):
	with open(json_file) as f:
		json_dict = json.load(f)
	return json_dict

## Directory ops
def create_folder_if_not_exists(path: str, exists_ok: bool=True
								):
    """
    Create a folder and any necessary parent folders if they do not exist.

    Args:
    path (str): The path to check and create if necessary.
    exists_ok (bool): If this flag is true, it will also create the non-existing sub folders.
    """
    try:
        os.makedirs(path, exist_ok=exists_ok)
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

if __name__ == '__main__':
    print('joey')
