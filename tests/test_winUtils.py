import os
import shutil
import pytest
from cuchillo_de_gaucho import winUtils as wu


def test_create_folder_if_not_exists():
	# Setup: Define a test folder structure
	base_path = "test_dir"
	nested_path = os.path.join(base_path, "subdir1", "subdir2", "subdir3")

	# Ensure the base path is clean before starting
	if os.path.exists(base_path):
		shutil.rmtree(base_path)

	# Check that the nested path does not initially exist
	assert not os.path.exists(nested_path)

	# Call the function to create the nested path
	wu.create_folder_if_not_exists(nested_path)

	# Assert that the nested path now exists
	assert os.path.exists(nested_path)

	# Cleanup: Remove the test directory
	shutil.rmtree(base_path)

