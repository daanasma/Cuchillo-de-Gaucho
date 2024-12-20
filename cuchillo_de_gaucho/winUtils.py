import os
import subprocess
import logging


def run_subprocess(command_list):
	try:
		logging.info("Start running subprocess!")
		subprocess.run(command_list, check=True)
		logging.info("Finished running subprocess!")
	except subprocess.CalledProcessError as e:
		logging.error(f"Error in running subprocess: {e}")
