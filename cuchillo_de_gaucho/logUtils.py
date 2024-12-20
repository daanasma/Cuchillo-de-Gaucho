import os
import json
import logging.config

def _set_basic_logging(level):
    logging.basicConfig(level=level)
    logging.info("Logging Config path not found - using basic setup")


def setup_logging(default_level=logging.INFO, env_key="LOG_CONFIG"):
    """
    Setup logging configuration
    """

    path = os.getenv(env_key, None)

    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        log_dir = config.get('log_directory')
        if log_dir:
            config["handlers"]["info_file_handler"]["filename"] = os.path.join(
                log_dir, config["handlers"]["info_file_handler"]["filename"]
            )
            config["handlers"]["error_file_handler"]["filename"] = os.path.join(
                log_dir, config["handlers"]["error_file_handler"]["filename"]
            )

            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            logging.config.dictConfig(config)
            logging.info("Logging Config setup success.")
        else:
            _set_basic_logging(default_level)
            logging.info("No log directory found, using basic setup")

    else:
        _set_basic_logging(default_level)
        logging.info("Logging Config path not found - using basic setup")


