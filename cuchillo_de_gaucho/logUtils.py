import os
import json
import logging.config
import psutil


class RAMLoggingFilter(logging.Filter):
    def filter(self, record):
        # Get available and total RAM
        used_ram_gb = (psutil.virtual_memory().total - psutil.virtual_memory().available) / (1024 ** 3)
        total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)  # in GB
        # Add custom attributes to the log record
        record.used_ram = f"{used_ram_gb:.2f}"
        record.total_ram = f"{total_ram_gb:.2f}"

        return True

def _set_basic_logging(level):
    logging.basicConfig(level=level)
    add_ram_filter()
    logging.info("Logging Config path not found - using basic setup")


def add_ram_filter():
    # Add the RAM filter to the root logger
    logger = logging.getLogger()
    logger.addFilter(RAMLoggingFilter())


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
            add_ram_filter()
            logging.info("Logging Config setup success.")
        else:
            _set_basic_logging(default_level)

            logging.info("No log directory found, using basic setup")

    else:
        _set_basic_logging(default_level)
        logging.info("Logging Config path not found - using basic setup")


def override_stream_log_level(new_level):
    """
    Temporarily override the log level of the StreamHandler (console logger).

    Args:
        new_level (int): The new logging level (e.g., logging.DEBUG).

    Usage:
        override_stream_log_level(logging.DEBUG)
        # Your debug-level code here
        override_stream_log_level(logging.INFO)  # Restore manually if desired
    """
    logger = logging.getLogger()
    original_level = None

    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(new_level)
            logging.info(f"Console log level changed to: {logging.getLevelName(new_level)}")
            break
