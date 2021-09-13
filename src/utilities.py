"""
Utility functions for the dbGaP file upload process
"""
import errno
import logging
import os


def setup_logger(name):
    """
    Returns a logger
    """
    log_path = '/scratch/log/{}.log'.format(name)
    formatter = logging.Formatter('%(asctime)s, %(name)s, %(levelname)s, %(message)s')

    if not os.path.exists('/scratch/log'):
        os.mkdir('/scratch/log')

    if not os.path.exists(log_path):
        os.mknod(log_path)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    return logger


def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def write_to_logs(message, logger=None):
    """
    Uses print statement to write message to CloudWatch log and optionally
    writes message to the logger if provided
    """
    print(message, flush=True)

    if logger:
        logger.debug(message)
