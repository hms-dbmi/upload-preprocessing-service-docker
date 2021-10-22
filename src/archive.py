"""
Code to tar files for sending to dbGaP
"""
import os
import tarfile
from src.utilities import silent_remove, write_to_logs


def tar_and_remove_files(tar_file_name, tar_file_path, files_to_tar, logger):
    """
    Tars the XML files
    """
    tar_file_name = os.path.join(tar_file_path, '{}.tar'.format(tar_file_name))
    with tarfile.open(tar_file_name, "a") as tar:
        for name in files_to_tar:
            write_to_logs("Step 2 - Processing File: Adding {} to tar file".format(name))
            try:
                tar.add(name, arcname=name, recursive=False)
                silent_remove(name)
            except Exception as exc:
                error_message = "Step 2 - Processing File: Error adding {} to tar file".format(name)
                write_to_logs(error_message, logger)
                raise Exception(error_message) from exc

    return tar_file_name
