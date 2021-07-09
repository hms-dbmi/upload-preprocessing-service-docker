"""
Utility functions for the dbGaP file upload process
"""
import boto3
import errno
import logging
import os
import uuid

from .aws import get_secret_from_secretes_manager


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


def upload_vcf_archive():
    if not os.path.exists('/scratch/vcf_archive.tar'):
        return

    return_value = True

    # create a unique name for the archive
    upload_file_name = 'vcf_archive_{}.tar'.format(uuid.uuid1())
    os.rename('/scratch/vcf_archive.tar',
              '/scratch/{}'.format(upload_file_name))

    # Do not upload to DbGap if testing
    if secret['status'] == 'test':
        s3_filename = testing_folder + '/' + upload_file_name
        print("[DEBUG] Attempting to copy file " + upload_file_name +
              " to S3 bucket for storage under " + s3_filename + ".", flush=True)
        testing_s3 = boto3.resource('s3')
        testing_s3.meta.client.upload_file(
            '/scratch/' + upload_file_name, testing_bucket, s3_filename)
    else:
        try:
            upload_location = "subasp@upload.ncbi.nlm.nih.gov:uploads/upload_requests/" + \
                aspera_vcf_location_code + "/"
            print("[DEBUG] Attempting to upload file " + upload_file_name +
                  " via Aspera to " + upload_location, flush=True)
            upload_output = check_output(
                ["/home/aspera/.aspera/connect/bin/ascp --file-crypt=encrypt -i /aspera/aspera_vcf.pk /scratch/" + upload_file_name + " " + upload_location], shell=True)
            print(upload_output, flush=True)
        except:
            print("[ERROR] Error sending files via Aspera - ",
                  sys.exc_info()[:2], flush=True)
            return_value = False
            message.change_visibility(VisibilityTimeout=0)

    silent_remove('/scratch/{}'.format(upload_file_name))

    return return_value
