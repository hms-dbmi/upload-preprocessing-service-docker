# -*- coding: utf-8 -*-

"""
Main workflow for sending files to dbGaP
"""
import os
import sys
import time
from subprocess import check_output
import botocore

from aws_utils import get_queue_by_name, get_s3_client, get_secret_from_secrets_manager, write_aspera_secrets_to_disk
from bams import process_bam
from udn_gateway import call_udngateway_mark_complete
from utilities import setup_logger, silent_remove, write_to_logs
from vcfs import process_vcf, upload_vcf_archive
from xml_utils import create_and_tar_xml

LOGGER = setup_logger('ups')

SECRET = get_secret_from_secrets_manager("ups-prod")
write_aspera_secrets_to_disk()

# If testing, do not upload files to dbGaP, but instead save the processed file to a special S3 bucket
if SECRET['status'] == 'test':
    TESTING = True
    TESTING_BUCKET = 'gateway-participant-sequencing-files-' + SECRET['sequencing-bucket']
    TESTING_FOLDER = 'ups-testing'

    print("[DEBUG] TEST mode. All files uploaded to {}".format(TESTING_BUCKET), flush=True)
else:
    TESTING = False
    TESTING_BUCKET = None
    TESTING_FOLDER = None
    ASPERA_LOCATION_CODE = SECRET['aspera-location-code']
    ASPERA_PASS = SECRET['aspera-pass']
    ASPERA_VCF_LOCATION_CODE = SECRET['aspera-location-code-vcf']
    os.environ["ASPERA_SCP_FILEPASS"] = ASPERA_PASS

QUEUE_NAME = 'ups'
SQS_QUEUE = get_queue_by_name(QUEUE_NAME)

S3_CLIENT = get_s3_client()

write_to_logs('Starting to Poll', LOGGER)

while True:
    write_to_logs("Step 1 - File Retrieval: Retrieving messages from queue - '{}'".format(QUEUE_NAME))

    messages = SQS_QUEUE.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=[
        'dna_data', 'exportfile_id', 'file_type', 'file_url', 'fileservice_uuid', 'instrument_model',
        'read_lengths', 'sample_id', 'sequence_type', 'udn_id'])

    write_to_logs("Step 1 - File Retrieval: Found {} messages".format(len(messages)))

    if len(messages) == 0:
        upload_vcf_archive(ASPERA_VCF_LOCATION_CODE, TESTING, TESTING_BUCKET, TESTING_FOLDER)
    else:
        for message in messages:
            CONTINUE_AND_DELETE = True

            if message.message_attributes is not None:
                dna_data = message.message_attributes.get('dna_data').get('StringValue')
                exportfile_id = message.message_attributes.get('exportfile_id').get('StringValue')
                file_type = message.message_attributes.get('file_type').get('StringValue')
                file_url = message.message_attributes.get('file_url').get('StringValue')
                fileservice_uuid = message.message_attributes.get('fileservice_uuid').get('StringValue')
                instrument_model = message.message_attributes.get('instrument_model').get('StringValue')
                read_lengths = message.message_attributes.get('read_lengths').get('StringValue')
                sample_id = message.message_attributes.get('sample_id').get('StringValue')
                sequence_type = int(message.message_attributes.get('sequence_type').get('StringValue'))
                udn_id = message.message_attributes.get('udn_id').get('StringValue')

                (dna_source, reference_genome) = dna_data.split('|')

                if file_type == 'BAM':
                    FILENAME_EXTENSION = '.bam'
                elif file_type == 'VCF':
                    FILENAME_EXTENSION = '.vcf'

                file_url_pieces = file_url.split('/')
                file_bucket = file_url_pieces[2]
                FILE_KEY = '/'.join(file_url_pieces[3:])

                upload_file_name = "%s%s" % (fileservice_uuid, FILENAME_EXTENSION)

                if (dna_source and exportfile_id and file_type and file_url and fileservice_uuid and
                    instrument_model and read_lengths and reference_genome and sample_id and sequence_type and
                        udn_id and file_bucket and FILE_KEY and upload_file_name):
                    write_to_logs(
                        "Step 1 - File Retrieval: Processing file {} for participant {}".format(
                            upload_file_name, udn_id), LOGGER)
                    write_to_logs(
                        "Step 1 - File Retrieval: Downloading file {} from bucket {}".format(FILE_KEY, file_bucket))

                    try:
                        TEMP_FILE = "/scratch/md5"
                        retrieveBucket = S3_CLIENT.Bucket(file_bucket)
                        retrieveBucket.download_file(FILE_KEY, TEMP_FILE)
                    except botocore.exceptions.ClientError as exc:
                        silent_remove(TEMP_FILE)
                        write_to_logs(
                            "[ERROR] Step 1 - File Retrieval: Error retrieving file from S3: {}".format(exc), LOGGER)
                        CONTINUE_AND_DELETE = False
                        message.change_visibility(VisibilityTimeout=0)
                        continue

                    if file_type == "BAM" and CONTINUE_AND_DELETE:
                        try:
                            MD5_CHECKSUM = process_bam(sample_id, upload_file_name, TEMP_FILE, LOGGER)

                            tar_file_name = create_and_tar_xml(
                                dna_source, fileservice_uuid, instrument_model, MD5_CHECKSUM, read_lengths,
                                reference_genome, sample_id, SECRET, sequence_type, upload_file_name, LOGGER)

                            if TESTING:
                                bam_file_path = os.path.join(TESTING_FOLDER, upload_file_name)

                                print("[TESTING] Step 3 - File Upload: Attempting to copy files (BAM and XML tar) for {} to S3 bucket for storage under {}".format(
                                    upload_file_name, TESTING_FOLDER), flush=True)
                                testing_s3 = get_s3_client()
                                testing_s3.meta.client.upload_file(
                                    os.path.join("/scratch", upload_file_name), TESTING_BUCKET, bam_file_path)
                                testing_s3.meta.client.upload_file(
                                    os.path.join("/scratch", tar_file_name), TESTING_BUCKET, tar_file_name)
                            else:
                                try:
                                    write_to_logs(
                                        "Step 3 - File Upload: Attempting to upload file {} via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:{}".format(
                                            upload_file_name, ASPERA_LOCATION_CODE))
                                    upload_output = check_output(
                                        ["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera-v2.pk -Q -l 5000m -k 1 /scratch/" +
                                         upload_file_name + " asp-dbgap@gap-submit.ncbi.nlm.nih.gov:" + ASPERA_LOCATION_CODE],shell=True)
                                    write_to_logs("Step 3 - File Upload: Aspera returned {}", format(upload_output))

                                    write_to_logs(
                                            "Step 3 - File Upload: Attempting to upload file {} via Aspera - asp-dbgap@gap-submit.ncbi.nlm.nih.gov:{}".format(
                                            tar_file_name, ASPERA_LOCATION_CODE))
                                    upload_output = check_output(
                                        ["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera-v2.pk -Q -l 5000m -k 1 " +
                                         tar_file_name + " asp-dbgap@gap-submit.ncbi.nlm.nih.gov:" + ASPERA_LOCATION_CODE], shell=True)
                                    write_to_logs("Step 3 - File Upload: Aspera returned {}", format(upload_output))

                                except Exception:
                                    write_to_logs(
                                        "[ERROR] Step 3 - File Upload: Error sending files via Aspera {}".format(sys.exc_info()[:2]), LOGGER)
                                    message.change_visibility(VisibilityTimeout=0)
                                    CONTINUE_AND_DELETE = False
                        except Exception:
                            write_to_logs("[ERROR] Processing BAM {}".format(sys.exc_info()[:2]), LOGGER)
                            CONTINUE_AND_DELETE = False
                            message.change_visibility(VisibilityTimeout=0)
                            continue
                        finally:
                            silent_remove("/scratch/md5")
                            silent_remove("/scratch/header.sam")
                            silent_remove(tar_file_name)
                    elif file_type == "VCF" and CONTINUE_AND_DELETE:
                        try:
                            CONTINUE_AND_DELETE = process_vcf(sample_id, upload_file_name, TEMP_FILE, LOGGER)

                            try:
                                ARCHIVE_SIZE = os.path.getsize('/scratch/vcf_archive.tar')
                                write_to_logs(
                                    "Step 3 - File Upload: Current archive size: {}".format(ARCHIVE_SIZE))
                            except OSError:
                                ARCHIVE_SIZE = 0
                                write_to_logs(
                                    "Step 3 - File Upload: Current archive size: {}".format(ARCHIVE_SIZE))

                            if ARCHIVE_SIZE > 250*1024**3:  # 250GB
                                write_to_logs("Step 3 - File Upload:")
                                upload_vcf_archive(ASPERA_VCF_LOCATION_CODE, TESTING, TESTING_BUCKET, TESTING_FOLDER)
                        except Exception:
                            write_to_logs("[ERROR] Processing VCF - {}".format(sys.exc_info()[:2]), LOGGER)
                            CONTINUE_AND_DELETE = False
                            message.change_visibility(VisibilityTimeout=0)
                            continue
                        finally:
                            silent_remove("/scratch/md5")
                            silent_remove("/scratch/header.sam")

                    silent_remove(TEMP_FILE)
                    silent_remove("/scratch/" + upload_file_name)

                    if CONTINUE_AND_DELETE:
                        call_udngateway_mark_complete(exportfile_id, SECRET, LOGGER)
                        message.delete()
                    else:
                        message.change_visibility(VisibilityTimeout=0)

                else:
                    write_to_logs(
                        "[ERROR] Step 1 - File Retrieval: Message failed to provide all required attributes {}".format(
                            message))
                    message.change_visibility(VisibilityTimeout=0)
                    continue

    time.sleep(10)
