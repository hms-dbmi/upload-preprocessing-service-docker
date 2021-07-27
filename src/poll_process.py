import os
import sys
import botocore
import time
from subprocess import check_output

from .aws import get_queue_by_name, get_s3_client, get_secret_from_secretes_manager, write_aspera_secrets_to_disk
from .bams import process_bam
from .udn_gateway import call_udngateway_mark_complete
from .utilities import setup_logger, silent_remove
from .vcfs import process_vcf, upload_vcf_archive
from .xml_utils import create_and_tar_xml

LOGGER = setup_logger('ups')
AUDIT_LOGGER = setup_logger('file_audit')

SECRET = get_secret_from_secretes_manager("ups-prod")
write_aspera_secrets_to_disk()

# If testing, do not upload files to dbGaP, but instead save the processed file to a special S3 bucket
if SECRET['status'] == 'test':
    TESTING = True
    TESTING_BUCKET = 'udn-files-test'
    TESTING_FOLDER = 'ups-testing'

    print("[DEBUG] TEST mode. All files uploaded to {}".format(TESTING_BUCKET), flush=True)
else:
    TESTING = False
    ASPERA_LOCATION_CODE = SECRET['aspera-location-code']
    ASPERA_PASS = SECRET['aspera-pass']
    ASPERA_VCF_LOCATION_CODE = SECRET['aspera-location-code-vcf']
    os.environ["ASPERA_SCP_FILEPASS"] = ASPERA_PASS

QUEUE_NAME = 'upload-preprocessing'
SQS_QUEUE = get_queue_by_name(QUEUE_NAME)

S3_CLIENT = get_s3_client()

LOGGER.debug('starting to poll')

while True:
    print("Retrieving messages from queue - '{}'".format(QUEUE_NAME), flush=True)

    messages = SQS_QUEUE.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=[
        'dna_source', 'exportfile_id', 'file_type', 'file_url', 'fileservice_uuid', 'instrument_model',
        'read_lengths', 'sample_id', 'sequence_type', 'udn_id'])

    print("[DEBUG] found {} messages".format(len(messages)))

    if len(messages) == 0:
        upload_vcf_archive(ASPERA_VCF_LOCATION_CODE, TESTING, TESTING_BUCKET, TESTING_FOLDER)
    else:
        for message in messages:
            continue_and_delete = True

            if message.message_attributes is not None:
                dna_source = message.message_attributes.get('dna_source').get('StringValue')
                exportfile_id = message.message_attributes.get('exportfile_id').get('StringValue')
                file_type = message.message_attributes.get('file_type').get('StringValue')
                file_url = message.message_attributes.get('file_url').get('StringValue')
                fileservice_uuid = message.message_attributes.get('fileservice_uuid').get('StringValue')
                instrument_model = message.message_attributes.get('instrument_model').get('StringValue')
                read_lengths = message.message_attributes.get('read_lengths').get('StringValue')
                sample_id = message.message_attributes.get('sample_id').get('StringValue')
                sequence_type = message.message_attributes.get('sequence_type').get('StringValue')
                udn_id = message.message_attributes.get('udn_id').get('StringValue')

                if file_type == 'BAM':
                    filename_extension = '.bam'
                elif file_type == 'VCF':
                    filename_extension = '.vcf'

                file_url_pieces = file_url.split('/')
                file_bucket = file_url_pieces[2]
                file_key = '/'.join(file_url_pieces[3:])

                upload_file_name = "%s%s" % (fileservice_uuid, filename_extension)
                LOGGER.debug('upload_filename: {}'.format(upload_file_name))

                if (dna_source and exportfile_id and file_type and file_url and fileservice_uuid and
                        instrument_model and read_lengths and sample_id and sequence_type and udn_id and
                        file_bucket and file_key and upload_file_name):
                    print("[DEBUG] Processing UDN_ID - {}.".format(udn_id), flush=True)
                    print("[DEBUG] Downloading file. Bucket - {} key - {}".format(file_bucket, file_key), flush=True)

                    try:
                        temp_file = "/scratch/md5"
                        retrieveBucket = S3_CLIENT.Bucket(file_bucket)
                        retrieveBucket.download_file(file_key, temp_file)
                    except botocore.exceptions.ClientError as e:
                        silent_remove(temp_file)
                        print("[ERROR] Error retrieving file from S3 - %s" % e, flush=True)
                        continue_and_delete = False
                        message.change_visibility(VisibilityTimeout=0)
                        continue

                    if file_type == "BAM" and continue_and_delete:
                        print("[DEBUG] Processing BAM with samtools.", flush=True)
                        try:
                            md5_checksum = process_bam(
                                udn_id, file_bucket, file_key, sample_id, upload_file_name, file_type, temp_file)

                            tar_file_name = create_and_tar_xml(
                                dna_source, fileservice_uuid, instrument_model, md5_checksum, read_lengths, sample_id, sequence_type, upload_file_name)

                            if TESTING:
                                bam_file_path = os.path.join(TESTING_FOLDER, upload_file_name)
                                xml_tar_path = os.path.join(TESTING_FOLDER, tar_file_name)

                                print("[DEBUG] Attempting to copy files (bam and xml tar) for {} to S3 bucket for storage under {}".format(
                                    upload_file_name, TESTING_FOLDER), flush=True)
                                testing_s3 = get_s3_client()
                                testing_s3.meta.client.upload_file(
                                    os.path.join("/scratch", upload_file_name), TESTING_BUCKET, bam_file_path)
                                testing_s3.meta.client.upload_file(
                                    os.path.join("/scratch", tar_file_name), TESTING_BUCKET, tar_file_name)
                            else:
                                try:
                                    print("[DEBUG] Attempting to upload file {} via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:{}".format(
                                        upload_file_name, ASPERA_LOCATION_CODE), flush=True)
                                    upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 5000m -k 1 /scratch/" +
                                                                  upload_file_name + " asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + ASPERA_LOCATION_CODE], shell=True)
                                    print(upload_output, flush=True)

                                    print("[DEBUG] Attempting to upload file {} via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:{}".format(
                                        tar_file_name, ASPERA_LOCATION_CODE), flush=True)
                                    upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 5000m -k 1 " +
                                                                  tar_file_name + " asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + ASPERA_LOCATION_CODE], shell=True)
                                    print(upload_output, flush=True)
                                except:
                                    print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
                                    message.change_visibility(VisibilityTimeout=0)
                                    return_continue_and_delete = False
                        except:
                            print("Error processing BAM - ", sys.exc_info()[:2], flush=True)
                            continue_and_delete = False
                            message.change_visibility(VisibilityTimeout=0)
                            continue
                        finally:
                            silent_remove("/scratch/md5")
                            silent_remove("/scratch/header.sam")
                            silent_remove(tar_file_name)
                    elif file_type == "VCF" and continue_and_delete:
                        try:
                            LOGGER.debug('starting to process vcf')
                            continue_and_delete = process_vcf(
                                udn_id, file_bucket, file_key, sample_id, upload_file_name, file_type, temp_file)

                            try:
                                archive_size = os.path.getsize('/scratch/vcf_archive.tar')
                                print("Current archive size: {}".format(archive_size))
                            except OSError:
                                archive_size = 0

                            if archive_size > 250*1024**3:  # 250GB
                                upload_vcf_archive(ASPERA_VCF_LOCATION_CODE, TESTING, TESTING_BUCKET, TESTING_FOLDER)
                        except:
                            print("[ERROR] Error processing VCF - {}".format(sys.exc_info()[:2]), flush=True)
                            continue_and_delete = False
                            message.change_visibility(VisibilityTimeout=0)
                            continue
                        finally:
                            silent_remove("/scratch/md5")
                            silent_remove("/scratch/header.sam")

                    silent_remove(temp_file)
                    silent_remove("/scratch/" + upload_file_name)

                    if continue_and_delete:
                        call_udngateway_mark_complete(exportfile_id, SECRET, AUDIT_LOGGER)

                        print("[COMPLETE] {}|{}|{}|{}|{}|{}".format(
                            udn_id, file_bucket, file_key, sample_id, upload_file_name, file_type), flush=True)
                        AUDIT_LOGGER.debug(file_key)
                        message.delete()
                    else:
                        message.change_visibility(VisibilityTimeout=0)

                else:
                    print("[ERROR] Message failed to provide all required attributes.", flush=True)
                    print(message)
                    message.change_visibility(VisibilityTimeout=0)
                    continue

    time.sleep(10)
