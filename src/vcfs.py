"""
Utilities for processing VCF files
"""
import gzip
import os
import re
import sys
import uuid
from subprocess import check_output
import pysam
from src.archive import tar_and_remove_files
from src.aws_utils import get_s3_client
from src.utilities import write_to_logs

# only these INFO annotations will be retained
WHITELISTED_ANNOTATIONS = {
    'AC', 'AF', 'AN', 'BaseQRankSum', 'ClippingRankSum', 'DP', 'FS', 'GQ_MEAN',
    'GQ_STDDEV', 'InbreedingCoeff', 'MQ', 'MQ0', 'MQRankSum', 'MS', 'NCC',
    'NEGATIVE_TRAIN_SITE', 'P', 'POSITIVE_TRAIN_SITE', 'QD', 'ReadPosRankSum',
    'SOR', 'VQSLOD', 'culprit'
}


def process_header(line, new_ids=None):
    """
    Removes header lines that feature extraneous data (command lines, etc)
    or INFO field annotations which are not whitelisted. Also replaces the
    sample IDs with the sequence of IDs in `new_ids`.
    """
    # extraneous headers
    if line.startswith('##') and not any(
            line.startswith('##' + header_type)
            for header_type in ('INFO', 'FILTER', 'FORMAT', 'ALT', 'contig')
    ):
        return None

    if line.startswith('#CHROM') and new_ids is not None:
        fields = line.strip().split('\t')[:9]  # fixed headers
        fields.extend(new_ids)
        line = '\t'.join(fields) + '\n'

    return line


def is_gzipped(file_path):
    """Check if the file is gzipped by reading its magic number."""
    with open(file_path, 'rb') as f:
        magic_number = f.read(2)
    return magic_number == b'\x1f\x8b'  


def trim_vcf(from_file, to_file, new_id):
    """
    Trims unwanted INFO annotations from a VCF file, including the header.
    Also replaces sample ID.
    """
    try:
        try:
            if is_gzipped(from_file):
                f_input = gzip.open(from_file, 'rt')
            else:
                f_input = open(from_file)
        except IOError as e:
            print(f"Error opening file {from_file}: {e}")
            f_input = None  # Handle the case where the file cannot be opened
    finally:
        with open(to_file, 'w') as f_output:
            for line in f_input:
                if line.startswith('#'):
                    result = process_header(line, (new_id,))
                    if result is not None:
                        f_output.write(result)
                else:
                    f_output.write(line)

        if not f_input.closed:
            f_input.close()
        if not f_output.closed:
            f_output.close()


def process_vcf(sample_id, upload_file_name, temp_file, logger):
    """
    manage the processing of VCF files
    """
    write_to_logs("Step 2 - Processing File: Renaming VCF file to {}".format(upload_file_name))
    os.rename(temp_file, "/scratch/{}.bak".format(upload_file_name))

    try:
        write_to_logs(
            "Step 2 - Processing File: Replacing sample_id and removing extra info for VCF file {}".format(
                upload_file_name))
        trim_vcf('/scratch/{}.bak'.format(upload_file_name), '/scratch/{}'.format(upload_file_name), sample_id)
    except Exception as exc:
        write_to_logs("[ERROR] Step 2 - Processing File: Failed to trim annotations for VCF file {} with error {}".format(
            upload_file_name, exc), logger)
        os.rename('/scratch/{}.bak'.format(upload_file_name), '/scratch/{}'.format(upload_file_name))

        return False

    write_to_logs("Step 2 - Processing File: Compressing and indexing VCF {}".format(upload_file_name))
    pysam.tabix_index('/scratch/{}'.format(upload_file_name), preset='vcf', force=True)

    files_to_tar = ['/scratch/{}.gz'.format(upload_file_name), '/scratch/{}.gz.tbi'.format(upload_file_name)]
    tar_and_remove_files('vcf_archive', '/scratch', files_to_tar, logger)

    return True


def upload_vcf_archive(aspera_vcf_location_code, testing, testing_bucket, testing_folder):
    if not os.path.exists('/scratch/vcf_archive.tar'):
        return

    upload_file_name = 'vcf_archive_{}.tar'.format(uuid.uuid1())
    os.rename('/scratch/vcf_archive.tar', '/scratch/{}'.format(upload_file_name))

    if testing:
        s3_filename = testing_folder + '/' + upload_file_name
        write_to_logs("[TESTING] Step 3 - File Upload: Attempting to copy file {} to S3 bucket for storage under {}".format(
            upload_file_name, s3_filename))
        testing_s3 = get_s3_client()
        testing_s3.meta.client.upload_file(
            '/scratch/' + upload_file_name, testing_bucket, s3_filename)
    else:
        try:
            upload_location = "subasp@upload.ncbi.nlm.nih.gov:uploads/upload_requests/{}/".format(
                aspera_vcf_location_code)
            write_to_logs("Step 3 - File Upload: Attempting to upload file {} via Aspera to {}".format(
                upload_file_name, upload_location))
            upload_output = check_output(
                ["/home/aspera/.aspera/connect/bin/ascp --file-crypt=encrypt -i /aspera/aspera_vcf.pk /scratch/" +
                 upload_file_name + " " + upload_location], shell=True)
            write_to_logs("Step 3 - File Upload: Aspera returned {}", format(upload_output))
        except Exception:
            write_to_logs(
                "[ERROR] Step 3 - File Upload: Failed to send archive file via Aspera with error {}".format(
                    sys.exc_info()[:2]))
