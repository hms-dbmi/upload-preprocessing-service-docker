"""
Utilities for processing VCF files
"""
import gzip
import logging
import os
import pysam
import re
import sys
import tarfile
import uuid
from subprocess import check_output
from aws_utils import get_s3_client

LOGGER = logging.getLogger('ups')

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

    # non-whitelisted annotations
    match = re.match(r'##INFO=<ID=([^,]+)', line)
    if match:
        info_name = match.group(1)
        if info_name not in WHITELISTED_ANNOTATIONS:
            return None

    # update sample IDs
    if line.startswith('#CHROM') and new_ids is not None:
        fields = line.strip().split('\t')[:9]  # fixed headers
        fields.extend(new_ids)
        line = '\t'.join(fields) + '\n'

    return line


def process_body(line):
    """
    Retains only whitelisted INFO annotations in each record.
    """

    fields = line.split('\t')  # preserves newline
    infos = fields[7].split(';')

    whitelisted = [
        info for info in infos
        if any(
            info.startswith(x + '=') or info == x
            for x in WHITELISTED_ANNOTATIONS
        )
    ]

    fields[7] = ';'.join(whitelisted)
    return '\t'.join(fields)


def trim_vcf(from_file, to_file, new_id):
    """
    Trims unwanted INFO annotations from a VCF file, including the header.
    Also replaces sample ID.
    """
    LOGGER.debug('vcf_trimmer starting trim on {}'.format(from_file))

    # need to test if the file has been zipped
    # this is a bad way to do it but only solution found in python3
    try:
        f_input = open(from_file)
        file_start = f_input.readline()
    except UnicodeDecodeError:
        LOGGER.debug('Unicode error')
        f_input = gzip.open(from_file, 'rt')
        file_start = f_input.readline()
    finally:
        LOGGER.debug('file start is: {}'.format(file_start))
        LOGGER.debug('starting read of {}'.format(from_file))

        with open(to_file, 'w') as f_output:
            for line in f_input:
                if line.startswith('#'):
                    result = process_header(line, (new_id,))
                else:
                    result = process_body(line)

                if result is not None:
                    f_output.write(result)

        if not f_input.closed:
            f_input.close()
        if not f_output.closed:
            f_output.close()


def process_vcf(udn_id, file_bucket, file_key, sample_id, upload_file_name, file_type, temp_file):
    """
    manage the processing of VCF files
    """
    print("[DEBUG] Renaming File to {}".format(upload_file_name), flush=True)
    os.rename(temp_file, "/scratch/{}.bak".format(upload_file_name))

    print("[DEBUG] Replacing sample_id and removing extra VCF info", flush=True)
    LOGGER.debug('process_vcf - upload_file_name: {}'.format(upload_file_name))

    try:
        trim_vcf('/scratch/{}.bak'.format(upload_file_name), '/scratch/{}'.format(upload_file_name), sample_id)
    except Exception as exc:
        print("[DEBUG] Error trimming VCF annotations. {}|{}|{}|{}|{}|{}".format(
            udn_id, file_bucket, file_key, sample_id, upload_file_name, file_type), flush=True)

        LOGGER.debug('process_vcf failed trim - {}'.format(exc))
        os.rename('/scratch/{}.bak'.format(upload_file_name), '/scratch/{}'.format(upload_file_name))

        return False

    print("[DEBUG] Compressing and indexing VCF", flush=True)
    pysam.tabix_index('/scratch/{}'.format(upload_file_name), preset='vcf', force=True)

    with tarfile.TarFile('/scratch/vcf_archive.tar', 'a') as archive:
        print("[DEBUG] adding {} to archive".format(upload_file_name))
        for name in ('{}.gz'.format(upload_file_name), '{}.gz.tbi'.format(upload_file_name)):
            archive.add('/scratch/{}'.format(name), arcname=name)
            os.remove('/scratch/{}'.format(name))

    return True


def upload_vcf_archive(aspera_vcf_location_code, testing=False, testing_bucket=None, testing_folder=None):
    if not os.path.exists('/scratch/vcf_archive.tar'):
        return

    upload_file_name = 'vcf_archive_{}.tar'.format(uuid.uuid1())
    os.rename('/scratch/vcf_archive.tar', '/scratch/{}'.format(upload_file_name))

    # Do not upload to DbGap if testing
    if testing:
        s3_filename = testing_folder + '/' + upload_file_name
        print("[DEBUG] Attempting to copy file " + upload_file_name +
              " to S3 bucket for storage under " + s3_filename + ".", flush=True)
        testing_s3 = get_s3_client()
        testing_s3.meta.client.upload_file(
            '/scratch/' + upload_file_name, testing_bucket, s3_filename)
    else:
        try:
            upload_location = "subasp@upload.ncbi.nlm.nih.gov:uploads/upload_requests/{}/".format(
                aspera_vcf_location_code)
            print("[DEBUG] Attempting to upload file {} via Aspera to {}".format(
                upload_file_name, upload_location), flush=True)
            upload_output = check_output(
                ["/home/aspera/.aspera/connect/bin/ascp --file-crypt=encrypt -i /aspera/aspera_vcf.pk /scratch/" + upload_file_name + " " + upload_location], shell=True)
            print(upload_output, flush=True)
        except:
            print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
