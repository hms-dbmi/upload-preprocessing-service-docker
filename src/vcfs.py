"""
Utilities for processing VCF files
"""
import re
import gzip
import logging
import os
import pysam
import tarfile

from .utilities import upload_vcf_archive

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
    return_continue_and_delete = True

    print("[DEBUG] Renaming File to " + upload_file_name, flush=True)
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

    print("[DEBUG] Compressing and indexing VCF", flush=True)
    pysam.tabix_index('/scratch/{}'.format(upload_file_name), preset='vcf', force=True)

    try:
        archive_size = os.path.getsize('/scratch/vcf_archive.tar')
        print("Current archive size: {}".format(archive_size))
    except OSError:
        archive_size = 0

    vcf_size = os.path.getsize('/scratch/{}.gz'.format(upload_file_name))

    if archive_size + vcf_size > 250*1024**3:  # 250GB
        result = upload_vcf_archive()
        if not result:
            return_continue_and_delete = False

    with tarfile.TarFile('/scratch/vcf_archive.tar', 'a') as archive:
        print("[DEBUG] adding {} to archive".format(upload_file_name))
        for name in ('{}.gz'.format(upload_file_name), '{}.gz.tbi'.format(upload_file_name)):
            archive.add('/scratch/{}'.format(name), arcname=name)
            os.remove('/scratch/{}'.format(name))

    return return_continue_and_delete
