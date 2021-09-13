"""
Utilities for processing BAM files
"""
import hashlib
from logging import error
import os
from subprocess import call, check_output, CalledProcessError
from utilities import write_to_logs


def process_bam(udn_id, file_bucket, file_key, sample_id, upload_file_name, file_type, temp_file, logger):
    bam_headers = list()
    write_to_logs("Step 2 - Processing File: Running samtools on BAM")

    try:
        bam_headers = check_output(['samtools', 'view', '-H', temp_file]).decode('ascii').split('\n')
    except CalledProcessError as exc:
        error_message = "[ERROR] Step 2 - Processing File: Unable to retrieve headers from BAM file {} with error {}".format(
            upload_file_name, exc)
        write_to_logs(error_message, logger)
        raise Exception(error_message)

    output_headers = list()
    for header in bam_headers:
        if header.startswith('@RG'):
            tag_pairs = [item.split(':', 1) for item in header.split('\t')[1:]]  # don't include @RG token

            # sometimes ID field has nontrivial identifiers in it
            output_tags = [('ID', '0')]
            output_tags.extend(
                (tag_name, data) for tag_name, data in tag_pairs if tag_name in ('PL', 'DT', 'CN'))  # retain these
            output_tags.append(('SM', sample_id))  # add in new sample ID
            new_header = '\t'.join(['@RG'] + [':'.join(pair) for pair in output_tags])
            output_headers.append(new_header)
        elif header.startswith('@PG'):
            continue  # remove @PG headers
        else:
            output_headers.append(header)

    with open('/scratch/new_headers.sam', 'w') as f:
        f.write('\n'.join(output_headers))

    with open('/scratch/md5_reheader', 'wb') as f:
        try:
            call(['samtools', 'reheader', '-P', '/scratch/new_headers.sam', temp_file], stdout=f)
        except CalledProcessError as exc:
            error_message = "[ERROR] Step 2 - Processing File: Unable to run samtools reheader command on BAM file {} with error {}".format(
                upload_file_name, exc)
            write_to_logs(error_message, logger)
            raise Exception(error_message)

    os.rename('/scratch/md5_reheader', os.path.join('/scratch', upload_file_name))

    write_to_logs("Step 2 - Processing File: Completed reheader now calling quickcheck")

    quickcheck_result = call('samtools quickcheck -v {} && exit 0 || exit 1'.format(temp_file), shell=True)

    if quickcheck_result == 0:
        write_to_logs("Step 2 - Processing File: Quickcheck completed successfully now attempting MD5")
    else:
        error_message = "[ERROR] Step 2 - Processing File: Quickcheck failed on reheadered BAM {}".format(
            upload_file_name)
        write_to_logs(error_message, logger)
        raise Exception(error_message)

    # bams are usually very big so stream instead of reading it all into memory
    md5_hash = hashlib.md5()

    with open('/scratch/' + upload_file_name, 'rb') as f:
        while True:
            buf = f.read(2**20)

            if not buf:
                break

            md5_hash.update(buf)

    write_to_logs("Step 2 - Processing File: MD5 completed successfully")

    return md5_hash.hexdigest()
