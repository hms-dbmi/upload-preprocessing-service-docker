import re
import gzip

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
            for header_type in (
                'fileformat', 'INFO', 'FILTER', 'FORMAT', 'ALT', 'contig'
            )
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


def trim(from_file, to_file, new_id):
    """
    Trims unwanted INFO annotations from a VCF file, including the header.
    Also replaces sample ID.
    """

    # need to test if the file has been zipped
    with open(from_file) as f_test:
        file_start = f.read(len("\x1f\x8b\x08")) # this should indicate if it's a gz file
    if not f_test.closed:
        f_test.close()
    
    if file_start == '\x1f\x8b\x08':
        f_input = gzip.open(from_file)
    else:
        f_input = open(from_file) 

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
