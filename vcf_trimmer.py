import re

# only these INFO annotations will be retained
WHITELISTED_ANNOTATIONS = {
    'AC', 'AF', 'AN', 'BaseQRankSum', 'ClippingRankSum', 'DP', 'FS', 'GQ_MEAN',
    'GQ_STDDEV', 'InbreedingCoeff', 'MQ', 'MQ0', 'MQRankSum', 'MS', 'NCC',
    'NEGATIVE_TRAIN_SITE', 'P', 'POSITIVE_TRAIN_SITE', 'QD', 'ReadPosRankSum',
    'SOR', 'VQSLOD', 'culprit'
}


def process_header(line):
    """Returns the header line unchanged, unless it's an INFO header for an
    annotation that isn't in the whitelist.
    """

    match = re.match(r'##INFO=<ID=([^,]+)', line)
    if not match:
        return line

    info_name = match.group(1)
    if info_name in WHITELISTED_ANNOTATIONS:
        return line

    return None  # will not be output to file


def process_body(line):
    """Retains only whitelisted INFO annotations in each record."""

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


def trim(from_file, to_file):
    """Trims unwanted INFO annotations from a VCF file, including the header."""

    with open(from_file) as f_input, open(to_file, 'w') as f_output:
        for line in f_input:
            if line.startswith('#'):
                result = process_header(line)
            else:
                result = process_body(line)

            if result is not None:
                f_output.write(result)
