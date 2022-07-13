"""
Utility functions for the dbGaP file upload process
"""
import boto3
import botocore
import codecs
import errno
import gzip
import hashlib
import json
import logging
import os
import pysam
import re
import requests
import sys
import tarfile
import uuid
from lxml import etree
from subprocess import call, check_output, CalledProcessError

ALIGNMENT_SOFTWARE = {
    2: 'BWA v0.6.2',
    3: 'BWA-mem v0.7.12',
    4: 'STAR'
}

DESIGN_DESC = {
    2: 'The exome capture utilizes the NimbleGen liquid capture on HGSC VCRome 2.1 that targets approximately 34 Mbp of genomic DNA including all coding exons of currently known disease genes (OMIM, HGMD, and GeneTests). To enhance the coverage of clinically relevant disease genes, the spike-in probe set (Exome 3 - PKV2) is used in 1:1.25 equimolar ratio with the VCRome exome capture design',
    3: 'DNA was sonicated to a specific fragment size and prepared as a paired-end library with ligation of Illumina-flowcell specific adapter sequences and a unique barcode. Prepared library was then quality checked for adequate yield through fluorescent methods and quantitative PCR, as well as accurate library size and profile using bioanalysis.',
    4: 'RNA-Seq consists of isolating RNA, converting it to complementary DNA (cDNA), enriching for polyadenylated transcripts or ribo-depletion to remove ribosomal RNAs, preparing the sequencing library and sequencing on an NGS platform.'
}

SEQUENCING_SOURCE = {
    2: 'GENOMIC',
    3: 'GENOMIC',
    4: 'TRANSCRIPTOMIC'
}

SEQUENCING_TYPE = {
    2: 'WXS',
    3: 'WGS',
    4: 'RNA-Seq'
}

XML_ACTIONS = [
    {'source': 'experiment.xml', 'schema': 'experiment'},
    {'source': 'run.xml', 'schema': 'run'}
]

XML_CONTACTS = [
    {'name': 'Cecilia Esteves', 'email': 'cecilia_esteves@hms.harvard.edu'},
    {'name': 'John Carmichael', 'email': 'john_carmichael@hms.harvard.edu'},
]

# only these INFO annotations will be retained
WHITELISTED_ANNOTATIONS = {
    'AC', 'AF', 'AN', 'BaseQRankSum', 'ClippingRankSum', 'DP', 'FS', 'GQ_MEAN',
    'GQ_STDDEV', 'InbreedingCoeff', 'MQ', 'MQ0', 'MQRankSum', 'MS', 'NCC',
    'NEGATIVE_TRAIN_SITE', 'P', 'POSITIVE_TRAIN_SITE', 'QD', 'ReadPosRankSum',
    'SOR', 'VQSLOD', 'culprit'
}

def get_secret_from_secrets_manager(secrets_client, secret_id):
    """
    Returns the secret string from Secrets Manager

    If unable to retrieve the secret it kills the running process
    """
    try:
        secret_response = secrets_client.get_secret_value(SecretId=secret_id)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_id + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        print('Fatal error. Stopping program.')
        sys.exit()
    else:
        if 'SecretString' in secret_response:
            return json.loads(secret_response['SecretString'])
        elif 'SecretBinary' in secret_response:
            return secret_response['SecretBinary']
        else:
            print('Fatal error. Unexpected secret type.')
            sys.exit()


def write_aspera_secrets_to_disk(secrets_client):
    """
    Fetches Aspera secrets from Secrets Manager and writes them to disk
    """
    aspera_key_secret = get_secret_from_secrets_manager(secrets_client, 'ups-prod-aspera-key')
    aspera_file = open("/aspera/aspera.pk", "wb")
    aspera_file.write(aspera_key_secret)
    aspera_file.flush()
    aspera_file.close()

    aspera_vcf_key_secret = get_secret_from_secrets_manager(secrets_client, 'ups-prod-aspera-vcf-key')
    aspera_vcf_file = open("/aspera/aspera_vcf.pk", "wb")
    aspera_vcf_file.write(aspera_vcf_key_secret)
    aspera_vcf_file.flush()
    aspera_vcf_file.close()

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
    """
    Removes file if it exists
    """
    try:
        os.remove(filename)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def write_to_logs(message, logger=None):
    """
    Uses print statement to write message to CloudWatch log and optionally
    writes message to the logger if provided
    """
    print(message, flush=True)

    if logger:
        logger.debug(message)

def process_bam(sample_id, upload_file_name, temp_file, logger):
    """
    Process the BAM - clean up headers and MD5
    """
    bam_headers = list()
    write_to_logs("Step 2 - Processing File: Running samtools on BAM")

    try:
        bam_headers = check_output(['samtools', 'view', '-H', temp_file]).decode('ascii').split('\n')
    except CalledProcessError as exc:
        error_message = "[ERROR] Step 2 - Processing File: Unable to retrieve headers from BAM file {} with error {}".format(
            upload_file_name, exc)
        write_to_logs(error_message, logger)
        raise Exception(error_message) from exc

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

    with open('/scratch/new_headers.sam', 'w') as new_headers:
        new_headers.write('\n'.join(output_headers))

    with open('/scratch/md5_reheader', 'wb') as reheader:
        try:
            call(['samtools', 'reheader', '-P', '/scratch/new_headers.sam', temp_file], stdout=reheader)
        except CalledProcessError as exc:
            error_message = "[ERROR] Step 2 - Processing File: Unable to run samtools reheader command on BAM file {} with error {}".format(
                upload_file_name, exc)
            write_to_logs(error_message, logger)
            raise Exception(error_message) from exc

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

    md5_hash = hashlib.md5()

    with open('/scratch/' + upload_file_name, 'rb') as upload_file:
        while True:
            buf = upload_file.read(2**20)

            if not buf:
                break

            md5_hash.update(buf)

    write_to_logs("Step 2 - Processing File: MD5 completed successfully")

    return md5_hash.hexdigest()

def call_udngateway_mark_complete(file_id, secret, logger):
    """
    Call the UDN Gateway API to mark the file as complete
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token {token}'.format(token=secret['udn_api_token'])
        }

        url = '{}/api/superadmin/dbgap/exported_files/{}/complete'.format(secret['udn_api_url'], file_id)

        resp = requests.post(url, headers=headers, verify=False, timeout=5)

        if resp.status_code == 200:
            msg = "Step 4: Mark File Complete: Successfully marked file {} complete".format(file_id)
        else:
            msg = "Step 4: Mark File Complete: Failed to mark file {} complete with status code {}".format(
                file_id, resp.status_code)

        write_to_logs(msg, logger)
    except Exception as exc:
        msg = "Step 4: Mark File Complete: Failed to mark file {} complete with error {}".format(id, exc)
        write_to_logs(msg, logger)

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
    try:
        f_input = open(from_file)
        file_start = f_input.readline()
    except UnicodeDecodeError:
        write_to_logs("Step 2 - Processing File: the file is gzipped")
        f_input = gzip.open(from_file, 'rt')
        file_start = f_input.readline()
    finally:
        write_to_logs("Step 2 - Processing File: File Start is {}".format(file_start))

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
        testing_s3 = boto3.resource('s3')
        testing_s3.meta.client.upload_file('/scratch/' + upload_file_name, testing_bucket, s3_filename)
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

def create_xml_library(
        dna_source, fileservice_uuid, instrument_model, md5_checksum, read_lengths, reference_genome, sample_id, secret,
        sequence_type, upload_file_name):
    """
    Create the library object used for creating the XML files
    """
    try:
        library = {}
        library['attributes'] = [['alignment_software', ALIGNMENT_SOFTWARE[sequence_type]]]
        library['center'] = 'HMS-CC'
        library['design_description'] = DESIGN_DESC[sequence_type]
        library['filename'] = fileservice_uuid
        library['instrument_model'] = instrument_model
        library['library_layout'] = 'PAIRED'
        library['md5_checksum'] = md5_checksum
        library['phs_accession'] = secret['accession']
        library['phs_accession_version'] = secret['accession_version']
        library['platform'] = 'ILLUMINA'
        library['read_lengths'] = make_read_length_list(read_lengths)
        library['reference'] = reference_genome
        library['sample_id'] = sample_id
        library['selection'] = 'RANDOM'
        library['source'] = SEQUENCING_SOURCE[sequence_type]
        library['strategy'] = SEQUENCING_TYPE[sequence_type]
        library['title'] = get_title_prefix(sequence_type, dna_source) + sample_id
        library['upload_file_name'] = upload_file_name
        library["latf_load"] = False

        return library
    except Exception as exc:
        error_message = "[ERROR] Step 2 - Processing File: Failed to create XML library object files for {} with error {}".format(
            upload_file_name, exc)
        write_to_logs(error_message)
        raise Exception(error_message) from exc


def xml_indent(elem, level=0):
    """
    Sets proper indent on xml
    """
    try:
        i = "\n" + level*"  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                xml_indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i
    except Exception as exc:
        error_message = "[ERROR] Step 2 - Processing File: Failed to indent XML with error {}".format(exc)
        write_to_logs(error_message)
        raise Exception(error_message) from exc


def xml_to_string(xml):
    """
    Properly formats XML as String
    """
    elem = xml.getroot()
    xml_indent(elem)
    return etree.tostring(elem, encoding="utf-8")


def get_title_prefix(sequence_type, dna_source):
    """
    Returns the appropriate title prefix for the BAM xml given
    the sequence type and dnasource of the ExportFile
    """
    title_prefix = {
        2: 'exome sequencing of homo sapiens: whole blood: Sample ',
        3: 'genome sequencing of homo sapiens: whole blood: Sample ',
        4: 'RNA sequencing of homo sapiens: {dna_source}: Sample '.format(dna_source=dna_source)
    }

    return title_prefix[sequence_type] if sequence_type in title_prefix else ''


def make_read_length_list(read_lengths):
    """
    Returns an array of the readlengths stored in the exportfile
    """
    if read_lengths:
        return [int(length) for length in read_lengths.rstrip(' ').rstrip(',').split(',')]

    return []


def format_experiment_xml(library):
    """
    Converts libraries into "experiment_set" ElementTree object
    """
    try:
        xml_exp_set = etree.Element("EXPERIMENT_SET")
        xml_exp = etree.SubElement(xml_exp_set, "EXPERIMENT")
        xml_e_identifiers = etree.SubElement(xml_exp, "IDENTIFIERS")
        xml_e_submitter_id = etree.SubElement(xml_e_identifiers, "SUBMITTER_ID")
        xml_e_submitter_id.set("namespace", library['center'])
        xml_e_submitter_id.text = library["sample_id"]
        xml_title = etree.SubElement(xml_exp, "TITLE")
        xml_title.text = library["title"]
        xml_study_ref = etree.SubElement(xml_exp, "STUDY_REF")
        xml_study_ref.set("accession", library["phs_accession"])
        xml_design = etree.SubElement(xml_exp, "DESIGN")
        xml_des_desc = etree.SubElement(xml_design, "DESIGN_DESCRIPTION")
        xml_des_desc.text = library["design_description"]
        xml_smp_desc = etree.SubElement(xml_design, "SAMPLE_DESCRIPTOR")
        xml_smp_desc.set("refname", library["sample_id"])
        xml_smp_desc.set("refcenter", library["phs_accession"])
        xml_lib_desc = etree.SubElement(xml_design, "LIBRARY_DESCRIPTOR")
        xml_lib_name = etree.SubElement(xml_lib_desc, "LIBRARY_NAME")
        xml_lib_name.text = library["sample_id"]
        xml_lib_strat = etree.SubElement(xml_lib_desc, "LIBRARY_STRATEGY")
        xml_lib_strat.text = library["strategy"]
        xml_lib_src = etree.SubElement(xml_lib_desc, "LIBRARY_SOURCE")
        xml_lib_src.text = library["source"]
        xml_lib_sel = etree.SubElement(xml_lib_desc, "LIBRARY_SELECTION")
        xml_lib_sel.text = library["selection"]
        xml_lib_layout = etree.SubElement(xml_lib_desc, "LIBRARY_LAYOUT")
        etree.SubElement(xml_lib_layout, library["library_layout"])

        if library["read_lengths"]:
            xml_spot_desc = etree.SubElement(xml_design, "SPOT_DESCRIPTOR")
            xml_decode_spec = etree.SubElement(xml_spot_desc, "SPOT_DECODE_SPEC")

            if sum(library["read_lengths"]) > 0:
                xml_spot_len = etree.SubElement(xml_decode_spec, "SPOT_LENGTH")
                xml_spot_len.text = str(sum(library["read_lengths"]))

            for i in range(0, len(library["read_lengths"])):
                xml_read_spec = etree.SubElement(xml_decode_spec, "READ_SPEC")
                xml_read_index = etree.SubElement(xml_read_spec, "READ_INDEX")
                xml_read_index.text = str(i)
                xml_read_class = etree.SubElement(xml_read_spec, "READ_CLASS")
                xml_read_class.text = "Application Read"

                if i == 0:
                    xml_read_type = etree.SubElement(xml_read_spec, "READ_TYPE")
                    xml_read_type.text = "Forward"
                    xml_base_coord = etree.SubElement(xml_read_spec, "BASE_COORD")
                    xml_base_coord.text = "1"
                elif i == 1:
                    xml_read_type = etree.SubElement(xml_read_spec, "READ_TYPE")
                    xml_read_type.text = "Reverse"
                    xml_base_coord = etree.SubElement(xml_read_spec, "BASE_COORD")
                    xml_base_coord.text = str(library["read_lengths"][i - 1] + 1)
                else:
                    sys.exit(5)

        xml_platform = etree.SubElement(xml_exp, "PLATFORM")
        xml_mftr = etree.SubElement(xml_platform, library["platform"])
        xml_model = etree.SubElement(xml_mftr, "INSTRUMENT_MODEL")
        xml_model.text = library["instrument_model"]

        if library["attributes"]:
            xml_e_attributes = etree.SubElement(xml_exp, "EXPERIMENT_ATTRIBUTES")

            for attribute in library["attributes"]:
                xml_e_attribute = etree.SubElement(
                    xml_e_attributes, "EXPERIMENT_ATTRIBUTE")
                xml_tag = etree.SubElement(xml_e_attribute, "TAG")
                xml_tag.text = attribute[0]
                xml_value = etree.SubElement(xml_e_attribute, "VALUE")
                xml_value.text = attribute[1]

        return etree.ElementTree(xml_exp_set)
    except Exception as exc:
        error_message = "[ERROR] Step 2 - Processing File: Failed to format experiment XML with error {}".format(exc)
        write_to_logs(error_message)
        raise Exception(error_message) from exc


def format_run_xml(library):
    """
    Converts libraries into "run_set" ElementTree object
    """
    try:
        xml_run_set = etree.Element("RUN_SET")
        xml_run = etree.SubElement(xml_run_set, "RUN")
        xml_r_identifiers = etree.SubElement(xml_run, "IDENTIFIERS")
        xml_r_submitter_id = etree.SubElement(xml_r_identifiers, "SUBMITTER_ID")
        xml_r_submitter_id.set("namespace", library['center'])
        xml_r_submitter_id.text = library["filename"]
        xml_experiment_ref = etree.SubElement(xml_run, "EXPERIMENT_REF")
        xml_e_identifiers = etree.SubElement(xml_experiment_ref, "IDENTIFIERS")
        xml_e_submitter_id = etree.SubElement(xml_e_identifiers, "SUBMITTER_ID")
        xml_e_submitter_id.set("namespace", library['center'])
        xml_e_submitter_id.text = library["sample_id"]
        xml_data_block = etree.SubElement(xml_run, "DATA_BLOCK")
        xml_files = etree.SubElement(xml_data_block, "FILES")
        xml_file = etree.SubElement(xml_files, "FILE")

        xml_file.set("checksum", library['md5_checksum'])
        xml_file.set("checksum_method", "MD5")
        xml_file.set("filename", library['upload_file_name'])
        xml_file.set("filetype", 'bam')

        if library["reference"] is not None or library["latf_load"]:
            xml_r_attributes = etree.SubElement(xml_run, "RUN_ATTRIBUTES")

            if library["reference"] is not None:
                xml_r_attribute = etree.SubElement(xml_r_attributes, "RUN_ATTRIBUTE")
                xml_tag = etree.SubElement(xml_r_attribute, "TAG")
                xml_tag.text = "assembly"
                xml_value = etree.SubElement(xml_r_attribute, "VALUE")
                xml_value.text = library["reference"]

            if library["latf_load"]:
                xml_r_attribute = etree.SubElement(xml_r_attributes, "RUN_ATTRIBUTE")
                xml_tag = etree.SubElement(xml_r_attribute, "TAG")
                xml_tag.text = "loader"
                xml_value = etree.SubElement(xml_r_attribute, "VALUE")
                xml_value.text = "latf-load"

        return etree.ElementTree(xml_run_set)
    except Exception as exc:
        error_message = "[ERROR] Step 2 - Processing File: Failed to format run XML with error {}".format(exc)
        write_to_logs(error_message)
        raise Exception(error_message) from exc


def format_submission_xml(library):
    """
    Converts a library into a "submission" ElementTree object
    """
    try:
        alias = '{}.{}'.format(library['phs_accession'], library['phs_accession_version'])
        namespace_map = {'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
        qname = etree.QName('http://www.w3.org/2001/XMLSchema-instance', 'noNamespaceSchemaLocation')

        xml_submission = etree.Element(
            "SUBMISSION", {qname: 'http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.submission.xsd?view=co'},
            nsmap=namespace_map)
        xml_submission.set('alias', alias)
        xml_submission.set('center_name', library['center'])

        xml_contacts = etree.SubElement(xml_submission, "CONTACTS")

        for contact in XML_CONTACTS:
            xml_contact = etree.SubElement(xml_contacts, "CONTACT")
            xml_contact.set('name', contact['name'])
            xml_contact.set('inform_on_error', contact['email'])
            xml_contact.set('inform_on_status', contact['email'])

        xml_actions = etree.SubElement(xml_submission, 'ACTIONS')

        for action in XML_ACTIONS:
            xml_action = etree.SubElement(xml_actions, 'ACTION')
            xml_action_add = etree.SubElement(xml_action, 'ADD')
            xml_action_add.set('source', action['source'])
            xml_action_add.set('schema', action['schema'])

        return etree.ElementTree(xml_submission)
    except Exception as exc:
        error_message = "[ERROR] Step 2 - Processing File: Failed to format submission XML with error {}".format(exc)
        write_to_logs(error_message)
        raise Exception(error_message) from exc


def create_and_tar_xml(
    dna_source, fileservice_uuid, instrument_model, md5_checksum, read_lengths, reference_genome, sample_id, secret,
        sequence_type, upload_file_name, logger):
    """
    Creates the XML files for the BAM file and
    """
    write_to_logs("Step 2 - Processing File: Creating XML for {}".format(upload_file_name))

    temp_experiment_file = '/scratch/experiment.xml'
    temp_run_file = "/scratch/run.xml"
    temp_submission_file = '/scratch/submission.xml'

    try:
        library = create_xml_library(
            dna_source, fileservice_uuid, instrument_model, md5_checksum, read_lengths, reference_genome, sample_id,
            secret, sequence_type, upload_file_name)

        experiment_xml = xml_to_string(format_experiment_xml(library))
        run_xml = xml_to_string(format_run_xml(library))
        submission_xml = xml_to_string(format_submission_xml(library))

        with codecs.open(temp_experiment_file, "w", "utf-8") as experiment_file_handle:
            experiment_file_handle.write(codecs.decode(experiment_xml, "utf-8"))

        with codecs.open(temp_run_file, "w", "utf-8") as run_file_handle:
            run_file_handle.write(codecs.decode(run_xml, "utf-8"))

        with codecs.open(temp_submission_file, "w", "utf-8") as submission_file_handle:
            submission_file_handle.write(codecs.decode(submission_xml, "utf-8"))

    except Exception as exc:
        error_message = "[ERROR] Step 2 - Processing File: Failed creation of XML files for {} with error {}".format(
            upload_file_name, exc)
        write_to_logs(error_message, logger)
        raise Exception(error_message) from exc

    write_to_logs("Step 2 - Processing File: Validating XML for {}".format(upload_file_name))

    exp_result = call(
        'xmllint --schema http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.experiment.xsd?view=co /scratch/experiment.xml > /dev/null', shell=True)
    run_result = call(
        'xmllint --schema http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.run.xsd?view=co /scratch/run.xml > /dev/null', shell=True)
    sub_result = call(
        'xmllint --schema http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.submission.xsd?view=co /scratch/submission.xml > /dev/null', shell=True)

    if exp_result == 0 and run_result == 0 and sub_result == 0:
        print("Step 2 - Processing File: Successful validation of XML files for {}".format(
            upload_file_name), flush=True)
    else:
        error_message = "[ERROR] Step 2 - Processing File: Failed validation of XML files for {} - exp_result ==> {}; run_result ==> {}; sub_result ==> {} ".format(
            upload_file_name, exp_result, run_result, sub_result)
        write_to_logs(error_message, logger)
        raise Exception(error_message)

    xml_files_to_tar = ['/scratch/experiment.xml', '/scratch/run.xml', '/scratch/submission.xml']
    tar_file_name = tar_and_remove_files(upload_file_name, '/scratch', xml_files_to_tar, logger)

    return tar_file_name

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
