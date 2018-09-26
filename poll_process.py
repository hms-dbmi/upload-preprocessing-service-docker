import boto3
import sys
import hashlib
import subprocess
import os
import json
import botocore
import errno
from subprocess import call, check_output, CalledProcessError
import time
import vcf_trimmer
import xml.etree.ElementTree as ET
import codecs
import tarfile
import pysam


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


# Get the name of the SQS queue from the passed in parameters.
currentQueue = 'upload-preprocessing'
# currentQueue = os.environ["QUEUE_NAME"]

# Get the keys from AWS Secrets Manager
secret_name = "ups-prod"
endpoint_url = "https://secretsmanager.us-east-1.amazonaws.com"
region_name = "us-east-1"

session = boto3.session.Session()
secrets_client = session.client(
    service_name='secretsmanager',
    region_name=region_name,
    endpoint_url=endpoint_url
)

try:
    secret_response = secrets_client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceNotFoundException':
        print("The requested secret " + secret_name + " was not found")
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        print("The request was invalid due to:", e)
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        print("The request had invalid params:", e)
    print('Fatal error. Stopping program.')
    sys.exit()
else:
    if 'SecretString' in secret_response:
        secret = json.loads(secret_response['SecretString'])
    else:
        print('Fatal error. Unexpected secret type.')
        sys.exit()

try: 
    aspera_key_secret = secrets_client.get_secret_value(SecretId='ups-prod-aspera-key')
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceNotFoundException':
        print("ups-prod-aspera-key not found")
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        print("Invalid Request: ", e)
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        print("Invalid parameters: ", e)
    print('Fatal error. Stopping program.')
    sys.exit()
else:
    if 'SecretBinary' in aspera_key_secret:
        aspera_file = open("/aspera/aspera.pk", "wb")
        aspera_file.write(aspera_key_secret['SecretBinary'])
        aspera_file.flush()
        aspera_file.close()
    else: 
        print('Fatal error. Unable to locate aspera-key')
        sys.exit()

try: 
    aspera_vcf_key_secret = secrets_client.get_secret_value(SecretId='ups-prod-aspera-vcf-key')
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceNotFoundException':
        print("ups-prod-aspera-key not found")
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        print("Invalid Request: ", e)
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        print("Invalid parameters: ", e)
    print('Fatal error. Stopping program.')
    sys.exit()
else:
    if 'SecretBinary' in aspera_vcf_key_secret:
        aspera_vcf_file = open("/aspera/aspera_vcf.pk", "wb")
        aspera_vcf_file.write(aspera_vcf_key_secret['SecretBinary'])
        aspera_vcf_file.flush()
        aspera_vcf_file.close()
    else: 
        print('Fatal error. Unable to locate aspera-vcf-key')
        sys.exit()

# If testing, do not upload files to DbGap, but instead save the processed file to a special S3 bucket
# use 'status' in the secret to control this.  anything other than 'test' will go outside
if secret['status'] == 'test':
    testing_bucket = 'udn-files-test'
    testing_folder = 'ups-testing'

    print("[DEBUG] Starting up in TEST mode. All processed files will be uploaded to the " + testing_bucket + " bucket in S3 instead of being uploaded to dbgap.", flush=True)
else:
    aspera_location_code = secret['aspera-location-code']
    aspera_pass = secret['aspera-pass']
    aspera_vcf_location_code = secret['aspera-location-code-vcf']

    # TODO does it?
    # This needs to be set for Aspera to run for VCFs.
    os.environ["ASPERA_SCP_FILEPASS"] = aspera_pass

# Initialize AWS objects.
s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=currentQueue)


def xmlIndent(elem, level=0):
    """
    sets proper indent on xml
    """
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            xmlIndent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def xmlToString(xml):
    """
    properly formats XML as String
    """
    elem = xml.getroot()
    xmlIndent(elem)
    return ET.tostring(elem, encoding="utf-8")


def update_and_ship_XML(upload_file_name, md5):
    """
    updates the run.xml file to include the MD5
    """
    
    file_key = upload_file_name.split('.')[0]

    # Retrieve the file from S3.
    try:
        temp_run_file = "/scratch/run.xml"
        temp_experiment_file = '/scratch/experiment.xml'
        temp_submission_file = '/scratch/submission.xml'
        
        # swap this out later 
        retrieveBucket = s3.Bucket('udn-prod-dbgap')

        run_file = 'xml/'+file_key+'/'+file_key+'-run.xml'
        exp_file = 'xml/'+file_key+'/'+file_key+'-experiment.xml'
        sub_file = 'xml/'+file_key+'/'+file_key+'-submission.xml'
        
        retrieveBucket.download_file(run_file, temp_run_file)
        retrieveBucket.download_file(exp_file, temp_experiment_file)
        retrieveBucket.download_file(sub_file, temp_submission_file)

    except botocore.exceptions.ClientError as e:
        silentremove(temp_run_file)
        silentremove(temp_experiment_file)
        silentremove(temp_submission_file)

        print("[ERROR] Error retrieving XML file from S3 - %s" % e, flush=True)
        continue_and_delete = False
        message.change_visibility(VisibilityTimeout=0)
        return False

    rum_xml_tree = ET.parse(temp_run_file)
    root = rum_xml_tree.getroot()
    run_element = root.find('RUN')
    data_element = run_element.find('DATA_BLOCK')
    files_element = data_element.find('FILES')

    xml_file = ET.SubElement(files_element, "FILE")
    xml_file.set("checksum", md5)
    xml_file.set("checksum_method", "MD5")
    xml_file.set("filename", upload_file_name)
    xml_file.set("filetype", 'bam')

    run_xml = xmlToString(ET.ElementTree(root))

    run_file_handle = codecs.open(temp_run_file, "w", "utf-8")
    run_file_handle.write(codecs.decode(run_xml, "utf-8"))
    run_file_handle.close()

    sub_result = call('xmllint --schema http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.submission.xsd?view=co /scratch/submission.xml > /dev/null', shell=True)
    exp_result = call('xmllint --schema http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.experiment.xsd?view=co /scratch/experiment.xml > /dev/null', shell=True)
    run_result = call('xmllint --schema http://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/sra/doc/SRA/SRA.run.xsd?view=co /scratch/run.xml > /dev/null', shell=True)

    if sub_result or exp_result or run_result == 0:
        print("[DEBUG] Successful validation of XML files for {}".format(upload_file_name), flush=True) 
    else:
        print("[DEBUG] ERROR - Failed validation of XML files for {} - sub_result ==> {}; exp_result ==> {}; run_result ==> {} ".format(upload_file_name, sub_result, exp_result, run_result), flush=True) 
        return False

    tar_file_name = '/scratch/'+upload_file_name+'.tar'
    tar = tarfile.open(tar_file_name, "w")
    for name in ['/scratch/submission.xml', '/scratch/experiment.xml', '/scratch/run.xml']:
        print("[DEBUG] Adding "+name+" to tar file")
        try:
            tar.add(name)
        except: 
            print("[DEBUG] Error adding " + name + " to tar file")
    tar.close()

    # Do not upload to DbGap if testing
    if secret['status'] == 'test':
        s3_filename = testing_folder + '/' + upload_file_name + '.tar'
        print("[DEBUG] Attempting to copy file " + upload_file_name + ".tar to S3 bucket for storage under " + s3_filename + ".", flush=True)
        testing_s3 = boto3.resource('s3')
        testing_s3.meta.client.upload_file(tar_file_name, testing_bucket, s3_filename)
    else:
        try:
            print("[DEBUG] Attempting to upload file " + tar_file_name + " via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + aspera_location_code, flush=True)
            upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 5000m -k 1 " + tar_file_name + " asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + aspera_location_code],shell=True)
            print(upload_output, flush=True)
        except:
            print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
            message.change_visibility(VisibilityTimeout=0)

    silentremove(tar_file_name)
    silentremove(temp_run_file)
    silentremove(temp_submission_file)
    silentremove(temp_experiment_file)

    return True


def upload_vcf_archive():
    if not os.path.exists('/scratch/vcf_archive.tar'):
        return

    return_value = True

    upload_file_name = time.strftime('vcf_archive_%Y%m%d%H%M%S.tar')
    os.rename('/scratch/vcf_archive.tar', '/scratch/{}'.format(upload_file_name))

    # Do not upload to DbGap if testing
    if secret['status'] == 'test':
        s3_filename = testing_folder + '/' + upload_file_name
        print("[DEBUG] Attempting to copy file " + upload_file_name + " to S3 bucket for storage under " + s3_filename + ".", flush=True)
        testing_s3 = boto3.resource('s3')
        testing_s3.meta.client.upload_file('/scratch/' + upload_file_name, testing_bucket, s3_filename)
    else:
        try:
            upload_location = "subasp@upload.ncbi.nlm.nih.gov:uploads/upload_requests/" + aspera_vcf_location_code + "/"
            print("[DEBUG] Attempting to upload file " + upload_file_name + " via Aspera to " + upload_location, flush=True)
            upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp --file-crypt=encrypt -i /aspera/aspera_vcf.pk /scratch/" + upload_file_name + " " + upload_location], shell=True)
            print(upload_output, flush=True)
        except:
            print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
            return_value = False
            message.change_visibility(VisibilityTimeout=0)

    silentremove('/scratch/{}'.format(upload_file_name))

    return return_value


def process_vcf(UDN_ID, sequence_core_alias, FileBucket, FileKey, Sample_ID, upload_file_name, file_type):

    return_continue_and_delete = True

    print("[DEBUG] Renaming File to " + upload_file_name, flush=True)

    os.rename(tempFile, "/scratch/" + upload_file_name)

    print("[DEBUG] Replacing sample_ID and removing extra VCF info", flush=True)

    os.rename('/scratch/{}'.format(upload_file_name), '/scratch/{}.bak'.format(upload_file_name))
    try:
        vcf_trimmer.trim('/scratch/{}.bak'.format(upload_file_name), '/scratch/{}'.format(upload_file_name), Sample_ID)
    except:
        print("[DEBUG] Error trimming VCF annotations. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)
        os.rename('/scratch/{}.bak'.format(upload_file_name), '/scratch/{}'.format(upload_file_name))

    print("[DEBUG] Compressing and indexing VCF", flush=True)
    pysam.tabix_index('/scratch/{}'.format(upload_file_name), preset='vcf')

    try:
        archive_size = os.path.getsize('/scratch/vcf_archive.tar')
    except OSError:
        archive_size = 0
    vcf_size = os.path.getsize('/scratch/{}.gz'.format(upload_file_name))
    if archive_size + vcf_size > 1024**4:  # 1TB
        result = upload_vcf_archive()
        if not result:
            return_continue_and_delete = False

    with tarfile.TarFile('/scratch/vcf_archive.tar', 'a') as archive:
        for name in ('{}.gz'.format(upload_file_name), '{}.gz.tbi'.format(upload_file_name)):
            archive.add('/scratch/{}'.format(name), arcname=name)
            os.remove('/scratch/{}'.format(name))

    return return_continue_and_delete


def process_bam(UDN_ID, sequence_core_alias, FileBucket, FileKey, Sample_ID, upload_file_name, file_type, md5):

    return_continue_and_delete = True

    bam_headers = list()
    try:
        bam_headers = check_output(['samtools', 'view', '-H', tempFile]).decode('ascii').split('\n')
    except CalledProcessError as e:
        print("[DEBUG] Error retrieving header from bam file. Error code {}. {}|{}|{}|{}|{}|{}".format(
            e.returncode, UDN_ID, FileBucket, FileKey, Sample_ID,
            upload_file_name, file_type
        ), flush=True)

    output_headers = list()
    for header in bam_headers:
        if header.startswith('@RG'):
            tag_pairs = [
                item.split(':', 1)
                for item in header.split('\t')[1:]  # don't include @RG token
            ]
            output_tags = [('ID', '0')]  # sometimes ID field has nontrivial identifiers in it
            output_tags.extend(
                (tag_name, data)
                for tag_name, data in tag_pairs
                if tag_name in ('PL', 'DT', 'CN')  # retain these
            )
            output_tags.append(('SM', Sample_ID))  # add in new sample ID
            new_header = '\t'.join(
                ['@RG'] + [':'.join(pair) for pair in output_tags]
            )
            output_headers.append(new_header)
        elif header.startswith('@PG'):
            continue  # remove @PG headers
        else:
            output_headers.append(header)

    with open('/scratch/new_headers.sam', 'w') as f:
        f.write('\n'.join(output_headers))

    with open('/scratch/md5_reheader', 'wb') as f:
        try:
            call([
                'samtools', 'reheader', '-P', '/scratch/new_headers.sam', tempFile
            ], stdout=f)  # more secure alternative to `shell=True` with redirection
        except CalledProcessError as e:
            print("[DEBUG] Error reheading bam file. Error code {}. {}|{}|{}|{}|{}|{}".format(
                e.returncode, UDN_ID, FileBucket, FileKey, Sample_ID,
                upload_file_name, file_type
            ), flush=True)

    os.rename('/scratch/md5_reheader', '/scratch/' + upload_file_name)
    print("[DEBUG] Done processing file. Verify MD5 if present.", flush=True)

    # bams are usually very big so stream instead of reading it all into memory
    md5_hash = hashlib.md5()
    with open('/scratch/' + upload_file_name, 'rb') as f:
        while True:
            buf = f.read(2**20)
            if not buf:
                break
            md5_hash.update(buf)

    md5 = md5_hash.hexdigest()

    # update the run.xml, archive all xml and send
    xml_success = update_and_ship_XML(upload_file_name, md5)
    if not xml_success:
        return False

    # Do not upload to DbGap if testing
    if secret['status'] == 'test':
        s3_filename = testing_folder + '/' + upload_file_name
        print("[DEBUG] Attempting to copy file " + upload_file_name + " to S3 bucket for storage under " + s3_filename + ".", flush=True)
        testing_s3 = boto3.resource('s3')
        testing_s3.meta.client.upload_file("/scratch/" + upload_file_name, testing_bucket, s3_filename)
    else:
        # then ship the BAM file
        try:
            print("[DEBUG] Attempting to upload file " + upload_file_name + " via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + aspera_location_code,flush=True)
            upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 5000m -k 1 /scratch/" + upload_file_name + " asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + aspera_location_code],shell=True)
            print(upload_output, flush=True)
        except:
            print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
            message.change_visibility(VisibilityTimeout=0)
            return_continue_and_delete = False

    return return_continue_and_delete


while True:

    print("Retrieving messages from queue - '" + currentQueue + "'", flush=True)

    for message in queue.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=['UDN_ID', 'FileBucket', 'FileKey', 'sample_ID', 'file_service_uuid', 'file_type', 'md5', 'sequence_core_alias']):
        print("Found Messages, processing.", flush=True)

        continue_and_delete = True

        if message.message_attributes is not None:
            UDN_ID = message.message_attributes.get('UDN_ID').get('StringValue')
            sequence_core_alias = message.message_attributes.get('sequence_core_alias').get('StringValue')
            FileBucket = message.message_attributes.get('FileBucket').get('StringValue')
            FileKey = message.message_attributes.get('FileKey').get('StringValue')
            Sample_ID = message.message_attributes.get('sample_ID').get('StringValue')
            file_type = message.message_attributes.get('file_type').get('StringValue')
            md5 = message.message_attributes.get('md5').get('StringValue')

            if file_type == 'BAM':
                filename_extension = '.bam'
            elif file_type == 'VCF':
                filename_extension = '.vcf'

            upload_file_name = "%s%s" % (message.message_attributes.get('file_service_uuid').get('StringValue'), filename_extension)

            if UDN_ID and sequence_core_alias and FileBucket and FileKey and Sample_ID and upload_file_name and file_type:
                print("[DEBUG] Processing UDN_ID - " + UDN_ID + ".", flush=True)
                print("[DEBUG] Downloading file. Bucket - " + FileBucket + " key - " + FileKey, flush=True)

                # Retrieve the file from S3.
                try:
                    tempFile = "/scratch/md5"
                    retrieveBucket = s3.Bucket(FileBucket)
                    retrieveBucket.download_file(FileKey, tempFile)
                except botocore.exceptions.ClientError as e:
                    silentremove(tempFile)
                    print("[ERROR] Error retrieving file from S3 - %s" % e, flush=True)
                    continue_and_delete = False
                    message.change_visibility(VisibilityTimeout=0)
                    continue

                if file_type == "BAM" and continue_and_delete:
                    

                    print("[DEBUG] Processing BAM with samtools.", flush=True)

                    try:
                        continue_and_delete = process_bam(UDN_ID, sequence_core_alias, FileBucket, FileKey, Sample_ID, upload_file_name, file_type, md5)
                    except:
                        print("Error processing BAM - ", sys.exc_info()[:2], flush=True)
                        continue_and_delete = False
                        message.change_visibility(VisibilityTimeout=0)
                        continue
                    finally:
                        silentremove("/scratch/md5")
                        silentremove("/scratch/header.sam")

                elif file_type == "VCF" and continue_and_delete:
                    try:
                        continue_and_delete = process_vcf(UDN_ID, sequence_core_alias, FileBucket, FileKey, Sample_ID, upload_file_name, file_type)
                    except:
                        print("[ERROR] Error processing VCF - ", sys.exc_info()[:2], flush=True)
                        continue_and_delete = False
                        message.change_visibility(VisibilityTimeout=0)
                        continue
                    finally:
                        silentremove("/scratch/md5")
                        silentremove("/scratch/header.sam")

                silentremove(tempFile)
                silentremove("/scratch/" + upload_file_name)

                # Let the queue know that the message is processed
                if continue_and_delete:
                    print("[COMPLETE] {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)
                    message.delete()
                else:
                    message.change_visibility(VisibilityTimeout=0)


            else:
                print("[ERROR] Message failed to provide all required attributes.", flush=True)
                print(message)
                message.change_visibility(VisibilityTimeout=0)
                continue

    upload_vcf_archive()

    time.sleep(10)
