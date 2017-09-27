import boto3
import sys
import hashlib
import subprocess
import os
import hvac
import botocore
import errno
from subprocess import call, check_output
import time
import xml.etree.ElementTree as ET
import codecs
import tarfile

def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


# Get the name of the SQS queue from the passed in parameters.
currentQueue = os.environ["QUEUE_NAME"]

# HVAC is vaults python package. Pull the secret key from vault, write it to a file, and base64 decode it.
client = hvac.Client(url=os.environ['VAULT_ADDRESS'], token=os.environ['ONETIME_TOKEN'], verify=False)
AsperaKey = client.read('secret/udn/ups/aspera_key')['data']['value']
aspera_file = open("/aspera/aspera.pk.64", "w")
aspera_file.write(AsperaKey)
aspera_file.flush()
aspera_file.close()
aspera_decoded_file = open("/aspera/aspera.pk", "w")
call(["base64", "-d", "/aspera/aspera.pk.64"], stdout=aspera_decoded_file)
aspera_decoded_file.close()
aspera_file.close()

LOCATION_CODE = client.read('secret/udn/ups/aspera_location_code')['data']['value']
IDSTORE_URL = client.read('secret/udn/ups/idstore_url')['data']['value']
ASPERA_PASS = client.read('secret/udn/ups/aspera_pass')['data']['value']
VCF_LOCATION_CODE = client.read('secret/udn/ups/aspera_location_code_vcf')['data']['value']

aspera_vcf_key = client.read('secret/udn/ups/aspera_vcf_key')['data']['value']
aspera_vcf_key_file = open("/aspera/aspera_vcf.pk.64", "w")
aspera_vcf_key_file.write(aspera_vcf_key)
aspera_vcf_key_file.flush()
aspera_vcf_key_file.close()
aspera_vcf_key_file_decoded = open("/aspera/aspera_vcf.pk", "w")
call(["base64", "-d", "/aspera/aspera_vcf.pk.64"], stdout=aspera_vcf_key_file_decoded)
aspera_vcf_key_file_decoded.close()
aspera_vcf_key_file.close()

# This needs to be set for Aspera to run for VCFs.
os.environ["ASPERA_SCP_FILEPASS"] = ASPERA_PASS

# Initialize AWS objects.
s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=currentQueue)


# --------------------

# --------------------

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
    
    # Retrieve the file from S3.
    try:
        temp_run_file = "/scratch/run.xml"
        temp_experiment_file = '/scratch/experiment.xml'
        temp_submission_file = '/scratch/submission.xml'
        
        # swap this out later 
        retrieveBucket = s3.Bucket('udn-prod-dbgap')

        run_file = 'xml/'+upload_file_name+'/'+upload_file_name+'-run.xml'
        exp_file = 'xml/'+upload_file_name+'/'+upload_file_name+'-experiment.xml'
        sub_file = 'xml/'+upload_file_name+'/'+upload_file_name+'-submission.xml'
        
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

    try:
        print("[DEBUG] Attempting to upload file " + tar_file_name + " via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + LOCATION_CODE,flush=True)
        upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 5000m -k 1 " + tar_file_name + " asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + LOCATION_CODE],shell=True)
        print(upload_output, flush=True)
    except:
        print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
        message.change_visibility(VisibilityTimeout=0)

    silentremove(tar_file_name)
    silentremove(temp_run_file)
    silentremove(temp_submission_file)
    silentremove(temp_experiment_file)

    return True


def process_vcf(UDN_ID, sequence_core_alias, FileBucket, FileKey, Sample_ID, upload_file_name, file_type):

    return_continue_and_delete = True

    print("[DEBUG] Renaming File to " + upload_file_name, flush=True)

    os.rename(tempFile, "/scratch/" + upload_file_name)

    print("[DEBUG] Replacing sample_ID", flush=True)

    try:
        os.remove('/scratch/changelog.txt')
    except OSError:
        pass

    empty_string = ''
    call(["sed -i -e 's/" + sequence_core_alias + "/" + Sample_ID + "/gw /scratch/changelog.txt'  /scratch/" + upload_file_name], shell=True)

    change_log_file_size = 0

    try:
        change_log_file_size = os.path.getsize('/scratch/changelog.txt')
    except OSError:
        print("[DEBUG] Error swapping identifiers in file. Changelog file not found after sed. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)

    if change_log_file_size == 0:
        print("[DEBUG] Error swapping identifiers in file. Changelog file is empty. {}|{}|{}|{}|{}|{}".format(UDN_ID,FileBucket,FileKey,Sample_ID,upload_file_name,file_type),flush=True)
        
        # try to remove shortened alias with exact match
        seq_alias_prefix = sequence_core_alias.split('-')[0]
        call(["sed -i -e 's/\<"+seq_alias_prefix+"\>/"+Sample_ID+"/gw /scratch/changelog.txt' /scratch/"+upload_file_name], shell=True)

        try:
            change_log_file_size = os.path.getsize('/scratch/changelog.txt')
        except OSError:
            print("[DEBUG] Error swapping identifiers in file. Changelog file not found after sed. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)

        if change_log_file_size == 0:
            print("[DEBUG] Error swapping alias prefix in file. Not sending file. Changelog file is empty {} | {} | {}".format(upload_file_name, UDN_ID, sequence_core_alias))
            return False

    try:
        os.remove('/scratch/changelog.txt')
    except OSError:
        pass

    call(["sed -i -e 's/" + UDN_ID + "/" + empty_string + "/gw /scratch/changelog.txt'  /scratch/" + upload_file_name], shell=True)

    try:
        change_log_file_size = os.path.getsize('/scratch/changelog.txt')
    except OSError:
        print("[DEBUG] Error swapping identifiers in file. Changelog file not found after sed. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)

    if change_log_file_size != 0:
        print("[DEBUG] Found UDN_ID in file. {}|{}|{}|{}|{}|{}".format(UDN_ID,FileBucket,FileKey,Sample_ID,upload_file_name,file_type),flush=True)


    try:
        print("[DEBUG] Attempting to upload file " + upload_file_name + " via Aspera - subasp@upload.ncbi.nlm.nih.gov:uploads:" + VCF_LOCATION_CODE, flush=True)
        upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp --file-crypt=encrypt -i /aspera/aspera_vcf.pk /scratch/" + upload_file_name + " subasp@upload.ncbi.nlm.nih.gov:uploads/" + VCF_LOCATION_CODE + "/"], shell=True)
        print(upload_output, flush=True)
    except:
        print("[ERROR] Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
        return_continue_and_delete = False
        message.change_visibility(VisibilityTimeout=0)

    return return_continue_and_delete


def process_bam(UDN_ID, sequence_core_alias, FileBucket, FileKey, Sample_ID, upload_file_name, file_type, md5):

    return_continue_and_delete = True

    # add extra checks here for other alias
    subprocess.call(["/output/bam_extract_header.sh", tempFile, sequence_core_alias, Sample_ID])

    change_log_file_size = 0

    try:
        change_log_file_size = os.path.getsize('/scratch/changelog.txt')
    except OSError:
        print("[DEBUG] Error swapping identifiers in file. Changelog file not found after sed. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)

    if change_log_file_size == 0:
        print("[DEBUG] Error swapping identifiers in file. Changelog file is empty. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)

        seq_alias_prefix = sequence_core_alias.split('-')[0]
        subprocess.call(["/output/bam_extract_header.sh", tempFile, seq_alias_prefix, Sample_ID])

        try:
            change_log_file_size = os.path.getsize('/scratch/changelog.txt')
        except OSError:
            print("[DEBUG] Error swapping identifiers in file. Changelog file not found after sed. {}|{}|{}|{}|{}|{}".format(UDN_ID, FileBucket, FileKey, Sample_ID, upload_file_name, file_type), flush=True)

        if change_log_file_size == 0:
            print("[DEBUG] Error swapping alias prefix in file. Not sending file. Changelog file is empty {} | {} | {}".format(upload_file_name, UDN_ID, sequence_core_alias))
            return False


    subprocess.call(["/output/bam_rehead.sh", tempFile])
    os.rename("/scratch/md5_reheader", "/scratch/" + upload_file_name)

    print("[DEBUG] Done processing file. Verify MD5 if present.", flush=True)

    md5 = hashlib.md5(open("/scratch/" + upload_file_name, 'rb').read()).hexdigest()    

    # update the run.xml, archive all xml and send
    xml_success = update_and_ship_XML(upload_file_name, md5)
    if not xml_success:
        return False

    # then ship the BAM file
    try:
        print("[DEBUG] Attempting to upload file " + upload_file_name + " via Aspera - asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + LOCATION_CODE,flush=True)
        upload_output = check_output(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 5000m -k 1 /scratch/" + upload_file_name + " asp-hms-cc@gap-submit.ncbi.nlm.nih.gov:" + LOCATION_CODE],shell=True)
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
            elif file_type = 'VCF':
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
                print("[ERROR] Message failed to provide all required attributes.", flush=True)
                print(message)
                message.change_visibility(VisibilityTimeout=0)
                continue

    time.sleep(10)

