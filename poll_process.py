import boto3
import sys
import time
import uuid
import subprocess
import os
import hvac
import botocore
import errno
from subprocess import call


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

LOCATION_CODE = client.read('secret/udn/ups/aspera_location_code')['data']['value']
IDSTORE_URL = client.read('secret/udn/ups/idstore_url')['data']['value']
ASPERA_PASS = client.read('secret/udn/ups/aspera_pass')['data']['value']

# Initialize AWS objects.
resource = boto3.resource('s3')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=currentQueue)

while True:
    print("Retrieving messages from queue - '" + currentQueue + "'", flush=True)

    for message in queue.receive_messages(MessageAttributeNames=['UDN_ID', 'FileBucket', 'FileKey', 'sample_ID']):
        print("Found Messages, processing.", flush=True)

        if message.message_attributes is not None:
            UDN_ID = message.message_attributes.get('UDN_ID').get('StringValue')

            FileBucket = message.message_attributes.get('FileBucket').get('StringValue')
            FileKey = message.message_attributes.get('FileKey').get('StringValue')

            Sample_ID = message.message_attributes.get('sample_ID').get('StringValue')

            if UDN_ID and FileBucket and FileKey and Sample_ID:
                print("Processing UDN_ID - " + UDN_ID + ".", flush=True)

                print("Downloading file from S3.", flush=True)

                # Retrieve the file from S3.
                try:
                    tempBAMFile = str(uuid.uuid4())
                    retrieveBucket = resource.Bucket(FileBucket)
                    retrieveBucket.download_file(FileKey, tempBAMFile)
                except botocore.exceptions.ClientError as e:
                    silentremove(tempBAMFile)
                    print("Error retrieving file from S3 - %s" % e, flush=True)
                    process_bam = False

                if process_bam:

                    print("Processing BAM with samtools.", flush=True)

                    tempBAMHeader = open("/scratch/header.sam", "w+")
                    tempBAMReheader = open("/scratch/" + str(uuid.uuid4()), "w")
                    replacement_regex = "s/" + UDN_ID + "/" + Sample_ID + "/"

                    try:
                        # Now we need to swap the ID's via samtools. Do some crazy piping.
                        p1 = subprocess.Popen(["samtools", "view", "-H", tempBAMFile], stdout=subprocess.PIPE)

                        p2 = subprocess.Popen(["sed", "-e", replacement_regex], stdin=p1.stdout, stdout=tempBAMHeader)
                        p1.stdout.close()

                        p3 = subprocess.Popen(["samtools", "reheader", "/scratch/header.sam", tempBAMFile], stdout=tempBAMReheader)

                        p3.communicate()

                        print("Done processing file. Begin upload.", flush=True)

                    except:
                        print("Error processing BAM - ", sys.exc_info()[:2], flush=True)
                        silentremove(tempBAMReheader.name)
                        silentremove(tempBAMHeader.name)
                        silentremove(tempBAMFile)

                    try:

                        print("Attempting to upload file via Aspera - asp-sra@gap-submit.ncbi.nlm.nih.gov" + LOCATION_CODE)
                        call(["/home/aspera/.aspera/connect/bin/ascp -i /aspera/aspera.pk -Q -l 200m -k 1 " + tempBAMReheader.name + " asp-sra@gap-submit.ncbi.nlm.nih.gov:" + LOCATION_CODE], shell=True)

                    except:
                        print("Error sending files via Aspera - ", sys.exc_info()[:2], flush=True)
                        silentremove(tempBAMReheader.name)
                        silentremove(tempBAMHeader.name)
                        silentremove(tempBAMFile)

                    silentremove(tempBAMReheader.name)
                    silentremove(tempBAMHeader.name)
                    silentremove(tempBAMFile)

                    # Let the queue know that the message is processed
                    message.delete()

            else:
                print("Message failed to provide all required attributes.", flush=True)
                print(message)

    time.sleep(10)
