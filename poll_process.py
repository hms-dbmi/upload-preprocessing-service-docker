import boto3
import sys
import time
import uuid
import subprocess
import requests
import json
from os import remove

currentQueue = sys.argv.pop()

resource = boto3.resource('s3')

sqs = boto3.resource('sqs')

queue = sqs.get_queue_by_name(QueueName=currentQueue)

while True:
	print("Retrieving messages from queue - '" + currentQueue + "'")
	
	for message in queue.receive_messages(MessageAttributeNames=['UDN_ID','FileBucket','FileKey','DestinationBucket','DestinationKey']):
		print("Found Messages, processing.")

		if message.message_attributes is not None:
			UDN_ID = message.message_attributes.get('UDN_ID').get('StringValue')
			
			FileBucket = message.message_attributes.get('FileBucket').get('StringValue')
			FileKey = message.message_attributes.get('FileKey').get('StringValue')
			
			DestinationBucket = message.message_attributes.get('DestinationBucket').get('StringValue')
			DestinationKey = message.message_attributes.get('DestinationKey').get('StringValue')
			
			if UDN_ID and FileBucket and FileKey and DestinationBucket and DestinationKey:
				print("Processing UDN_ID - " + UDN_ID + ".")
				
				#Get the new ID to use instead of the UDN_ID.
				request_params = {"udn_id" : UDN_ID}
				r = requests.get('http://ups.aws.dbmi.hms.harvard.edu/id_pair/create_or_retrieve/',params=request_params)
				external_id = r.json()[0]["external_id"]
				
				print("Retrieved external ID.")
				
				print("Processing BAM header via samtools.")
				
				#Generate ID for downloaded file.
				tempBAMFile = str(uuid.uuid4())
				tempBAMHeader = open("/output/header.sam","w+")
				tempBAMReheader = open("/output/" + str(uuid.uuid4()),"w")
				
				replacement_regex = "s/" + UDN_ID + "/" + external_id + "/"
				
				#Retrieve the file from S3.
				retrieveBucket = resource.Bucket(FileBucket)
				retrieveBucket.download_file(FileKey, tempBAMFile)

				#Now we need to swap the ID's via samtools. Do some crazy piping.
				p1 = subprocess.Popen(["samtools","view","-H",tempBAMFile], stdout=subprocess.PIPE)
				
				p2 = subprocess.Popen(["sed","-e",replacement_regex], stdin=p1.stdout, stdout=tempBAMHeader)
				p1.stdout.close()
				
				p3 = subprocess.Popen(["samtools","reheader","/output/header.sam",tempBAMFile], stdout=tempBAMReheader)
				
				p3.communicate()
				
				print("Done processing file. Begin upload.")
				
				os.remove(tempBAMReheader)
				os.remove(tempBAMFile)
				
			else:
				print("Message failed to provide all required attributes.")
				print(message)
				
		# Let the queue know that the message is processed
		message.delete()
	
	time.sleep(10)
	
	