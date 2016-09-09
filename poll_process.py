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
	print("Retrieving messages from queue - '" + currentQueue + "'", flush=True)
	
	for message in queue.receive_messages(MessageAttributeNames=['UDN_ID','FileBucket','FileKey','DestinationBucket','DestinationKey']):
		print("Found Messages, processing.", flush=True)

		if message.message_attributes is not None:
			UDN_ID = message.message_attributes.get('UDN_ID').get('StringValue')
			
			FileBucket = message.message_attributes.get('FileBucket').get('StringValue')
			FileKey = message.message_attributes.get('FileKey').get('StringValue')
			
			DestinationBucket = message.message_attributes.get('DestinationBucket').get('StringValue')
			DestinationKey = message.message_attributes.get('DestinationKey').get('StringValue')
			
			if UDN_ID and FileBucket and FileKey and DestinationBucket and DestinationKey:
				print("Processing UDN_ID - " + UDN_ID + ".", flush=True)
				
				#Get the new ID to use instead of the UDN_ID.
				request_params = {"udn_id" : UDN_ID}
				
				try:			
					r = requests.get('http://ups.aws.dbmi.hms.harvard.edu/id_pair/create_or_retrieve/',params=request_params)
					external_id = r.json()[0]["external_id"]
				except:
					print("Error retrieving external ID - ", sys.exc_info()[0], flush=True)

				if(external_id):
					print("Retrieved external ID.", flush=True)

					#Generate file IDs and paths.
					tempBAMFile = str(uuid.uuid4())
					tempBAMHeader = open("/output/header.sam","w+")
					tempBAMReheader = open("/output/" + str(uuid.uuid4()),"w")
					replacement_regex = "s/" + UDN_ID + "/" + external_id + "/"
					
					process_bam = True
					
					print("Downloading file from S3.", flush=True)
					
					#Retrieve the file from S3.
					try:
						retrieveBucket = resource.Bucket(FileBucket)
						retrieveBucket.download_file(FileKey, tempBAMFile)
					except:
						print("Error retrieving file from S3 - ", sys.exc_info()[0], flush=True)				
						process_bam = False		
					
					if(process_bam):
					
						print("Processing BAM with samtools.", flush=True)
						
						try:
							#Now we need to swap the ID's via samtools. Do some crazy piping.
							p1 = subprocess.Popen(["samtools","view","-H",tempBAMFile], stdout=subprocess.PIPE)
				
							p2 = subprocess.Popen(["sed","-e",replacement_regex], stdin=p1.stdout, stdout=tempBAMHeader)
							p1.stdout.close()
				
							p3 = subprocess.Popen(["samtools","reheader","/output/header.sam",tempBAMFile], stdout=tempBAMReheader)
				
							p3.communicate()
				
							print("Done processing file. Begin upload.", flush=True)
						except:
							print("Error processing BAM - ", sys.exc_info()[0], flush=True)				

						os.remove(tempBAMReheader)
						os.remove(tempBAMFile)
				
						# Let the queue know that the message is processed
						message.delete()				
				
			else:
				print("Message failed to provide all required attributes.", flush=True)
				print(message)
				

	
	time.sleep(10)
	
	