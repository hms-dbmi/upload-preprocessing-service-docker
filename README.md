# upload-preprocessing-service

This repo contains a docker container that will process VCF and BAM files to send to DBGap. The code relies on messages from SQS to know which files to process. Use the `ups-aws-infrastructure` repo to stand up the necessary AWS services (EC2 & ECS) to run this container. For more information, please read: https://hms-dbmi.atlassian.net/wiki/spaces/UDN/pages/74383364/UPS.

## Uploading docker images to Amazon ECR
`$(aws ecr get-login --no-include-email --region us-east-1)`

`docker build -t ups .`

`docker tag ups:latest 646975045128.dkr.ecr.us-east-1.amazonaws.com/ups:latest`

`docker push 646975045128.dkr.ecr.us-east-1.amazonaws.com/ups:latest`

## Testing
If in testing mode, you can fire messages off to SQS to have the UPS docker process a real production file and save it in S3 for inspection.

You'll want to create a virtualenv that has boto3 installed. Then run the following:

`workon upload-preprocessing-service-docker`

`python`

`import boto3`

`sqs = boto3.resource(service_name='sqs', region_name='us-east-1', endpoint_url='[GET THE SQS URL]')`

`queue = sqs.get_queue_by_name(QueueName='upload-preprocessing')`

Now replace the key parts below and then paste in your terminal to send:

queue.send_message(MessageBody='queue_file', \
   MessageAttributes={ \
       'UDN_ID': {'StringValue': '[UDN ID]', \
                  'DataType': 'String'}, \
       'sample_ID': {'StringValue': '[guid of the sample]', \
                     'DataType': 'String'}, \
       'FileBucket': {'StringValue': 'udnarchive', \
                      'DataType': 'String'}, \
       'FileKey': {'StringValue':  '[exportfile.file_url]', \
                   'DataType': 'String'}, \
       'file_service_uuid': {'StringValue': '[exportfile.file_uuid]', \
                             'DataType': 'String'}, \
       'file_type': {'StringValue': '[VCF or BAM]', \
                     'DataType': 'String'}, \
       'sequence_core_alias': {'StringValue': '[sequence --> sequencingcorealias.alias]', \
                               'DataType': 'String'}, \
       'md5': {'StringValue': ' ', \
               'DataType': 'String'} \
   })

Helpful queries to get the above:

`from dbgap.models import ExportFile`

`from patient.models import Sequence`

`from patient.models import SequenceCoreAlias`

`file = ExportFile.objects.get(filename='filename.vcf')`

`sequence = Sequence.objects.filter(patient__simpleid=file.exportlog.patient.simpleid)`

`sequence_core_alias = SequenceCoreAlias.objects.filter(sequence=sequence.first()).first()`

^ If that fails then:

`sequence_core_alias = SequenceCoreAlias.objects.create(sequence=sequence.first(), alias='unknown')`

And get the values by:

UDN_ID = `file.exportlog.patient.simpleid`

sample_id = `sequence.first().sampleid`

FileKey = `'/'.join(file.file_url.split('/')[3:5])`

file_service_uuid = `file.file_uuid`

sequence_core_alias = `sequence_core_alias.alias`

Give the EC2 instance some time to process the file and then look for it in the S3 under udn-files-test/ups-testing. You can monitor the ECS task's logs to see what it is doing.